from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path
from uuid import uuid4

from app.analysis.facets import get_facets_for_mode
from app.stone_preprocess import StoneDocumentSnapshot
from app.stone_v3_checkpoint import load_stone_v3_checkpoint, stone_v3_checkpoint_path
from app.models import utcnow
from app.schemas import ChatCompletionResult, DEFAULT_ANALYSIS_CONCURRENCY, ToolRoundResult
from app.storage import repository
from app.web.routes import _resolve_stone_writing_status
from app.llm.client import OpenAICompatibleClient
from app.web import routes as web_routes
from app.agents.stone.writing_service import _fit_word_count, _light_trim_to_word_count


def _mock_result(content: str) -> ChatCompletionResult:
    return ChatCompletionResult(
        content=content,
        model="demo-model",
        usage={"prompt_tokens": 32, "completion_tokens": 16, "total_tokens": 48},
        request_url="https://example.com/v1/responses",
        request_payload={"messages": []},
    )


def _ensure_service_config(app, service_name: str, *, model: str = "demo-model") -> None:
    with app.state.db.session() as session:
        repository.upsert_setting(
            session,
            service_name,
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": model,
                "api_mode": "responses",
            },
        )


def _wait_for_ready(client, project_id: str, document_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    latest = {}
    while time.time() < deadline:
        latest = client.get(f"/api/projects/{project_id}/documents").json()
        for item in latest.get("documents", []):
            if item["id"] == document_id and item["ingest_status"] == "ready":
                return item
        time.sleep(0.1)
    raise AssertionError(f"document {document_id} did not become ready: {latest}")


def _wait_for_stone_preprocess(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    return payload


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _collect_sse_events(client, url: str, *, method: str = "GET", json_payload: dict | None = None) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    with client.stream(method, url, json=json_payload) as response:
        assert response.status_code == 200
        current_event: str | None = None
        data_lines: list[str] = []
        for raw_line in response.iter_lines():
            line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
            if line == "":
                if current_event is not None:
                    payload = json.loads("\n".join(data_lines)) if data_lines else {}
                    events.append((current_event, payload))
                current_event = None
                data_lines = []
                continue
            if line.startswith("event: "):
                current_event = line[7:]
            elif line.startswith("data: "):
                data_lines.append(line[6:])
    return events


def _extract_writing_bootstrap_payload(html: str) -> dict:
    match = re.search(
        r'<script type="application/json" id="writing-bootstrap">(.*?)</script>',
        html,
        re.DOTALL,
    )
    assert match, "writing bootstrap payload not found"
    return json.loads(match.group(1))


def _extract_target_word_count(text: str) -> int:
    match = re.search(r"Target Word Count:\s*(\d+)", text)
    return int(match.group(1)) if match else 400


def _build_mock_article(topic: str, target_word_count: int) -> str:
    paragraph = (
        f"{topic} at night pulls me through the door again, not because it is good, but because the paper bag is warm, "
        "the counter light is too bright, and the walk back home gives every bite a little more shame than comfort."
    )
    text = "\n\n".join([paragraph] * 12)
    while len(re.findall(r"[A-Za-z0-9_]+", text)) < int(target_word_count * 0.9):
        text = f"{text}\n\n{paragraph}"
    return text


def _install_stone_v3_mocks(monkeypatch, *, fail_profile_on_text: str | None = None) -> None:
    def fake_chat_completion_result(self, messages, **kwargs):
        del self, kwargs
        system_text = str(messages[0].get("content") or "")
        user_text = str(messages[-1].get("content") or "")

        if "Stone v3 document profile" in system_text or "chunk-level Stone v3 profiles" in system_text:
            article_text = user_text.split("Article text:\n", 1)[-1].strip()
            if fail_profile_on_text and fail_profile_on_text in article_text:
                raise RuntimeError("mock profile failure")
            lines = [line.strip() for line in article_text.splitlines() if line.strip()]
            opening = lines[0] if lines else article_text[:120]
            closing = lines[-1] if lines else article_text[-120:]
            payload = {
                "document_core": {
                    "summary": "Night food confession with cost and residue.",
                    "length_band": "short",
                    "surface_form": "scene_vignette",
                    "dominant_theme": "cheap comfort with lingering cost",
                },
                "voice_contract": {
                    "person": "first",
                    "address_target": "self",
                    "distance": "回收",
                    "self_position": "自损",
                    "cadence": "restrained",
                    "sentence_shape": "mixed",
                    "tone_words": ["restrained", "stubborn", "residual"],
                },
                "structure_moves": {
                    "opening_move": "Open from a concrete action or object.",
                    "development_move": "Let pressure rise through visible detail.",
                    "turning_move": "small bodily recoil",
                    "closure_move": "Leave residue instead of summary.",
                    "paragraph_strategy": "2-4 paragraphs",
                },
                "motif_and_scene_bank": {
                    "motif_tags": ["night", "door", "road", "grease"],
                    "scene_terms": ["counter", "paper bag", "walk home"],
                    "sensory_terms": ["warm", "salty", "bright"],
                    "lexicon_markers": ["night", "door", "road", "cost"],
                },
                "value_and_judgment": {
                    "judgment_target": "cheap comfort ritual",
                    "judgment_mode": "stabilize through self-aware repetition",
                    "value_lens": "cost",
                    "felt_cost": "the body and wallet both pay a little",
                },
                "prototype_affordances": {
                    "prototype_family": "scene_vignette|night_cost",
                    "cluster_hint": "night comfort cost residue",
                    "suitable_for": ["food ritual", "cheap comfort", "private shame"],
                    "anti_drift_focus": ["Do not explain the ritual.", "Keep the ending unresolved."],
                },
                "anchor_windows": {
                    "opening": opening,
                    "pivot": opening,
                    "closing": closing,
                    "signature_lines": [opening, closing],
                },
                "retrieval_handles": {
                    "keywords": ["kfc", "night", "door", "cost", "road"],
                    "routing_text": "night food ritual cost residue",
                },
                "anti_patterns": ["Do not explain the writing process.", "Do not flatten into summary."],
            }
            return _mock_result(json.dumps(payload, ensure_ascii=False))

        if "Stone v3 family induction stage" in system_text or "Stone v3 family synthesis stage" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "families": [
                            {
                                "family_id": "night-cost",
                                "label": "night_cost",
                                "description": "Concrete night scenes where comfort and cost stay tangled.",
                                "selection_cues": ["night ritual", "cheap comfort", "residue"],
                                "motif_tags": ["night", "door", "road"],
                                "member_count": 1,
                            }
                        ]
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 author-model synthesizer" in system_text or "Stone v3 author-model finalizer" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "author_core": {
                            "voice_summary": "Concrete first-person scenes with restrained pressure.",
                            "worldview_summary": "Translates topics into lived cost before explanation.",
                            "tone_summary": "Low-key, bodily, and unresolved.",
                            "signature_motifs": ["night", "door", "road", "grease"],
                        },
                        "translation_rules": [
                            {
                                "value_lens": "cost",
                                "preferred_motifs": ["night", "door", "road"],
                                "opening_moves": ["Open from a concrete action or object."],
                                "closure_moves": ["Leave residue instead of summary."],
                            }
                        ],
                        "stable_moves": [
                            "Open from a concrete action, object, or scene.",
                            "Let pressure rise from visible detail.",
                            "Keep closure unresolved when possible.",
                        ],
                        "forbidden_moves": [
                            "Do not turn the piece into explanation.",
                            "Do not flatten the ending into summary.",
                        ],
                        "family_map": [
                            {
                                "family_id": "night-cost",
                                "label": "night_cost",
                                "description": "Concrete night scenes where comfort and cost stay tangled.",
                                "selection_cues": ["night ritual", "cheap comfort", "residue"],
                                "motif_tags": ["night", "door", "road"],
                                "member_count": 1,
                            }
                        ],
                        "critic_rubrics": {
                            "feature_density": ["Do not overfit signature ticks or over-stack hallmark phrases."],
                            "cross_domain_generalization": ["Translate the topic into lived cost, not labels."],
                            "rhythm_entropy": ["Keep sentence pressure and density uneven in the right places."],
                            "extreme_state_handling": ["Preserve the author's emotional downgrade mechanism."],
                            "ending_landing": ["Keep the ending on residue, not explanation."],
                        },
                        "global_evidence": [
                            {
                                "document_id": "seed",
                                "title": "seed",
                                "summary": "Night food confession with cost and residue.",
                                "opening": "opening",
                                "closing": "closing",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 prototype-card synthesis stage" in system_text:
            return _mock_result(json.dumps({"documents": []}, ensure_ascii=False))

        if "Stone v3 prototype-index finalizer" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "documents": [],
                        "families": [
                            {
                                "family_id": "night-cost",
                                "label": "night_cost",
                                "description": "Concrete night scenes where comfort and cost stay tangled.",
                                "selection_cues": ["night ritual", "cheap comfort", "residue"],
                                "motif_tags": ["night", "door", "road"],
                                "member_count": 1,
                            }
                        ],
                        "retrieval_policy": {
                            "shortlist_formula": "keyword overlap + routing facets + family cues",
                            "target_shortlist_size": 12,
                            "target_anchor_budget": 8,
                            "notes": ["Shortlist is lightweight.", "Final choice belongs to the reranker."],
                        },
                        "selection_guides": {
                            "when_to_expand": ["Expand when the shortlist is too homogeneous."],
                            "when_to_prune": ["Prune duplicate family moves."],
                            "quality_checks": ["Keep opening and closing anchors."],
                        },
                        "anchor_registry": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 baseline critic" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "verdict": "approve",
                        "score": 0.96,
                        "strengths": ["grounded in anchors", "clear retrieval policy"],
                        "risks": [],
                        "repair_notes": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 request adapter" in system_text or "请求适配器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "desired_length_band": "medium",
                        "surface_form": "scene_vignette",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through lived detail",
                        "distance": "recycled first person",
                        "entry_scene": "Start with the walk into the bright store.",
                        "felt_cost": "Make the comfort feel a little embarrassing and expensive.",
                        "query_terms": ["kfc", "night", "door", "road"],
                        "motif_terms": ["night", "door", "road", "grease"],
                        "anchor_preferences": ["opening", "closing", "signature"],
                        "hard_constraints": [],
                        "reasoning": "Use a night ritual and keep the cost bodily.",
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 reranker" in system_text or "重排器" in system_text:
            document_ids = []
            for value in re.findall(r'"document_id"\s*:\s*"([^"]+)"', user_text):
                if value not in document_ids:
                    document_ids.append(value)
            anchor_ids = []
            for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text):
                if value.startswith("anchor:") and value not in anchor_ids:
                    anchor_ids.append(value)
            return _mock_result(
                json.dumps(
                    {
                        "selected_documents": document_ids[:6],
                        "anchor_ids": anchor_ids[:8],
                        "selection_reason": "Night ritual anchors match the request and the author's cost lens.",
                        "rerank_notes": ["Keep the closure unresolved."],
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 style packet builder" in system_text or "写作包组装器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "entry_scene": "Enter through the bright counter and the warm paper bag.",
                        "felt_cost": "Comfort should feel useful and a little humiliating at once.",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through repetition",
                        "distance": "recycled first person",
                        "family_labels": ["night_cost"],
                        "lexicon_keep": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "syntax_rules": ["Prefer concrete pressure over explanation."],
                        "structure_recipe": [
                            "Open on the walk in.",
                            "Push pressure through small bodily details.",
                            "Leave the ending unresolved.",
                        ],
                        "do_not_do": ["Do not explain the ritual."],
                        "style_thesis": "Night comfort should carry cost and residue.",
                        "coverage_warnings": ["Sparse profile mode is active."],
                    },
                    ensure_ascii=False,
                )
            )

        if "Stone v3 blueprint composer" in system_text or "蓝图规划器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "paragraph_count": 4,
                        "shape_note": "Scene vignettes stacking bodily pressure.",
                        "entry_move": "Open from the bright door and the walk inside.",
                        "development_move": "Keep pressure in the body, wallet, and walk home.",
                        "turning_device": "small recoil after the first bite",
                        "closure_residue": "End on the road home, not a conclusion.",
                        "keep_terms": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "steps": [
                            "Open on action.",
                            "Accumulate concrete detail.",
                            "Let the cost surface without thesis.",
                            "Close on residue.",
                        ],
                        "do_not_do": ["Do not explain the process."],
                        "axis_map": {
                            "voice_signature": {"goal": "Keep the opening restrained and bodily.", "paragraph_hint": 1, "anchor_ids": []},
                            "emotional_arc": {"goal": "Let embarrassment rise slowly.", "paragraph_hint": 3, "anchor_ids": []},
                        },
                        "paragraph_map": [
                            {"paragraph_index": 1, "role": "opening", "objective": "Open from the bright door.", "axis_keys": ["voice_signature"], "anchor_ids": []},
                            {"paragraph_index": 2, "role": "development", "objective": "Accumulate bodily detail.", "axis_keys": ["imagery_theme"], "anchor_ids": []},
                            {"paragraph_index": 3, "role": "development", "objective": "Surface the cost without thesis.", "axis_keys": ["emotional_arc"], "anchor_ids": []},
                            {"paragraph_index": 4, "role": "closing", "objective": "Land on residue.", "axis_keys": ["structure_composition"], "anchor_ids": []},
                        ],
                        "anchor_ids": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "feature_density" in system_text:
            anchor_ids = [value for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text) if value.startswith("anchor:")]
            return _mock_result(
                json.dumps(
                    {
                        "pass": True,
                        "score": 0.95,
                        "verdict": "approve",
                        "anchor_ids": anchor_ids[:3],
                        "matched_signals": ["特征浓度克制", "没有过拟合堆叠"],
                        "must_keep_spans": [],
                        "line_edits": [],
                        "redraft_reason": "",
                        "risks": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "cross_domain_generalization" in system_text:
            anchor_ids = [value for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text) if value.startswith("anchor:")]
            return _mock_result(
                json.dumps(
                    {
                        "pass": True,
                        "score": 0.94,
                        "verdict": "approve",
                        "anchor_ids": anchor_ids[:3],
                        "matched_signals": ["题目被翻译进成本镜头"],
                        "must_keep_spans": [],
                        "line_edits": [],
                        "redraft_reason": "",
                        "risks": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "rhythm_entropy" in system_text or "extreme_state_handling" in system_text or "ending_landing" in system_text:
            anchor_ids = [value for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text) if value.startswith("anchor:")]
            return _mock_result(
                json.dumps(
                    {
                        "pass": True,
                        "score": 0.93,
                        "verdict": "approve",
                        "anchor_ids": anchor_ids[:3],
                        "matched_signals": ["节奏和收尾都保持稳定"],
                        "must_keep_spans": [],
                        "line_edits": [],
                        "redraft_reason": "",
                        "risks": [],
                    },
                    ensure_ascii=False,
                )
            )

        if any(
            marker in system_text
            for marker in (
                "Stone v3 drafter",
                "Stone v3 redrafter",
                "Stone v3 line editor",
                "正文起草器",
                "重写器",
                "逐句修订器",
            )
        ):
            target = _extract_target_word_count(user_text)
            return _mock_result(_build_mock_article("KFC", target))

        if "请求适配器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "desired_length_band": "medium",
                        "surface_form": "scene_vignette",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through lived detail",
                        "distance": "recycled first person",
                        "entry_scene": "Start with the walk into the bright store.",
                        "felt_cost": "Make the comfort feel a little embarrassing and expensive.",
                        "query_terms": ["kfc", "night", "door", "road"],
                        "motif_terms": ["night", "door", "road", "grease"],
                        "anchor_preferences": ["opening", "closing", "signature"],
                        "hard_constraints": [],
                        "reasoning": "Use a night ritual and keep the cost bodily.",
                    },
                    ensure_ascii=False,
                )
            )

        if "重排器" in system_text:
            document_ids = []
            for value in re.findall(r'"document_id"\s*:\s*"([^"]+)"', user_text):
                if value not in document_ids:
                    document_ids.append(value)
            anchor_ids = []
            for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text):
                if value.startswith("anchor:") and value not in anchor_ids:
                    anchor_ids.append(value)
            return _mock_result(
                json.dumps(
                    {
                        "selected_documents": document_ids[:6],
                        "anchor_ids": anchor_ids[:8],
                        "selection_reason": "Night ritual anchors match the request and the author's cost lens.",
                        "rerank_notes": ["Keep the closure unresolved."],
                    },
                    ensure_ascii=False,
                )
            )

        if "写作包组装器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "entry_scene": "Enter through the bright counter and the warm paper bag.",
                        "felt_cost": "Comfort should feel useful and a little humiliating at once.",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through repetition",
                        "distance": "recycled first person",
                        "family_labels": ["night_cost"],
                        "lexicon_keep": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "syntax_rules": ["Prefer concrete pressure over explanation."],
                        "structure_recipe": [
                            "Open on the walk in.",
                            "Push pressure through small bodily details.",
                            "Leave the ending unresolved.",
                        ],
                        "do_not_do": ["Do not explain the ritual."],
                        "style_thesis": "Night comfort should carry cost and residue.",
                        "coverage_warnings": ["Sparse profile mode is active."],
                    },
                    ensure_ascii=False,
                )
            )

        if "蓝图规划器" in system_text or "改稿蓝图规划器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "paragraph_count": 4,
                        "shape_note": "Scene vignettes stacking bodily pressure.",
                        "entry_move": "Open from the bright door and the walk inside.",
                        "development_move": "Keep pressure in the body, wallet, and walk home.",
                        "turning_device": "small recoil after the first bite",
                        "closure_residue": "End on the road home, not a conclusion.",
                        "keep_terms": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "steps": [
                            "Open on action.",
                            "Accumulate concrete detail.",
                            "Let the cost surface without thesis.",
                            "Close on residue.",
                        ],
                        "do_not_do": ["Do not explain the process."],
                        "axis_map": {
                            "voice_signature": {"goal": "Keep the opening restrained and bodily.", "paragraph_hint": 1, "anchor_ids": []},
                            "emotional_arc": {"goal": "Let embarrassment rise slowly.", "paragraph_hint": 3, "anchor_ids": []},
                        },
                        "paragraph_map": [
                            {"paragraph_index": 1, "role": "opening", "objective": "Open from the bright door.", "axis_keys": ["voice_signature"], "anchor_ids": []},
                            {"paragraph_index": 2, "role": "development", "objective": "Accumulate bodily detail.", "axis_keys": ["imagery_theme"], "anchor_ids": []},
                            {"paragraph_index": 3, "role": "development", "objective": "Surface the cost without thesis.", "axis_keys": ["emotional_arc"], "anchor_ids": []},
                            {"paragraph_index": 4, "role": "closing", "objective": "Land on residue.", "axis_keys": ["structure_composition"], "anchor_ids": []},
                        ],
                        "anchor_ids": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "language_fluency" in system_text or "logic_flow" in system_text:
            anchor_ids = [value for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text) if value.startswith("anchor:")]
            return _mock_result(
                json.dumps(
                    {
                        "pass": True,
                        "score": 0.93,
                        "verdict": "approve",
                        "anchor_ids": anchor_ids[:3],
                        "matched_signals": ["语言与逻辑在改稿后保持顺滑"],
                        "must_keep_spans": [],
                        "line_edits": [],
                        "redraft_reason": "",
                        "risks": [],
                    },
                    ensure_ascii=False,
                )
            )

        if any(
            marker in system_text
            for marker in (
                "正文起草器",
                "改稿重写器",
                "重写器",
                "逐句修订器",
            )
        ):
            target = _extract_target_word_count(user_text)
            return _mock_result(_build_mock_article("KFC", target))

        if "请求适配器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "desired_length_band": "medium",
                        "surface_form": "scene_vignette",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through lived detail",
                        "distance": "recycled first person",
                        "entry_scene": "Start with the walk into the bright store.",
                        "felt_cost": "Make the comfort feel a little embarrassing and expensive.",
                        "query_terms": ["kfc", "night", "door", "road"],
                        "motif_terms": ["night", "door", "road", "grease"],
                        "anchor_preferences": ["opening", "closing", "signature"],
                        "hard_constraints": [],
                        "reasoning": "Use a night ritual and keep the cost bodily.",
                    },
                    ensure_ascii=False,
                )
            )

        if "重排器" in system_text:
            document_ids = []
            for value in re.findall(r'"document_id"\s*:\s*"([^"]+)"', user_text):
                if value not in document_ids:
                    document_ids.append(value)
            anchor_ids = []
            for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text):
                if value.startswith("anchor:") and value not in anchor_ids:
                    anchor_ids.append(value)
            return _mock_result(
                json.dumps(
                    {
                        "selected_documents": document_ids[:6],
                        "anchor_ids": anchor_ids[:8],
                        "selection_reason": "Night ritual anchors match the request and the author's cost lens.",
                        "rerank_notes": ["Keep the closure unresolved."],
                    },
                    ensure_ascii=False,
                )
            )

        if "写作包组装器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "entry_scene": "Enter through the bright counter and the warm paper bag.",
                        "felt_cost": "Comfort should feel useful and a little humiliating at once.",
                        "value_lens": "cost",
                        "judgment_mode": "stabilize through repetition",
                        "distance": "recycled first person",
                        "family_labels": ["night_cost"],
                        "lexicon_keep": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "syntax_rules": ["Prefer concrete pressure over explanation."],
                        "structure_recipe": [
                            "Open on the walk in.",
                            "Push pressure through small bodily details.",
                            "Leave the ending unresolved.",
                        ],
                        "do_not_do": ["Do not explain the ritual."],
                        "style_thesis": "Night comfort should carry cost and residue.",
                        "coverage_warnings": ["Sparse profile mode is active."],
                    },
                    ensure_ascii=False,
                )
            )

        if "蓝图规划器" in system_text or "改稿蓝图规划器" in system_text:
            return _mock_result(
                json.dumps(
                    {
                        "paragraph_count": 4,
                        "shape_note": "Scene vignettes stacking bodily pressure.",
                        "entry_move": "Open from the bright door and the walk inside.",
                        "development_move": "Keep pressure in the body, wallet, and walk home.",
                        "turning_device": "small recoil after the first bite",
                        "closure_residue": "End on the road home, not a conclusion.",
                        "keep_terms": ["night", "door", "road", "grease"],
                        "motif_obligations": ["night", "door", "road"],
                        "steps": [
                            "Open on action.",
                            "Accumulate concrete detail.",
                            "Let the cost surface without thesis.",
                            "Close on residue.",
                        ],
                        "do_not_do": ["Do not explain the process."],
                        "axis_map": {
                            "voice_signature": {"goal": "Keep the opening restrained and bodily.", "paragraph_hint": 1, "anchor_ids": []},
                            "emotional_arc": {"goal": "Let embarrassment rise slowly.", "paragraph_hint": 3, "anchor_ids": []},
                        },
                        "paragraph_map": [
                            {"paragraph_index": 1, "role": "opening", "objective": "Open from the bright door.", "axis_keys": ["voice_signature"], "anchor_ids": []},
                            {"paragraph_index": 2, "role": "development", "objective": "Accumulate bodily detail.", "axis_keys": ["imagery_theme"], "anchor_ids": []},
                            {"paragraph_index": 3, "role": "development", "objective": "Surface the cost without thesis.", "axis_keys": ["emotional_arc"], "anchor_ids": []},
                            {"paragraph_index": 4, "role": "closing", "objective": "Land on residue.", "axis_keys": ["structure_composition"], "anchor_ids": []},
                        ],
                        "anchor_ids": [],
                    },
                    ensure_ascii=False,
                )
            )

        if "写作包审判器" in system_text or "packet critic" in system_text:
            anchor_ids = [value for value in re.findall(r'"id"\s*:\s*"([^"]+)"', user_text) if value.startswith("anchor:")]
            return _mock_result(
                json.dumps(
                    {
                        "pass": True,
                        "score": 0.91,
                        "verdict": "approve",
                        "anchor_ids": anchor_ids[:3],
                        "matched_signals": ["关键字段基本贴住了画像切片和锚点"],
                        "unsupported_fields": [],
                        "defaulted_fields": [],
                        "overfit_risks": [],
                        "repair_instructions": [],
                        "risks": [],
                    },
                    ensure_ascii=False,
                )
            )

        if any(
            marker in system_text
            for marker in (
                "正文起草器",
                "改稿重写器",
                "重写器",
                "逐句修订器",
            )
        ):
            target = _extract_target_word_count(user_text)
            return _mock_result(_build_mock_article("KFC", target))

        raise AssertionError(f"Unhandled Stone v3 prompt: {system_text}")

    def fake_tool_round(self, messages, tools, **kwargs):
        del self, kwargs
        tool_names = {
            str((tool.get("function") or {}).get("name") or "")
            for tool in (tools or [])
            if isinstance(tool, dict)
        }
        if "get_corpus_overview" in tool_names:
            facet_label = "当前维度"
            system_text = str(messages[0].get("content") or "") if messages else ""
            for candidate in (
                "声音指纹",
                "词汇私方言",
                "结构与构图",
                "意象与母题",
                "立场与价值",
                "情绪弧线",
                "临床与心理动力",
                "创作约束",
            ):
                if candidate in system_text:
                    facet_label = candidate
                    break
            payload = {
                "summary": f"围绕{facet_label}，现有已预处理文章已经能支撑稳定归纳。",
                "bullets": [
                    f"{facet_label}主要通过具体场景和重复母题显影。",
                    f"{facet_label}更依赖可见细节，而不是直接解释。",
                ],
                "confidence": 0.84,
                "fewshots": [],
                "conflicts": [],
                "notes": "",
            }
            return ToolRoundResult(
                content=json.dumps(payload, ensure_ascii=False),
                model="demo-model",
                usage={"prompt_tokens": 40, "completion_tokens": 20, "total_tokens": 60},
                tool_calls=[],
                provider_response_id="stone-tool-round-mock",
            )
        raise AssertionError(f"Unhandled Stone tool round: {tool_names}")

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)
    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)


def _create_v3_preprocessed_project(client, app, monkeypatch, *, name: str) -> tuple[str, dict]:
    project_id, preprocess_payload = _create_v3_preprocessed_project_without_analysis(client, app, monkeypatch, name=name)
    _seed_stone_analysis_run(app, project_id)
    return project_id, preprocess_payload


def _create_v3_preprocessed_project_without_analysis(client, app, monkeypatch, *, name: str) -> tuple[str, dict]:
    create_response = client.post("/api/projects", json={"name": name, "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    upload_response = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "I still walk into KFC at night. The door is bright, the paper bag is warm, and the road home makes every bite feel slightly expensive.",
            "source_type": "essay",
        },
    )
    assert upload_response.status_code == 200
    document_id = upload_response.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    _ensure_service_config(app, "chat_service")
    _install_stone_v3_mocks(monkeypatch)
    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_response.json()["id"])
    assert preprocess_payload["status"] == "completed"
    return project_id, preprocess_payload


def _seed_stone_analysis_run(app, project_id: str) -> str:
    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id=project_id,
            status="completed",
            summary_json={"analysis_ready": True},
        )
        run.started_at = utcnow()
        run.finished_at = run.started_at
        for facet in get_facets_for_mode("stone"):
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="completed",
                confidence=0.92,
                findings_json={
                    "summary": f"{facet.label} stays stable across the sampled Stone corpus.",
                    "bullets": [
                        f"{facet.label} remains reusable.",
                        f"{facet.label} can be traced back to concrete evidence.",
                    ],
                    "anchor_ids": ["anchor:seed:opening", "anchor:seed:closing"],
                },
                evidence_json=[
                    {
                        "id": f"evidence:{facet.key}:1",
                        "document_id": "seed",
                        "document_title": "seed",
                        "quote": "A bright door, a walk home, and cost residue.",
                        "expression": facet.label,
                        "reason": facet.purpose,
                    }
                ],
                conflicts_json=[],
            )
        return run.id


def _seed_legacy_v2_fallback(app) -> str:
    raw_text = "KFC at night is cheap comfort that always leaves a little shame on the walk home."
    profile_v2 = {
        "length_band": "short",
        "content_kernel": "Night comfort with cost residue.",
        "surface_form": "scene_vignette",
        "motif_tags": ["night", "door", "road"],
        "prototype_family": "scene_vignette|night_cost",
        "anchor_spans": {
            "opening": raw_text,
            "pivot": "",
            "closing": raw_text,
            "signature": [raw_text],
        },
    }

    with app.state.db.session() as session:
        project = repository.create_project(session, name="Legacy V2", mode="stone")
        project_id = project.id
        storage_path = app.state.config.upload_dir / project_id / "legacy-v2.txt"
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        storage_path.write_text(raw_text, encoding="utf-8")
        repository.create_document(
            session,
            id=str(uuid4()),
            project_id=project_id,
            filename="legacy-v2.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="essay",
            title="Legacy V2",
            author_guess="Author",
            created_at_guess=None,
            raw_text=raw_text,
            clean_text=raw_text,
            language="en",
            metadata_json={"stone_profile_v2": profile_v2},
            ingest_status="ready",
            error_message=None,
            storage_path=str(storage_path),
        )
        preprocess_run = repository.create_stone_preprocess_run(
            session,
            project_id=project_id,
            status="completed",
            summary_json={"stone_profile_total": 1, "stone_profile_completed": 1},
        )
        preprocess_run.started_at = utcnow()
        preprocess_run.finished_at = preprocess_run.started_at
        repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=preprocess_run.id,
            asset_kind="stone_author_model_v2",
            markdown_text="# Stone Author Model V2",
            json_payload={"asset_kind": "stone_author_model_v2", "legacy_seed": True},
            prompt_text=json.dumps({"asset_kind": "stone_author_model_v2", "legacy_seed": True}, ensure_ascii=False, indent=2),
            notes="seed",
        )
        repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=preprocess_run.id,
            asset_kind="stone_prototype_index_v2",
            markdown_text="# Stone Prototype Index V2",
            json_payload={"asset_kind": "stone_prototype_index_v2", "legacy_seed": True},
            prompt_text=json.dumps({"asset_kind": "stone_prototype_index_v2", "legacy_seed": True}, ensure_ascii=False, indent=2),
            notes="seed",
        )
    return project_id

def test_stone_preprocess_generates_v3_profiles_and_baseline(client, app, monkeypatch):
    project_id, preprocess_payload = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Preprocess")

    assert preprocess_payload["profile_version"] == "v3"
    assert preprocess_payload["baseline_version"] == "v3"
    assert preprocess_payload["baseline_review_v3"]["verdict"] == "approve"
    stages = [item["stage"] for item in preprocess_payload["stage_trace"]]
    assert "document_profile_v3" in stages
    assert "family_induction_v3_finalize" in stages
    assert "author_model_v3" in stages
    assert "prototype_index_v3_finalize" in stages
    assert "baseline_critic_v3" in stages

    with app.state.db.session() as session:
        documents = repository.list_project_documents(session, project_id)
        assert isinstance(documents[0].metadata_json.get("stone_profile_v3"), dict)
        assert repository.get_latest_asset_draft(session, project_id, asset_kind="stone_author_model_v3") is not None
        assert repository.get_latest_asset_draft(session, project_id, asset_kind="stone_prototype_index_v3") is not None


def test_stone_author_model_v3_includes_style_fingerprint(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Style Fingerprint")

    with app.state.db.session() as session:
        draft = repository.get_latest_asset_draft(session, project_id, asset_kind="stone_author_model_v3")
        assert draft is not None
        payload = dict(draft.json_payload or {})
        fingerprint = dict(payload.get("style_fingerprint") or {})

    assert set(fingerprint) >= {
        "narrator_profile",
        "lexicon_profile",
        "rhythm_profile",
        "closure_profile",
        "extreme_state_profile",
    }
    assert isinstance(fingerprint["narrator_profile"], dict)
    assert isinstance(fingerprint["lexicon_profile"], dict)
    assert isinstance(fingerprint["rhythm_profile"], dict)


def test_stone_preprocess_defaults_to_analysis_concurrency(client, app, monkeypatch):
    project_id, preprocess_payload = _create_v3_preprocessed_project(
        client,
        app,
        monkeypatch,
        name="Stone V3 Default Concurrency",
    )

    assert preprocess_payload["concurrency"] == DEFAULT_ANALYSIS_CONCURRENCY

    with app.state.db.session() as session:
        run = repository.get_stone_preprocess_run(session, preprocess_payload["id"])
        assert run is not None
        assert dict(run.summary_json or {}).get("concurrency") == DEFAULT_ANALYSIS_CONCURRENCY


def test_long_document_profiles_are_chunked_under_budget(app, monkeypatch):
    _ensure_service_config(app, "chat_service")
    _install_stone_v3_mocks(monkeypatch)

    with app.state.db.session() as session:
        chat_config = repository.get_service_config(session, "chat_service")

    worker = app.state.stone_preprocess_worker
    document = StoneDocumentSnapshot(
        id=str(uuid4()),
        title="Long Night Essay",
        filename="long-night.txt",
        source_type="essay",
        created_at_guess=None,
        clean_text="夜里走进肯德基。" * 70000,
        raw_text=None,
        metadata_json={},
    )

    result = worker._build_stone_profile_payload(
        document,
        project_name="Long Stone",
        chat_config=chat_config,
    )

    profile = result.profile
    source_meta = dict((profile.get("evidence_trace") or {}).get("source_meta") or {})
    assert source_meta.get("chunked_profile") is True
    assert int(source_meta.get("chunk_total") or 0) > 1


def test_writing_postprocessing_does_not_trim_model_output():
    text = (
        "第一段写得很长，已经足够超过目标字数，而且后面还有真正的收口。\n\n"
        "第二段继续铺陈细节，不应该因为目标字数被硬砍掉。\n\n"
        "第三段才是真正的结尾，必须原样保留。"
    )

    assert _light_trim_to_word_count(text, 100) == text
    assert _fit_word_count(text, 100, None, "主题", None) == text


def test_manual_asset_generation_supports_stone_v3_kinds(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Assets")

    response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "stone_author_model_v3"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["asset_kind"] == "stone_author_model_v3"
    assert payload["json_payload"]["asset_kind"] == "stone_author_model_v3"
    assert payload["json_payload"]["version"] == "v3"
    assert "Stone Author Model V3" in payload["markdown_text"]


def test_stream_asset_generation_supports_stone_v3_kinds(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Asset Stream")

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/assets/generate/stream",
        method="POST",
        json_payload={"asset_kind": "stone_author_model_v3"},
    )
    done_events = [payload for name, payload in events if name == "done"]
    assert done_events
    draft = done_events[-1]["draft"]

    assert draft["asset_kind"] == "stone_author_model_v3"
    assert draft["json_payload"]["asset_kind"] == "stone_author_model_v3"
    assert draft["json_payload"]["version"] == "v3"
    assert "Stone Author Model V3" in draft["markdown_text"]

    status_phases = [payload.get("phase") for name, payload in events if name == "status"]
    assert "family_induction_v3" in status_phases
    assert "author_model_v3" in status_phases
    assert "prototype_index_v3" in status_phases
    assert "baseline_critic_v3" in status_phases


def test_stream_asset_generation_accepts_failed_preprocess_run_when_v3_profiles_exist(client, app, monkeypatch):
    project_id, preprocess_payload = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Failed Run Asset Stream")

    with app.state.db.session() as session:
        run = repository.get_stone_preprocess_run(session, preprocess_payload["id"])
        run.status = "failed"
        run.error_message = "legacy failed status with usable profiles"

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/assets/generate/stream",
        method="POST",
        json_payload={"asset_kind": "stone_prototype_index_v3"},
    )
    done_events = [payload for name, payload in events if name == "done"]
    error_events = [payload for name, payload in events if name == "error"]

    assert not error_events
    assert done_events
    draft = done_events[-1]["draft"]
    assert draft["asset_kind"] == "stone_prototype_index_v3"
    assert draft["json_payload"]["asset_kind"] == "stone_prototype_index_v3"
    assert draft["json_payload"]["version"] == "v3"


def test_stream_asset_generation_times_out_after_inactivity(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Timeout")

    monkeypatch.setattr(web_routes, "ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(web_routes, "ASSET_STREAM_QUEUE_POLL_SECONDS", 0.01)

    def fake_generate_asset_draft(
        request,
        session,
        project_id,
        *,
        asset_kind,
        progress_callback=None,
        cancel_requested=None,
    ):
        del request, session, project_id, asset_kind, progress_callback
        deadline = time.time() + 0.2
        while time.time() < deadline:
            if cancel_requested and cancel_requested():
                raise TimeoutError("cancelled after inactivity")
            time.sleep(0.01)
        raise AssertionError("stream timeout did not trigger")

    monkeypatch.setattr(web_routes, "_generate_asset_draft", fake_generate_asset_draft)

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/assets/generate/stream",
        method="POST",
        json_payload={"asset_kind": "stone_author_model_v3"},
    )

    error_events = [payload for name, payload in events if name == "error"]
    assert error_events
    assert "timed out" in str(error_events[-1]["message"]).lower()


def test_stream_asset_generation_resumes_from_checkpoint_after_critic_failure(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Resume")
    checkpoint_path = stone_v3_checkpoint_path(app.state.config.assets_dir, project_id)

    _install_stone_v3_mocks(monkeypatch)
    original = OpenAICompatibleClient.chat_completion_result
    critic_failures = {"remaining": 3}

    def flaky_chat_completion_result(self, messages, **kwargs):
        system_text = str(messages[0].get("content") or "") if messages else ""
        if "Stone v3 baseline critic" in system_text and critic_failures["remaining"] > 0:
            critic_failures["remaining"] -= 1
            raise RuntimeError("forced critic failure")
        return original(self, messages, **kwargs)

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", flaky_chat_completion_result)

    failed_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/assets/generate/stream",
        method="POST",
        json_payload={"asset_kind": "stone_author_model_v3"},
    )
    assert [payload for name, payload in failed_events if name == "error"]
    assert checkpoint_path.exists()
    checkpoint_payload = load_stone_v3_checkpoint(app.state.config.assets_dir, project_id)
    assert checkpoint_payload.get("prototype_index")

    resumed_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/assets/generate/stream",
        method="POST",
        json_payload={"asset_kind": "stone_author_model_v3"},
    )

    assert not [payload for name, payload in resumed_events if name == "error"]
    assert [payload for name, payload in resumed_events if name == "done"]
    assert "resume_checkpoint_v3" in [payload.get("phase") for name, payload in resumed_events if name == "status"]
    assert not checkpoint_path.exists()


def test_stone_writing_status_flags_legacy_v2_data_as_requires_rebuild(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Status")
    legacy_project_id = _seed_legacy_v2_fallback(app)

    with app.state.db.session() as session:
        v3_status = _resolve_stone_writing_status(session, project_id)
        legacy_status = _resolve_stone_writing_status(session, legacy_project_id)

    assert v3_status["status"] == "ready"
    assert v3_status["profile_version"] == "v3"
    assert v3_status["baseline_version"] == "v3"
    assert v3_status["analysis_ready"] is True
    assert v3_status["writing_packet_ready"] is True
    assert v3_status["analysis_run_id"] is not None
    assert v3_status["author_model_v3_ready"] is True
    assert v3_status["prototype_index_v3_ready"] is True
    assert "legacy_fallback_active" not in v3_status

    assert legacy_status["status"] == "requires_rebuild"
    assert legacy_status["profile_version"] is None
    assert legacy_status["baseline_version"] is None
    assert legacy_status["rebuild_required"] is True


def test_stone_writing_status_requires_analysis_before_ready(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project_without_analysis(
        client,
        app,
        monkeypatch,
        name="Stone V3 Missing Analysis",
    )

    with app.state.db.session() as session:
        status = _resolve_stone_writing_status(session, project_id)

    assert status["status"] == "missing_analysis"
    assert status["analysis_ready"] is False
    assert status["writing_packet_ready"] is False
    assert status["analysis_run_id"] is None


def test_stone_writing_status_flags_incomplete_analysis(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project_without_analysis(
        client,
        app,
        monkeypatch,
        name="Stone V3 Incomplete Analysis",
    )

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id=project_id,
            status="completed",
            summary_json={"analysis_ready": False},
        )
        run.started_at = utcnow()
        run.finished_at = run.started_at
        facets = get_facets_for_mode("stone")
        for facet in facets[:-1]:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="completed",
                confidence=0.9,
                findings_json={"summary": facet.label, "bullets": [facet.purpose]},
                evidence_json=[],
                conflicts_json=[],
            )
        status = _resolve_stone_writing_status(session, project_id)

    assert status["status"] == "analysis_incomplete"
    assert status["analysis_ready"] is False
    assert status["writing_packet_ready"] is False
    assert status["analysis_run_id"] == run.id


def test_invalid_latest_v3_drafts_do_not_block_writing(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Ignore Invalid Drafts")

    with app.state.db.session() as session:
        repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_author_model_v3",
            markdown_text="# Broken",
            json_payload={"asset_kind": "stone_author_model_v3"},
            prompt_text="broken",
            notes="invalid newer draft",
        )
        repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_prototype_index_v3",
            markdown_text="# Broken",
            json_payload={"asset_kind": "stone_prototype_index_v3"},
            prompt_text="broken",
            notes="invalid newer draft",
        )
        status = _resolve_stone_writing_status(session, project_id)

    assert status["status"] == "ready"
    assert status["analysis_ready"] is True
    assert status["writing_packet_ready"] is True
    assert status["author_model_v3_ready"] is True
    assert status["prototype_index_v3_ready"] is True

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Recovery"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]


def test_writing_uses_valid_v3_baseline_even_if_latest_preprocess_run_failed(client, app, monkeypatch):
    project_id, preprocess_payload = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Failed Run Writing")

    with app.state.db.session() as session:
        run = repository.get_stone_preprocess_run(session, preprocess_payload["id"])
        run.status = "failed"
        run.error_message = "legacy failed status with usable profiles"

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Failed Run"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]


def test_writing_service_uses_v3_pipeline_by_default(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Writing")

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Session"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]
    stage_names = [payload.get("stage") for name, payload in events if name == "stage"]

    assert "request_adapter_v3" in stage_names
    assert "profile_selection_v3" in stage_names
    assert "candidate_shortlist_v3" in stage_names
    assert "llm_rerank_v3" in stage_names
    assert "writing_packet_v3" in stage_names
    assert "packet_critic" in stage_names
    assert "blueprint_v3" in stage_names
    assert "draft_v3" in stage_names
    assert "feature_density" in stage_names
    assert "cross_domain_generalization" in stage_names
    assert "rhythm_entropy" in stage_names
    assert "extreme_state_handling" in stage_names
    assert "ending_landing" in stage_names
    assert "sample_routing" not in stage_names
    assert "local_decomposition" not in stage_names

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    latest_turn = assistant_turns[-1]
    trace = latest_turn["trace"]

    assert trace["baseline_source"] == "stone_v3_baseline"
    assert trace["generation_packet"]["baseline"]["stone_v3"] is True
    assert trace["generation_packet"]["baseline"]["analysis_ready"] is True
    assert trace["generation_packet"]["baseline"]["writing_packet_ready"] is True
    assert trace["generation_packet"]["baseline"]["author_model_v3_ready"] is True
    assert trace["generation_packet"]["baseline"]["prototype_index_v3_ready"] is True
    assert "request_adapter_v3" in trace
    assert "profile_selection_v3" in trace
    assert "candidate_shortlist_v3" in trace
    assert "llm_rerank_v3" in trace
    assert "writing_packet_v3" in trace
    assert "packet_critic_v3" in trace
    assert "blueprint_v3" in trace
    assert "coverage_warnings" in trace
    assert "axis_source_map" in trace
    assert "local_router" not in trace


def test_stone_writing_settings_persist_and_bootstrap(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone V3 Settings", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        assert repository.get_project_stone_writing_settings(project)["max_concurrency"] == 4

    update_response = client.patch(
        f"/api/projects/{project_id}/writing/settings",
        json={"max_concurrency": 6},
    )
    assert update_response.status_code == 200
    assert update_response.json()["stone_writing"]["max_concurrency"] == 6

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        assert project is not None
        assert dict(project.metadata_json or {}).get("stone_writing", {}).get("max_concurrency") == 6
        assert repository.get_project_stone_writing_settings(project)["max_concurrency"] == 6

    page_response = client.get(f"/projects/{project_id}/writing")
    assert page_response.status_code == 200
    bootstrap = _extract_writing_bootstrap_payload(page_response.text)
    assert bootstrap["writing_settings"]["max_concurrency"] == 6


def test_writing_trace_exposes_usage_summary_and_project_concurrency(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Usage Ledger")

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Usage"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words", "max_concurrency": 3},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]

    request_stage = next(
        payload
        for name, payload in events
        if name == "stage" and payload.get("stage") == "request_adapter_v3"
    )
    assert request_stage["usage"]["prompt_tokens"] == 32
    assert request_stage["usage"]["completion_tokens"] == 16
    assert request_stage["usage"]["total_tokens"] == 48
    assert request_stage["detail"]["usage"]["total_tokens"] == 48
    assert request_stage["detail"]["billed_usage_total"]["total_tokens"] == 48
    assert request_stage["detail"]["attempt_count"] == 1
    assert request_stage["detail"]["retry_count"] == 0

    done_payload = next(payload for name, payload in events if name == "done")
    assert done_payload["resolved_max_concurrency"] == 3
    assert done_payload["usage_summary"]["billed_total"]["total_tokens"] > 0
    assert done_payload["usage_summary"]["successful_stage_total"]["total_tokens"] > 0
    assert done_payload["usage_summary"]["llm_call_count"] >= 1
    assert isinstance(done_payload["style_fingerprint_brief"], dict)
    assert isinstance(done_payload["draft_fingerprint_report"], dict)

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    trace = assistant_turns[-1]["trace"]
    timeline_request = next(item for item in trace["timeline"] if item.get("stage") == "request_adapter_v3")

    assert trace["resolved_max_concurrency"] == 3
    assert trace["usage_summary"]["billed_total"]["total_tokens"] > 0
    assert isinstance(trace["style_fingerprint_brief"], dict)
    assert isinstance(trace["draft_fingerprint_report"], dict)
    assert timeline_request["detail"]["usage"]["total_tokens"] == 48
    assert timeline_request["detail"]["attempt_count"] == 1


def test_writing_usage_summary_counts_retries_in_billed_total(client, app, monkeypatch):
    import app.agents.stone.writing.packet_builder as packet_builder_module

    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Retry Usage")

    original_chat_completion_result = OpenAICompatibleClient.chat_completion_result
    original_parse_json_response = packet_builder_module.parse_json_response
    call_counts = {"request_adapter": 0}

    def flaky_chat_completion_result(self, messages, **kwargs):
        system_text = str(messages[0].get("content") or "") if messages else ""
        if "Stone v3 request adapter" in system_text:
            call_counts["request_adapter"] += 1
            if call_counts["request_adapter"] == 1:
                return _mock_result("not valid json")
        return original_chat_completion_result(self, messages, **kwargs)

    def flaky_parse_json_response(text: str, fallback: bool = False):
        if text == "not valid json":
            raise ValueError("forced retry")
        return original_parse_json_response(text, fallback=fallback)

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", flaky_chat_completion_result)
    monkeypatch.setattr(packet_builder_module, "parse_json_response", flaky_parse_json_response)

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Retry"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]

    request_stage = next(
        payload
        for name, payload in events
        if name == "stage" and payload.get("stage") == "request_adapter_v3"
    )
    assert call_counts["request_adapter"] == 2
    assert request_stage["detail"]["attempt_count"] == 2
    assert request_stage["detail"]["retry_count"] == 1
    assert request_stage["detail"]["usage"]["total_tokens"] == 48
    assert request_stage["detail"]["billed_usage_total"]["total_tokens"] == 96


def test_writing_message_accepts_freeform_topic_without_explicit_word_count(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Freeform Writing")

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Freeform"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    freeform_message = "写雨夜车站，克制一点，别解释太多，让情绪停在回家路上。"
    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": freeform_message},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    assert not [payload for name, payload in events if name == "error"]

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    user_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "user"]
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]

    assert user_turns[-1]["content"] == freeform_message
    assert assistant_turns[-1]["trace"]["topic"] == freeform_message
    assert assistant_turns[-1]["trace"]["raw_message"] == freeform_message


def test_follow_up_feedback_uses_revision_pipeline_from_blueprint_down(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 Revision Flow")

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 Revision"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    first_message = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert first_message.status_code == 200
    first_stream_id = first_message.json()["stream_id"]

    first_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{first_stream_id}",
    )
    assert not [payload for name, payload in first_events if name == "error"]

    revision_message = "把结尾再收一点，语言更顺一点，逻辑更清楚，不要解释得太满。"
    second_message = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": revision_message},
    )
    assert second_message.status_code == 200
    second_stream_id = second_message.json()["stream_id"]

    second_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{second_stream_id}",
    )
    assert not [payload for name, payload in second_events if name == "error"]
    second_stage_names = [payload.get("stage") for name, payload in second_events if name == "stage"]

    assert "blueprint_v3" in second_stage_names
    assert "redraft" in second_stage_names
    assert "language_fluency" in second_stage_names
    assert "logic_flow" in second_stage_names
    assert "request_adapter_v3" not in second_stage_names
    assert "candidate_shortlist_v3" not in second_stage_names
    assert "llm_rerank_v3" not in second_stage_names
    assert "writing_packet_v3" not in second_stage_names
    assert "draft_v3" not in second_stage_names

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    result_turns = [
        turn
        for turn in detail_payload["turns"]
        if turn["role"] == "assistant" and isinstance(turn.get("trace"), dict) and turn["trace"].get("kind") == "writing_result"
    ]
    assert len(result_turns) >= 2

    first_result_turn = result_turns[-2]
    latest_result_turn = result_turns[-1]
    first_trace = first_result_turn["trace"]
    latest_trace = latest_result_turn["trace"]

    assert first_trace.get("request_mode", "draft") == "draft"
    assert latest_trace["request_mode"] == "revision"
    assert latest_trace["revision_mode"] is True
    assert latest_trace["revision_source_turn_id"]
    assert latest_trace["revision_source"]["turn_id"] == latest_trace["revision_source_turn_id"]
    assert latest_trace["revision_request"] == revision_message
    assert latest_trace["target_word_count"] == 400
    assert latest_trace["revision_source"]["final_text"] == first_result_turn["content"]
    assert latest_trace["writing_packet_v3"]
    critic_keys = {critic["critic_key"] for critic in latest_trace["critics"]}
    assert "language_fluency" in critic_keys
    assert "logic_flow" in critic_keys


def test_v3_writing_tolerates_string_translation_rules_in_author_model(client, app, monkeypatch):
    project_id, _ = _create_v3_preprocessed_project(client, app, monkeypatch, name="Stone V3 String Rules")

    with app.state.db.session() as session:
        draft = repository.get_latest_asset_draft(session, project_id, asset_kind="stone_author_model_v3")
        assert draft is not None
        payload = dict(draft.json_payload or {})
        payload["translation_rules"] = [
            "Interpret analogies as core arguments.",
            "Distance cues should guide stance selection.",
            "Surface form should follow rhetorical pressure.",
        ]
        draft.json_payload = payload

    session_response = client.post(f"/api/projects/{project_id}/writing/sessions", json={"title": "Stone V3 String Rules"})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    message_response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "Write about KFC at night, 400 words"},
    )
    assert message_response.status_code == 200
    stream_id = message_response.json()["stream_id"]

    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )

    assert not [payload for name, payload in events if name == "error"]
    stage_names = [payload.get("stage") for name, payload in events if name == "stage"]
    assert "request_adapter_v3" in stage_names
    assert "draft_v3" in stage_names

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    trace = assistant_turns[-1]["trace"]
    assert trace["request_adapter_v3"]["value_lens"] == "cost"


def test_v3_revision_action_uses_balance_mode_hard_failures_only():
    from app.agents.stone.writing.packet_builder import _build_v3_draft_fingerprint_report, _revision_action_v3

    writing_packet = {
        "style_fingerprint_brief": {
            "narrator_profile": {"person": "first"},
            "rhythm_profile": {"sentence_length_buckets": {"short": 3, "medium": 1, "long": 0}},
            "closure_profile": {"closure_moves": ["Leave residue instead of summary."]},
        },
        "connective_keep": ["可是", "然后"],
        "lexicon_keep": ["夜里", "门口", "路上"],
        "overfit_limits": ["夜里", "门口", "路上"],
    }
    blueprint = {"closure_residue": "End on the walk home, not a conclusion."}
    critics = [{"critic_key": "feature_density", "pass": True, "verdict": "approve", "line_edits": []}]

    mild_report = _build_v3_draft_fingerprint_report(
        "我夜里走回去，门口的灯还是亮着。可是我没有再解释什么，只是继续往前走。",
        writing_packet,
        blueprint,
    )
    hard_report = _build_v3_draft_fingerprint_report(
        "他们最后总而言之都明白了。夜里夜里夜里，门口门口门口，路上路上路上。",
        writing_packet,
        blueprint,
    )

    assert mild_report["hard_failures"] == []
    assert _revision_action_v3(critics, mild_report) == "none"
    assert "pronoun_drift" in hard_report["hard_failures"]
    assert "closure_summary" in hard_report["hard_failures"]
    assert "overfit_stack" in hard_report["hard_failures"]
    assert _revision_action_v3(critics, hard_report) == "line_edit"


def test_v3_critics_respect_max_concurrency_and_preserve_order(monkeypatch):
    import app.agents.stone.writing.critics as critics_module
    from app.agents.stone.writing.service import WritingStreamState

    expected_keys = [
        "feature_density",
        "cross_domain_generalization",
        "rhythm_entropy",
        "extreme_state_handling",
        "ending_landing",
        "language_fluency",
        "logic_flow",
    ]

    def run_with_limit(limit: int) -> tuple[list[dict], int]:
        active = 0
        max_active = 0
        lock = threading.Lock()

        def fake_review(
            self,
            state,
            critic_key,
            draft,
            analysis_bundle,
            request_adapter,
            rerank,
            writing_packet,
            blueprint,
            client,
            **kwargs,
        ):
            del self, state, draft, analysis_bundle, request_adapter, rerank, writing_packet, blueprint, client, kwargs
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return {"critic_key": critic_key, "pass": True, "verdict": "approve", "line_edits": []}

        monkeypatch.setattr(critics_module, "_review_with_v3_critic", fake_review)
        state = WritingStreamState(
            id=f"stream-{limit}",
            project_id="project-1",
            session_id="session-1",
            user_turn_id="turn-1",
            topic="topic",
            target_word_count=400,
            extra_requirements=None,
            raw_message=None,
            resolved_max_concurrency=limit,
        )
        results = critics_module._run_v3_critics(
            object(),
            state,
            None,
            "draft text",
            {},
            {},
            {},
            {},
            None,
            previous_final_text="previous draft",
            revision_request="tighten the ending",
        )
        return results, max_active

    serial_results, serial_max_active = run_with_limit(1)
    parallel_results, parallel_max_active = run_with_limit(4)

    assert [item["critic_key"] for item in serial_results] == expected_keys
    assert [item["critic_key"] for item in parallel_results] == expected_keys
    assert serial_max_active == 1
    assert 2 <= parallel_max_active <= 4


def test_partial_preprocess_still_allows_author_analysis(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone V3 Partial", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    contents = [
        "Night food and a bright door keep turning cheap comfort into visible cost.",
        "The road home makes every bite feel slightly expensive, but I still go back.",
        "FAILME this document should fail profiling while the others still complete.",
    ]
    for index, content in enumerate(contents, start=1):
        upload_response = client.post(
            f"/api/projects/{project_id}/documents/text",
            json={"content": content, "source_type": "essay"},
        )
        assert upload_response.status_code == 200
        _wait_for_ready(client, project_id, upload_response.json()["id"])

    _ensure_service_config(app, "chat_service")
    _install_stone_v3_mocks(monkeypatch, fail_profile_on_text="FAILME")
    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_response.json()["id"])

    assert preprocess_payload["status"] == "partial_failed"
    assert preprocess_payload["stone_profile_completed"] == 2
    assert preprocess_payload["stone_profile_total"] == 3

    analysis_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone corpus", "target_role": "Author"},
    )
    assert analysis_response.status_code == 200
    run_id = analysis_response.json()["id"]
    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"


def test_stale_stone_preprocess_run_recovers_from_running_state(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone V3 Stale", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    upload_response = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "A bright door, a night road, and the cost of cheap comfort.",
            "source_type": "essay",
        },
    )
    assert upload_response.status_code == 200
    document_id = upload_response.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    with app.state.db.session() as session:
        document = repository.get_document(session, document_id)
        metadata = dict(document.metadata_json or {})
        metadata["stone_profile_v3"] = {
            "document_core": {
                "summary": "Night food and cost residue.",
                "length_band": "short",
                "surface_form": "scene_vignette",
                "dominant_theme": "cost residue",
            },
            "voice_contract": {
                "person": "first",
                "address_target": "self",
                "distance": "回收",
                "self_position": "自损",
                "cadence": "restrained",
                "sentence_shape": "mixed",
                "tone_words": ["restrained", "residual"],
            },
            "structure_moves": {
                "opening_move": "Open from a concrete action.",
                "development_move": "Let pressure rise through visible detail.",
                "turning_move": "none",
                "closure_move": "Leave residue instead of summary.",
                "paragraph_strategy": "2-4 paragraphs",
            },
            "motif_and_scene_bank": {
                "motif_tags": ["night", "door", "road"],
                "scene_terms": ["counter", "walk home"],
                "sensory_terms": ["bright", "warm"],
                "lexicon_markers": ["night", "door", "road"],
            },
            "value_and_judgment": {
                "judgment_target": "cheap comfort",
                "judgment_mode": "stabilize through repetition",
                "value_lens": "cost",
                "felt_cost": "the wallet and body pay a little",
            },
            "prototype_affordances": {
                "prototype_family": "scene_vignette|night_cost",
                "cluster_hint": "night cost",
                "suitable_for": ["night ritual"],
                "anti_drift_focus": ["Do not explain the process."],
            },
            "anchor_windows": {
                "opening": "A bright door, a night road, and the cost of cheap comfort.",
                "pivot": "",
                "closing": "A bright door, a night road, and the cost of cheap comfort.",
                "signature_lines": ["A bright door, a night road, and the cost of cheap comfort."],
            },
            "retrieval_handles": {
                "keywords": ["night", "door", "road"],
                "routing_text": "night cost residue",
            },
            "anti_patterns": ["Do not explain the process."],
        }
        document.metadata_json = metadata
        run = repository.create_stone_preprocess_run(
            session,
            project_id=project_id,
            status="running",
            summary_json={
                "stone_profile_total": 1,
                "stone_profile_completed": 1,
                "concurrency": 1,
                "current_stage": "family_induction_v3",
            },
        )
        run.started_at = utcnow()
        run.current_stage = "family_induction_v3"
        run.progress_percent = 100
        stale_run_id = run.id

    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{stale_run_id}").json()
    assert payload["status"] == "partial_failed"
    assert payload["analysis_ready"] is True
