from __future__ import annotations

import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from sqlalchemy import func, select

from app.analysis.facets import get_facets_for_mode
from app.analysis.stone_v2 import (
    build_short_text_clusters,
    build_stone_author_model_v2,
    build_stone_prototype_index_v2,
    normalize_stone_profile_v2,
)
from app.analysis.stone_agent import StoneAnalysisAgent
from app.llm.client import OpenAICompatibleClient
from app.models import TextChunk
from app.schemas import ChatCompletionResult, LLMToolCall, ToolRoundResult
from app.storage import repository
from app.writing.service import _normalize_review_plan_payload, _review_plan_has_valid_anchors


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


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _collect_sse_events(client, url: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    with client.stream("GET", url) as response:
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


def _parse_sse_events(text: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    current_event: str | None = None
    data_lines: list[str] = []
    for line in text.splitlines():
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
    if current_event is not None:
        payload = json.loads("\n".join(data_lines)) if data_lines else {}
        events.append((current_event, payload))
    return events


def _ensure_service_config(app, service_name: str, *, model: str) -> None:
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


def _install_stone_agent_mocks(monkeypatch) -> None:
    def fake_chat_completion_result(self, messages, **kwargs):
        del self, kwargs
        article_text = str(messages[-1].get("content") or "")
        article_text = article_text.split("文章原文：\n", 1)[-1].strip()
        payload = {
            "length_band": "short",
            "content_kernel": "stone profile summary",
            "surface_form": "scene_vignette",
            "voice_mask": {
                "person": "first",
                "address_target": "self",
                "distance": "回收",
                "self_position": "none",
            },
            "lexicon_markers": ["代价", "沉默"],
            "syntax_signature": {
                "cadence": "顿挫",
                "sentence_shape": "混合",
                "punctuation_habits": ["，", "。"],
            },
            "segment_map": ["opening", "residue"],
            "opening_move": "从场景或物件切入",
            "turning_move": "none",
            "closure_move": "把情绪收进意象残响",
            "motif_tags": ["夜", "桌面"],
            "stance_vector": {
                "target": "关系处境",
                "judgment": "悬置",
                "value_lens": "代价",
            },
            "emotion_curve": ["压低", "压着不说", "回落"],
            "rhetorical_devices": ["留白"],
            "prototype_family": "scene_vignette|从场景或物件切入|把情绪收进意象残响|回收|悬置|夜|桌面",
            "anchor_spans": {
                "opening": article_text,
                "pivot": "",
                "closing": article_text,
                "signature": [article_text],
            },
            "anti_patterns": ["不要写成解释性分析"],
        }
        return ChatCompletionResult(
            content=json.dumps(payload, ensure_ascii=False),
            model="demo-model",
            usage={"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
            request_url="https://example.com/v1/responses",
            request_payload={"messages": []},
        )

    def fake_tool_round(self, messages, tools, **kwargs):
        del self, tools, kwargs
        has_tool_result = any(message.get("role") == "tool" for message in messages)
        if not has_tool_result:
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
                tool_calls=[
                    LLMToolCall(
                        id="call-page-1",
                        name="list_article_profiles_page",
                        arguments_json=json.dumps({"offset": 0, "limit": 4}, ensure_ascii=False),
                        arguments={"offset": 0, "limit": 4},
                    )
                ],
                provider_response_id="resp-page-1",
            )

        tool_message = next(message for message in reversed(messages) if message.get("role") == "tool")
        page_payload = json.loads(tool_message["content"])
        first_profile = page_payload["profiles"][0]
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "stone facet summary",
                    "bullets": [
                        "profile paging returned enough direct article evidence",
                        "the facet stayed inside stone direct-article mode",
                    ],
                    "confidence": 0.81,
                    "fewshots": [
                        {
                            "document_id": first_profile["document_id"],
                            "document_title": first_profile["title"],
                            "situation": "profile paging",
                            "expression": "direct article evidence",
                            "quote": first_profile["content_summary"],
                            "reason": "supports the current stone facet",
                        }
                    ],
                    "conflicts": [],
                    "notes": "started from corpus overview and paged article profiles before summarizing",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 18, "completion_tokens": 8, "total_tokens": 26},
            tool_calls=[],
            provider_response_id="resp-page-2",
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)
    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)


def _extract_anchor_ids(text: str) -> list[str]:
    anchor_ids: list[str] = []
    for match in re.findall(r'"id"\s*:\s*"([^"]+)"', text):
        if match not in anchor_ids:
            anchor_ids.append(match)
    return anchor_ids


def _extract_topic(text: str) -> str:
    match = re.search(r"Topic:\s*(.+)", text)
    return match.group(1).strip() if match else "Topic"


def _extract_target_word_count(text: str) -> int:
    match = re.search(r"Target Word Count:\s*(\d+)", text)
    return int(match.group(1)) if match else 600


def _build_mock_article(topic: str, target_word_count: int) -> str:
    paragraphs = [
        f"{topic}先把声调压低，像夜里站台边那点风，不响，却一直贴着人。灯光照在玻璃和铁轨上，所有话都像被磨薄了一层，谁也不肯先把真正难受的部分拿出来。",
        "人站在原地的时候，最怕的不是冷，也不是等，而是已经知道要失去什么，却还得装作眼前这一切只是普通的耽搁。越是这样，动作越轻，连抬头和回身都像是在替自己留退路。",
        "等车迟迟不来，沉默就开始有了重量。关系里的亏欠、判断里的迟疑、那些白天压过去的话，一件件从衣角、指节、眼神里露出来。没有人解释，可代价已经摆在那儿。",
        "到最后也不用把话说尽。只要看见有人把手从口袋里拿出来，又重新放回去，就知道这一夜没有白等。真正留下来的，不是结论，而是那一点收不干净的余温。",
    ]
    text = "\n\n".join(paragraphs)
    while len(text) < max(220, int(target_word_count * 0.7)):
        text = f"{text}\n\n站台还是安静，风从边上擦过去，像把没有说完的话再往心里按了一次。"
    return text


def _install_writing_mocks(monkeypatch, *, capture: dict | None = None, fail_stage: str | None = None) -> None:
    def fake_chat_completion_result(self, messages, **kwargs):
        del self, kwargs
        system_text = str(messages[0].get("content") or "")
        user_text = str(messages[1].get("content") or "") if len(messages) > 1 else ""
        topic = _extract_topic(user_text)
        target_word_count = _extract_target_word_count(user_text)
        anchor_ids = _extract_anchor_ids(user_text)
        primary_anchor = anchor_ids[0] if anchor_ids else "profile:seed:passage:1"

        stage = "unknown"
        if "topic adapter" in system_text:
            stage = "topic_adapter"
            payload = {
                "author_angle": "先写动作和空气，再把代价慢慢显出来",
                "entry_scene": "雨夜站台，灯下玻璃",
                "felt_cost": "关系迟迟不说破的代价",
                "judgment_target": "关系处境",
                "value_lens": "代价",
                "desired_judgment": "悬置",
                "desired_distance": "回收",
                "motif_path": ["站台", "玻璃", "灯影"],
                "forbidden_drift": ["不要写成诊断报告", "不要写成写作说明"],
                "prototype_family_hints": ["scene_vignette"],
                "anchor_ids": anchor_ids[:4] or [primary_anchor],
            }
            content = json.dumps(payload, ensure_ascii=False)
        elif "blueprint composer" in system_text:
            stage = "blueprint"
            payload = {
                "paragraph_count": 3 if target_word_count <= 800 else 4,
                "shape_note": "起笔压低，中段显影，结尾回收。",
                "entry_move": "先让场景和动作落地",
                "development_move": "沿着代价和关系压力推进",
                "turning_device": "用反差拧转句意",
                "closure_residue": "不要把话说尽",
                "keep_terms": ["代价", "沉默", "回身"],
                "motif_obligations": ["站台", "玻璃"],
                "steps": ["起笔落地", "沿压力推进", "在结尾收回去"],
                "do_not_do": ["不要补段凑字", "不要解释分析过程"],
                "anchor_ids": anchor_ids[:4] or [primary_anchor],
            }
            content = json.dumps(payload, ensure_ascii=False)
        elif "prototype-grounded drafter" in system_text:
            stage = "draft"
            content = _build_mock_article(topic, target_word_count)
        elif "critic" in system_text and "Stone v2" in system_text:
            stage = "critic"
            keep_span = f"{topic}先把声调压低"
            payload = {
                "pass": True,
                "score": 0.87,
                "verdict": "approve",
                "anchor_ids": anchor_ids[:2] or [primary_anchor],
                "matched_signals": ["起笔克制", "结尾没有说尽"],
                "must_keep_spans": [keep_span],
                "line_edits": [],
                "redraft_reason": "",
                "risks": [],
            }
            content = json.dumps(payload, ensure_ascii=False)
        elif "whole-article redrafter" in system_text:
            stage = "redraft"
            content = _build_mock_article(topic, target_word_count)
        elif "line editor" in system_text:
            stage = "line_edit"
            content = _build_mock_article(topic, target_word_count)
        elif "structured writing guide from author analysis" in system_text:
            stage = "writing_guide"
            payload = {
                "author_snapshot": "克制、低声、把代价放回具体场景里。",
                "voice_dna": {
                    "tone_profile": "压低声调，少解释",
                    "signature_phrases": ["代价", "沉默", "回身"],
                    "distance_rules": ["先写动作，再露情绪"],
                },
                "sentence_mechanics": {
                    "cadence": ["句子偏短", "转折收着来"],
                    "closure_style": "收口留余味",
                },
                "structure_patterns": ["起笔压低", "中段显影", "结尾回收"],
                "motif_theme_bank": ["站台", "灯影", "玻璃"],
                "worldview_and_stance": ["不替任何人开脱", "让代价自己显形"],
                "emotional_tendencies": ["压低", "显影", "回落"],
                "nonclinical_psychodynamics": ["防卫", "迟疑"],
                "do_and_dont": {
                    "do": ["先写动作", "少解释"],
                    "dont": ["不要诊断", "不要提示词腔"],
                },
                "topic_translation_rules": ["把题目翻进关系压力和具体意象"],
                "word_count_strategies": {"medium": "四段推进"},
                "revision_rubric": ["先查句法，再查解释味"],
                "fewshot_anchors": [{"title": "seed", "quote": "夜里写字的人，总会回到代价、关系和沉默。"}],
                "external_slots": {
                    "clinical_profile": {"mental_state": "克制"},
                    "vulnerability_map": {"pain_points": ["失去"], "fragility_triggers": ["沉默"], "compensatory_moves": ["回收"]},
                },
            }
            content = json.dumps(payload, ensure_ascii=False)
        else:
            raise AssertionError(f"unexpected writing stage: {system_text}")

        if capture is not None:
            capture.setdefault("calls", []).append({"stage": stage, "system": system_text, "user": user_text})
        if fail_stage and stage == fail_stage:
            raise RuntimeError(f"{stage} failed")

        return ChatCompletionResult(
            content=content,
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
            request_url="https://example.com/v1/responses",
            request_payload={"messages": messages},
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)


def test_stone_writing_review_plan_repairs_unanchored_synthesis():
    reviews = [
        {
            "dimension_label": "声线",
            "pass": False,
            "anchor_ids": ["profile:seed:passage:1"],
            "violations": [
                {
                    "anchor_id": "profile:seed:passage:1",
                    "span": "突然喊出口号",
                    "issue": "声调过高",
                }
            ],
            "must_keep_spans": ["雨夜站台先把声调压低"],
            "must_rewrite_spans": [
                {
                    "anchor_id": "profile:seed:passage:1",
                    "span": "突然喊出口号",
                    "instruction": "压回低声叙述",
                }
            ],
            "revision_instructions": [
                {
                    "anchor_id": "profile:seed:passage:1",
                    "instruction": "删掉口号式判断",
                }
            ],
        }
    ]
    payload = {
        "summary": "模型给出了方向，但漏掉 anchor。",
        "must_keep_spans": ["雨夜站台先把声调压低"],
        "must_rewrite_spans": [{"span": "突然喊出口号", "instruction": "改得更低"}],
        "revision_instructions": ["删掉口号式判断"],
        "risk_watch": [],
    }

    plan = _normalize_review_plan_payload(payload, reviews)

    assert _review_plan_has_valid_anchors(plan, reviews)
    assert plan["must_rewrite_spans"][0]["anchor_id"] == "profile:seed:passage:1"
    assert plan["revision_instructions"][0]["anchor_id"] == "profile:seed:passage:1"


def test_stone_writing_review_plan_accepts_source_anchor_alias():
    reviews = [{"anchor_ids": ["profile:seed:passage:2"]}]
    payload = {
        "revision_instructions": [
            {
                "source_anchor_id": "profile:seed:passage:2",
                "instruction": "保留这个动作，不要解释。",
            }
        ]
    }

    plan = _normalize_review_plan_payload(payload, reviews)

    assert _review_plan_has_valid_anchors(plan, reviews)
    assert plan["revision_instructions"][0]["anchor_id"] == "profile:seed:passage:2"


def _create_preprocessed_stone_project(client, app, monkeypatch, *, name: str) -> str:
    create_response = client.post("/api/projects", json={"name": name, "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "Stone mode should analyze direct article evidence without touching chunk retrieval or embeddings.",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_stone_agent_mocks(monkeypatch)
    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_response.json()["id"])
    assert preprocess_payload["status"] == "completed"
    return project_id


def _count_document_chunks(app, document_id: str) -> int:
    with app.state.db.session() as session:
        return int(
            session.scalar(
                select(func.count()).select_from(TextChunk).where(TextChunk.document_id == document_id)
            )
            or 0
        )


def _seed_stone_analysis(app, project_id: str) -> None:
    facet_catalog = get_facets_for_mode("stone")
    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    storage_path = upload_dir / "seed-stone.txt"
    storage_path.write_text("夜里写字的人，总会回到代价、关系和沉默。", encoding="utf-8")
    raw_text = "夜里写字的人，总会回到代价、关系和沉默。"
    profile_v2 = normalize_stone_profile_v2(
        {
            "length_band": "micro",
            "content_kernel": "夜里写字的人总会回到代价、关系和沉默这些母题。",
            "surface_form": "aphorism",
            "voice_mask": {
                "person": "third",
                "address_target": "none",
                "distance": "回收",
                "self_position": "none",
            },
            "lexicon_markers": ["代价", "关系", "沉默"],
            "syntax_signature": {
                "cadence": "顿挫",
                "sentence_shape": "短句群",
                "punctuation_habits": ["，", "。"],
            },
            "segment_map": ["opening", "residue"],
            "opening_move": "从场景或物件切入",
            "turning_move": "none",
            "closure_move": "把情绪收进意象残响",
            "motif_tags": ["夜", "沉默"],
            "stance_vector": {
                "target": "关系处境",
                "judgment": "悬置",
                "value_lens": "代价",
            },
            "emotion_curve": ["压低", "压着不说", "回落"],
            "rhetorical_devices": ["留白"],
            "prototype_family": "aphorism|从场景或物件切入|把情绪收进意象残响|回收|悬置|夜|沉默",
            "anchor_spans": {
                "opening": raw_text,
                "pivot": "",
                "closing": raw_text,
                "signature": [raw_text],
            },
            "anti_patterns": ["不要写成解释性分析"],
        },
        article_text=raw_text,
        fallback_title="Seed Stone",
    )
    with app.state.db.session() as session:
        document = repository.create_document(
            session,
            id=str(uuid4()),
            project_id=project_id,
            filename="seed-stone.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="essay",
            title="Seed Stone",
            author_guess="Author",
            created_at_guess=None,
            raw_text=raw_text,
            clean_text=raw_text,
            language="zh",
            metadata_json={
                "stone_profile_v2": profile_v2,
            },
            ingest_status="ready",
            error_message=None,
            storage_path=str(storage_path),
        )
        seeded_profiles = [{"document_id": document.id, "title": "Seed Stone", **profile_v2}]
        short_clusters = build_short_text_clusters(seeded_profiles)
        author_model = build_stone_author_model_v2(
            project_name="Stone Writing",
            profiles=seeded_profiles,
            short_text_clusters=short_clusters,
        )
        prototype_index = build_stone_prototype_index_v2(
            project_name="Stone Writing",
            profiles=seeded_profiles,
            documents=[{"document_id": document.id, "title": "Seed Stone", "text": raw_text, "clean_text": raw_text, "raw_text": raw_text}],
        )
        preprocess_run = repository.create_stone_preprocess_run(
            session,
            project_id=project_id,
            status="completed",
            summary_json={
                "stone_profile_total": 1,
                "stone_profile_completed": 1,
                "stone_author_model_v2_draft_id": None,
                "stone_prototype_index_v2_draft_id": None,
            },
        )
        preprocess_run.started_at = preprocess_run.created_at
        preprocess_run.finished_at = preprocess_run.created_at
        author_draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_author_model_v2",
            markdown_text="# Stone Author Model V2",
            json_payload=author_model,
            prompt_text=json.dumps(author_model, ensure_ascii=False, indent=2),
            notes="seed",
        )
        prototype_draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_prototype_index_v2",
            markdown_text="# Stone Prototype Index V2",
            json_payload=prototype_index,
            prompt_text=json.dumps(prototype_index, ensure_ascii=False, indent=2),
            notes="seed",
        )
        preprocess_run.summary_json = {
            "stone_profile_total": 1,
            "stone_profile_completed": 1,
            "stone_author_model_v2_draft_id": author_draft.id,
            "stone_prototype_index_v2_draft_id": prototype_draft.id,
        }
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "facet_keys": [facet.key for facet in facet_catalog],
                "facet_labels": [facet.label for facet in facet_catalog],
                "target_role": "Author",
                "analysis_context": "stone corpus",
            },
        )
        for facet in facet_catalog:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="completed",
                confidence=0.84,
                findings_json={
                    "label": facet.label,
                    "summary": f"{facet.label} summary",
                    "bullets": [f"{facet.key} bullet 1", f"{facet.key} bullet 2"],
                },
                evidence_json=[],
                conflicts_json=[],
                error_message=None,
            )


def _wait_for_stone_preprocess(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    return payload


def test_stone_asset_stream_generates_distinct_v2_baselines(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Stream", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)

    def generate(kind: str) -> dict:
        response = client.post(f"/api/projects/{project_id}/assets/generate/stream", json={"asset_kind": kind})
        assert response.status_code == 200
        events = _parse_sse_events(response.text)
        assert not [payload for name, payload in events if name == "error"]
        done_events = [payload for name, payload in events if name == "done"]
        assert done_events
        return done_events[-1]["draft"]

    author_draft = generate("stone_author_model_v2")
    author_payload = author_draft["json_payload"]
    assert author_draft["asset_kind"] == "stone_author_model_v2"
    assert author_payload["asset_kind"] == "stone_author_model_v2"
    assert "style_invariants" in author_payload
    assert "documents" not in author_payload
    assert "# Stone Author Model V2" in author_draft["markdown_text"]
    assert "用户画像报告" not in author_draft["markdown_text"]

    prototype_draft = generate("stone_prototype_index_v2")
    prototype_payload = prototype_draft["json_payload"]
    assert prototype_draft["asset_kind"] == "stone_prototype_index_v2"
    assert prototype_payload["asset_kind"] == "stone_prototype_index_v2"
    assert "style_invariants" not in prototype_payload
    assert prototype_payload["documents"]
    assert prototype_payload["documents"][0]["windows"]["opening"]
    assert "# Stone Prototype Index V2" in prototype_draft["markdown_text"]
    assert "用户画像报告" not in prototype_draft["markdown_text"]


def test_stone_asset_save_and_publish_reject_invalid_v2_payload(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Invalid Asset", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    bad_payload = {
        "headline": "不是 Stone V2",
        "executive_summary": "这是用户画像报告形状的错误 payload。",
        "target_role": "Author",
        "source_context": "stone",
    }
    with app.state.db.session() as session:
        draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_author_model_v2",
            markdown_text="# Stone Author Model V2\n\n用户画像报告",
            json_payload=bad_payload,
            prompt_text=json.dumps(bad_payload, ensure_ascii=False),
            notes="bad",
        )
        draft_id = draft.id

    save_response = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/save",
        json={
            "asset_kind": "stone_author_model_v2",
            "markdown_text": "# Stone Author Model V2\n\n用户画像报告",
            "json_payload": bad_payload,
            "prompt_text": "{}",
            "notes": "bad",
        },
    )
    assert save_response.status_code == 400
    assert "asset_kind" in save_response.json()["detail"]

    publish_response = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "stone_author_model_v2"},
    )
    assert publish_response.status_code == 400
    assert "asset_kind" in publish_response.json()["detail"]


def test_stone_mode_text_document_api_and_analysis_flow(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Project", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    home = client.get("/")
    assert home.status_code == 200
    assert 'value="stone"' in home.text

    project_page = client.get(f"/projects/{project_id}")
    assert project_page.status_code == 200
    assert 'id="upload-dropzone"' in project_page.text

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。",
            "source_type": "essay",
            "user_note": "first import",
        },
    )
    assert create_doc.status_code == 200
    document_payload = create_doc.json()
    document_id = document_payload["id"]
    assert document_payload["request_status"] == "ok"
    assert document_payload["source_type"] == "essay"
    assert document_payload["status"] in {"queued", "parsing", "chunking", "storing", "completed"}

    document_detail = _wait_for_ready(client, project_id, document_id)
    assert document_detail["ingest_status"] == "ready"
    assert document_detail["metadata_json"]["user_note"] == "first import"
    assert document_detail["metadata_json"]["stone_text_entry"] is True
    assert _count_document_chunks(app, document_id) == 0

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_run_id = preprocess_response.json()["id"]
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_run_id)
    assert preprocess_payload["status"] == "completed"
    detail_profile_v2 = preprocess_payload["documents"][0]["stone_profile_v2"]
    assert detail_profile_v2["length_band"] in {"micro", "short", "medium", "long"}
    assert detail_profile_v2["opening_move"]
    assert detail_profile_v2["closure_move"]
    assert detail_profile_v2["prototype_family"]
    assert detail_profile_v2["content_kernel"]
    assert detail_profile_v2["anchor_spans"]["signature"]

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone corpus", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    run_id = run_response.json()["id"]

    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"
    stone_keys = [facet.key for facet in get_facets_for_mode("stone")]
    assert analysis_payload["summary"]["facet_keys"] == stone_keys
    assert analysis_payload["summary"]["chunk_count"] == 0
    assert len(analysis_payload["facets"]) == len(stone_keys)

    refreshed_docs = client.get(f"/api/projects/{project_id}/documents").json()["documents"]
    profiled_doc = next(item for item in refreshed_docs if item["id"] == document_id)
    stone_profile_v2 = profiled_doc["metadata_json"]["stone_profile_v2"]
    assert "stone_profile" not in profiled_doc["metadata_json"]
    assert stone_profile_v2["content_kernel"] == "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。"
    assert stone_profile_v2["length_band"] == "micro"
    assert stone_profile_v2["anchor_spans"]["signature"] == [
        "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。"
    ]


def test_stone_preprocess_form_route_redirects_and_completes(client):
    create_response = client.post("/api/projects", json={"name": "Stone Form", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "这是一段足够长的石川文风样本文本，用来验证表单入口触发预分析时会正确创建并完成后台任务。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    response = client.post(
        f"/projects/{project_id}/preprocess/run",
        data={"concurrency": "1"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    location = response.headers["location"]
    parsed = urlparse(location)
    assert parsed.path == f"/projects/{project_id}/preprocess"
    run_id = parse_qs(parsed.query)["run_id"][0]

    preprocess_payload = _wait_for_stone_preprocess(client, project_id, run_id)
    assert preprocess_payload["status"] == "completed"


def test_stone_preprocess_reuses_failed_run(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Resume", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "这段文本用于验证失败后的预分析 run 会被复用，而不是重新生成一条新的 run 记录。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    with app.state.db.session() as session:
        run = repository.create_stone_preprocess_run(
            session,
            project_id=project_id,
            status="failed",
            summary_json={
                "concurrency": 1,
                "stone_profile_total": 1,
                "stone_profile_completed": 0,
            },
        )
        run.progress_percent = 47
        run.current_stage = "Failed"
        run.error_message = "previous failure"
        failed_run_id = run.id

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    assert preprocess_response.json()["id"] == failed_run_id

    preprocess_payload = _wait_for_stone_preprocess(client, project_id, failed_run_id)
    assert preprocess_payload["status"] == "completed"
    assert preprocess_payload["progress_percent"] == 100


def test_stone_json_upload_splits_articles_and_filters_noise(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone JSON", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    payload = {
        "name": "Stone Feed",
        "messages": [
            {"id": 1, "type": "message", "text": "太短了，不算文章。"},
            {
                "id": 2,
                "type": "message",
                "text": "This is a long English only paragraph with more than fifty characters, but it should still be filtered out.",
            },
            {
                "id": 3,
                "type": "message",
                "text": "第一篇文章在这里展开。它有足够长的中文内容，会被系统识别成一篇完整文章，而且不需要再拆分成 chunks。",
            },
            {
                "id": 4,
                "type": "message",
                "text": [
                    "第二篇文章来自富文本数组，它同样有足够长的中文正文，",
                    {"type": "hashtag", "text": "#忽略这个标签"},
                    "并且应该被导入成一篇独立文章，而不是保留成原始 JSON 消息。",
                ],
            },
        ],
    }
    response = client.post(
        f"/api/projects/{project_id}/documents",
        files=[
            (
                "files",
                ("articles.json", json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json"),
            )
        ],
    )
    assert response.status_code == 200
    response_payload = response.json()
    assert len(response_payload["documents"]) == 2
    assert len(response_payload["tasks"]) == 2

    document_ids = [item["id"] for item in response_payload["documents"]]
    for document_id in document_ids:
        ready_document = _wait_for_ready(client, project_id, document_id)
        assert ready_document["ingest_status"] == "ready"
        assert ready_document["metadata_json"]["stone_json_import"]["source_filename"] == "articles.json"
        assert _count_document_chunks(app, document_id) == 0


def test_stone_analysis_agent_records_raw_text_tool_usage(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Agent", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    _ensure_service_config(app, "chat_service", model="demo-model")

    def fake_chat_completion_result(self, messages, **kwargs):
        payload = {
            "length_band": "short",
            "content_kernel": "作者习惯先压低声调，再把情绪往句子深处回收。",
            "surface_form": "confession",
            "voice_mask": {
                "person": "first",
                "address_target": "self",
                "distance": "回收",
                "self_position": "自嘲",
            },
            "lexicon_markers": ["声调", "句子深处"],
            "syntax_signature": {
                "cadence": "顿挫",
                "sentence_shape": "短句群",
                "punctuation_habits": ["，", "。"],
            },
            "segment_map": ["opening", "residue"],
            "opening_move": "先压低声调",
            "turning_move": "none",
            "closure_move": "留一个没有完全关上的收口",
            "motif_tags": ["声调", "收口"],
            "stance_vector": {
                "target": "写法",
                "judgment": "悬置",
                "value_lens": "代价",
            },
            "emotion_curve": ["压低", "延迟释放", "回落"],
            "rhetorical_devices": ["留白"],
            "prototype_family": "confession|先压低声调|留一个没有完全关上的收口|回收|悬置|声调|收口",
            "anchor_spans": {
                "opening": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
                "pivot": "",
                "closing": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
                "signature": ["作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。"],
            },
            "anti_patterns": ["不要写成说明书"],
        }
        return ChatCompletionResult(
            content=json.dumps(payload, ensure_ascii=False),
            model="demo-model",
            usage={"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
            request_url="https://example.com/v1/responses",
            request_payload={"messages": messages},
        )

    def fake_tool_round(self, messages, tools, **kwargs):
        has_tool_result = any(message.get("role") == "tool" for message in messages)
        if not has_tool_result:
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
                tool_calls=[
                    LLMToolCall(
                        id="call-1",
                        name="read_article_text",
                        arguments_json=json.dumps({"document_id": document_id, "max_chars": 300}, ensure_ascii=False),
                        arguments={"document_id": document_id, "max_chars": 300},
                    )
                ],
                provider_response_id="resp-1",
            )
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "作者在这个维度上依赖压低声调、延迟释放和半开式收束。",
                    "bullets": [
                        "经常先把声调压低，再把核心情绪往后放。",
                        "句尾常常留一个没有完全封死的收口。",
                    ],
                    "confidence": 0.88,
                    "fewshots": [
                        {
                            "document_id": document_id,
                            "situation": "为了验证句尾收束方式，回读原文",
                            "expression": "先压低声调，再把情绪回收",
                            "quote": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
                            "reason": "直接支持当前 facet 的判断",
                        }
                    ],
                    "conflicts": [],
                    "notes": "基于文章画像总结，并用原文回读做了核对。",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
            tool_calls=[],
            provider_response_id="resp-2",
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)
    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_run_id = preprocess_response.json()["id"]
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_run_id)
    assert preprocess_payload["status"] == "completed"
    assert preprocess_payload["prompt_tokens"] == 12
    assert preprocess_payload["completion_tokens"] == 8
    assert preprocess_payload["total_tokens"] == 20
    assert preprocess_payload["documents"][0]["stone_profile_v2"]["content_kernel"] == "作者习惯先压低声调，再把情绪往句子深处回收。"
    assert preprocess_payload["documents"][0]["stone_profile_v2"]["surface_form"] == "confession"
    assert preprocess_payload["documents"][0]["stone_profile_v2"]["anchor_spans"]["signature"] == [
        "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。"
    ]

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "tool-augmented stone run", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    run_id = run_response.json()["id"]

    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"
    first_facet = analysis_payload["facets"][0]
    retrieval_trace = first_facet["findings"]["retrieval_trace"]
    assert retrieval_trace["mode"] == "stone_agent"
    assert retrieval_trace["queried_document_ids"] == [document_id]
    assert retrieval_trace["tool_calls"]

    refreshed_docs = client.get(f"/api/projects/{project_id}/documents").json()["documents"]
    assert "stone_profile" not in refreshed_docs[0]["metadata_json"]
    assert refreshed_docs[0]["metadata_json"]["stone_profile_v2"]["content_kernel"] == "作者习惯先压低声调，再把情绪往句子深处回收。"
    assert refreshed_docs[0]["metadata_json"]["stone_profile_v2"]["surface_form"] == "confession"


def test_stone_analysis_skips_embeddings_when_embedding_service_is_configured(client, app, monkeypatch):
    project_id = _create_preprocessed_stone_project(client, app, monkeypatch, name="Stone No Embedding")
    _ensure_service_config(app, "embedding_service", model="demo-embedding")

    def fail_embeddings(self, inputs, *, model=None, timeout=None):
        del self, inputs, model, timeout
        raise AssertionError("Stone analysis should not call embeddings.")

    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fail_embeddings)

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone direct article mode", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    analysis_payload = _wait_for_analysis(client, project_id, run_response.json()["id"])

    assert analysis_payload["status"] == "completed"
    assert all(facet["findings"]["retrieval_trace"]["mode"] == "stone_agent" for facet in analysis_payload["facets"])
    assert all(facet["findings"]["retrieval_trace"]["embedding_attempted"] is False for facet in analysis_payload["facets"])
    assert all(facet["findings"]["retrieval_trace"]["embedding_api_called"] is False for facet in analysis_payload["facets"])
    assert all(
        facet["findings"]["retrieval_trace"]["embedding_skip_reason"] == "stone_direct_article_mode"
        for facet in analysis_payload["facets"]
    )


def test_stone_facet_rerun_uses_stone_agent_even_when_embeddings_fail(client, app, monkeypatch):
    project_id = _create_preprocessed_stone_project(client, app, monkeypatch, name="Stone Rerun No Embedding")
    _ensure_service_config(app, "embedding_service", model="demo-embedding")

    def fail_embeddings(self, inputs, *, model=None, timeout=None):
        del self, inputs, model, timeout
        raise AssertionError("Stone rerun should not call embeddings.")

    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fail_embeddings)

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone rerun baseline", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    baseline_payload = _wait_for_analysis(client, project_id, run_response.json()["id"])
    assert baseline_payload["status"] == "completed"

    facet_key = get_facets_for_mode("stone")[0].key
    rerun_response = client.post(f"/api/projects/{project_id}/analysis/{facet_key}/rerun")
    assert rerun_response.status_code == 200
    analysis_payload = _wait_for_analysis(client, project_id, rerun_response.json()["id"])

    facet_payload = next(item for item in analysis_payload["facets"] if item["facet_key"] == facet_key)
    retrieval_trace = facet_payload["findings"]["retrieval_trace"]
    assert analysis_payload["status"] == "completed"
    assert facet_payload["status"] == "completed"
    assert retrieval_trace["mode"] == "stone_agent"
    assert retrieval_trace["embedding_attempted"] is False
    assert retrieval_trace["embedding_api_called"] is False
    assert retrieval_trace["tool_calls"]
    assert any(item["tool"] == "list_article_profiles_page" for item in retrieval_trace["tool_calls"])


def test_stone_facet_rerun_does_not_use_generic_retrieval_service(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Rerun Direct Agent", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_stone_agent_mocks(monkeypatch)

    def fail_search(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Stone rerun should not use RetrievalService.search.")

    monkeypatch.setattr("app.retrieval.service.RetrievalService.search", fail_search)

    facet_key = get_facets_for_mode("stone")[0].key
    rerun_response = client.post(f"/api/projects/{project_id}/analysis/{facet_key}/rerun")
    assert rerun_response.status_code == 200
    analysis_payload = _wait_for_analysis(client, project_id, rerun_response.json()["id"])

    facet_payload = next(item for item in analysis_payload["facets"] if item["facet_key"] == facet_key)
    assert analysis_payload["status"] == "completed"
    assert facet_payload["status"] == "completed"
    assert facet_payload["findings"]["retrieval_trace"]["mode"] == "stone_agent"


def test_normalize_stone_profile_v2_supports_raw_kernel_sentinel_and_short_text_anchor_fill():
    article_text = "就这一句，但我今天确实有点不舒服。"
    profile = normalize_stone_profile_v2(
        {
            "content_kernel": "raw",
            "surface_form": "confession",
            "length_band": "micro",
            "emotion_curve": ["轻微烦闷"],
        },
        article_text=article_text,
        fallback_title="短文测试",
    )
    assert profile["content_kernel"] == article_text
    assert profile["surface_form"] == "confession"
    assert profile["length_band"] == "micro"
    assert profile["anchor_spans"]["opening"] == article_text
    assert profile["anchor_spans"]["signature"] == [article_text]


def test_stone_project_detail_exposes_analysis_concurrency_input(client):
    create_response = client.post("/api/projects", json={"name": "Stone Console", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    response = client.get(f"/projects/{project_id}")
    assert response.status_code == 200
    assert 'textarea name="analysis_context"' in response.text
    assert 'input type="number" name="concurrency"' in response.text
    assert 'type="hidden" name="concurrency"' not in response.text


def test_stone_analysis_agent_starts_from_corpus_overview_and_pages_profiles(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Paging", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _ensure_service_config(app, "chat_service", model="demo-model")

    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    with app.state.db.session() as session:
        for index in range(30):
            doc_id = str(uuid4())
            text = f"第{index}篇文章独有标记_{index}，作者在这里反复写深夜、疲惫和关系。"
            storage_path = upload_dir / f"paging-{index}.txt"
            storage_path.write_text(text, encoding="utf-8")
            emotion = "低落" if index < 18 else "轻松"
            surface_form = "rant" if index < 18 else "scene_vignette"
            repository.create_document(
                session,
                id=doc_id,
                project_id=project_id,
                filename=f"paging-{index}.txt",
                mime_type="text/plain",
                extension=".txt",
                source_type="essay",
                title=f"第{index}篇文章",
                author_guess="Author",
                created_at_guess=None,
                raw_text=text,
                clean_text=text,
                language="zh",
                metadata_json={
                    "stone_profile_v2": normalize_stone_profile_v2(
                        {
                            "length_band": "micro",
                            "content_kernel": text,
                            "surface_form": surface_form,
                            "voice_mask": {
                                "person": "first",
                                "address_target": "self",
                                "distance": "回收",
                                "self_position": "none",
                            },
                            "lexicon_markers": ["深夜", "疲惫", "关系"],
                            "syntax_signature": {
                                "cadence": "顿挫",
                                "sentence_shape": "短句群",
                                "punctuation_habits": ["，", "。"],
                            },
                            "segment_map": ["opening", "residue"],
                            "opening_move": "直接落句",
                            "turning_move": "none",
                            "closure_move": "回收到疲惫和关系压力",
                            "motif_tags": ["深夜", "关系"],
                            "stance_vector": {
                                "target": "关系处境",
                                "judgment": "悬置",
                                "value_lens": "代价",
                            },
                            "emotion_curve": [emotion],
                            "rhetorical_devices": ["重复"],
                            "prototype_family": f"{surface_form}|直接落句|回收到疲惫和关系压力|回收|悬置|深夜|关系",
                            "anchor_spans": {
                                "opening": text,
                                "pivot": "",
                                "closing": text,
                                "signature": [text],
                            },
                            "anti_patterns": ["不要写成分析报告"],
                        },
                        article_text=text,
                        fallback_title=f"第{index}篇文章",
                    )
                },
                ingest_status="ready",
                error_message=None,
                storage_path=str(storage_path),
            )
        session.commit()

    captured: dict[str, object] = {}

    def fake_tool_round(self, messages, tools, **kwargs):
        has_tool_result = any(message.get("role") == "tool" for message in messages)
        if not has_tool_result:
            prompt_text = "\n\n".join(
                str(message.get("content") or "")
                for message in messages
                if message.get("role") in {"system", "user"}
            )
            captured["first_prompt"] = prompt_text
            assert "作品总数：30" in prompt_text
            assert "性质分布" in prompt_text
            assert "情绪分布" in prompt_text
            assert "独有标记_29" not in prompt_text
            assert "第29篇文章独有标记_29" not in prompt_text
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 20, "completion_tokens": 6, "total_tokens": 26},
                tool_calls=[
                    LLMToolCall(
                        id="call-page-1",
                        name="list_article_profiles_page",
                        arguments_json=json.dumps({"offset": 0, "limit": 6, "emotion_label": "低落"}, ensure_ascii=False),
                        arguments={"offset": 0, "limit": 6, "emotion_label": "低落"},
                    )
                ],
                provider_response_id="resp-page-1",
            )

        tool_message = next(message for message in messages if message.get("role") == "tool")
        page_payload = json.loads(tool_message["content"])
        captured["page_payload"] = page_payload
        assert page_payload["returned"] == 6
        assert page_payload["total_profiles"] == 18
        assert page_payload["has_more"] is True
        assert 0 <= int(page_payload["remaining_profile_budget"]) < page_payload["total_profiles"]
        serialized_page = json.dumps(page_payload, ensure_ascii=False)
        assert "独有标记_29" not in serialized_page

        first_profile = page_payload["profiles"][0]
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "作者在这一维度上主要表现为持续低落、自我消耗和深夜叙述。",
                    "bullets": [
                        "前几个分页样本集中指向疲惫、深夜和关系压力。",
                        "低落情绪在抽样页面中明显高频出现。",
                    ],
                    "confidence": 0.81,
                    "fewshots": [
                        {
                            "document_id": first_profile["document_id"],
                            "document_title": first_profile["title"],
                            "situation": "分页抽样时观察作者低落表达",
                            "expression": "把疲惫和关系压力直接写进短文",
                            "quote": first_profile["content_summary"],
                            "reason": "足以支撑当前 facet 的初步判断",
                        }
                    ],
                    "conflicts": [],
                    "notes": "先基于总体分布筛选低落样本，再按分页读取文章画像。",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 18, "completion_tokens": 8, "total_tokens": 26},
            tool_calls=[],
            provider_response_id="resp-page-2",
        )

    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        assert project is not None
        chat_config = repository.get_service_config(session, "chat_service")
        assert chat_config is not None
        facet = get_facets_for_mode("stone")[0]
        agent = StoneAnalysisAgent(session, project, llm_config=chat_config)
        result = agent.analyze_facet(
            facet,
            target_role="Author",
            analysis_context="分页读取测试",
        )

    assert "作品总数：30" in str(captured["first_prompt"])
    assert result.retrieval_trace["tool_calls"][0]["tool"] == "list_article_profiles_page"
    assert result.retrieval_trace["corpus_overview"]["total_documents"] == 30
    assert result.payload["fewshots"][0]["document_id"] == captured["page_payload"]["profiles"][0]["document_id"]


def test_stone_profile_paging_ignores_none_like_filter_strings(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone None Filters", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    with app.state.db.session() as session:
        for index in range(6):
            doc_id = str(uuid4())
            text = f"Sample article {index} repeats a frustrated voice and plain spoken complaints."
            storage_path = upload_dir / f"none-filter-{index}.txt"
            storage_path.write_text(text, encoding="utf-8")
            surface_form = "rant" if index < 5 else "aphorism"
            repository.create_document(
                session,
                id=doc_id,
                project_id=project_id,
                filename=f"none-filter-{index}.txt",
                mime_type="text/plain",
                extension=".txt",
                source_type="essay",
                title=f"Sample {index}",
                author_guess="Author",
                created_at_guess=None,
                raw_text=text,
                clean_text=text,
                language="en",
                metadata_json={
                    "stone_profile_v2": normalize_stone_profile_v2(
                        {
                            "length_band": "short",
                            "content_kernel": text,
                            "surface_form": surface_form,
                            "voice_mask": {
                                "person": "first",
                                "address_target": "self",
                                "distance": "回收",
                                "self_position": "none",
                            },
                            "lexicon_markers": ["frustrated", "complaints"],
                            "syntax_signature": {
                                "cadence": "顿挫",
                                "sentence_shape": "混合",
                                "punctuation_habits": ["."],
                            },
                            "segment_map": ["opening", "residue"],
                            "opening_move": "plain-spoken complaint",
                            "turning_move": "none",
                            "closure_move": "leave the frustration hanging",
                            "motif_tags": ["complaint"],
                            "stance_vector": {
                                "target": "daily hassle",
                                "judgment": "厌恶",
                                "value_lens": "代价",
                            },
                            "emotion_curve": ["grim"],
                            "rhetorical_devices": ["plain-spoken complaint"],
                            "prototype_family": f"{surface_form}|plain-spoken complaint|leave the frustration hanging|回收|厌恶|complaint",
                            "anchor_spans": {
                                "opening": text,
                                "pivot": "",
                                "closing": text,
                                "signature": [text],
                            },
                            "anti_patterns": ["do not explain"],
                        },
                        article_text=text,
                        fallback_title=f"Sample {index}",
                    )
                },
                ingest_status="ready",
                error_message=None,
                storage_path=str(storage_path),
            )
        session.commit()

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        assert project is not None
        agent = StoneAnalysisAgent(session, project, llm_config=None)
        documents = agent._load_ready_documents()
        profiles = [agent._profile_snapshot(document) for document in documents]
        corpus_overview = agent._build_corpus_overview(profiles)
        payload, state = agent._execute_tool(
            "list_article_profiles_page",
            {
                "offset": 0,
                "limit": 4,
                "query": "None",
                "content_type": "rant",
                "emotion_label": "None",
                "length_label": "None",
            },
            documents=documents,
            profiles=profiles,
            corpus_overview=corpus_overview,
            tool_state={
                "profile_reads": 0,
                "profile_budget": agent._profile_read_budget(len(profiles)),
                "text_chars_read": 0,
            },
        )

    assert payload["returned"] == 4
    assert payload["total_profiles"] == 5
    assert payload["filters"] == {
        "query": None,
        "content_type": "rant",
        "emotion_label": None,
        "length_label": None,
    }
    assert len(state["document_ids"]) == 4
    assert all(item["content_type"] == "rant" for item in payload["profiles"])


def test_stone_writing_ignores_invalid_latest_v2_asset_drafts(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Bad Draft Recovery", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)
    bad_payload = {
        "headline": "错误的用户画像报告",
        "executive_summary": "这个 payload 没有 Stone V2 source anchors。",
        "target_role": "Author",
        "source_context": "stone",
    }
    with app.state.db.session() as session:
        bad_author = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_author_model_v2",
            markdown_text="# Stone Bad\n\n用户画像报告",
            json_payload=bad_payload,
            prompt_text=json.dumps(bad_payload, ensure_ascii=False),
            notes="bad author",
        )
        bad_prototype = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_prototype_index_v2",
            markdown_text="# Stone Bad\n\n用户画像报告",
            json_payload=bad_payload,
            prompt_text=json.dumps(bad_payload, ensure_ascii=False),
            notes="bad prototype",
        )
        bad_ids = {bad_author.id, bad_prototype.id}

    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_writing_mocks(monkeypatch)
    session_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Recovery Session"},
    ).json()
    session_id = session_payload["id"]
    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "写我吃肯德基的故事", "target_word_count": 400, "extra_requirements": "模仿作者"},
    ).json()
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{message_payload['stream_id']}",
    )
    assert not [payload for name, payload in events if name == "error"]
    assert [payload for name, payload in events if name == "done"]

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    latest_turn = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"][-1]
    trace = latest_turn["trace"]
    assert trace["generation_packet"]["baseline"]["source_anchor_count"] > 0
    assert trace["topic_adapter"]["anchor_ids"]
    assert trace["prototype_selection"]["selected_windows"]
    with app.state.db.session() as session:
        latest_author = repository.get_latest_asset_draft(session, project_id, asset_kind="stone_author_model_v2")
        latest_prototype = repository.get_latest_asset_draft(session, project_id, asset_kind="stone_prototype_index_v2")
        assert latest_author.id not in bad_ids
        assert latest_author.json_payload["asset_kind"] == "stone_author_model_v2"
        assert latest_prototype.id not in bad_ids
        assert latest_prototype.json_payload["asset_kind"] == "stone_prototype_index_v2"
        assert latest_prototype.json_payload["documents"]


def test_stone_writing_workspace_uses_latest_analysis_even_if_writing_guide_exists(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_writing_mocks(monkeypatch)

    assets_page = client.get(f"/projects/{project_id}/assets")
    assert assets_page.status_code == 200
    assert "Stone Author Model V2" in assets_page.text
    assert "Stone Prototype Index V2" in assets_page.text
    assert "Claude Code Skill" not in assets_page.text

    writing_page = client.get(f"/projects/{project_id}/writing")
    assert writing_page.status_code == 200
    assert "analysis" in writing_page.text
    assert "writing_guide" not in writing_page.text
    assert "Author的石生产线" in writing_page.text
    assert "theme-ambient--writing" in writing_page.text
    assert "data-stage-feed-list" not in writing_page.text
    assert "preprocess-context" not in writing_page.text

    session_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Draft Session"},
    ).json()
    session_id = session_payload["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "Rainy Night Station", "target_word_count": 600, "extra_requirements": "keep it restrained"},
    ).json()
    stream_id = message_payload["stream_id"]
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    stage_events = [payload for name, payload in events if name == "stage"]
    done_events = [payload for name, payload in events if name == "done"]
    assert [payload["message_kind"] for payload in stage_events[:3]] == [
        "topic_adapter",
        "prototype_selector",
        "blueprint",
    ]
    assert stage_events[3]["message_kind"] == "draft"
    assert [payload["message_kind"] for payload in stage_events[4:-1]] == ["critic"] * 3
    assert all(payload["actor_role"] == "critic" for payload in stage_events[4:-1])
    assert done_events[-1]["message_kind"] == "final"
    assert done_events[-1]["actor_role"] == "writer"

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assert detail_payload["timeline_turn_count"] == 9
    assert detail_payload["turns"][0]["role"] == "user"
    assert [turn["message_kind"] for turn in detail_payload["turns"][1:4]] == [
        "topic_adapter",
        "prototype_selector",
        "blueprint",
    ]
    assert detail_payload["turns"][-2]["message_kind"] == "critic"
    assert detail_payload["turns"][-1]["message_kind"] == "final"
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    assert assistant_turns
    latest_turn = assistant_turns[-1]
    assert latest_turn["trace"]["baseline_source"] == "stone_v2_baseline"
    assert latest_turn["trace"]["analysis_facets"] == []
    assert latest_turn["trace"]["degraded_mode"] is False
    assert latest_turn["trace"]["generation_packet"]["baseline"]["author_model_ready"] is True
    assert latest_turn["trace"]["generation_packet"]["baseline"]["prototype_index_ready"] is True
    assert latest_turn["trace"]["topic_adapter"]["anchor_ids"]
    assert latest_turn["trace"]["prototype_selection"]["selected_documents"]
    assert latest_turn["trace"]["blueprint"]["anchor_ids"]
    assert latest_turn["trace"]["anchor_ids"]
    assert len(latest_turn["trace"]["critics"]) == 3
    assert all(critic["anchor_ids"] for critic in latest_turn["trace"]["critics"])
    assert latest_turn["trace"]["final_assessment"]
    assert "如果沿着" not in latest_turn["content"]
    assert "分析里最能充当锚点" not in latest_turn["content"]
    assert "这次修订最重要" not in latest_turn["content"]

    draft_response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "stone_author_model_v2"})
    assert draft_response.status_code == 200
    draft_payload = draft_response.json()
    draft_id = draft_payload["id"]
    assert draft_payload["asset_kind"] == "stone_author_model_v2"

    publish_response = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "stone_author_model_v2"},
    )
    assert publish_response.status_code == 200
    assert publish_response.json()["asset_kind"] == "stone_author_model_v2"

    published_session = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Published Session"},
    ).json()
    published_session_id = published_session["id"]
    published_message = client.post(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/messages",
        json={"topic": "Window Before Dawn", "target_word_count": 550, "extra_requirements": "leave some aftertaste"},
    ).json()
    published_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/streams/{published_message['stream_id']}",
    )
    assert [payload for name, payload in published_events if name == "done"][-1]["message_kind"] == "final"

    published_detail = client.get(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}"
    ).json()
    published_assistant_turns = [turn for turn in published_detail["turns"] if turn["role"] == "assistant"]
    assert published_assistant_turns[-1]["trace"]["baseline_source"] == "stone_v2_baseline"
    assert len(published_assistant_turns[-1]["trace"]["critics"]) == 3
    assert published_assistant_turns[-1]["trace"]["generation_packet"]["baseline"]["author_model_ready"] is True


def test_stone_writing_message_parser_accepts_natural_language_payload(client, app, monkeypatch):
    project_id = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"}).json()["id"]
    _seed_stone_analysis(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_writing_mocks(monkeypatch)

    session_id = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Natural Session"},
    ).json()["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "写一篇雨夜车站，800字，克制一点"},
    ).json()
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{message_payload['stream_id']}",
    )
    assert [payload["message_kind"] for name, payload in events if name == "stage"][:3] == [
        "topic_adapter",
        "prototype_selector",
        "blueprint",
    ]

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assert detail_payload["turns"][0]["content"] == "写一篇雨夜车站，800字，克制一点"


def test_stone_writing_message_parser_rejects_missing_word_count(client, app):
    project_id = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"}).json()["id"]
    _seed_stone_analysis(app, project_id)

    session_id = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Parser Session"},
    ).json()["id"]

    response = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"message": "写一篇雨夜车站，克制一点"},
    )
    assert response.status_code == 400
    assert "800字" in response.json()["detail"]


def test_stone_writing_pipeline_fails_without_silent_fallback_when_llm_stage_errors(client, app, monkeypatch):
    project_id = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"}).json()["id"]
    _seed_stone_analysis(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")
    _install_writing_mocks(monkeypatch, fail_stage="draft")

    session_id = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Failure Session"},
    ).json()["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "Rainy Night Station", "target_word_count": 600},
    ).json()
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{message_payload['stream_id']}",
    )

    error_events = [payload for name, payload in events if name == "error"]
    done_events = [payload for name, payload in events if name == "done"]
    assert not done_events
    assert error_events
    assert error_events[-1]["status"] == "failed"
    assert error_events[-1]["stage"] == "draft"

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    assert assistant_turns
    latest_turn = assistant_turns[-1]
    assert latest_turn["trace"]["status"] == "failed"
    assert latest_turn["trace"]["degraded_mode"] is True
    assert latest_turn["trace"]["failed_stage"] == "draft"
    assert "草稿不可用，需要重试" in latest_turn["content"]


def test_stone_drafter_prompt_includes_topic_translation_outline_and_constraints(client, app, monkeypatch):
    project_id = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"}).json()["id"]
    _seed_stone_analysis(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")
    capture: dict[str, list[dict[str, str]]] = {}
    _install_writing_mocks(monkeypatch, capture=capture)

    session_id = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Prompt Session"},
    ).json()["id"]
    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "Rainy Night Station", "target_word_count": 600, "extra_requirements": "keep it restrained"},
    ).json()
    _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{message_payload['stream_id']}",
    )

    draft_call = next(item for item in capture["calls"] if item["stage"] == "draft")
    assert "topic_adapter JSON" in draft_call["user"]
    assert "prototype_selection JSON" in draft_call["user"]
    assert "blueprint JSON" in draft_call["user"]
    assert "stone_v2_generation_packet JSON" in draft_call["user"]
    assert "anti_patterns" in draft_call["user"]
    assert "nonclinical_psychodynamics" not in draft_call["user"]
    assert "Stone multi-facet baseline" not in draft_call["user"]
