from __future__ import annotations

import json
import re
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Empty, Queue
from threading import Event, Lock
from typing import Any
from uuid import uuid4

from app.analysis.stone import STONE_REVIEW_DIMENSIONS, collect_style_markers, estimate_word_count, render_writing_request
from app.db import Database
from app.llm.client import OpenAICompatibleClient
from app.runtime_limits import background_task_slot
from app.storage import repository


@dataclass(slots=True)
class WritingStreamState:
    id: str
    project_id: str
    session_id: str
    user_turn_id: str
    topic: str
    target_word_count: int
    extra_requirements: str | None
    events: Queue[dict[str, Any]] = field(default_factory=Queue)
    done: Event = field(default_factory=Event)
    cancelled: Event = field(default_factory=Event)


class WritingAgentService:
    def __init__(
        self,
        db: Database,
        config,
        *,
        max_workers: int = 4,
        run_inline: bool = False,
    ) -> None:
        self.db = db
        self.config = config
        self.run_inline = run_inline
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="writing-agent")
        self.streams: dict[str, WritingStreamState] = {}
        self.futures: dict[str, Future[None]] = {}
        self.lock = Lock()

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def cancel_project(self, project_id: str) -> None:
        with self.lock:
            stream_ids = [stream_id for stream_id, state in self.streams.items() if state.project_id == project_id]
        for stream_id in stream_ids:
            with self.lock:
                state = self.streams.get(stream_id)
                future = self.futures.get(stream_id)
            if not state:
                continue
            state.cancelled.set()
            if future:
                future.cancel()

    def has_project_activity(self, project_id: str) -> bool:
        with self.lock:
            return any(not state.done.is_set() for state in self.streams.values() if state.project_id == project_id)

    def start_stream(
        self,
        *,
        project_id: str,
        session_id: str,
        topic: str,
        target_word_count: int,
        extra_requirements: str | None,
    ) -> dict[str, str]:
        normalized_topic = str(topic or "").strip()
        if not normalized_topic:
            raise ValueError("Topic is required.")
        normalized_target = max(100, int(target_word_count or 0))
        normalized_extra = str(extra_requirements or "").strip() or None
        with self.db.session() as session:
            chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
            if not chat_session or chat_session.project_id != project_id:
                raise ValueError("Writing session not found.")
            user_turn = repository.add_chat_turn(
                session,
                session_id=session_id,
                role="user",
                content=render_writing_request(normalized_topic, normalized_target, normalized_extra),
                trace_json={
                    "kind": "writing_request",
                    "topic": normalized_topic,
                    "target_word_count": normalized_target,
                    "extra_requirements": normalized_extra,
                },
            )
            if not chat_session.title:
                repository.rename_chat_session(session, chat_session, title=_derive_session_title(normalized_topic))
            stream_id = str(uuid4())
            state = WritingStreamState(
                id=stream_id,
                project_id=project_id,
                session_id=session_id,
                user_turn_id=user_turn.id,
                topic=normalized_topic,
                target_word_count=normalized_target,
                extra_requirements=normalized_extra,
            )
            with self.lock:
                self.streams[stream_id] = state
        if self.run_inline:
            self._execute(state)
        else:
            future = self.executor.submit(self._execute, state)
            self.futures[stream_id] = future
        return {"stream_id": stream_id, "user_turn_id": user_turn.id}

    def stream_events(self, stream_id: str):
        with self.lock:
            state = self.streams.get(stream_id)
        if not state:
            raise KeyError(stream_id)
        while True:
            try:
                event = state.events.get(timeout=0.25)
            except Empty:
                if state.done.is_set():
                    break
                continue
            yield _format_sse(event["type"], event["payload"])
            if state.done.is_set() and state.events.empty():
                break
        with self.lock:
            self.streams.pop(stream_id, None)
            self.futures.pop(stream_id, None)

    def _execute(self, state: WritingStreamState) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self._run_turn(session, state)
        except RuntimeError as exc:
            if str(exc) == "Writing stream cancelled.":
                self._emit(state, "status", {"label": "Writing cancelled"})
            else:
                raise
        except Exception as exc:
            self._emit(state, "error", {"message": str(exc)})
            with self.db.session() as session:
                chat_session = repository.get_chat_session(session, state.session_id, session_kind="writing")
                if chat_session:
                    repository.add_chat_turn(
                        session,
                        session_id=state.session_id,
                        role="assistant",
                        content=f"Writing failed: {exc}",
                        trace_json={
                            "kind": "writing_result",
                            "blocks": [{"type": "error", "message": str(exc)}],
                            "reviews": [],
                            "judge_rounds": [],
                        },
                    )
        finally:
            state.done.set()

    def _run_turn(self, session, state: WritingStreamState) -> None:
        self._ensure_stream_active(state)
        project = repository.get_project(session, state.project_id)
        if not project:
            raise ValueError("Project not found.")
        if project.mode != "stone":
            raise ValueError("Only stone projects can use the writing workspace.")

        guide_bundle = self._resolve_guide_bundle(session, state.project_id)
        guide_payload = dict(guide_bundle["payload"] or {})
        self._emit(
            state,
            "stage",
            {
                "stage": "guide_loaded",
                "guide_source": guide_bundle["source"],
                "guide_asset_id": guide_bundle["asset_id"],
                "guide_version": guide_bundle["version_label"],
                "label": "Loaded writing guide",
            },
        )

        config = repository.get_service_config(session, "chat_service")
        initial_draft = self._generate_initial_draft(state, guide_payload, config)
        self._emit(
            state,
            "stage",
            {
                "stage": "drafter",
                "label": "Draft generated",
                "draft": initial_draft,
                "word_count": estimate_word_count(initial_draft),
            },
        )

        revision_rounds: list[dict[str, Any]] = []
        current_draft = initial_draft
        for revision_round in range(1, 3):
            reviews = [
                _review_dimension(
                    dimension,
                    current_draft,
                    guide_payload,
                    state.topic,
                    state.target_word_count,
                )
                for dimension in STONE_REVIEW_DIMENSIONS
            ]
            for review in reviews:
                self._emit(
                    state,
                    "stage",
                    {
                        "stage": "reviewer",
                        "label": f"Reviewer {review['dimension']}",
                        "dimension": review["dimension"],
                        "review": review,
                    },
                )

            revised_draft = _revise_draft(
                current_draft,
                reviews,
                guide_payload,
                state.topic,
                state.target_word_count,
                state.extra_requirements,
            )
            judge = _judge_draft(
                revised_draft,
                guide_payload,
                state.topic,
                state.target_word_count,
            )
            revision_rounds.append(
                {
                    "round": revision_round,
                    "draft_before": current_draft,
                    "draft_after": revised_draft,
                    "reviews": reviews,
                    "judge": judge,
                }
            )
            self._emit(
                state,
                "stage",
                {
                    "stage": "reviser",
                    "label": f"Revision round {revision_round}",
                    "revision_round": revision_round,
                    "draft": revised_draft,
                    "word_count": estimate_word_count(revised_draft),
                },
            )
            self._emit(
                state,
                "stage",
                {
                    "stage": "judge",
                    "label": f"Judge round {revision_round}",
                    "revision_round": revision_round,
                    "result": judge,
                },
            )
            current_draft = revised_draft
            if judge["pass"] or revision_round >= 2:
                break

        final_text = current_draft
        final_round = revision_rounds[-1] if revision_rounds else {"judge": _judge_draft(final_text, guide_payload, state.topic, state.target_word_count), "reviews": []}
        trace = {
            "kind": "writing_result",
            "topic": state.topic,
            "target_word_count": state.target_word_count,
            "extra_requirements": state.extra_requirements,
            "guide_source": guide_bundle["source"],
            "guide_asset_id": guide_bundle["asset_id"],
            "guide_version": guide_bundle["version_label"],
            "blocks": _build_trace_blocks(guide_bundle, revision_rounds),
            "reviews": final_round.get("reviews") or [],
            "judge_rounds": [item["judge"] for item in revision_rounds],
            "draft": initial_draft,
            "final_text": final_text,
            "final_judge": final_round["judge"],
        }
        assistant_turn = repository.add_chat_turn(
            session,
            session_id=state.session_id,
            role="assistant",
            content=final_text,
            trace_json=trace,
        )
        self._emit(
            state,
            "done",
            {
                "assistant_turn_id": assistant_turn.id,
                "final_text": final_text,
                "word_count": estimate_word_count(final_text),
                "guide_source": guide_bundle["source"],
                "judge": final_round["judge"],
            },
        )

    def _ensure_stream_active(self, state: WritingStreamState) -> None:
        if state.cancelled.is_set():
            raise RuntimeError("Writing stream cancelled.")

    def _emit(self, state: WritingStreamState, event_type: str, payload: dict[str, Any]) -> None:
        self._ensure_stream_active(state)
        state.events.put({"type": event_type, "payload": payload})

    def _resolve_guide_bundle(self, session, project_id: str) -> dict[str, Any]:
        version = repository.get_latest_asset_version(session, project_id, asset_kind="writing_guide")
        if version:
            return {
                "source": "published",
                "asset_id": version.id,
                "version_label": f"v{version.version_number}",
                "payload": version.json_payload or {},
            }
        draft = repository.get_latest_asset_draft(session, project_id, asset_kind="writing_guide")
        if draft:
            return {
                "source": "draft",
                "asset_id": draft.id,
                "version_label": "draft",
                "payload": draft.json_payload or {},
            }
        raise ValueError("No writing guide is available. Generate a writing_guide asset first.")

    def _generate_initial_draft(self, state: WritingStreamState, guide_payload: dict[str, Any], config) -> str:
        if config:
            try:
                client = OpenAICompatibleClient(config, log_path=str(self.config.llm_log_path))
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are the drafter in a constrained writing pipeline.\n"
                            "Follow the writing guide exactly, ignore external_slots, and output only the article body.\n"
                            "Keep the style stable, concrete, and internally consistent."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing guide JSON:\n{json.dumps(guide_payload, ensure_ascii=False, indent=2)}\n\n"
                            f"Topic: {state.topic}\n"
                            f"Target Word Count: {state.target_word_count}\n"
                            f"Extra Requirements: {state.extra_requirements or ''}\n"
                            "Return only the article body."
                        ),
                    },
                ]
                response = client.chat_completion_result(messages, model=config.model, temperature=0.6, max_tokens=None)
                candidate = str(response.content or "").strip()
                if candidate:
                    return _fit_word_count(candidate, state.target_word_count, guide_payload, state.topic, state.extra_requirements)
            except Exception:
                pass
        return _heuristic_draft(state.topic, state.target_word_count, guide_payload, state.extra_requirements)


def _derive_session_title(topic: str) -> str:
    clean = re.sub(r"\s+", " ", str(topic or "").strip())
    return clean[:48] or "New writing session"


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _heuristic_draft(topic: str, target_word_count: int, guide_payload: dict[str, Any], extra_requirements: str | None) -> str:
    motifs = _flatten_list(guide_payload.get("motif_theme_bank"))[:4]
    worldview = _flatten_list(guide_payload.get("worldview_and_stance"))[:3]
    emotional = _flatten_list(guide_payload.get("emotional_tendencies"))[:3]
    do_items = _flatten_list((guide_payload.get("do_and_dont") or {}).get("do"))[:3]
    anchors = guide_payload.get("fewshot_anchors") if isinstance(guide_payload.get("fewshot_anchors"), list) else []
    anchor_quote = ""
    for item in anchors:
        if isinstance(item, dict):
            anchor_quote = str(item.get("quote") or item.get("line") or "").strip()
        else:
            anchor_quote = str(item or "").strip()
        if anchor_quote:
            break

    paragraphs = [
        f"{topic}并不是一个突然闯进来的题目，它更像一块早就压在桌角的石头，碰一下就知道分量，也知道它会把哪一层旧尘再翻出来。",
        f"我更愿意先从{ '、'.join(motifs[:2]) if motifs else '那些看上去细小却不会自行消失的细节' }写起，因为真正把人逼近边缘的，往往不是戏剧性的事件，而是日常里反复出现、又被每个人假装可以承受的部分。",
        f"等视线慢慢沉下去，{ '、'.join(worldview[:2]) if worldview else '现实与关系的代价' }就会自己浮出来。那不是口号，也不是结论，只是一种越来越无法绕开的判断：一个人最终怎么说话，通常取决于他先失去过什么、又忍住过什么。",
        f"所以这件事里真正值得写的，不是姿态，而是那一点{ '、'.join(emotional[:2]) if emotional else '克制、迟疑与回身' }。它让文章不必一直向外解释，却能在每个转折里留下重量。",
    ]
    if do_items:
        paragraphs.append(f"如果一定要把这篇文章收住，我会把它收在{ '、'.join(do_items[:2]) }这样的动作里，而不是收在一个漂亮的答案里。")
    if anchor_quote:
        paragraphs.append(f"有时一句旧话就够了：{anchor_quote}")
    if extra_requirements:
        paragraphs.append(f"题外的要求我也会留着，但只保留最必要的部分：{extra_requirements}")
    text = "\n\n".join(paragraphs)
    return _fit_word_count(text, target_word_count, guide_payload, topic, extra_requirements)


def _fit_word_count(
    text: str,
    target_word_count: int,
    guide_payload: dict[str, Any],
    topic: str,
    extra_requirements: str | None,
) -> str:
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.9)
    upper = int(target * 1.05)
    current = estimate_word_count(text)
    worldview = _flatten_list(guide_payload.get("worldview_and_stance"))[:3]
    motifs = _flatten_list(guide_payload.get("motif_theme_bank"))[:3]
    while current < lower:
        addition = (
            f"\n\n说到底，{topic}之所以会成立，并不是因为它新，而是因为它总会再次碰到"
            f"{'、'.join(motifs[:2]) or '那些旧的裂口'}，再把{'、'.join(worldview[:2]) or '现实里的秩序与代价'}重新照亮。"
        )
        if extra_requirements and current < lower - 40:
            addition += f" 我会顺手把{extra_requirements}压进句子里，但不会让它盖过本来的纹理。"
        text += addition
        current = estimate_word_count(text)
        if len(addition) < 16:
            break
    if current > upper:
        trimmed = text
        while estimate_word_count(trimmed) > upper and len(trimmed) > 40:
            trimmed = trimmed[:-20].rstrip("，,；;。.!?！？ ")
        if not trimmed.endswith(("。", ".", "！", "!", "？", "?")):
            trimmed = f"{trimmed}。"
        text = trimmed
    return text.strip()


def _review_dimension(
    dimension: str,
    draft: str,
    guide_payload: dict[str, Any],
    topic: str,
    target_word_count: int,
) -> dict[str, Any]:
    markers = collect_style_markers(guide_payload)
    lexical_hits = [item for item in markers[:8] if item and item in draft]
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n+", draft) if item.strip()]
    word_count = estimate_word_count(draft)
    topic_hit = str(topic).strip() in draft
    must_fix: list[str] = []
    keep: list[str] = []
    evidence = markers[:3]
    risk_notes: list[str] = []
    score = 0.78

    if dimension == "style_consistency":
        if len(lexical_hits) < 2:
            must_fix.append("Inject more guide-native diction and tonal markers.")
            score = 0.62
        else:
            keep.append("Draft already keeps recurring guide diction.")
    elif dimension == "structure_and_pacing":
        if len(paragraphs) < 3:
            must_fix.append("Split the article into clearer progression beats.")
            score = 0.64
        elif len(paragraphs) > 6:
            must_fix.append("Tighten paragraph count and reduce over-explaining.")
            score = 0.68
        else:
            keep.append("Paragraph pacing is broadly usable.")
    elif dimension == "lexicon_and_rhythm":
        if len(lexical_hits) < 1:
            must_fix.append("Recover signature lexicon from the writing guide.")
            score = 0.6
        else:
            keep.append("Some signature lexical markers are already visible.")
    elif dimension == "theme_and_worldview":
        worldview_hits = [item for item in _flatten_list(guide_payload.get("worldview_and_stance"))[:6] if item and item in draft]
        if not topic_hit:
            must_fix.append("Make the assigned topic visible in the body.")
            score = 0.58
        if len(worldview_hits) < 1:
            must_fix.append("Translate the topic through the guide's worldview, not generic commentary.")
            score = min(score, 0.64)
        if topic_hit and worldview_hits:
            keep.append("Topic and worldview are linked.")
    else:
        repeated = _duplicate_sentence_ratio(draft)
        if repeated > 0.32:
            must_fix.append("Reduce repetition and overlap between adjacent sentences.")
            score = 0.61
        elif abs(word_count - int(target_word_count or 0)) > int(target_word_count * 0.15):
            risk_notes.append("Length is drifting toward judge threshold.")
            score = 0.72
        else:
            keep.append("No obvious overlap problem.")

    return {
        "dimension": dimension,
        "score": round(score, 3),
        "must_fix": must_fix,
        "keep": keep,
        "evidence_from_guide": evidence,
        "risk_notes": risk_notes,
    }


def _revise_draft(
    draft: str,
    reviews: list[dict[str, Any]],
    guide_payload: dict[str, Any],
    topic: str,
    target_word_count: int,
    extra_requirements: str | None,
) -> str:
    revised = str(draft or "").strip()
    worldview = _flatten_list(guide_payload.get("worldview_and_stance"))[:3]
    motifs = _flatten_list(guide_payload.get("motif_theme_bank"))[:3]
    lexical = _flatten_list((guide_payload.get("voice_dna") or {}).get("signature_phrases"))[:4]
    if not lexical:
        lexical = _flatten_list(guide_payload.get("voice_dna"))[:4]

    must_fix = [item for review in reviews for item in review.get("must_fix", [])]
    if any("topic" in item.lower() for item in must_fix) and topic not in revised:
        revised = f"{topic}，这件事真正难写的，不是表面，而是它把人重新拖回哪一种现实里。\n\n{revised}"
    if any("worldview" in item.lower() for item in must_fix) and worldview:
        revised += f"\n\n我宁愿把它理解成{'、'.join(worldview[:2])}，而不是把它写成一段干净的结论。"
    if any("lexicon" in item.lower() or "diction" in item.lower() for item in must_fix) and lexical:
        revised += f"\n\n到最后，真正留下来的还是{'、'.join(lexical[:2])}这类说法，它们比修辞更接近人的底色。"
    if any("progression" in item.lower() or "paragraph" in item.lower() for item in must_fix):
        sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", revised) if item.strip()]
        if len(sentences) >= 4:
            revised = "\n\n".join(
                [
                    "".join(sentences[:2]).strip(),
                    "".join(sentences[2:4]).strip(),
                    "".join(sentences[4:]).strip(),
                ]
            ).strip()
    if any("overlap" in item.lower() or "repetition" in item.lower() for item in must_fix):
        revised = _dedupe_sentences(revised)
    if motifs and all(motif not in revised for motif in motifs[:2]):
        revised += f"\n\n它最终还是会回到{'、'.join(motifs[:2])}这些意象上，因为作者通常不是在换题，而是在换角度反复逼近同一处裂纹。"
    if extra_requirements and extra_requirements not in revised and estimate_word_count(revised) < int(target_word_count * 0.95):
        revised += f"\n\n另外，我会把{extra_requirements}留在句子内部，而不是把它单独拎出来当口号。"
    return _fit_word_count(revised, target_word_count, guide_payload, topic, extra_requirements)


def _judge_draft(draft: str, guide_payload: dict[str, Any], topic: str, target_word_count: int) -> dict[str, Any]:
    word_count = estimate_word_count(draft)
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.85)
    upper = int(target * 1.15)
    markers = collect_style_markers(guide_payload)[:8]
    marker_hits = sum(1 for item in markers if item and item in draft)
    style_fidelity = min(1.0, 0.55 + (marker_hits / max(len(markers), 1)) * 0.45)
    brief_fit = 0.88 if topic in draft else 0.62
    worldview_hits = sum(1 for item in _flatten_list(guide_payload.get("worldview_and_stance"))[:6] if item and item in draft)
    originality = max(0.4, 0.9 - _duplicate_sentence_ratio(draft))
    if worldview_hits:
        style_fidelity = min(1.0, style_fidelity + 0.06)
    length_fit = 1.0 if lower <= word_count <= upper else max(0.0, 1.0 - (abs(word_count - target) / max(target, 1)))
    blocking_issues: list[str] = []
    if style_fidelity < 0.8:
        blocking_issues.append("Style fidelity below threshold.")
    if brief_fit < 0.75:
        blocking_issues.append("Topic execution is too weak.")
    if originality < 0.7:
        blocking_issues.append("Repetition or overlap is too high.")
    if not lower <= word_count <= upper:
        blocking_issues.append("Word count is outside the allowed range.")
    return {
        "pass": not blocking_issues,
        "style_fidelity": round(style_fidelity, 3),
        "brief_fit": round(brief_fit, 3),
        "length_fit": round(length_fit, 3),
        "originality": round(originality, 3),
        "blocking_issues": blocking_issues,
    }


def _build_trace_blocks(guide_bundle: dict[str, Any], revision_rounds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "guide_loaded",
            "label": f"Guide source: {guide_bundle['source']} ({guide_bundle['version_label']})",
        }
    ]
    if revision_rounds:
        blocks.append({"type": "stage", "stage": "drafter", "label": "Initial draft completed"})
    for item in revision_rounds:
        for review in item["reviews"]:
            blocks.append(
                {
                    "type": "review",
                    "dimension": review["dimension"],
                    "score": review["score"],
                    "must_fix": review["must_fix"],
                    "keep": review["keep"],
                }
            )
        blocks.append(
            {
                "type": "stage",
                "stage": "reviser",
                "label": f"Revision round {item['round']}",
            }
        )
        blocks.append(
            {
                "type": "judge",
                "round": item["round"],
                "result": item["judge"],
            }
        )
    return blocks


def _flatten_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = re.split(r"[，,、；;\n]+", value)
        return [part.strip() for part in parts if part.strip()]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_flatten_list(item))
        return flattened
    if isinstance(value, (list, tuple)):
        flattened = []
        for item in value:
            flattened.extend(_flatten_list(item))
        return flattened
    return [str(value).strip()]


def _duplicate_sentence_ratio(text: str) -> float:
    sentences = [item.strip() for item in re.split(r"[。！？!?]+", text) if item.strip()]
    if not sentences:
        return 0.0
    unique_count = len(set(sentences))
    return max(0.0, 1.0 - (unique_count / len(sentences)))


def _dedupe_sentences(text: str) -> str:
    sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", text) if item.strip()]
    unique: list[str] = []
    for sentence in sentences:
        if sentence in unique:
            continue
        unique.append(sentence)
    return "".join(unique).strip()
