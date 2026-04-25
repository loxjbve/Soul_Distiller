from __future__ import annotations

import json
import re
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, Lock
from typing import Any
from uuid import uuid4

from app.service.common.facets import FacetDefinition, get_facets_for_mode
from app.service.stone.analysis_support import estimate_word_count, render_writing_request
from app.service.common.writing_guide import build_writing_guide_payload_from_facets
from app.db import Database
from app.service.common.llm.client import OpenAICompatibleClient, parse_json_response
from app.runtime_limits import background_task_slot
from app.stone_runtime import get_latest_usable_stone_preprocess_run, load_latest_valid_asset_payload
from app.storage import repository
from app.utils.text import normalize_whitespace

STONE_WRITING_FACETS: tuple[FacetDefinition, ...] = get_facets_for_mode("stone")
WRITER_ACTOR_NAME = "写作 Agent"


@dataclass(slots=True)
class WritingStreamState:
    id: str
    project_id: str
    session_id: str
    user_turn_id: str
    topic: str
    target_word_count: int
    extra_requirements: str | None
    raw_message: str | None
    events: Queue[dict[str, Any]] = field(default_factory=Queue)
    done: Event = field(default_factory=Event)
    cancelled: Event = field(default_factory=Event)


@dataclass(slots=True)
class StoneWritingFacetContext:
    key: str
    label: str
    purpose: str
    confidence: float
    summary: str
    bullets: list[str]
    fewshots: list[dict[str, str]]
    conflicts: list[dict[str, str]]
    evidence: list[dict[str, Any]] = field(default_factory=list)
    anchor_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StoneWritingAnalysisBundle:
    run_id: str
    source: str
    version_label: str
    target_role: str | None
    analysis_context: str | None
    facets: list[StoneWritingFacetContext]
    prompt_text: str
    writing_guide: dict[str, Any] = field(default_factory=dict)
    guide_source: str = "derived"
    stone_profiles: list[dict[str, Any]] = field(default_factory=list)
    source_anchors: list[dict[str, Any]] = field(default_factory=list)
    generation_packet: dict[str, Any] = field(default_factory=dict)
    author_model: dict[str, Any] = field(default_factory=dict)
    prototype_index: dict[str, Any] = field(default_factory=dict)
    short_text_clusters: list[dict[str, Any]] = field(default_factory=list)


class WritingPipelineError(RuntimeError):
    def __init__(self, stage: str, message: str) -> None:
        super().__init__(message)
        self.stage = stage


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
        raw_message: str | None = None,
    ) -> dict[str, str]:
        normalized_topic = str(topic or "").strip()
        if not normalized_topic:
            raise ValueError("Topic is required.")
        normalized_target = max(100, int(target_word_count or 0))
        normalized_extra = str(extra_requirements or "").strip() or None
        normalized_message = str(raw_message or "").strip() or None

        with self.db.session() as session:
            chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
            if not chat_session or chat_session.project_id != project_id:
                raise ValueError("Writing session not found.")
            user_turn = repository.add_chat_turn(
                session,
                session_id=session_id,
                role="user",
                content=normalized_message or render_writing_request(normalized_topic, normalized_target, normalized_extra),
                trace_json={
                    "kind": "writing_request",
                    "topic": normalized_topic,
                    "target_word_count": normalized_target,
                    "extra_requirements": normalized_extra,
                    "raw_message": normalized_message,
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
                raw_message=normalized_message,
            )
            with self.lock:
                self.streams[stream_id] = state

        if self.run_inline:
            self._execute(state)
        else:
            future = self.executor.submit(self._execute, state)
            with self.lock:
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
        except WritingPipelineError as exc:
            self._emit(state, "error", {"message": str(exc), "stage": exc.stage, "status": "failed"})
            with self.db.session() as session:
                chat_session = repository.get_chat_session(session, state.session_id, session_kind="writing")
                if chat_session:
                    repository.add_chat_turn(
                        session,
                        session_id=state.session_id,
                        role="assistant",
                        content=f"草稿不可用，需要重试。\n\n失败阶段：{exc.stage}\n原因：{exc}",
                        trace_json={
                            "kind": "writing_result",
                            "status": "failed",
                            "degraded_mode": True,
                            "failed_stage": exc.stage,
                            "timeline": [],
                            "critics": [],
                            "final_assessment": None,
                        },
                    )
        except RuntimeError as exc:
            if str(exc) == "Writing stream cancelled.":
                self._emit(state, "status", {"label": "Writing cancelled"})
            else:
                raise
        except Exception as exc:
            failed_stage = exc.stage if isinstance(exc, WritingPipelineError) else "unknown"
            self._emit(state, "error", {"message": str(exc), "stage": failed_stage, "status": "failed"})
            with self.db.session() as session:
                chat_session = repository.get_chat_session(session, state.session_id, session_kind="writing")
                if chat_session:
                    repository.add_chat_turn(
                        session,
                        session_id=state.session_id,
                        role="assistant",
                        content=f"草稿不可用，需要重试。\n\n失败阶段：{failed_stage}\n原因：{exc}",
                        trace_json={
                            "kind": "writing_result",
                            "status": "failed",
                            "degraded_mode": True,
                            "failed_stage": failed_stage,
                            "timeline": [],
                            "critics": [],
                            "final_assessment": None,
                        },
                    )
        finally:
            state.done.set()

    def _run_turn(self, session, state: WritingStreamState) -> None:
        return _run_turn_v3(self, session, state)

    def _ensure_stream_active(self, state: WritingStreamState) -> None:
        if state.cancelled.is_set():
            raise RuntimeError("Writing stream cancelled.")

    def _emit(self, state: WritingStreamState, event_type: str, payload: dict[str, Any]) -> None:
        self._ensure_stream_active(state)
        state.events.put({"type": event_type, "payload": payload})

    def _build_client(self, config) -> OpenAICompatibleClient | None:
        if not config:
            return None
        log_path = getattr(self.config, "llm_log_path", None)
        return OpenAICompatibleClient(config, log_path=str(log_path) if log_path else None)

    def _stream_key(self, state: WritingStreamState, stage: str, *, suffix: str | None = None) -> str:
        key = f"{state.id}:{stage}"
        return f"{key}:{suffix}" if suffix else key

    def _emit_live_writer_message(
        self,
        state: WritingStreamState,
        *,
        message_kind: str,
        label: str,
        body: str,
        detail: dict[str, Any] | None = None,
        stage: str | None = None,
        stream_key: str | None = None,
        stream_state: str = "complete",
        render_mode: str = "markdown",
        actor_name: str | None = None,
        actor_id: str | None = None,
        actor_role: str = "assistant",
    ) -> None:
        payload = {
            "message_kind": message_kind,
            "label": label,
            "body": body,
            "detail": detail or {},
            "stage": stage or message_kind,
            "stream_key": stream_key or self._stream_key(state, stage or message_kind),
            "stream_state": stream_state,
            "render_mode": render_mode,
            "actor_name": actor_name or WRITER_ACTOR_NAME,
            "actor_id": actor_id or f"writer-{stage or message_kind}",
            "actor_role": actor_role,
            "created_at": _iso_now(),
        }
        self._emit(state, "stream_update", payload)

    def _make_stage_stream_handler(
        self,
        state: WritingStreamState,
        *,
        message_kind: str,
        label: str,
        stage: str,
        stream_key: str,
        actor_name: str | None = None,
        actor_id: str | None = None,
        actor_role: str = "assistant",
        render_mode: str = "markdown",
    ):
        buffer: list[str] = []

        def handler(delta: str) -> None:
            if not delta:
                return
            buffer.append(delta)
            self._emit(
                state,
                "stream_update",
                {
                    "message_kind": message_kind,
                    "label": label,
                    "body": "".join(buffer),
                    "detail": {},
                    "stage": stage,
                    "stream_key": stream_key,
                    "stream_state": "streaming",
                    "render_mode": render_mode,
                    "actor_name": actor_name or WRITER_ACTOR_NAME,
                    "actor_id": actor_id or f"writer-{stage}",
                    "actor_role": actor_role,
                    "created_at": _iso_now(),
                },
            )

        def finalize() -> None:
            if not buffer:
                return
            self._emit(
                state,
                "stream_update",
                {
                    "message_kind": message_kind,
                    "label": label,
                    "body": "".join(buffer),
                    "detail": {},
                    "stage": stage,
                    "stream_key": stream_key,
                    "stream_state": "complete",
                    "render_mode": render_mode,
                    "actor_name": actor_name or WRITER_ACTOR_NAME,
                    "actor_id": actor_id or f"writer-{stage}",
                    "actor_role": actor_role,
                    "created_at": _iso_now(),
                },
            )

        return handler, finalize


def _derive_session_title(topic: str) -> str:
    clean = re.sub(r"\s+", " ", str(topic or "").strip())
    return clean[:48] or "New writing session"


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"






































def _build_writer_message_payload(
    *,
    message_kind: str,
    label: str,
    body: str,
    detail: dict[str, Any] | None = None,
    stage: str = "writer",
    stream_key: str | None = None,
    stream_state: str = "complete",
    render_mode: str = "markdown",
) -> dict[str, Any]:
    return {
        "stage": stage,
        "label": label,
        "actor_id": f"writer-{message_kind}",
        "actor_name": WRITER_ACTOR_NAME,
        "actor_role": "writer",
        "message_kind": message_kind,
        "body": body,
        "detail": detail or {},
        "created_at": _iso_now(),
        "stream_key": stream_key,
        "stream_state": stream_state,
        "render_mode": render_mode,
    }








def _fit_word_count(
    text: str,
    target_word_count: int,
    analysis_bundle: StoneWritingAnalysisBundle,
    topic: str,
    extra_requirements: str | None,
) -> str:
    del target_word_count, analysis_bundle, topic, extra_requirements
    return str(text or "").strip()
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.9)
    upper = int(target * 1.05)
    current = estimate_word_count(text)

    while current < lower:
        text = f"{text}\n\n{_expansion_paragraph(analysis_bundle, topic, extra_requirements)}".strip()
        next_count = estimate_word_count(text)
        if next_count <= current:
            break
        current = next_count

    if current <= upper:
        return text.strip()

    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]
    while paragraphs and estimate_word_count("\n\n".join(paragraphs)) > upper:
        if len(paragraphs[-1]) > 120:
            paragraphs[-1] = paragraphs[-1][:-40].rstrip("，。；：、 ")
            if paragraphs[-1] and paragraphs[-1][-1] not in "。！？":
                paragraphs[-1] = f"{paragraphs[-1]}。"
        else:
            paragraphs.pop()
    trimmed = "\n\n".join(paragraphs).strip()
    return trimmed or text.strip()




def _expansion_paragraph(
    analysis_bundle: StoneWritingAnalysisBundle,
    topic: str,
    extra_requirements: str | None,
) -> str:
    imagery_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:2], fallback="旧物和夜色")
    stance_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:2], fallback="代价与边界")
    emotion_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:2], fallback="克制和回落")
    note = f" 同时继续守住“{extra_requirements}”这个要求。" if extra_requirements else ""
    return (
        f"{topic}真正难写的地方，不在于事件本身，而在于它总会重新碰到{imagery_hint}，"
        f"再把{stance_hint}慢慢照亮。视线只要再往里收一层，情绪就会回到{emotion_hint}这条线上。{note}"
    ).strip()




















def _normalize_string_list(value: Any, *, limit: int = 6) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        pieces = re.split(r"[\n;,，；]+", value)
        return [piece.strip() for piece in pieces if piece.strip()][:limit]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_normalize_string_list(item, limit=limit))
        return _unique_preserve_order(flattened)[:limit]
    if isinstance(value, (list, tuple)):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_normalize_string_list(item, limit=limit))
        return _unique_preserve_order(flattened)[:limit]
    text = str(value).strip()
    return [text] if text else []












def _clean_model_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def _facet_lookup(bundle: StoneWritingAnalysisBundle, key: str) -> StoneWritingFacetContext | None:
    return next((item for item in bundle.facets if item.key == key), None)


def _facet_terms(facet: StoneWritingFacetContext | None) -> list[str]:
    if not facet:
        return []
    candidates: list[str] = []
    for text in [facet.summary, *facet.bullets]:
        candidates.extend(_extract_terms(text))
    for item in facet.fewshots:
        candidates.extend(_extract_terms(item.get("expression")))
        candidates.extend(_extract_terms(item.get("quote")))
    return _unique_preserve_order(candidates)[:10]


def _extract_terms(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,}", text)[:12]


def _available_anchor_ids(bundle: StoneWritingAnalysisBundle) -> list[str]:
    return [
        str(item.get("id") or "").strip()
        for item in bundle.source_anchors
        if str(item.get("id") or "").strip()
    ]






























































































































def _critic_spec_v2(critic_key: str) -> dict[str, str]:
    mapping = {
        "formal_fidelity": {
            "label": "formal_fidelity",
            "focus": "只检查形式保真：开头动作、推进方式、句法压力、收口残响是否像作者本人。",
        },
        "worldview_translation": {
            "label": "worldview_translation",
            "focus": "只检查题目是否被翻进作者的价值镜头、判断对象和代价逻辑，而不是只换皮。",
        },
        "syntheticness": {
            "label": "syntheticness",
            "focus": "只检查这篇是否像拼出来的仿写、像 checklist、像分析结论复述；句子有没有概念词堆叠、解释味过重或读起来不顺的地方。",
        },
    }
    return mapping.get(critic_key, mapping["formal_fidelity"])






def _build_critic_message_payload_v2(
    critic: dict[str, Any],
    *,
    stream_key: str | None = None,
    stream_state: str = "complete",
    render_mode: str = "markdown",
    stage: str = "critic",
    label_suffix: str = "",
) -> dict[str, Any]:
    key = str(critic.get("critic_key") or "critic").strip() or "critic"
    label = str(critic.get("critic_label") or key).strip() or key
    return {
        "stage": stage,
        "label": f"{label} critic{label_suffix}",
        "actor_id": f"critic-{key}",
        "actor_name": label,
        "actor_role": "critic",
        "message_kind": "critic",
        "body": _render_critic_message_v2(critic),
        "detail": critic,
        "created_at": _iso_now(),
        "stream_key": stream_key,
        "stream_state": stream_state,
        "render_mode": render_mode,
    }


def _render_critic_message_v2(critic: dict[str, Any]) -> str:
    lines = [
        f"结论：{critic.get('verdict')}",
        f"分数：{int(round(float(critic.get('score') or 0.0) * 100))}/100",
    ]
    if critic.get("matched_signals"):
        lines.append("")
        lines.append("命中信号：")
        lines.extend(f"- {item}" for item in (critic.get("matched_signals") or [])[:5])
    if critic.get("line_edits"):
        lines.append("")
        lines.append("局部修改：")
        lines.extend(f"- {item}" for item in (critic.get("line_edits") or [])[:6])
    if critic.get("redraft_reason"):
        lines.append("")
        lines.append(f"整篇重写原因：{critic.get('redraft_reason')}")
    if critic.get("risks"):
        lines.append("")
        lines.append("风险：")
        lines.extend(f"- {item}" for item in (critic.get("risks") or [])[:4])
    return "\n".join(lines).strip()




def _build_final_assessment_v2(
    final_text: str,
    critics: list[dict[str, Any]],
    topic: str,
    target_word_count: int,
    *,
    revision_action: str,
) -> dict[str, Any]:
    task_assessment = _assess_task_compliance_v2(final_text, topic, target_word_count)
    remaining_risks = _unique_preserve_order(
        [risk for critic in critics for risk in (critic.get("risks") or [])]
    )[:4]
    if not task_assessment["length_ok"]:
        remaining_risks.append("字数仍需人工复核。")
    if topic and not task_assessment["topic_visible"]:
        remaining_risks.append("主题词在正文里的显性可见度偏低。")
    return {
        "critic_pass_count": sum(1 for critic in critics if critic.get("pass")),
        "critic_total": len(critics),
        "length_ok": task_assessment["length_ok"],
        "topic_visible": task_assessment["topic_visible"],
        "matched_topic_terms": task_assessment["matched_terms"],
        "revision_action": revision_action,
        "remaining_risks": remaining_risks[:4],
    }


def _assess_task_compliance_v2(final_text: str, topic: str, target_word_count: int) -> dict[str, Any]:
    word_count = estimate_word_count(final_text)
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.88)
    upper = int(target * 1.08)
    topic_visible, matched_terms = _topic_visible_v2(topic, final_text)
    return {
        "word_count": word_count,
        "target_word_count": target,
        "length_ok": lower <= word_count <= upper,
        "topic_visible": topic_visible,
        "matched_terms": matched_terms[:6],
    }


def _topic_visible_v2(topic: str, final_text: str) -> tuple[bool, list[str]]:
    normalized_topic = normalize_whitespace(topic).lower()
    normalized_text = normalize_whitespace(final_text).lower()
    if not normalized_topic:
        return True, []
    if normalized_topic in normalized_text:
        return True, [normalized_topic]
    keywords = _extract_topic_keywords_v2(normalized_topic)
    matched = [term for term in keywords if term and term in normalized_text]
    if not matched:
        return False, []
    if any(len(term) >= 4 for term in matched):
        return True, matched
    return len(matched) >= min(2, max(1, len(keywords))), matched


def _extract_topic_keywords_v2(topic: str) -> list[str]:
    stop_terms = {
        "一篇",
        "文章",
        "故事",
        "一下",
        "这个",
        "那个",
        "今天",
        "晚上",
        "今晚",
        "现在",
        "什么",
        "为什么",
        "怎么",
        "自己",
        "我的",
    }
    splitter_chars = "的了和在是把给跟与及就还都也又并但却而着过地得让把将被向对把呢吗啊呀吧"
    candidates: list[str] = []
    for token in re.findall(r"[a-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", topic):
        if token in stop_terms:
            continue
        if re.fullmatch(r"[\u4e00-\u9fff]{2,}", token):
            parts = [piece for piece in re.split(f"[{splitter_chars}]+", token) if len(piece) >= 2]
            if not parts:
                parts = [token]
            for part in parts:
                part = _strip_topic_action_prefix_v2(part)
                if len(part) < 2:
                    continue
                if part in stop_terms:
                    continue
                candidates.append(part)
                if 4 <= len(part) <= 6:
                    tail = part[-3:] if len(part) >= 3 else ""
                    if tail and tail not in stop_terms:
                        candidates.append(tail)
                    if len(part) >= 2:
                        tail2 = part[-2:]
                        if tail2 not in stop_terms:
                            candidates.append(tail2)
            continue
        candidates.append(token)
    return _unique_preserve_order(sorted(candidates, key=len, reverse=True))[:10]


def _strip_topic_action_prefix_v2(value: str) -> str:
    text = str(value or "").strip()
    prefixes = (
        "我",
        "你",
        "他",
        "她",
        "它",
        "我们",
        "你们",
        "他们",
        "她们",
        "它们",
        "自己",
        "正在",
        "突然",
        "还是",
        "就是",
    )
    verbs = (
        "吃",
        "喝",
        "写",
        "讲",
        "说",
        "做",
        "去",
        "来",
        "想",
        "要",
        "点",
        "买",
        "用",
        "聊",
        "谈",
        "回",
        "进",
        "出",
        "看",
        "逛",
        "住",
        "玩",
    )
    changed = True
    while changed and len(text) >= 2:
        changed = False
        for prefix in prefixes:
            if text.startswith(prefix) and len(text) - len(prefix) >= 2:
                text = text[len(prefix):]
                changed = True
        for verb in verbs:
            if text.startswith(verb) and len(text) - len(verb) >= 2:
                text = text[len(verb):]
                changed = True
    return text
















































def _should_run_second_critic_round_v2(critics: list[dict[str, Any]]) -> bool:
    if not critics:
        return False
    verdicts = [str(item.get("verdict") or "approve") for item in critics]
    scores = [_clamp_score(item.get("score"), default=0.0) for item in critics]
    if any(verdict == "redraft" for verdict in verdicts):
        return True
    if any(score < 0.72 for score in scores):
        return True
    if min(scores) < 0.82 and any(item.get("risks") or item.get("line_edits") for item in critics):
        return True
    return False












def _contains_banned_meta(text: str) -> bool:
    banned = (
        "分析里最能充当锚点",
        "如果沿着",
        "这次修订最重要",
        "写作任务",
        "topic_translation",
        "topic_adapter",
        "prototype_selection",
        "prototype_selector",
        "blueprint JSON",
        "critic feedback",
        "generation_packet",
        "local_router",
        "local_style_bundle",
        "sample_routing",
        "sample_selection",
        "local_decomposition",
        "anchor id",
        "Anchor ID",
        "DSM",
        "诊断",
        "病理标签",
    )
    return any(item in text for item in banned)


def _light_trim_to_word_count(text: str, target_word_count: int) -> str:
    del target_word_count
    return str(text or "").strip()
    target = max(100, int(target_word_count or 0))
    upper = int(target * 1.08)
    if estimate_word_count(text) <= upper:
        return text.strip()
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]
    while len(paragraphs) > 1 and estimate_word_count("\n\n".join(paragraphs)) > upper:
        last = paragraphs[-1]
        if len(last) <= 80:
            paragraphs.pop()
            continue
        paragraphs[-1] = last[:-40].rstrip("，。；：、 ")
        if paragraphs[-1] and paragraphs[-1][-1] not in "。！？":
            paragraphs[-1] = f"{paragraphs[-1]}。"
        break
    return "\n\n".join(paragraphs).strip() or text.strip()


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _default_paragraph_count(target_word_count: int) -> int:
    if target_word_count <= 450:
        return 3
    if target_word_count <= 800:
        return 4
    if target_word_count <= 1200:
        return 5
    return 6




def _join_terms(terms: list[str], *, fallback: str) -> str:
    cleaned = [item for item in terms if item]
    if not cleaned:
        return fallback
    return "、".join(cleaned[:2])


def _trim_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return f"{text[: limit - 3]}..."


def _clamp_score(value: Any, *, default: float) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(score, 1.0))


def _unique_preserve_order(values) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered






def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_stone_profiles_v3(session, project_id: str) -> list[dict[str, Any]]:
    from app.service.stone.assets_support import STONE_V3_PROFILE_KEY, normalize_stone_profile_v3

    profiles: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        metadata = dict(document.metadata_json or {})
        profile = metadata.get(STONE_V3_PROFILE_KEY)
        if not isinstance(profile, dict):
            continue
        normalized = normalize_stone_profile_v3(
            profile,
            article_text=str(document.clean_text or document.raw_text or ""),
            fallback_title=document.title or document.filename,
            document_id=document.id,
            source_meta={
                "created_at_guess": document.created_at_guess,
                "source_type": document.source_type,
            },
        )
        normalized["document_id"] = document.id
        normalized["title"] = document.title or document.filename
        profiles.append(normalized)
    return profiles




def _load_v3_asset_payload(session, project_id: str, *, asset_kind: str) -> dict[str, Any]:
    from app.service.stone.assets_support import is_valid_stone_v3_asset_payload

    return load_latest_valid_asset_payload(
        session,
        project_id,
        asset_kind=asset_kind,
        validator=is_valid_stone_v3_asset_payload,
    )


def _build_source_anchors_v3(prototype_index: dict[str, Any]) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def add_anchor(item: dict[str, Any]) -> None:
        anchor_id = str(item.get("id") or "").strip()
        quote = _trim_text(item.get("quote"), 260)
        if not anchor_id or not quote or anchor_id in seen_ids:
            return
        seen_ids.add(anchor_id)
        anchors.append(
            {
                "id": anchor_id,
                "source": "stone_prototype_index_v3",
                "document_id": str(item.get("document_id") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "role": str(item.get("role") or "signature").strip() or "signature",
                "quote": quote,
                "note": _trim_text(item.get("reason"), 120),
            }
        )

    for item in prototype_index.get("anchor_registry") or []:
        if isinstance(item, dict):
            add_anchor(item)
    if anchors:
        return anchors[:80]

    for document in prototype_index.get("documents") or []:
        if not isinstance(document, dict):
            continue
        for anchor in document.get("anchor_registry") or []:
            if isinstance(anchor, dict):
                add_anchor(anchor)
    return anchors[:80]


def _build_analysis_prompt_text_v3(bundle: StoneWritingAnalysisBundle) -> str:
    author_core = dict(bundle.author_model.get("author_core") or {})
    retrieval_policy = dict(bundle.prototype_index.get("retrieval_policy") or {})
    parts = [
        "Stone v3 writing baseline",
        f"Preprocess run: {bundle.run_id}",
        f"Target role: {bundle.target_role or ''}",
        f"Profile count: {len(bundle.stone_profiles)}",
        f"Prototype documents: {len((bundle.prototype_index or {}).get('documents') or [])}",
        "",
        "Author core:",
        f"- voice: {author_core.get('voice_summary') or ''}",
        f"- worldview: {author_core.get('worldview_summary') or ''}",
        f"- tone: {author_core.get('tone_summary') or ''}",
        "",
        "Retrieval policy:",
        f"- shortlist_formula: {retrieval_policy.get('shortlist_formula') or ''}",
        f"- target_shortlist_size: {retrieval_policy.get('target_shortlist_size') or 12}",
        f"- target_anchor_budget: {retrieval_policy.get('target_anchor_budget') or 8}",
    ]
    return "\n".join(parts).strip()


def _build_generation_packet_v3(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    author_core = dict(bundle.author_model.get("author_core") or {})
    return {
        "baseline": {
            "stone_v3": True,
            "preprocess_ready": True,
            "corpus_ready": bool(bundle.stone_profiles),
            "profile_count": len(bundle.stone_profiles),
            "profile_version": "v3",
            "baseline_version": "v3",
            "author_model_ready": bool(bundle.author_model),
            "prototype_index_ready": bool(bundle.prototype_index),
            "author_model_v3_ready": bool(bundle.author_model),
            "prototype_index_v3_ready": bool(bundle.prototype_index),
            "source_anchor_count": len(bundle.source_anchors),
            "source": "stone_profile_v3 + stone_author_model_v3 + stone_prototype_index_v3",
        },
        "analysis_run": {
            "run_id": bundle.run_id,
            "version_label": bundle.version_label,
            "target_role": bundle.target_role,
            "analysis_context": bundle.analysis_context,
        },
        "author_model": {
            "author_core": author_core,
            "translation_rules": list(bundle.author_model.get("translation_rules") or [])[:8],
            "stable_moves": list(bundle.author_model.get("stable_moves") or [])[:8],
            "forbidden_moves": list(bundle.author_model.get("forbidden_moves") or [])[:8],
        },
        "prototype_index": {
            "document_count": int(bundle.prototype_index.get("document_count") or 0),
            "family_count": int(bundle.prototype_index.get("family_count") or 0),
            "retrieval_policy": dict(bundle.prototype_index.get("retrieval_policy") or {}),
            "selection_guides": dict(bundle.prototype_index.get("selection_guides") or {}),
        },
        "source_anchors": bundle.source_anchors[:24],
    }


def _v3_keyword_units(*values: Any, limit: int = 16) -> list[str]:
    items: list[str] = []
    for value in values:
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,24}|[\u4e00-\u9fff]{1,4}", normalize_whitespace(str(value or "")).lower()):
            if token in items:
                continue
            items.append(token)
            if len(items) >= limit:
                return items
    return items


def _resolve_length_band_v3(target_word_count: int) -> str:
    if target_word_count <= 160:
        return "micro"
    if target_word_count <= 420:
        return "short"
    if target_word_count <= 1000:
        return "medium"
    return "long"


def _normalize_request_adapter_v3(
    payload: dict[str, Any],
    state: WritingStreamState,
    bundle: StoneWritingAnalysisBundle,
) -> dict[str, Any]:
    translation_rules = list(bundle.author_model.get("translation_rules") or [])
    first_rule = next((dict(item) for item in translation_rules if isinstance(item, dict)), {})
    desired_length_band = str(payload.get("desired_length_band") or _resolve_length_band_v3(state.target_word_count)).strip().lower()
    if desired_length_band not in {"micro", "short", "medium", "long"}:
        desired_length_band = _resolve_length_band_v3(state.target_word_count)
    surface_form = str(payload.get("surface_form") or "").strip().lower() or "scene_vignette"
    query_terms = _unique_preserve_order(
        [
            *_normalize_string_list(payload.get("query_terms"), limit=10),
            *_v3_keyword_units(state.topic, state.extra_requirements, limit=10),
        ]
    )[:10]
    motif_terms = _unique_preserve_order(
        [
            *_normalize_string_list(payload.get("motif_terms"), limit=8),
            *_normalize_string_list(first_rule.get("preferred_motifs"), limit=6),
            *_normalize_string_list((bundle.author_model.get("author_core") or {}).get("signature_motifs"), limit=6),
        ]
    )[:8]
    anchor_preferences = _unique_preserve_order(
        [
            *_normalize_string_list(payload.get("anchor_preferences"), limit=6),
            "opening",
            "closing",
        ]
    )[:6]
    hard_constraints = _unique_preserve_order(
        [
            "全文必须使用简体中文，避免英文句子或整段英文表达。",
            *_normalize_string_list(payload.get("hard_constraints"), limit=6),
            *_normalize_string_list(state.extra_requirements, limit=4),
        ]
    )[:6]
    return {
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "desired_length_band": desired_length_band,
        "surface_form": surface_form,
        "value_lens": str(payload.get("value_lens") or first_rule.get("value_lens") or "").strip() or "cost",
        "judgment_mode": str(payload.get("judgment_mode") or "").strip() or "通过贴身细节稳定判断",
        "distance": str(payload.get("distance") or "").strip() or "回收式第一人称",
        "entry_scene": str(payload.get("entry_scene") or "").strip() or "从一个具体动作或物件进入。",
        "felt_cost": str(payload.get("felt_cost") or "").strip() or "先把压力落成体感代价，再进入解释。",
        "query_terms": query_terms,
        "motif_terms": motif_terms,
        "anchor_preferences": anchor_preferences,
        "hard_constraints": hard_constraints,
        "reasoning": _trim_text(payload.get("reasoning"), 220),
    }


def _score_v3_shortlist_candidate(
    document: dict[str, Any],
    request_adapter: dict[str, Any],
) -> tuple[float, list[str]]:
    handles = dict(document.get("retrieval_handles") or {})
    routing_text = normalize_whitespace(handles.get("routing_text") or "")
    keywords = [str(item).lower() for item in (handles.get("keywords") or []) if str(item).strip()]
    routing_facets = dict(handles.get("routing_facets") or {})
    best_for = [str(item).lower() for item in ((document.get("selection_guides") or {}).get("best_for") or []) if str(item).strip()]
    lift_signals = [str(item).lower() for item in ((document.get("selection_guides") or {}).get("lift_signals") or []) if str(item).strip()]
    all_text = " ".join(
        [
            routing_text.lower(),
            " ".join(keywords),
            " ".join(best_for),
            " ".join(lift_signals),
            str(document.get("family_label") or "").lower(),
            str(document.get("document_summary") or "").lower(),
        ]
    )
    score = 0.0
    reasons: list[str] = []
    for term in request_adapter.get("query_terms") or []:
        token = str(term).lower()
        if not token:
            continue
        if token in keywords:
            score += 3.0
            reasons.append(f"keyword:{token}")
        elif token in all_text:
            score += 1.4
            reasons.append(f"text:{token}")
    for motif in request_adapter.get("motif_terms") or []:
        token = str(motif).lower()
        if token and token in all_text:
            score += 1.8
            reasons.append(f"motif:{token}")
    if str(document.get("length_band") or "").lower() == str(request_adapter.get("desired_length_band") or "").lower():
        score += 2.0
        reasons.append("length_match")
    if str(document.get("surface_form") or "").lower() == str(request_adapter.get("surface_form") or "").lower():
        score += 2.0
        reasons.append("surface_match")
    if str(routing_facets.get("value_lens") or "").strip() == str(request_adapter.get("value_lens") or "").strip():
        score += 1.5
        reasons.append("value_lens_match")
    if str(routing_facets.get("distance") or "").strip() == str(request_adapter.get("distance") or "").strip():
        score += 1.0
        reasons.append("distance_match")
    if str(routing_facets.get("judgment_mode") or "").strip() == str(request_adapter.get("judgment_mode") or "").strip():
        score += 1.0
        reasons.append("judgment_match")
    return score, _unique_preserve_order(reasons)[:8]


def _build_candidate_shortlist_v3(
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    anchor_budget = int(((bundle.prototype_index.get("retrieval_policy") or {}).get("target_anchor_budget")) or 8)
    for document in bundle.prototype_index.get("documents") or []:
        if not isinstance(document, dict):
            continue
        score, reasons = _score_v3_shortlist_candidate(document, request_adapter)
        candidates.append(
            {
                "document_id": str(document.get("document_id") or "").strip(),
                "title": str(document.get("title") or "").strip(),
                "family_id": str(document.get("family_id") or "").strip(),
                "family_label": str(document.get("family_label") or "").strip(),
                "length_band": str(document.get("length_band") or "").strip(),
                "surface_form": str(document.get("surface_form") or "").strip(),
                "score": round(score, 4),
                "reasons": reasons,
                "summary": _trim_text(document.get("document_summary"), 180),
                "retrieval_handles": dict(document.get("retrieval_handles") or {}),
                "selection_guides": dict(document.get("selection_guides") or {}),
                "anchor_registry": list(document.get("anchor_registry") or [])[:anchor_budget],
            }
        )
    candidates.sort(key=lambda item: (float(item.get("score") or 0.0), item.get("document_id") or ""), reverse=True)
    shortlist_size = int(((bundle.prototype_index.get("retrieval_policy") or {}).get("target_shortlist_size")) or 12)
    shortlisted = [item for item in candidates if item.get("document_id")][: max(1, shortlist_size)]
    return {
        "desired_length_band": request_adapter.get("desired_length_band"),
        "surface_form": request_adapter.get("surface_form"),
        "query_terms": list(request_adapter.get("query_terms") or []),
        "motif_terms": list(request_adapter.get("motif_terms") or []),
        "shortlist_size": len(shortlisted),
        "documents": shortlisted,
    }


def _compact_shortlist_for_prompt_v3(shortlist: dict[str, Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in shortlist.get("documents") or []:
        anchors = []
        for anchor in item.get("anchor_registry") or []:
            if not isinstance(anchor, dict):
                continue
            anchors.append(
                {
                    "id": anchor.get("id"),
                    "role": anchor.get("role"),
                    "quote": _trim_text(anchor.get("quote"), 180),
                }
            )
        compact.append(
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "family_id": item.get("family_id"),
                "family_label": item.get("family_label"),
                "length_band": item.get("length_band"),
                "surface_form": item.get("surface_form"),
                "score": item.get("score"),
                "summary": item.get("summary"),
                "reasons": item.get("reasons"),
                "selection_guides": item.get("selection_guides"),
                "anchors": anchors[:4],
            }
        )
    return compact


def _normalize_rerank_v3(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    shortlist: dict[str, Any],
) -> dict[str, Any]:
    shortlist_docs = {
        str(item.get("document_id") or "").strip(): item
        for item in shortlist.get("documents") or []
        if str(item.get("document_id") or "").strip()
    }
    shortlist_anchor_ids: set[str] = set()
    document_anchor_ids: dict[str, list[str]] = {}
    for document_id, item in shortlist_docs.items():
        ids = [
            str(anchor.get("id") or "").strip()
            for anchor in (item.get("anchor_registry") or [])
            if isinstance(anchor, dict) and str(anchor.get("id") or "").strip()
        ]
        document_anchor_ids[document_id] = ids
        shortlist_anchor_ids.update(ids)

    selected_documents: list[str] = []
    for item in payload.get("selected_documents") or []:
        if isinstance(item, dict):
            document_id = str(item.get("document_id") or item.get("id") or "").strip()
        else:
            document_id = str(item or "").strip()
        if document_id and document_id in shortlist_docs and document_id not in selected_documents:
            selected_documents.append(document_id)
    selected_documents = selected_documents[:6]

    anchor_ids: list[str] = []
    for item in payload.get("anchor_ids") or []:
        if isinstance(item, dict):
            anchor_id = str(item.get("anchor_id") or item.get("id") or "").strip()
        else:
            anchor_id = str(item or "").strip()
        if anchor_id and anchor_id in shortlist_anchor_ids and anchor_id not in anchor_ids:
            anchor_ids.append(anchor_id)

    if not selected_documents and anchor_ids:
        anchor_to_document = {
            str(anchor.get("id") or "").strip(): str(anchor.get("document_id") or "").strip()
            for anchor in bundle.source_anchors
            if str(anchor.get("id") or "").strip()
        }
        for anchor_id in anchor_ids:
            document_id = anchor_to_document.get(anchor_id)
            if document_id and document_id in shortlist_docs and document_id not in selected_documents:
                selected_documents.append(document_id)
        selected_documents = selected_documents[:6]

    if not selected_documents:
        raise ValueError("llm_rerank_v3 returned no valid shortlist documents.")

    for document_id in selected_documents:
        for anchor_id in document_anchor_ids.get(document_id, []):
            if anchor_id not in anchor_ids:
                anchor_ids.append(anchor_id)
            if len(anchor_ids) >= 8:
                break
        if len(anchor_ids) >= 8:
            break
    anchor_ids = anchor_ids[:8]
    if not anchor_ids:
        raise ValueError("llm_rerank_v3 returned no valid anchor ids.")

    selected_doc_payload = []
    for document_id in selected_documents:
        document = shortlist_docs[document_id]
        selected_doc_payload.append(
            {
                "document_id": document_id,
                "title": document.get("title"),
                "family_id": document.get("family_id"),
                "family_label": document.get("family_label"),
                "score": document.get("score"),
                "reasons": document.get("reasons"),
            }
        )
    return {
        "selected_documents": selected_doc_payload,
        "anchor_ids": anchor_ids,
        "selection_reason": _trim_text(payload.get("selection_reason"), 220),
        "rerank_notes": _normalize_string_list(payload.get("rerank_notes"), limit=6),
    }


def _selected_anchor_records_v3(bundle: StoneWritingAnalysisBundle, rerank: dict[str, Any]) -> list[dict[str, Any]]:
    selected_ids = set(rerank.get("anchor_ids") or [])
    anchors = [item for item in bundle.source_anchors if item.get("id") in selected_ids]
    if anchors:
        return anchors[:8]
    return bundle.source_anchors[:8]


def _normalize_style_packet_v3(
    payload: dict[str, Any],
    *,
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
) -> dict[str, Any]:
    selected_docs = rerank.get("selected_documents") or []
    family_labels = _unique_preserve_order([item.get("family_label") for item in selected_docs if isinstance(item, dict)])
    return {
        "entry_scene": str(payload.get("entry_scene") or request_adapter.get("entry_scene") or "").strip()
        or "从一个具体物件或动作进入。",
        "felt_cost": str(payload.get("felt_cost") or request_adapter.get("felt_cost") or "").strip()
        or "把压力翻译成能被身体感到的代价。",
        "value_lens": str(payload.get("value_lens") or request_adapter.get("value_lens") or "").strip() or "cost",
        "judgment_mode": str(payload.get("judgment_mode") or request_adapter.get("judgment_mode") or "").strip()
        or "稳住判断，不要解释过量",
        "distance": str(payload.get("distance") or request_adapter.get("distance") or "").strip() or "回收式第一人称",
        "family_labels": family_labels[:6],
        "lexicon_keep": _unique_preserve_order(
            [
                *_normalize_string_list(payload.get("lexicon_keep"), limit=8),
                *_normalize_string_list((bundle.author_model.get("author_core") or {}).get("signature_motifs"), limit=6),
            ]
        )[:8],
        "motif_obligations": _unique_preserve_order(
            [
                *_normalize_string_list(payload.get("motif_obligations"), limit=6),
                *_normalize_string_list(request_adapter.get("motif_terms"), limit=6),
            ]
        )[:6],
        "syntax_rules": _normalize_string_list(payload.get("syntax_rules"), limit=6)
        or _normalize_string_list(bundle.author_model.get("stable_moves"), limit=6),
        "structure_recipe": _normalize_string_list(payload.get("structure_recipe"), limit=8)
        or [
            "从一个具体动作进入。",
            "通过可见细节把压力往前推。",
            "让结尾留下余味。",
        ],
        "do_not_do": _unique_preserve_order(
            [
                *_normalize_string_list(payload.get("do_not_do"), limit=8),
                *_normalize_string_list(bundle.author_model.get("forbidden_moves"), limit=8),
            ]
        )[:8],
        "anchor_ids": list(rerank.get("anchor_ids") or [])[:8],
        "style_thesis": _trim_text(payload.get("style_thesis"), 220),
    }


def _normalize_blueprint_v3(
    payload: dict[str, Any],
    state: WritingStreamState,
    style_packet: dict[str, Any],
) -> dict[str, Any]:
    paragraph_count = _clamp_int(
        payload.get("paragraph_count"),
        default=_default_paragraph_count(state.target_word_count),
        minimum=2,
        maximum=6,
    )
    anchor_ids = _unique_preserve_order(
        [*_normalize_string_list(payload.get("anchor_ids"), limit=8), *(style_packet.get("anchor_ids") or [])]
    )[:8]
    return {
        "paragraph_count": paragraph_count,
        "shape_note": str(payload.get("shape_note") or "").strip() or "通过选中的原型动作建立克制的压力。",
        "entry_move": str(payload.get("entry_move") or style_packet.get("entry_scene") or "").strip()
        or "从一个可见动作起笔。",
        "development_move": str(payload.get("development_move") or "").strip()
        or "让压力沿着反复出现的具体细节慢慢抬升。",
        "turning_device": str(payload.get("turning_device") or "").strip() or "轻微转向，不要上纲成论点",
        "closure_residue": str(payload.get("closure_residue") or "").strip()
        or "收在余味上，不要写成总结。",
        "keep_terms": _unique_preserve_order(
            [*_normalize_string_list(payload.get("keep_terms"), limit=8), *(style_packet.get("lexicon_keep") or [])]
        )[:8],
        "motif_obligations": _unique_preserve_order(
            [
                *_normalize_string_list(payload.get("motif_obligations"), limit=6),
                *(style_packet.get("motif_obligations") or []),
            ]
        )[:6],
        "steps": _normalize_string_list(payload.get("steps"), limit=8)
        or list(style_packet.get("structure_recipe") or [])[:8],
        "do_not_do": _unique_preserve_order(
            [*_normalize_string_list(payload.get("do_not_do"), limit=8), *(style_packet.get("do_not_do") or [])]
        )[:8],
        "anchor_ids": anchor_ids,
    }


def _normalize_v3_critic_payload(
    payload: dict[str, Any],
    *,
    critic_key: str,
    selected_anchor_ids: list[str],
) -> dict[str, Any]:
    allowed_ids = set(selected_anchor_ids)
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=8)
        if anchor_id in allowed_ids
    ]
    if not anchor_ids:
        anchor_ids = list(selected_anchor_ids)[:3]
    verdict = str(payload.get("verdict") or "").strip()
    if verdict not in {"approve", "line_edit", "redraft"}:
        verdict = "approve" if payload.get("pass", True) else "line_edit"
    return {
        "critic_key": critic_key,
        "critic_label": _critic_spec_v2(critic_key)["label"],
        "pass": bool(payload.get("pass", verdict == "approve")),
        "score": _clamp_score(payload.get("score"), default=0.72 if verdict == "approve" else 0.58),
        "verdict": verdict,
        "anchor_ids": anchor_ids,
        "matched_signals": _normalize_string_list(payload.get("matched_signals"), limit=5),
        "must_keep_spans": _normalize_string_list(payload.get("must_keep_spans"), limit=4),
        "line_edits": _normalize_string_list(payload.get("line_edits"), limit=6),
        "redraft_reason": str(payload.get("redraft_reason") or "").strip(),
        "risks": _normalize_string_list(payload.get("risks"), limit=4),
    }


def _build_v3_author_floor(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    return {
        "author_core": dict(bundle.author_model.get("author_core") or {}),
        "stable_moves": list(bundle.author_model.get("stable_moves") or [])[:8],
        "forbidden_moves": list(bundle.author_model.get("forbidden_moves") or [])[:8],
        "critic_rubrics": dict(bundle.author_model.get("critic_rubrics") or {}),
    }


def _build_v3_draft_guardrails(style_packet: dict[str, Any], blueprint: dict[str, Any]) -> dict[str, Any]:
    return {
        "language_constraint": "全文必须使用简体中文，必要专有名词或用户明确要求保留的引用除外。",
        "entry_scene": style_packet.get("entry_scene"),
        "felt_cost": style_packet.get("felt_cost"),
        "motif_obligations": list(blueprint.get("motif_obligations") or [])[:6],
        "movement_steps": list(blueprint.get("steps") or [])[:8],
        "negative_constraints": list(style_packet.get("do_not_do") or [])[:8],
    }


def _build_v3_line_edit_brief(
    draft: str,
    critics: list[dict[str, Any]],
    blueprint: dict[str, Any],
    target_word_count: int,
) -> dict[str, Any]:
    return {
        "target_word_count": target_word_count,
        "current_word_count": estimate_word_count(draft),
        "language_constraint": "保留全文简体中文；不要改成英文或中英混写。",
        "must_keep_spans": _unique_preserve_order(
            [span for critic in critics for span in (critic.get("must_keep_spans") or [])]
        )[:6],
        "line_edits": _unique_preserve_order(
            [edit for critic in critics for edit in (critic.get("line_edits") or [])]
        )[:8],
        "shape_note": blueprint.get("shape_note"),
        "closure_residue": blueprint.get("closure_residue"),
    }


def _revision_action_v3(critics: list[dict[str, Any]]) -> str:
    if any(str(item.get("verdict") or "") == "redraft" for item in critics):
        return "redraft"
    if any((not item.get("pass")) or item.get("line_edits") for item in critics):
        return "line_edit"
    return "none"


def _stone_json_chinese_instruction(*, preserve_tokens: str | None = None) -> str:
    base = (
        "除固定枚举、ID 与必须原样复用的来源 token 外，"
        "JSON 里的所有自然语言字符串都必须使用简体中文。"
    )
    if preserve_tokens:
        return f"{base}\n以下字段或 token 需要保留原格式：{preserve_tokens}。"
    return base


_STONE_BODY_CHINESE_ONLY = (
    "正文必须只使用自然、完整的简体中文。\n"
    "不要输出英文句子、双语复述、提示词字段名或分析术语。\n"
    "除非用户明确要求，只有无法翻译的专有名词或引用才保留原文。"
)


def _render_request_adapter_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"desired_length_band: {payload.get('desired_length_band') or ''}",
        f"surface_form: {payload.get('surface_form') or ''}",
        f"value_lens: {payload.get('value_lens') or ''}",
        f"judgment_mode: {payload.get('judgment_mode') or ''}",
        f"distance: {payload.get('distance') or ''}",
        "",
        f"entry_scene: {payload.get('entry_scene') or ''}",
        f"felt_cost: {payload.get('felt_cost') or ''}",
        "",
        "query_terms:",
        *[f"- {item}" for item in (payload.get("query_terms") or [])[:8]],
    ]
    return "\n".join(lines).strip()


def _render_candidate_shortlist_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"shortlist_size: {payload.get('shortlist_size') or 0}",
        f"desired_length_band: {payload.get('desired_length_band') or ''}",
        f"surface_form: {payload.get('surface_form') or ''}",
        "",
        "documents:",
    ]
    for item in (payload.get("documents") or [])[:12]:
        lines.extend(
            [
                f"- {item.get('title') or item.get('document_id') or 'document'}",
                f"  family: {item.get('family_label') or item.get('family_id') or ''}",
                f"  score: {round(float(item.get('score') or 0.0), 2)}",
                f"  reasons: {', '.join(item.get('reasons') or [])}",
            ]
        )
    return "\n".join(lines).strip()


def _render_rerank_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"selected_documents: {len(payload.get('selected_documents') or [])}",
        f"selected_anchors: {len(payload.get('anchor_ids') or [])}",
    ]
    if payload.get("selection_reason"):
        lines.extend(["", f"selection_reason: {payload.get('selection_reason')}"])
    lines.extend(["", "documents:"])
    for item in payload.get("selected_documents") or []:
        lines.append(
            f"- {item.get('title') or item.get('document_id') or 'document'} | "
            f"{item.get('family_label') or item.get('family_id') or ''}"
        )
    return "\n".join(lines).strip()


def _render_style_packet_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"entry_scene: {payload.get('entry_scene') or ''}",
        f"felt_cost: {payload.get('felt_cost') or ''}",
        f"value_lens: {payload.get('value_lens') or ''}",
        f"distance: {payload.get('distance') or ''}",
        "",
        "family_labels:",
        *[f"- {item}" for item in (payload.get("family_labels") or [])[:6]],
        "",
        "motif_obligations:",
        *[f"- {item}" for item in (payload.get("motif_obligations") or [])[:6]],
        "",
        "lexicon_keep:",
        *[f"- {item}" for item in (payload.get("lexicon_keep") or [])[:8]],
    ]
    return "\n".join(lines).strip()


def _render_blueprint_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"paragraph_count: {payload.get('paragraph_count') or 0}",
        f"shape_note: {payload.get('shape_note') or ''}",
        f"entry_move: {payload.get('entry_move') or ''}",
        f"development_move: {payload.get('development_move') or ''}",
        f"turning_device: {payload.get('turning_device') or ''}",
        f"closure_residue: {payload.get('closure_residue') or ''}",
        "",
        "steps:",
        *[f"- {item}" for item in (payload.get("steps") or [])[:8]],
    ]
    return "\n".join(lines).strip()


def _collect_v3_trace_anchor_ids(
    bundle: StoneWritingAnalysisBundle,
    rerank: dict[str, Any],
    style_packet: dict[str, Any],
    blueprint: dict[str, Any],
    revision_rounds: list[dict[str, Any]],
) -> list[str]:
    values: list[str] = []
    values.extend(_available_anchor_ids(bundle)[:12])
    values.extend(rerank.get("anchor_ids") or [])
    values.extend(style_packet.get("anchor_ids") or [])
    values.extend(blueprint.get("anchor_ids") or [])
    for round_payload in revision_rounds:
        for critic in round_payload.get("critics") or []:
            values.extend(critic.get("anchor_ids") or [])
    return _unique_preserve_order(values)


def _build_trace_blocks_v3(
    analysis_bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    shortlist: dict[str, Any],
    rerank: dict[str, Any],
    style_packet: dict[str, Any],
    blueprint: dict[str, Any],
    revision_rounds: list[dict[str, Any]],
    revision_action: str,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "generation_packet",
            "label": f"Stone v3 baseline ready ({analysis_bundle.version_label})",
            "baseline": analysis_bundle.generation_packet.get("baseline", {}),
        },
        {
            "type": "stage",
            "stage": "request_adapter_v3",
            "label": "Request adapted into author space",
            "query_terms": request_adapter.get("query_terms") or [],
        },
        {
            "type": "stage",
            "stage": "candidate_shortlist_v3",
            "label": "Rule shortlist prepared",
            "candidate_count": len(shortlist.get("documents") or []),
        },
        {
            "type": "stage",
            "stage": "llm_rerank_v3",
            "label": "LLM rerank finalized evidence",
            "anchor_ids": rerank.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "style_packet_v3",
            "label": "Style packet ready",
            "anchor_ids": style_packet.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "blueprint_v3",
            "label": "Blueprint ready",
            "anchor_ids": blueprint.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "draft_v3",
            "label": "First draft completed",
        },
    ]
    for round_payload in revision_rounds:
        blocks.append(
            {
                "type": "revision_round",
                "round": round_payload.get("round"),
                "stage": round_payload.get("stage"),
                "revision_action": round_payload.get("revision_action"),
                "word_count": round_payload.get("word_count"),
                "critic_count": len(round_payload.get("critics") or []),
            }
        )
    blocks.append(
        {
            "type": "stage",
            "stage": "revision",
            "label": f"Revision action: {revision_action}",
        }
    )
    return blocks


def _call_writer_json_stage_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    client: OpenAICompatibleClient | None,
    *,
    stage: str,
    label: str,
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    if not client:
        raise WritingPipelineError(stage, f"{label} requires a configured writing model.")
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = client.chat_completion_result(
                messages,
                model=client.config.model,
                temperature=0.2,
                max_tokens=2200,
            )
            payload = parse_json_response(response.content, fallback=True)
            if not isinstance(payload, dict):
                raise ValueError(f"{stage} did not return a JSON object.")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 3:
                self._emit_live_writer_message(
                    state,
                    message_kind=stage,
                    label=f"{label} retry {attempt + 1}",
                    body=f"{label} retrying after attempt {attempt} failed: {_trim_text(exc, 160)}",
                    stage=stage,
                    stream_key=self._stream_key(state, stage),
                    render_mode="plain",
                )
    raise WritingPipelineError(stage, f"{label} failed after 3 attempts: {last_error}")


def _call_writer_text_stage_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    client: OpenAICompatibleClient | None,
    *,
    stage: str,
    label: str,
    messages: list[dict[str, Any]],
    temperature: float,
) -> str:
    if not client:
        raise WritingPipelineError(stage, f"{label} requires a configured writing model.")
    last_error: Exception | None = None
    for attempt in range(1, 4):
        stream_handler = None
        finalize_stream = lambda: None
        if attempt == 3:
            stream_handler, finalize_stream = self._make_stage_stream_handler(
                state,
                message_kind=stage,
                label=label,
                stage=stage,
                stream_key=self._stream_key(state, stage),
            )
        try:
            response = client.chat_completion_result(
                messages,
                model=client.config.model,
                temperature=temperature,
                max_tokens=None,
                stream_handler=stream_handler,
            )
            finalize_stream()
            candidate = _clean_model_text(response.content)
            if not candidate:
                raise ValueError(f"{stage} returned empty text.")
            if _contains_banned_meta(candidate):
                raise ValueError(f"{stage} leaked backstage prompt language.")
            return _light_trim_to_word_count(candidate, state.target_word_count)
        except Exception as exc:  # noqa: BLE001
            finalize_stream()
            last_error = exc
            if attempt < 3:
                self._emit_live_writer_message(
                    state,
                    message_kind=stage,
                    label=f"{label} retry {attempt + 1}",
                    body=f"{label} retrying after attempt {attempt} failed: {_trim_text(exc, 160)}",
                    stage=stage,
                    stream_key=self._stream_key(state, stage),
                    render_mode="plain",
                )
    raise WritingPipelineError(stage, f"{label} failed after 3 attempts: {last_error}")


def _resolve_analysis_bundle_v3(self: WritingAgentService, session, project_id: str) -> StoneWritingAnalysisBundle:
    from app.service.stone.assets_support import STONE_V3_PROFILE_KEY

    project = repository.get_project(session, project_id)
    if not project:
        raise ValueError("Project not found.")
    preprocess_run = get_latest_usable_stone_preprocess_run(
        session,
        project_id,
        profile_key=STONE_V3_PROFILE_KEY,
    )
    if preprocess_run:
        stone_profiles = _load_stone_profiles_v3(session, project_id)
        author_model = _load_v3_asset_payload(session, project_id, asset_kind="stone_author_model_v3")
        prototype_index = _load_v3_asset_payload(session, project_id, asset_kind="stone_prototype_index_v3")
        if stone_profiles and author_model and prototype_index:
            source_anchors = _build_source_anchors_v3(prototype_index)
            version_label = (
                f"preprocess {preprocess_run.created_at.isoformat(timespec='minutes')}"
                if preprocess_run.created_at
                else "latest"
            )
            bundle = StoneWritingAnalysisBundle(
                run_id=preprocess_run.id,
                source="stone_v3_baseline",
                version_label=version_label,
                target_role=project.name,
                analysis_context="stone_v3_preprocess",
                facets=[],
                prompt_text="",
                stone_profiles=stone_profiles,
                source_anchors=source_anchors,
                author_model=author_model,
                prototype_index=prototype_index,
                short_text_clusters=[],
            )
            bundle.prompt_text = _build_analysis_prompt_text_v3(bundle)
            bundle.generation_packet = _build_generation_packet_v3(bundle)
            return bundle

    raise ValueError("No Stone v3 baseline is available yet. Run Stone preprocess and generate the v3 baseline first.")


def _review_with_v3_critic(
    self: WritingAgentService,
    state: WritingStreamState,
    critic_key: str,
    draft: str,
    analysis_bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    style_packet: dict[str, Any],
    blueprint: dict[str, Any],
    client: OpenAICompatibleClient | None,
    *,
    round_index: int = 1,
) -> dict[str, Any]:
    spec = _critic_spec_v2(critic_key)
    if not client:
        raise WritingPipelineError("critic", f"{spec['label']} critic requires a configured writing model.")
    stage_name = critic_key
    label = f"{spec['label']} critic" if round_index == 1 else f"{spec['label']} critic round {round_index}"
    stream_handler, finalize_stream = self._make_stage_stream_handler(
        state,
        message_kind="critic",
        label=label,
        stage=stage_name,
        stream_key=self._stream_key(state, stage_name, suffix=critic_key),
        actor_name=spec["label"],
        actor_id=f"critic-{critic_key}",
        actor_role="critic",
    )
    try:
        response = client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        f"You are the Stone v3 {spec['label']} critic.\n"
                        "Review only this dimension.\n"
                        "Anchor every judgment in the selected v3 evidence.\n"
                        f"{_stone_json_chinese_instruction(preserve_tokens='verdict, anchor_ids, quoted draft spans')}\n"
                        "Return JSON only."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"critic focus: {spec['focus']}\n\n"
                        f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        f"llm_rerank_v3 JSON:\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                        f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"blueprint_v3 JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                        f"critic_rubric JSON:\n{json.dumps((analysis_bundle.author_model.get('critic_rubrics') or {}).get(critic_key) or [], ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                        f"Draft:\n{draft}\n\n"
                        "除 `verdict`、`anchor_ids` 与直接引用正文片段外，其余字段请用简体中文填写。\n"
                        "Return JSON:\n"
                        "{\n"
                        '  "pass": boolean,\n'
                        '  "score": number,\n'
                        '  "verdict": "approve|line_edit|redraft",\n'
                        '  "anchor_ids": ["selected anchor ids"],\n'
                        '  "matched_signals": ["命中的信号，简体中文"],\n'
                        '  "must_keep_spans": ["必须保留的正文片段，直接引用原文"],\n'
                        '  "line_edits": ["引用要修改的句子，并用简体中文说明怎么改"],\n'
                        '  "redraft_reason": "需要整篇重写的原因，简体中文",\n'
                        '  "risks": ["剩余风险，简体中文"]\n'
                        "}"
                    ),
                },
            ],
            model=client.config.model,
            temperature=0.12,
            max_tokens=1400,
            stream_handler=stream_handler,
        )
    except Exception as exc:
        finalize_stream()
        raise WritingPipelineError(stage_name, f"{spec['label']} critic failed: {exc}") from exc
    finalize_stream()
    payload = parse_json_response(response.content, fallback=True)
    return _normalize_v3_critic_payload(
        payload if isinstance(payload, dict) else {},
        critic_key=critic_key,
        selected_anchor_ids=list(rerank.get("anchor_ids") or []),
    )


def _run_v3_critics(
    self: WritingAgentService,
    state: WritingStreamState,
    analysis_bundle: StoneWritingAnalysisBundle,
    draft: str,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    style_packet: dict[str, Any],
    blueprint: dict[str, Any],
    client: OpenAICompatibleClient | None,
    *,
    round_index: int = 1,
) -> list[dict[str, Any]]:
    critics: list[dict[str, Any]] = []
    for critic_key in ("formal_fidelity", "worldview_translation", "syntheticness"):
        critics.append(
            _review_with_v3_critic(
                self,
                state,
                critic_key,
                draft,
                analysis_bundle,
                request_adapter,
                rerank,
                style_packet,
                blueprint,
                client,
                round_index=round_index,
            )
        )
    return critics


def _run_llm_first_pipeline_v3(
    self: WritingAgentService,
    session,
    state: WritingStreamState,
    *,
    analysis_bundle: StoneWritingAnalysisBundle,
    client: OpenAICompatibleClient,
) -> None:
    revision_rounds: list[dict[str, Any]] = []

    self._emit_live_writer_message(
        state,
        message_kind="request_adapter_v3",
        label="Request adapter v3",
        body="Adapting the request into the author's value lens, distance, and entry move...",
        stage="request_adapter_v3",
        stream_key=self._stream_key(state, "request_adapter_v3"),
    )
    request_adapter_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="request_adapter_v3",
        label="Request adapter v3",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 request adapter.\n"
                    "Translate the writing request into the author's world before drafting.\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='desired_length_band, surface_form, anchor_preferences')}\n"
                    "Return JSON only."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"author_model_v3 JSON:\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"prototype_families JSON:\n{json.dumps((analysis_bundle.author_model.get('family_map') or [])[:8], ensure_ascii=False, indent=2)}\n\n"
                    "除固定枚举字段外，其余自然语言字段请用简体中文填写。\n\n"
                    "Return JSON:\n"
                    "{\n"
                        '  "desired_length_band": "micro|short|medium|long",\n'
                        '  "surface_form": "scene_vignette|rant|confession|anecdote|aphorism|dialogue_bit|manifesto|list_bit",\n'
                        '  "value_lens": "中文短标签",\n'
                        '  "judgment_mode": "中文短标签",\n'
                        '  "distance": "中文短标签",\n'
                        '  "entry_scene": "作品如何起笔，用简体中文",\n'
                        '  "felt_cost": "压力应该以什么体感落地，用简体中文",\n'
                        '  "query_terms": ["检索词，优先简体中文"],\n'
                        '  "motif_terms": ["要召回的母题，优先简体中文"],\n'
                        '  "anchor_preferences": ["opening|pivot|closing|signature"],\n'
                        '  "hard_constraints": ["约束条件，简体中文"],\n'
                        '  "reasoning": "简短理由，简体中文"\n'
                    "}"
                ),
            },
        ],
    )
    request_adapter = _normalize_request_adapter_v3(request_adapter_raw, state, analysis_bundle)
    request_adapter_payload = _build_writer_message_payload(
        message_kind="request_adapter_v3",
        label="Request adapter v3",
        body=_render_request_adapter_v3(request_adapter),
        detail=request_adapter,
        stage="request_adapter_v3",
        stream_key=self._stream_key(state, "request_adapter_v3"),
    )
    self._emit(state, "stage", request_adapter_payload)

    shortlist = _build_candidate_shortlist_v3(analysis_bundle, request_adapter)
    if not shortlist.get("documents"):
        raise WritingPipelineError("candidate_shortlist_v3", "No prototype candidates were available for v3 reranking.")
    shortlist_payload = _build_writer_message_payload(
        message_kind="candidate_shortlist_v3",
        label="Candidate shortlist v3",
        body=_render_candidate_shortlist_v3(shortlist),
        detail=shortlist,
        stage="candidate_shortlist_v3",
        stream_key=self._stream_key(state, "candidate_shortlist_v3"),
    )
    self._emit(state, "stage", shortlist_payload)

    self._emit_live_writer_message(
        state,
        message_kind="llm_rerank_v3",
        label="LLM rerank v3",
        body="Reranking the shortlist with the v3 author model and evidence budget...",
        stage="llm_rerank_v3",
        stream_key=self._stream_key(state, "llm_rerank_v3"),
    )
    rerank_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="llm_rerank_v3",
        label="LLM rerank v3",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 reranker.\n"
                    "Pick the final exemplar documents and anchor ids from the shortlist.\n"
                    "The shortlist is only for prompt size control; your choice is final.\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='selected_documents, anchor_ids')}\n"
                    "Return JSON only."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"author_model_v3 JSON:\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"prototype_index_v3.retrieval_policy JSON:\n{json.dumps(analysis_bundle.prototype_index.get('retrieval_policy') or {}, ensure_ascii=False, indent=2)}\n\n"
                    f"candidate_shortlist_v3 JSON:\n{json.dumps(_compact_shortlist_for_prompt_v3(shortlist), ensure_ascii=False, indent=2)}\n\n"
                    "除 document id 与 anchor id 外，其余说明请用简体中文填写。\n\n"
                    "Return JSON:\n"
                    "{\n"
                    '  "selected_documents": ["document ids"],\n'
                    '  "anchor_ids": ["anchor ids"],\n'
                    '  "selection_reason": "简短中文理由",\n'
                    '  "rerank_notes": ["补充说明，简体中文"]\n'
                    "}"
                ),
            },
        ],
    )
    rerank = _normalize_rerank_v3(rerank_raw, analysis_bundle, shortlist)
    rerank_payload = _build_writer_message_payload(
        message_kind="llm_rerank_v3",
        label="LLM rerank v3",
        body=_render_rerank_v3(rerank),
        detail=rerank,
        stage="llm_rerank_v3",
        stream_key=self._stream_key(state, "llm_rerank_v3"),
    )
    self._emit(state, "stage", rerank_payload)

    self._emit_live_writer_message(
        state,
        message_kind="style_packet_v3",
        label="Style packet v3",
        body="Compressing selected evidence into a drafting packet...",
        stage="style_packet_v3",
        stream_key=self._stream_key(state, "style_packet_v3"),
    )
    style_packet_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="style_packet_v3",
        label="Style packet v3",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 style packet builder.\n"
                    "Turn selected exemplars into a compact drafting packet.\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='family_labels, anchor_ids')}\n"
                    "Return JSON only."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"llm_rerank_v3 JSON:\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    f"author_model_v3 JSON:\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    "除复用的 family label / anchor id 外，其余自然语言字段请用简体中文填写。\n\n"
                    "Return JSON:\n"
                    "{\n"
                    '  "entry_scene": "起笔方向，简体中文",\n'
                    '  "felt_cost": "体感代价，简体中文",\n'
                    '  "value_lens": "中文短标签",\n'
                    '  "judgment_mode": "中文短标签",\n'
                    '  "distance": "中文短标签",\n'
                    '  "family_labels": ["family labels"],\n'
                    '  "lexicon_keep": ["要保留的词，简体中文"],\n'
                    '  "motif_obligations": ["必须落地的母题，简体中文"],\n'
                    '  "syntax_rules": ["句法规则，简体中文"],\n'
                    '  "structure_recipe": ["结构步骤，简体中文"],\n'
                    '  "do_not_do": ["需要避开的漂移，简体中文"],\n'
                    '  "style_thesis": "简短说明，简体中文"\n'
                    "}"
                ),
            },
        ],
    )
    style_packet = _normalize_style_packet_v3(
        style_packet_raw,
        bundle=analysis_bundle,
        request_adapter=request_adapter,
        rerank=rerank,
    )
    style_packet_payload = _build_writer_message_payload(
        message_kind="style_packet_v3",
        label="Style packet v3",
        body=_render_style_packet_v3(style_packet),
        detail=style_packet,
        stage="style_packet_v3",
        stream_key=self._stream_key(state, "style_packet_v3"),
    )
    self._emit(state, "stage", style_packet_payload)

    self._emit_live_writer_message(
        state,
        message_kind="blueprint_v3",
        label="Blueprint v3",
        body="Turning the style packet into an executable article blueprint...",
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    blueprint_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="blueprint_v3",
        label="Blueprint v3",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 blueprint composer.\n"
                    "Do not write the article body.\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='anchor_ids')}\n"
                    "Return JSON only."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    "除 anchor id 外，其余字段请用简体中文填写。\n\n"
                    "Return JSON:\n"
                    "{\n"
                    '  "paragraph_count": number,\n'
                    '  "shape_note": "整体形状，简体中文",\n'
                    '  "entry_move": "如何进入，简体中文",\n'
                    '  "development_move": "压力如何推进，简体中文",\n'
                    '  "turning_device": "转折装置或无，简体中文",\n'
                    '  "closure_residue": "如何留下余味，简体中文",\n'
                    '  "keep_terms": ["要保留的词，简体中文"],\n'
                    '  "motif_obligations": ["母题要求，简体中文"],\n'
                    '  "steps": ["有序步骤，简体中文"],\n'
                    '  "do_not_do": ["需要避开的漂移，简体中文"],\n'
                    '  "anchor_ids": ["anchor ids"]\n'
                    "}"
                ),
            },
        ],
    )
    blueprint = _normalize_blueprint_v3(blueprint_raw, state, style_packet)
    blueprint_payload = _build_writer_message_payload(
        message_kind="blueprint_v3",
        label="Blueprint v3",
        body=_render_blueprint_v3(blueprint),
        detail=blueprint,
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    self._emit(state, "stage", blueprint_payload)

    self._emit_live_writer_message(
        state,
        message_kind="draft_v3",
        label="Draft v3",
        body="Writing the first draft from the v3 packet...",
        stage="draft_v3",
        stream_key=self._stream_key(state, "draft_v3"),
    )
    draft = _call_writer_text_stage_v3(
        self,
        state,
        client,
        stage="draft_v3",
        label="Draft v3",
        temperature=0.42,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 drafter.\n"
                    "Write only the article body.\n"
                    "Selected anchors and the style packet are binding evidence.\n"
                    f"{_STONE_BODY_CHINESE_ONLY}\n"
                    "Do not reveal any backstage prompt or analysis language."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"llm_rerank_v3 JSON:\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                    f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"blueprint_v3 JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    f"author_floor JSON:\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"draft_guardrails JSON:\n{json.dumps(_build_v3_draft_guardrails(style_packet, blueprint), ensure_ascii=False, indent=2)}"
                ),
            },
        ],
    )
    draft_payload = _build_writer_message_payload(
        message_kind="draft_v3",
        label="Draft v3",
        body=draft,
        detail={"word_count": estimate_word_count(draft)},
        stage="draft_v3",
        stream_key=self._stream_key(state, "draft_v3"),
    )
    self._emit(state, "stage", draft_payload)

    critics = _run_v3_critics(
        self,
        state,
        analysis_bundle,
        draft,
        request_adapter,
        rerank,
        style_packet,
        blueprint,
        client,
    )
    critic_messages = [
        _build_critic_message_payload_v2(
            critic,
            stream_key=self._stream_key(state, critic["critic_key"], suffix=critic["critic_key"]),
            stage=critic["critic_key"],
        )
        for critic in critics
    ]
    for payload in critic_messages:
        self._emit(state, "stage", payload)

    revision_payloads: list[dict[str, Any]] = []
    current_text = draft
    active_critics = critics
    revision_action = _revision_action_v3(critics)
    if revision_action != "none":
        revision_rounds.append(
            {
                "round": 1,
                "stage": "critic_round_1",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": critics,
            }
        )
    if revision_action == "redraft":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="redraft",
            label="Redraft",
            temperature=0.4,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are the Stone v3 redrafter.\n"
                        "Discard the weak draft and write a new article from the same v3 packet.\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "Use critic feedback only to avoid drift."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"request_adapter_v3 JSON:\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"blueprint_v3 JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                        f"critic feedback JSON:\n{json.dumps(critics, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        redraft_payload = _build_writer_message_payload(
            message_kind="redraft",
            label="Redraft",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft"},
            stage="redraft",
            stream_key=self._stream_key(state, "redraft"),
        )
        revision_payloads.append(redraft_payload)
        self._emit(state, "stage", redraft_payload)
    elif revision_action == "line_edit":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="line_edit",
            label="Line edit",
            temperature=0.15,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are the Stone v3 line editor.\n"
                        "Keep the piece's structure and closure energy.\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "Edit only the weak, synthetic, or drifting sentences."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"Current draft:\n{current_text}\n\n"
                        f"line_edit_brief JSON:\n{json.dumps(_build_v3_line_edit_brief(current_text, critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                        f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON:\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        line_edit_payload = _build_writer_message_payload(
            message_kind="line_edit",
            label="Line edit",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit"},
            stage="line_edit",
            stream_key=self._stream_key(state, "line_edit"),
        )
        revision_payloads.append(line_edit_payload)
        self._emit(state, "stage", line_edit_payload)

    if revision_action != "none" and _should_run_second_critic_round_v2(active_critics):
        round_two_critics = _run_v3_critics(
            self,
            state,
            analysis_bundle,
            current_text,
            request_adapter,
            rerank,
            style_packet,
            blueprint,
            client,
            round_index=2,
        )
        round_two_messages = [
            _build_critic_message_payload_v2(
                critic,
                stream_key=self._stream_key(state, f"{critic['critic_key']}_round_2", suffix=critic["critic_key"]),
                stage=critic["critic_key"],
                label_suffix=" round 2",
            )
            for critic in round_two_critics
        ]
        for payload in round_two_messages:
            self._emit(state, "stage", payload)
        revision_rounds.append(
            {
                "round": 2,
                "stage": "critic_round_2",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": round_two_critics,
            }
        )
        active_critics = round_two_critics
        second_action = _revision_action_v3(round_two_critics)
        if second_action == "redraft":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="redraft",
                label="Redraft round 2",
                temperature=0.38,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are the Stone v3 redrafter. Rewrite the article body only.\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint_v3 JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"critic feedback JSON:\n{json.dumps(round_two_critics, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="redraft",
                label="Redraft round 2",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft_round_2"},
                stage="redraft",
                stream_key=self._stream_key(state, "redraft_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "redraft"
        elif second_action == "line_edit":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="line_edit",
                label="Line edit round 2",
                temperature=0.14,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are the Stone v3 line editor. Return only the article body.\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Current draft:\n{current_text}\n\n"
                            f"line_edit_brief JSON:\n{json.dumps(_build_v3_line_edit_brief(current_text, round_two_critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                            f"style_packet_v3 JSON:\n{json.dumps(style_packet, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="line_edit",
                label="Line edit round 2",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit_round_2"},
                stage="line_edit",
                stream_key=self._stream_key(state, "line_edit_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "line_edit"
        else:
            revision_action = second_action
    final_text = current_text
    final_assessment = _build_final_assessment_v2(
        final_text,
        active_critics,
        state.topic,
        state.target_word_count,
        revision_action=revision_action,
    )
    final_payload = _build_writer_message_payload(
        message_kind="final",
        label="Final",
        body=final_text,
        detail={"word_count": estimate_word_count(final_text), "final_assessment": final_assessment},
        stage="final",
        stream_key=self._stream_key(state, "final"),
    )
    self._emit(state, "stage", final_payload)

    timeline = [
        request_adapter_payload,
        shortlist_payload,
        rerank_payload,
        style_packet_payload,
        blueprint_payload,
        draft_payload,
        *critic_messages,
        *revision_payloads,
        final_payload,
    ]
    if revision_rounds and "round_two_messages" in locals():
        timeline = [
            request_adapter_payload,
            shortlist_payload,
            rerank_payload,
            style_packet_payload,
            blueprint_payload,
            draft_payload,
            *critic_messages,
            *round_two_messages,
            *revision_payloads,
            final_payload,
        ]
    trace = {
        "kind": "writing_result",
        "status": "completed",
        "degraded_mode": False,
        "degradation_reasons": [],
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "raw_message": state.raw_message,
        "baseline_source": analysis_bundle.source,
        "analysis_run_id": analysis_bundle.run_id,
        "analysis_version": analysis_bundle.version_label,
        "analysis_target_role": analysis_bundle.target_role,
        "analysis_context": analysis_bundle.analysis_context,
        "analysis_facets": [],
        "generation_packet": analysis_bundle.generation_packet,
        "request_adapter_v3": request_adapter,
        "candidate_shortlist_v3": shortlist,
        "llm_rerank_v3": rerank,
        "style_packet_v3": style_packet,
        "blueprint_v3": blueprint,
        "anchor_ids": _collect_v3_trace_anchor_ids(
            analysis_bundle,
            rerank,
            style_packet,
            blueprint,
            revision_rounds,
        ),
        "blocks": _build_trace_blocks_v3(
            analysis_bundle,
            request_adapter,
            shortlist,
            rerank,
            style_packet,
            blueprint,
            revision_rounds,
            revision_action,
        ),
        "critics": active_critics,
        "revision_rounds": revision_rounds,
        "draft": draft,
        "final_text": final_text,
        "final_assessment": final_assessment,
        "timeline": timeline,
    }
    assistant_turn = repository.add_chat_turn(
        session,
        session_id=state.session_id,
        role="assistant",
        content=final_text,
        trace_json=trace,
    )
    done_payload = {
        **final_payload,
        "assistant_turn_id": assistant_turn.id,
        "baseline_source": analysis_bundle.source,
        "analysis_run_id": analysis_bundle.run_id,
        "review_count": len(active_critics),
        "generation_packet": analysis_bundle.generation_packet.get("baseline", {}),
        "final_assessment": final_assessment,
    }
    self._emit(state, "done", done_payload)


def _run_turn_v3(self: WritingAgentService, session, state: WritingStreamState) -> None:
    self._ensure_stream_active(state)
    project = repository.get_project(session, state.project_id)
    if not project:
        raise ValueError("Project not found.")
    if project.mode != "stone":
        raise ValueError("Only stone projects can use the writing workspace.")

    analysis_bundle = _resolve_analysis_bundle_v3(self, session, state.project_id)
    baseline = dict(analysis_bundle.generation_packet.get("baseline") or {})
    label = "Loaded Stone v3 baseline from preprocess, author model, and prototype index"
    self._emit(
        state,
        "status",
        {
            "stage": "generation_packet",
            "label": label,
            "baseline_source": analysis_bundle.source,
            "analysis_run_id": analysis_bundle.run_id,
            "analysis_version": analysis_bundle.version_label,
            "analysis_target_role": analysis_bundle.target_role,
            "baseline_components": baseline,
        },
    )
    self._emit_live_writer_message(
        state,
        message_kind="generation_packet",
        label="Stone baseline loaded",
        body=f"{label}\nprofiles {len(analysis_bundle.stone_profiles)} + author_model_v3 + prototype_index_v3",
        detail=baseline,
        stage="generation_packet",
        stream_key=self._stream_key(state, "generation_packet"),
        stream_state="complete",
    )

    config = repository.get_service_config(session, "chat_service")
    client = self._build_client(config)
    if not client:
        raise WritingPipelineError("generation_packet", "Writing model is not configured.")

    _run_llm_first_pipeline_v3(
        self,
        session,
        state,
        analysis_bundle=analysis_bundle,
        client=client,
    )


WritingAgentService._resolve_analysis_bundle = _resolve_analysis_bundle_v3
WritingAgentService._run_turn = _run_turn_v3
WritingAgentService._run_llm_first_pipeline_v3 = _run_llm_first_pipeline_v3
