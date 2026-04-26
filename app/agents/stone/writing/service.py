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

from app.analysis.facets import FacetDefinition, get_facets_for_mode
from app.analysis.stone import estimate_word_count, render_writing_request
from app.analysis.writing_guide import build_writing_guide_payload_from_facets
from app.db import Database
from app.llm.client import OpenAICompatibleClient, parse_json_response
from app.runtime_limits import background_task_slot
from app.stone_runtime import get_latest_usable_stone_preprocess_run, load_latest_valid_asset_payload
from app.storage import repository
from app.utils.text import normalize_whitespace
from app.analysis.stone_v3 import compact_stone_profile_v3

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
    request_mode: str = "draft"
    revision_source_turn_id: str | None = None
    revision_source_text: str | None = None
    revision_source_trace: dict[str, Any] = field(default_factory=dict)
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
    evidence_ids: list[str] = field(default_factory=list)
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
    analysis_summary: dict[str, Any] = field(default_factory=dict)
    analysis_ready: bool = False
    writing_guide: dict[str, Any] = field(default_factory=dict)
    guide_source: str = "derived"
    stone_profiles: list[dict[str, Any]] = field(default_factory=list)
    profile_index: dict[str, Any] = field(default_factory=dict)
    profile_slices: list[dict[str, Any]] = field(default_factory=list)
    selected_profile_ids: list[str] = field(default_factory=list)
    source_anchors: list[dict[str, Any]] = field(default_factory=list)
    coverage_warnings: list[str] = field(default_factory=list)
    generation_packet: dict[str, Any] = field(default_factory=dict)
    author_model: dict[str, Any] = field(default_factory=dict)
    prototype_index: dict[str, Any] = field(default_factory=dict)
    writing_packet: dict[str, Any] = field(default_factory=dict)
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
        target_word_count_source: str = "explicit",
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
            revision_source_turn, revision_source_trace = _find_latest_completed_writing_result(session, session_id)
            if (
                revision_source_turn
                and revision_source_trace
                and target_word_count_source != "explicit"
            ):
                inherited_target = int(revision_source_trace.get("target_word_count") or 0)
                if inherited_target >= 100:
                    normalized_target = inherited_target
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
                    "request_mode": "revision" if revision_source_turn and revision_source_trace else "draft",
                    "revision_source_turn_id": revision_source_turn.id if revision_source_turn else None,
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
                request_mode="revision" if revision_source_turn and revision_source_trace else "draft",
                revision_source_turn_id=revision_source_turn.id if revision_source_turn else None,
                revision_source_text=str((revision_source_trace or {}).get("final_text") or "").strip() or None,
                revision_source_trace=dict(revision_source_trace or {}),
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
                        content=f"写作失败，需要重试。\n\n失败阶段：{exc.stage}\n原因：{exc}",
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
                        content=f"写作失败，需要重试。\n\n失败阶段：{failed_stage}\n原因：{exc}",
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

from app.agents.stone.writing.streaming import _build_writer_message_payload, _format_sse
from app.agents.stone.writing.text_utils import _fit_word_count, _light_trim_to_word_count


def _normalize_string_list(value: Any, *, limit: int = 6, item_limit: int | None = None) -> list[str]:
    def _clip(text: str) -> str:
        normalized = str(text or "").strip()
        if item_limit is None or len(normalized) <= item_limit:
            return normalized
        if item_limit <= 3:
            return normalized[:item_limit]
        return f"{normalized[: item_limit - 3]}..."

    if value is None:
        return []
    if isinstance(value, str):
        pieces = re.split(r"[\n;,锛岋紱]+", value)
        return [_clip(piece) for piece in pieces if _clip(piece)][:limit]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_normalize_string_list(item, limit=limit, item_limit=item_limit))
        return _unique_preserve_order(flattened)[:limit]
    if isinstance(value, (list, tuple)):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_normalize_string_list(item, limit=limit, item_limit=item_limit))
        return _unique_preserve_order(flattened)[:limit]
    text = _clip(value)
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
            "focus": "只检查形式保真：开头动作、推进方式、句法压力和收口残响是否像作者本人。",
        },
        "worldview_translation": {
            "label": "worldview_translation",
            "focus": "只检查题目是否被翻进作者的价值镜头、判断对象和代价逻辑，而不只是换皮。",
        },
        "syntheticness": {
            "label": "syntheticness",
            "focus": "只检查这篇是否像拼出来的仿写、像 checklist、像分析结论复述；句子里有没有概念词堆叠、解释味过重或阅读不顺的地方。",
        },
    }
    return mapping.get(critic_key, mapping["formal_fidelity"])


def _critic_spec_v3(critic_key: str) -> dict[str, str]:
    mapping = {
        "feature_density": {
            "label": "特征浓度",
            "focus": "只检查特征浓度是否失衡：高频口癖、结构花样、显性标签和标志性修辞有没有过拟合堆叠，是否像在强行模仿作者。",
        },
        "cross_domain_generalization": {
            "label": "跨域泛化",
            "focus": "只检查题目是否真正被翻译进作者的思维路径和价值镜头，而不是把原作者常见词汇机械套到陌生题材上。",
        },
        "rhythm_entropy": {
            "label": "节奏与信息熵",
            "focus": "只检查节奏、断句、信息密度和文本呼吸感是否接近目标作者，避免句式波形过平、解释味过重或信息熵失真。",
        },
        "extreme_state_handling": {
            "label": "极值状态处理",
            "focus": "只检查情绪极值处的处理方式：在愤怒、悲伤、发呆或自嘲时，是否保留作者惯常的防御机制和降级方式。",
        },
        "ending_landing": {
            "label": "结尾降落",
            "focus": "只检查结尾的降落姿态：是否保住作者习惯的余味、留白或收束方式，避免多解释、硬升华或突兀收尾。",
        },
        "language_fluency": {
            "label": "语言通顺",
            "focus": "只检查句子是否顺滑、语义是否明确、局部是否别扭拧巴或有明显病句，避免为了模仿而牺牲可读性。",
        },
        "logic_flow": {
            "label": "逻辑顺畅",
            "focus": "只检查段落推进、句间衔接和论述顺序是否顺畅，尤其要对照用户修改意见，看改后是否出现新的跳跃、断裂或自相矛盾。",
        },
    }
    return mapping.get(critic_key, mapping["feature_density"])






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
        "label": f"{label}{label_suffix}",
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
        "一个",
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
    splitter_chars = "的了和在是把给跟与及就还都也又并但却而着过地得让将被向对吗啊呀呢"
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
        "让",
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
        "送",
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
        "璇婃柇",
        "鐥呯悊鏍囩",
    )
    return any(item in text for item in banned)


def _light_trim_to_word_count(text: str, target_word_count: int) -> str:
    text = str(text or "").strip()
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
        paragraphs[-1] = last[:-40].rstrip("，。；：!?")
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


_STONE_SIGNAL_REPAIRS = {
    "鍥炴敹": "回收",
    "浠ｄ环": "代价",
    "鎮疆": "悬置",
    "杞诲井杞悜锛屼笉瑕佷笂绾叉垚璁虹偣": "轻微转向，不要上升成论点",
}


def _repair_stone_signal_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _STONE_SIGNAL_REPAIRS.get(text, text)


def _repair_stone_signal_list(value: Any, *, limit: int = 8, item_limit: int | None = None) -> list[str]:
    repaired = [_repair_stone_signal_text(item) for item in _normalize_string_list(value, limit=limit, item_limit=item_limit)]
    return _unique_preserve_order(repaired)[:limit]


def _first_supported_signal_v3(*candidates: tuple[str, Any]) -> tuple[str, str]:
    for source, value in candidates:
        text = _repair_stone_signal_text(value)
        if text:
            return text, source
    return "", "missing"


def _signal_counts_from_profiles_v3(profiles: list[dict[str, Any]], key: str, *, limit: int = 8) -> list[str]:
    counts: Counter[str] = Counter()
    for profile in profiles:
        value = _repair_stone_signal_text(profile.get(key))
        if value:
            counts[value] += 1
    return [value for value, _count in counts.most_common(limit)]


def _value_overlap_v3(left: Any, right: Any) -> bool:
    left_text = normalize_whitespace(_repair_stone_signal_text(left)).lower()
    right_text = normalize_whitespace(_repair_stone_signal_text(right)).lower()
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    return left_text in right_text or right_text in left_text


def _text_match_score_v3(term: str, *fields: Any) -> float:
    token = normalize_whitespace(str(term or "")).lower()
    if not token:
        return 0.0
    best = 0.0
    for field in fields:
        text = normalize_whitespace(str(field or "")).lower()
        if not text:
            continue
        if token == text:
            best = max(best, 2.4)
        elif token in text:
            best = max(best, 1.2)
    return best





def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _find_latest_completed_writing_result(session, session_id: str):
    for turn in reversed(repository.list_chat_turns(session, session_id)):
        if turn.role != "assistant":
            continue
        trace = dict(turn.trace_json or {})
        if trace.get("kind") != "writing_result":
            continue
        if str(trace.get("status") or "") != "completed":
            continue
        final_text = str(trace.get("final_text") or turn.content or "").strip()
        if not final_text:
            continue
        writing_packet = trace.get("writing_packet_v3")
        if not isinstance(writing_packet, dict) or not writing_packet:
            continue
        return turn, trace
    return None, {}

from app.agents.stone.writing.packet_builder import *
from app.agents.stone.writing.bundle_loader import *
from app.agents.stone.writing.critics import *
from app.agents.stone.writing.pipeline import *


WritingAgentService._resolve_analysis_bundle = _resolve_analysis_bundle_v3
WritingAgentService._run_turn = _run_turn_v3
WritingAgentService._run_llm_first_pipeline_v3 = _run_llm_first_pipeline_v3


