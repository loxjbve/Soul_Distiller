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
from app.analysis.stone_v2 import (
    build_short_text_clusters,
    build_stone_author_model_v2,
    build_stone_prototype_index_v2,
    build_short_text_cluster_key,
    expand_stone_profile_v2_for_analysis,
    is_valid_stone_v2_asset_payload,
    normalize_stone_profile_v2,
    render_stone_author_model_markdown,
    render_stone_prototype_index_markdown,
)
from app.analysis.writing_guide import build_writing_guide_payload_from_facets
from app.db import Database
from app.llm.client import OpenAICompatibleClient, parse_json_response
from app.runtime_limits import background_task_slot
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
        self._ensure_stream_active(state)
        project = repository.get_project(session, state.project_id)
        if not project:
            raise ValueError("Project not found.")
        if project.mode != "stone":
            raise ValueError("Only stone projects can use the writing workspace.")

        analysis_bundle = self._resolve_analysis_bundle(session, state.project_id)
        self._emit(
            state,
            "status",
            {
                "stage": "generation_packet",
                "label": "Loaded Stone v2 baseline from preprocess, author model, and prototype index",
                "baseline_source": analysis_bundle.source,
                "analysis_run_id": analysis_bundle.run_id,
                "analysis_version": analysis_bundle.version_label,
                "analysis_target_role": analysis_bundle.target_role,
                "baseline_components": analysis_bundle.generation_packet.get("baseline", {}),
            },
        )
        self._emit_live_writer_message(
            state,
            message_kind="generation_packet",
            label="Stone 基线已载入",
            body=(
                f"已载入 Stone v2 基线：{analysis_bundle.version_label}\n"
                f"profiles {len(analysis_bundle.stone_profiles)} + author_model + prototype_index"
            ),
            detail=analysis_bundle.generation_packet.get("baseline", {}),
            stage="generation_packet",
            stream_key=self._stream_key(state, "generation_packet"),
            stream_state="complete",
        )

        config = repository.get_service_config(session, "chat_service")
        client = self._build_client(config)
        if not client:
            raise WritingPipelineError("generation_packet", "写作模型未配置，不能生成可交付正文。")

        self._emit_live_writer_message(
            state,
            message_kind="evidence_plan",
            label="证据规划进行中",
            body="正在检索 source anchors、prototype documents 和 stone profiles，准备起笔证据...",
            stage="evidence_plan",
            stream_key=self._stream_key(state, "evidence_plan"),
        )
        evidence_plan = self._plan_evidence_v2(state, analysis_bundle, client)
        analysis_bundle.generation_packet["evidence_plan"] = evidence_plan
        evidence_payload = _build_writer_message_payload(
            message_kind="evidence_plan",
            label="证据规划已完成" if not evidence_plan.get("fallback_reason") else "证据规划已完成（已降级）",
            body=_render_evidence_plan_v2(evidence_plan),
            detail=evidence_plan,
            stage="evidence_plan",
            stream_key=self._stream_key(state, "evidence_plan"),
        )
        self._emit(state, "stage", evidence_payload)

        self._emit_live_writer_message(
            state,
            message_kind="topic_adapter",
            label="题目适配进行中",
            body="正在把题目翻进作者的切口、代价和叙述距离...",
            stage="topic_adapter",
            stream_key=self._stream_key(state, "topic_adapter"),
        )
        topic_adapter = self._adapt_topic_v2(state, analysis_bundle, client, evidence_plan=evidence_plan)
        topic_payload = _build_writer_message_payload(
            message_kind="topic_adapter",
            label="题目适配已完成",
            body=_render_topic_adapter_v2(topic_adapter),
            detail=topic_adapter,
            stage="topic_adapter",
            stream_key=self._stream_key(state, "topic_adapter"),
        )
        self._emit(state, "stage", topic_payload)

        self._emit_live_writer_message(
            state,
            message_kind="prototype_selector",
            label="原型检索进行中",
            body="正在按 family、motif、length 和 stance 匹配最贴近的原型文档...",
            stage="prototype_selector",
            stream_key=self._stream_key(state, "prototype_selector"),
        )
        prototype_selection = self._select_prototypes_v2(
            state,
            analysis_bundle,
            topic_adapter,
            evidence_plan=evidence_plan,
        )
        prototype_payload = _build_writer_message_payload(
            message_kind="prototype_selector",
            label="原型检索已完成",
            body=_render_prototype_selection_v2(prototype_selection),
            detail=prototype_selection,
            stage="prototype_selector",
            stream_key=self._stream_key(state, "prototype_selector"),
        )
        self._emit(state, "stage", prototype_payload)

        self._emit_live_writer_message(
            state,
            message_kind="blueprint",
            label="写作蓝图进行中",
            body="正在把证据压成可执行的开头、推进、转折和收口...",
            stage="blueprint",
            stream_key=self._stream_key(state, "blueprint"),
        )
        blueprint = self._compose_blueprint_v2(
            state,
            analysis_bundle,
            topic_adapter,
            prototype_selection,
            client,
            evidence_plan=evidence_plan,
        )
        blueprint_payload = _build_writer_message_payload(
            message_kind="blueprint",
            label="写作蓝图已完成",
            body=_render_blueprint_v2(blueprint),
            detail=blueprint,
            stage="blueprint",
            stream_key=self._stream_key(state, "blueprint"),
        )
        self._emit(state, "stage", blueprint_payload)

        self._emit_live_writer_message(
            state,
            message_kind="draft",
            label="首稿生成中",
            body="正在起草正文...",
            stage="draft",
            stream_key=self._stream_key(state, "draft"),
        )
        initial_draft = self._generate_initial_draft_v2(
            state,
            analysis_bundle,
            topic_adapter,
            prototype_selection,
            blueprint,
            client,
            evidence_plan=evidence_plan,
        )
        draft_payload = _build_writer_message_payload(
            message_kind="draft",
            label="首稿已完成",
            body=initial_draft,
            detail={"word_count": estimate_word_count(initial_draft), "blueprint": blueprint},
            stage="draft",
            stream_key=self._stream_key(state, "draft"),
        )
        self._emit(state, "stage", draft_payload)

        critics = self._run_holistic_critics_v2(
            state,
            analysis_bundle,
            initial_draft,
            topic_adapter,
            prototype_selection,
            blueprint,
            repository.get_service_config(session, "chat_service"),
            evidence_plan=evidence_plan,
        )
        critic_messages: list[dict[str, Any]] = []
        for critic in critics:
            critic_payload = _build_critic_message_payload_v2(
                critic,
                stream_key=self._stream_key(state, "critic", suffix=str(critic.get("critic_key") or "critic")),
            )
            critic_messages.append(critic_payload)
            self._emit(state, "stage", critic_payload)

        revision_action = _resolve_critic_action_v2(
            critics,
            draft_text=initial_draft,
            topic=state.topic,
            target_word_count=state.target_word_count,
        )
        revision_payload = None
        final_text = initial_draft
        if revision_action == "redraft":
            self._emit_live_writer_message(
                state,
                message_kind="redraft",
                label="整篇重写进行中",
                body="critic 判定需要整篇重写，正在基于蓝图重写正文...",
                stage="redraft",
                stream_key=self._stream_key(state, "redraft"),
            )
            final_text = self._redraft_from_critics_v2(
                state,
                analysis_bundle,
                topic_adapter,
                prototype_selection,
                blueprint,
                critics,
                client,
                evidence_plan=evidence_plan,
            )
            revision_payload = _build_writer_message_payload(
                message_kind="redraft",
                label="整篇重写已完成",
                body=final_text,
                detail={"word_count": estimate_word_count(final_text), "reason": "critic_redraft"},
                stage="redraft",
                stream_key=self._stream_key(state, "redraft"),
            )
        elif revision_action == "line_edit":
            self._emit_live_writer_message(
                state,
                message_kind="line_edit",
                label="局部修订进行中",
                body="正在按 critic 指令做局部修订...",
                stage="line_edit",
                stream_key=self._stream_key(state, "line_edit"),
            )
            final_text = self._line_edit_from_critics_v2(
                state,
                analysis_bundle,
                initial_draft,
                topic_adapter,
                prototype_selection,
                blueprint,
                critics,
                client,
                evidence_plan=evidence_plan,
            )
            revision_payload = _build_writer_message_payload(
                message_kind="line_edit",
                label="局部修订已完成",
                body=final_text,
                detail={"word_count": estimate_word_count(final_text), "reason": "critic_line_edit"},
                stage="line_edit",
                stream_key=self._stream_key(state, "line_edit"),
            )
        if revision_payload:
            self._emit(state, "stage", revision_payload)

        final_assessment = _build_final_assessment_v2(
            final_text,
            critics,
            state.topic,
            state.target_word_count,
            revision_action=revision_action,
        )
        final_payload = _build_writer_message_payload(
            message_kind="final",
            label="终稿已完成",
            body=final_text,
            detail={
                "word_count": estimate_word_count(final_text),
                "final_assessment": final_assessment,
            },
            stage="final",
            stream_key=self._stream_key(state, "final"),
        )
        self._emit(state, "stage", final_payload)

        trace = {
            "kind": "writing_result",
            "status": "completed",
            "degraded_mode": False,
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
            "evidence_plan": evidence_plan,
            "topic_adapter": topic_adapter,
            "prototype_selection": prototype_selection,
            "blueprint": blueprint,
            "anchor_ids": _collect_trace_anchor_ids_v2(
                analysis_bundle,
                evidence_plan,
                topic_adapter,
                prototype_selection,
                blueprint,
                critics,
            ),
            "blocks": _build_trace_blocks_v2(
                analysis_bundle,
                evidence_plan,
                topic_adapter,
                prototype_selection,
                blueprint,
                critics,
                revision_action,
            ),
            "critics": critics,
            "draft": initial_draft,
            "final_text": final_text,
            "final_assessment": final_assessment,
            "timeline": [
                evidence_payload,
                topic_payload,
                prototype_payload,
                blueprint_payload,
                draft_payload,
                *critic_messages,
                *([revision_payload] if revision_payload else []),
                final_payload,
            ],
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
            "review_count": len(critics),
            "generation_packet": analysis_bundle.generation_packet.get("baseline", {}),
            "final_assessment": final_assessment,
        }
        self._emit(state, "done", done_payload)

    def _build_client(self, config) -> OpenAICompatibleClient | None:
        if not config:
            return None
        try:
            return OpenAICompatibleClient(config, log_path=str(self.config.llm_log_path))
        except Exception:
            return None

    def _stream_key(self, state: WritingStreamState, message_kind: str, *, suffix: str | None = None) -> str:
        key = f"{state.id}:{message_kind}"
        if suffix:
            key = f"{key}:{suffix}"
        return key

    def _emit_live_writer_message(
        self,
        state: WritingStreamState,
        *,
        message_kind: str,
        label: str,
        body: str,
        stage: str,
        stream_key: str,
        detail: dict[str, Any] | None = None,
        stream_state: str = "streaming",
        render_mode: str = "plain",
    ) -> None:
        self._emit(
            state,
            "stream_update",
            _build_writer_message_payload(
                message_kind=message_kind,
                label=label,
                body=body,
                detail=detail,
                stage=stage,
                stream_key=stream_key,
                stream_state=stream_state,
                render_mode=render_mode,
            ),
        )

    def _emit_live_critic_message(
        self,
        state: WritingStreamState,
        *,
        critic_key: str,
        label: str,
        body: str,
        stream_state: str = "streaming",
        render_mode: str = "plain",
    ) -> None:
        self._emit(
            state,
            "stream_update",
            {
                "stage": "critic",
                "label": label,
                "actor_id": f"critic-{critic_key}",
                "actor_name": label,
                "actor_role": "critic",
                "message_kind": "critic",
                "body": body,
                "detail": {},
                "created_at": _iso_now(),
                "stream_key": self._stream_key(state, "critic", suffix=critic_key),
                "stream_state": stream_state,
                "render_mode": render_mode,
            },
        )

    def _make_stage_stream_handler(
        self,
        state: WritingStreamState,
        *,
        message_kind: str,
        label: str,
        stage: str,
        stream_key: str,
        actor_name: str = WRITER_ACTOR_NAME,
        actor_id: str | None = None,
        actor_role: str = "writer",
        render_mode: str = "plain",
    ):
        buffer: list[str] = []
        emitted_length = 0
        last_emit_at = 0.0

        def flush(*, force: bool = False) -> None:
            nonlocal emitted_length, last_emit_at
            text = "".join(buffer)
            if not text:
                return
            if not force:
                delta_size = len(text) - emitted_length
                tail = text[-1:]
                if delta_size < 48 and tail not in {"\n", "。", "！", "？", "}", "]"}:
                    return
            self._emit(
                state,
                "stream_update",
                {
                    "stage": stage,
                    "label": label,
                    "actor_id": actor_id or f"writer-{message_kind}",
                    "actor_name": actor_name,
                    "actor_role": actor_role,
                    "message_kind": message_kind,
                    "body": text,
                    "detail": {},
                    "created_at": _iso_now(),
                    "stream_key": stream_key,
                    "stream_state": "streaming",
                    "render_mode": render_mode,
                },
            )
            emitted_length = len(text)
            last_emit_at = time.monotonic()

        def handler(delta: str) -> None:
            nonlocal last_emit_at
            if not delta:
                return
            buffer.append(delta)
            now = time.monotonic()
            text = "".join(buffer)
            if (
                len(text) - emitted_length >= 96
                or now - last_emit_at >= 0.18
                or text.endswith(("\n", "。", "！", "？", "}", "]"))
            ):
                flush()

        def finalize() -> str:
            flush(force=True)
            return "".join(buffer)

        return handler, finalize

    def _resolve_analysis_bundle(self, session, project_id: str) -> StoneWritingAnalysisBundle:
        project = repository.get_project(session, project_id)
        if not project:
            raise ValueError("Project not found.")
        preprocess_run = repository.get_latest_successful_stone_preprocess_run(session, project_id)
        if not preprocess_run:
            raise ValueError("No Stone preprocess baseline is available yet. Run preprocess first.")

        stone_profiles = _load_stone_profiles_v2(session, project_id)
        if not stone_profiles:
            raise ValueError("Stone v2 writing needs stone_profile_v2 on the corpus. Run preprocess again.")

        documents = _load_stone_documents_v2(session, project_id)
        clusters = build_short_text_clusters(stone_profiles)
        author_model = _load_v2_asset_payload(session, project_id, asset_kind="stone_author_model_v2")
        if not author_model:
            author_model = build_stone_author_model_v2(
                project_name=project.name,
                profiles=stone_profiles,
                short_text_clusters=clusters,
            )
            repository.create_asset_draft(
                session,
                project_id=project_id,
                run_id=None,
                asset_kind="stone_author_model_v2",
                markdown_text=render_stone_author_model_markdown(author_model),
                json_payload=author_model,
                prompt_text=json.dumps(author_model, ensure_ascii=False, indent=2),
                notes="Stone v2 writing auto-generated author model baseline.",
            )
        prototype_index = _load_v2_asset_payload(session, project_id, asset_kind="stone_prototype_index_v2")
        if not prototype_index:
            prototype_index = build_stone_prototype_index_v2(
                project_name=project.name,
                profiles=stone_profiles,
                documents=documents,
            )
            repository.create_asset_draft(
                session,
                project_id=project_id,
                run_id=None,
                asset_kind="stone_prototype_index_v2",
                markdown_text=render_stone_prototype_index_markdown(prototype_index),
                json_payload=prototype_index,
                prompt_text=json.dumps(prototype_index, ensure_ascii=False, indent=2),
                notes="Stone v2 writing auto-generated prototype index baseline.",
            )

        source_anchors = _build_source_anchors_v2(prototype_index)
        version_label = f"preprocess {preprocess_run.created_at.isoformat(timespec='minutes')}" if preprocess_run.created_at else "latest"
        bundle = StoneWritingAnalysisBundle(
            run_id=preprocess_run.id,
            source="stone_v2_baseline",
            version_label=version_label,
            target_role=project.name,
            analysis_context="stone_v2_preprocess",
            facets=[],
            prompt_text="",
            stone_profiles=stone_profiles,
            source_anchors=source_anchors,
            author_model=author_model,
            prototype_index=prototype_index,
            short_text_clusters=clusters,
        )
        bundle.prompt_text = _build_analysis_prompt_text_v2(bundle)
        bundle.generation_packet = _build_generation_packet_v2(bundle)
        return bundle

    def _plan_evidence_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("evidence_plan", "写作模型未配置，无法规划证据。")
        try:
            response = self._run_evidence_tool_loop_v2(state, analysis_bundle, client)
            payload = parse_json_response(response["content"], fallback=True)
            plan = _normalize_evidence_plan_payload_v2(
                payload,
                analysis_bundle,
                query_trace=response.get("tool_trace") or [],
                queried_anchor_ids=response.get("queried_anchor_ids") or [],
                queried_document_ids=response.get("queried_document_ids") or [],
            )
            plan["planner_mode"] = "tool_loop"
            return plan
        except Exception as exc:
            return _build_fallback_evidence_plan_v2(
                state,
                analysis_bundle,
                reason=str(exc),
            )

    def _run_evidence_tool_loop_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient,
    ) -> dict[str, Any]:
        messages = _build_evidence_planner_messages_v2(state, analysis_bundle)
        tool_trace: list[dict[str, Any]] = []
        queried_anchor_ids: set[str] = set()
        queried_document_ids: set[str] = set()
        model_name = client.config.model
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        progress_lines = ["正在规划证据..."]

        for iteration in range(1, 6):
            progress_lines.append(f"第 {iteration} 轮检索：准备调用 evidence planner。")
            self._emit_live_writer_message(
                state,
                message_kind="evidence_plan",
                label="证据规划进行中",
                body="\n".join(progress_lines[-8:]),
                stage="evidence_plan",
                stream_key=self._stream_key(state, "evidence_plan"),
            )
            round_result = client.tool_round(
                messages,
                self._evidence_tool_schemas_v2(),
                model=client.config.model,
                temperature=0.2,
                max_tokens=900,
                timeout=35.0,
            )
            model_name = round_result.model or model_name
            for key in usage:
                usage[key] += int(round_result.usage.get(key, 0) or 0)
            if not round_result.tool_calls:
                if round_result.content:
                    progress_lines.append("证据规划器已返回归纳结果，正在整理。")
                    self._emit_live_writer_message(
                        state,
                        message_kind="evidence_plan",
                        label="证据规划进行中",
                        body="\n".join(progress_lines[-8:]),
                        stage="evidence_plan",
                        stream_key=self._stream_key(state, "evidence_plan"),
                    )
                return {
                    "content": round_result.content,
                    "usage": usage,
                    "tool_trace": tool_trace,
                    "queried_anchor_ids": sorted(queried_anchor_ids),
                    "queried_document_ids": sorted(queried_document_ids),
                    "iterations": iteration,
                    "model": model_name,
                }

            messages.append(
                {
                    "role": "assistant",
                    "content": round_result.content,
                    "tool_calls": [
                        {
                            "id": call.id,
                            "name": call.name,
                            "arguments_json": call.arguments_json,
                        }
                        for call in round_result.tool_calls
                    ],
                }
            )
            for call in round_result.tool_calls:
                progress_lines.append(
                    f"调用工具：{call.name} {json.dumps(call.arguments, ensure_ascii=False)}"
                )
                output, state_delta = self._execute_evidence_tool_v2(
                    call.name,
                    call.arguments,
                    analysis_bundle=analysis_bundle,
                )
                queried_anchor_ids.update(state_delta.get("anchor_ids", set()))
                queried_document_ids.update(state_delta.get("document_ids", set()))
                tool_trace.append(
                    {
                        "tool": call.name,
                        "arguments": call.arguments,
                        "anchor_ids": sorted(state_delta.get("anchor_ids", set())),
                        "document_ids": sorted(state_delta.get("document_ids", set())),
                        "result_preview": _trim_text(output, 420),
                    }
                )
                preview = tool_trace[-1]["result_preview"]
                if preview:
                    progress_lines.append(f"结果预览：{preview}")
                self._emit_live_writer_message(
                    state,
                    message_kind="evidence_plan",
                    label="证据规划进行中",
                    body="\n".join(progress_lines[-10:]),
                    stage="evidence_plan",
                    stream_key=self._stream_key(state, "evidence_plan"),
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "name": call.name,
                        "content": json.dumps(output, ensure_ascii=False),
                    }
                )

        raise WritingPipelineError("evidence_plan", "证据规划超过了最大工具调用轮数。")

    def _execute_evidence_tool_v2(
        self,
        name: str,
        args: dict[str, Any],
        *,
        analysis_bundle: StoneWritingAnalysisBundle,
    ) -> tuple[dict[str, Any], dict[str, set[str]]]:
        if name == "search_source_anchors":
            output, state_delta = _search_source_anchors_v2(analysis_bundle, args)
            return output, state_delta
        if name == "read_source_anchor":
            output, state_delta = _read_source_anchor_v2(analysis_bundle, args)
            return output, state_delta
        if name == "search_stone_profiles":
            output, state_delta = _search_stone_profiles_v2(analysis_bundle, args)
            return output, state_delta
        if name == "read_stone_profile":
            output, state_delta = _read_stone_profile_v2(analysis_bundle, args)
            return output, state_delta
        if name == "list_prototype_families":
            output, state_delta = _list_prototype_families_v2(analysis_bundle)
            return output, state_delta
        if name == "search_prototype_documents":
            output, state_delta = _search_prototype_documents_v2(analysis_bundle, args)
            return output, state_delta
        if name == "read_prototype_document":
            output, state_delta = _read_prototype_document_v2(analysis_bundle, args)
            return output, state_delta
        return {"error": f"未知 Stone evidence 工具：{name}"}, {"anchor_ids": set(), "document_ids": set()}

    @staticmethod
    def _evidence_tool_schemas_v2() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "search_source_anchors",
                    "description": "按关键词或来源过滤 source anchors，找到更细的原文片段。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "source": {"type": "string"},
                            "facet_key": {"type": "string"},
                            "role": {"type": "string"},
                            "document_id": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "read_source_anchor",
                    "description": "按 anchor_id 读取单条 source anchor 的完整内容。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "anchor_id": {"type": "string"},
                        },
                        "required": ["anchor_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_stone_profiles",
                    "description": "在 stone_profile_v2 中搜索更细的文章画像切片。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "content_type": {"type": "string"},
                            "length_band": {"type": "string"},
                            "emotion_label": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "read_stone_profile",
                    "description": "按 document_id 读取单篇 Stone 画像的详细信息。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "document_id": {"type": "string"},
                        },
                        "required": ["document_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_prototype_families",
                    "description": "列出 prototype family 的聚类信息。",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_prototype_documents",
                    "description": "在 prototype_index 中按关键词和 family 过滤，找到更贴近的原型文档。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "family_key": {"type": "string"},
                            "length_band": {"type": "string"},
                            "surface_form": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "read_prototype_document",
                    "description": "按 document_id 读取 prototype document 的详细窗口。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "document_id": {"type": "string"},
                        },
                        "required": ["document_id"],
                    },
                },
            },
        ]

    def _adapt_topic_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient | None,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("topic_adapter", "写作模型未配置，无法适配题目。")
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="topic_adapter",
            label="题目适配进行中",
            stage="topic_adapter",
            stream_key=self._stream_key(state, "topic_adapter"),
        )
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v2 写作链路里的 topic adapter。\n"
                            "不要写正文，只把用户题目改写成作者会真正下手的切入口。\n"
                            "只返回 JSON，不要输出解释。\n"
                            "禁止使用 DSM、诊断、病理标签。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"stone_v2_generation_packet JSON:\n{json.dumps(_build_topic_adapter_packet_v2(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            "请返回 JSON：\n"
                            "{\n"
                            '  "author_angle": "作者会从什么角度切入",\n'
                            '  "entry_scene": "最适合的起笔场景或动作",\n'
                            '  "felt_cost": "题目背后的代价",\n'
                            '  "judgment_target": "作者会在判断谁/什么",\n'
                            '  "value_lens": "代价|资格|体面|生存|虚假",\n'
                            '  "desired_judgment": "厌恶|怜悯|自损|讥讽|悬置",\n'
                            '  "desired_distance": "贴脸|回收|旁观|宣判",\n'
                            '  "motif_path": ["建议使用的意象"],\n'
                            '  "forbidden_drift": ["不要写偏成什么"],\n'
                            '  "prototype_family_hints": ["优先命中的 family key 或 label"],\n'
                            '  "anchor_ids": ["可参考的 source anchor id"]\n'
                            "}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.25,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("topic_adapter", f"题目适配失败：{exc}") from exc
        finalize_stream()
        payload = parse_json_response(response.content, fallback=True)
        adapted = _normalize_topic_adapter_payload_v2(payload, analysis_bundle, evidence_plan=evidence_plan)
        if not adapted["anchor_ids"]:
            raise WritingPipelineError("topic_adapter", "题目适配没有绑定任何 source anchor。")
        return adapted

    def _select_prototypes_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_adapter: dict[str, Any],
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        selection = _select_prototypes_for_topic_v2(
            analysis_bundle,
            topic_adapter,
            target_word_count=state.target_word_count,
            evidence_plan=evidence_plan,
        )
        if not selection["selected_documents"]:
            raise WritingPipelineError("prototype_selector", "原型检索为空，无法继续写作。")
        return selection

    def _compose_blueprint_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        client: OpenAICompatibleClient | None,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("blueprint", "写作模型未配置，无法生成蓝图。")
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="blueprint",
            label="写作蓝图进行中",
            stage="blueprint",
            stream_key=self._stream_key(state, "blueprint"),
        )
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v2 写作链路里的 blueprint composer。\n"
                            "不要写正文，只负责把原型拆成可执行的开头动作、推进方式、转折装置和收口残响。\n"
                            "短文允许 1-3 段，长文按需要扩展，不要强制中心转折。\n"
                            "只返回 JSON。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"topic_adapter JSON:\n{json.dumps(topic_adapter, ensure_ascii=False, indent=2)}\n\n"
                            f"prototype_selection JSON:\n{json.dumps(prototype_selection, ensure_ascii=False, indent=2)}\n\n"
                            f"author_model JSON:\n{json.dumps(_compact_author_model_for_blueprint_v2(analysis_bundle.author_model), ensure_ascii=False, indent=2)}\n\n"
                            "请返回 JSON：\n"
                            "{\n"
                            '  "paragraph_count": number,\n'
                            '  "shape_note": "整体形状",\n'
                            '  "entry_move": "如何起笔",\n'
                            '  "development_move": "如何推进",\n'
                            '  "turning_device": "如何转折，可为 none",\n'
                            '  "closure_residue": "如何留下残响",\n'
                            '  "keep_terms": ["最好保留的词或句法材料"],\n'
                            '  "motif_obligations": ["必须落地的意象"],\n'
                            '  "steps": ["按先后顺序的写作动作"],\n'
                            '  "do_not_do": ["不要犯的偏移"],\n'
                            '  "anchor_ids": ["绑定到 source anchor id"]\n'
                            "}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.25,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("blueprint", f"蓝图生成失败：{exc}") from exc
        finalize_stream()
        payload = parse_json_response(response.content, fallback=True)
        blueprint = _normalize_blueprint_payload_v2(payload, analysis_bundle, state.target_word_count)
        if not blueprint["anchor_ids"]:
            raise WritingPipelineError("blueprint", "蓝图没有绑定任何 source anchor。")
        return blueprint

    def _generate_initial_draft_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        blueprint: dict[str, Any],
        client: OpenAICompatibleClient | None,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> str:
        if not client:
            raise WritingPipelineError("draft", "写作模型未配置，无法起草正文。")
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="draft",
            label="首稿生成中",
            stage="draft",
            stream_key=self._stream_key(state, "draft"),
        )
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are the Stone v2 prototype-grounded drafter.\n"
                            "Write article prose only.\n"
                            "Follow the blueprint, selected prototypes, and author model constraints.\n"
                            "Imitate the author's structural pressure, cadence, lexicon, and closure residue from the author_style_pack.\n"
                            "Do not write a generic essay; make the topic sound native to this author.\n"
                            "Do not explain your plan or mention JSON, anchors, critic feedback, or analysis terms.\n"
                            "Do not use DSM, diagnosis, or pathology labels.\n"
                            "Return only the article body."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"topic_adapter JSON:\n{json.dumps(topic_adapter, ensure_ascii=False, indent=2)}\n\n"
                            f"prototype_selection JSON:\n{json.dumps(_compact_prototype_selection_for_draft_v2(prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"author_style_pack JSON:\n{json.dumps(_build_author_style_pack_v2(analysis_bundle, prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"stone_v2_generation_packet JSON:\n{json.dumps(_build_drafting_packet_v2(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            "Hard bans:\n"
                            "- No prompt language, no analysis language, no anchor ids.\n"
                            "- No DSM, diagnosis, pathology labels, or psychological reports.\n"
                            "- Micro/short targets may use 1-3 paragraphs; do not force a central twist.\n"
                            "- Keep the closure unresolved if the blueprint says so."
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.45,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("draft", f"首稿生成失败：{exc}") from exc
        finalize_stream()
        candidate = _clean_model_text(response.content)
        if not candidate:
            raise WritingPipelineError("draft", "首稿生成失败：模型返回为空。")
        if _contains_banned_meta(candidate):
            raise WritingPipelineError("draft", "首稿含有元分析提示词，已拒绝交付。")
        return _fit_word_count(candidate, state.target_word_count, analysis_bundle, state.topic, state.extra_requirements)

    def _run_holistic_critics_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        blueprint: dict[str, Any],
        config,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        critics = ("formal_fidelity", "worldview_translation", "syntheticness")
        client = self._build_client(config)
        if not client:
            raise WritingPipelineError("critic", "写作模型未配置，无法执行 critic。")
        results: list[dict[str, Any]] = []
        for critic_key in critics:
            results.append(
                self._review_with_critic_v2(
                    state,
                    critic_key,
                    draft,
                    analysis_bundle,
                    topic_adapter,
                    prototype_selection,
                    blueprint,
                    evidence_plan,
                    client,
                )
            )
        return results

    def _review_with_critic_v2(
        self,
        state: WritingStreamState,
        critic_key: str,
        draft: str,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        blueprint: dict[str, Any],
        evidence_plan: dict[str, Any] | None,
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("critic", "写作模型未配置，无法执行 critic。")
        spec = _critic_spec_v2(critic_key)
        self._emit_live_critic_message(
            state,
            critic_key=critic_key,
            label=spec["label"],
            body=f"正在执行 {spec['label']} critic...",
        )
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="critic",
            label=spec["label"],
            stage="critic",
            stream_key=self._stream_key(state, "critic", suffix=critic_key),
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
                            f"你是 Stone v2 的 {spec['label']} critic。\n"
                            "只审当前这一项，不评价其他维度。\n"
                            "你只能给 approve / line_edit / redraft 三种 verdict。\n"
                            "只返回 JSON。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"critic focus：{spec['focus']}\n\n"
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"topic_adapter JSON:\n{json.dumps(topic_adapter, ensure_ascii=False, indent=2)}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"prototype_selection JSON:\n{json.dumps(_compact_prototype_selection_for_draft_v2(prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"author_model slice JSON:\n{json.dumps(_critic_packet_v2(analysis_bundle, critic_key), ensure_ascii=False, indent=2)}\n\n"
                            f"候选正文：\n{draft}\n\n"
                            "请返回 JSON：\n"
                            "{\n"
                            '  "pass": boolean,\n'
                            '  "score": number,\n'
                            '  "verdict": "approve|line_edit|redraft",\n'
                            '  "anchor_ids": ["source anchor id"],\n'
                            '  "matched_signals": ["已命中的信号"],\n'
                            '  "must_keep_spans": ["必须保留的正文片段"],\n'
                            '  "line_edits": ["局部修改建议"],\n'
                            '  "redraft_reason": "若 verdict=redraft，说明整篇为何要重写",\n'
                            '  "risks": ["残余风险"]\n'
                            "}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.15,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("critic", f"{spec['label']} critic 失败：{exc}") from exc
        finalize_stream()
        payload = parse_json_response(response.content, fallback=True)
        return _normalize_critic_payload_v2(payload, critic_key, analysis_bundle)

    def _redraft_from_critics_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        blueprint: dict[str, Any],
        critics: list[dict[str, Any]],
        client: OpenAICompatibleClient | None,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> str:
        if not client:
            raise WritingPipelineError("redraft", "写作模型未配置，无法整篇重写。")
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="redraft",
            label="整篇重写进行中",
            stage="redraft",
            stream_key=self._stream_key(state, "redraft"),
        )
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are the Stone v2 whole-article redrafter.\n"
                            "Throw away the failed draft and write a fresh article from the blueprint.\n"
                            "Use critic feedback only to avoid syntheticness and formal drift.\n"
                            "Imitate the author's structural pressure, cadence, lexicon, and closure residue from the author_style_pack.\n"
                            "Do not write a generic essay; make the topic sound native to this author.\n"
                            "Return only the article body."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"topic_adapter JSON:\n{json.dumps(topic_adapter, ensure_ascii=False, indent=2)}\n\n"
                            f"prototype_selection JSON:\n{json.dumps(_compact_prototype_selection_for_draft_v2(prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"critic feedback JSON:\n{json.dumps(critics, ensure_ascii=False, indent=2)}\n\n"
                            f"author_style_pack JSON:\n{json.dumps(_build_author_style_pack_v2(analysis_bundle, prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"stone_v2_generation_packet JSON:\n{json.dumps(_build_drafting_packet_v2(analysis_bundle), ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.42,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("redraft", f"整篇重写失败：{exc}") from exc
        finalize_stream()
        candidate = _clean_model_text(response.content)
        if not candidate:
            raise WritingPipelineError("redraft", "整篇重写失败：模型返回为空。")
        if _contains_banned_meta(candidate):
            raise WritingPipelineError("redraft", "重写稿含有元分析提示词，已拒绝交付。")
        return _light_trim_to_word_count(candidate, state.target_word_count)

    def _line_edit_from_critics_v2(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        topic_adapter: dict[str, Any],
        prototype_selection: dict[str, Any],
        blueprint: dict[str, Any],
        critics: list[dict[str, Any]],
        client: OpenAICompatibleClient | None,
        *,
        evidence_plan: dict[str, Any] | None = None,
    ) -> str:
        if not client:
            raise WritingPipelineError("line_edit", "写作模型未配置，无法局部修订。")
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind="line_edit",
            label="局部修订进行中",
            stage="line_edit",
            stream_key=self._stream_key(state, "line_edit"),
        )
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are the Stone v2 line editor.\n"
                            "Keep the working draft structure and closing energy.\n"
                            "Only apply local edits requested by the critics.\n"
                            "Tighten cadence, lexicon, and closure residue toward the author_style_pack without adding analysis language.\n"
                            "Return only the article body."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"Current draft:\n{draft}\n\n"
                            f"evidence_plan JSON:\n{json.dumps(_compact_evidence_plan_for_prompt_v2(evidence_plan or {}), ensure_ascii=False, indent=2)}\n\n"
                            f"topic_adapter JSON:\n{json.dumps(topic_adapter, ensure_ascii=False, indent=2)}\n\n"
                            f"prototype_selection JSON:\n{json.dumps(_compact_prototype_selection_for_draft_v2(prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint JSON:\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"critic feedback JSON:\n{json.dumps(critics, ensure_ascii=False, indent=2)}\n\n"
                            f"author_style_pack JSON:\n{json.dumps(_build_author_style_pack_v2(analysis_bundle, prototype_selection), ensure_ascii=False, indent=2)}\n\n"
                            f"stone_v2_generation_packet JSON:\n{json.dumps(_build_drafting_packet_v2(analysis_bundle), ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.32,
                max_tokens=None,
                stream_handler=stream_handler,
            )
        except Exception as exc:
            raise WritingPipelineError("line_edit", f"局部修订失败：{exc}") from exc
        finalize_stream()
        candidate = _clean_model_text(response.content)
        if not candidate:
            raise WritingPipelineError("line_edit", "局部修订失败：模型返回为空。")
        if _contains_banned_meta(candidate):
            raise WritingPipelineError("line_edit", "修订稿含有元分析提示词，已拒绝交付。")
        return _fit_word_count(candidate, state.target_word_count, analysis_bundle, state.topic, state.extra_requirements)

    def _translate_topic(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("topic_translation", "写作模型未配置，无法翻译题目。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone 仿写流水线的题目翻译 agent。\n"
                            "你的任务不是写正文，而是把用户题目翻译进作者世界。\n"
                            "只返回 JSON，不要输出额外说明。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"generation_packet：\n{json.dumps(_build_translation_packet(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            "请返回 JSON，字段如下：\n"
                            "{\n"
                            '  "scene": [作者世界里可落地的场景],\n'
                            '  "imagery": [可使用意象],\n'
                            '  "felt_cost": [题目背后的代价],\n'
                            '  "relationship_pressure": [关系压力],\n'
                            '  "stance": [作者会站在哪里判断],\n'
                            '  "emotional_arc": [起点, 转折, 回落],\n'
                            '  "not_to_write": [明确不要写成什么],\n'
                            '  "anchor_ids": [可借鉴的 source anchor id]\n'
                            "}\n"
                            "not_to_write 必须包含禁止诊断式、解释式、提示词式写法。"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.25,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("topic_translation", f"题目翻译失败：{exc}") from exc
        payload = parse_json_response(response.content, fallback=True)
        translated = _normalize_topic_translation_payload(payload, analysis_bundle)
        if not translated["anchor_ids"]:
            raise WritingPipelineError("topic_translation", "题目翻译没有绑定任何 source anchor。")
        return translated

    def _plan_outline(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_translation: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("outline", "写作模型未配置，无法规划段落。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone 仿写流水线的 outline planner。\n"
                            "把题目翻译结果拆成 3-6 段可执行段落计划。\n"
                            "字数控制必须在 outline 完成；不要把扩写任务留给末端。\n"
                            "只返回 JSON。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"topic_translation：\n{json.dumps(topic_translation, ensure_ascii=False, indent=2)}\n\n"
                            f"generation_packet：\n{json.dumps(_build_outline_packet(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            "请返回 JSON，字段如下：\n"
                            "{\n"
                            '  "target_word_count": number,\n'
                            '  "paragraph_count": number,\n'
                            '  "word_count_strategy": 中文字符串,\n'
                            '  "paragraphs": [\n'
                            "    {\n"
                            '      "index": number,\n'
                            '      "function": 中文字符串,\n'
                            '      "emotional_position": 中文字符串,\n'
                            '      "anchor_ids": [source anchor id],\n'
                            '      "target_words": number,\n'
                            '      "closing_move": 中文字符串\n'
                            "    }\n"
                            "  ],\n"
                            '  "not_to_write": [中文字符串]\n'
                            "}\n"
                            "每段必须至少绑定一个 anchor_ids。"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.25,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("outline", f"段落计划失败：{exc}") from exc
        payload = parse_json_response(response.content, fallback=True)
        outline = _normalize_outline_payload(payload, analysis_bundle, state.target_word_count)
        if not outline["paragraphs"]:
            raise WritingPipelineError("outline", "段落计划为空。")
        missing_anchor = [item for item in outline["paragraphs"] if not item.get("anchor_ids")]
        if missing_anchor:
            raise WritingPipelineError("outline", "段落计划存在未绑定 source anchor 的段落。")
        return outline

    def _generate_initial_draft(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_translation: dict[str, Any],
        outline: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> str:
        if not client:
            raise WritingPipelineError("draft", "写作模型未配置，无法起草正文。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are the constrained Stone article drafter.\n"
                            "Write article prose only; do not explain the plan, analysis, anchors, or revision logic.\n"
                            "Never use meta-analysis phrasing such as '如果沿着…去写', '分析里最能充当锚点', or '这次修订最重要'.\n"
                            "Use the topic translation, paragraph outline, exemplar anchors, and do/don't rules as constraints.\n"
                            "Return only the article body."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"topic_translation JSON:\n{json.dumps(topic_translation, ensure_ascii=False, indent=2)}\n\n"
                            f"outline JSON:\n{json.dumps(outline, ensure_ascii=False, indent=2)}\n\n"
                            f"drafting_generation_packet JSON:\n{json.dumps(_build_drafting_packet(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            "Hard bans:\n"
                            "- No diagnostic, DSM, pathology, or clinical labels in the article body.\n"
                            "- No prompt/analysis language, no self-description of writing choices.\n"
                            "- Do not quote anchor ids or mention exemplars directly.\n"
                            "- Follow outline paragraph count and target paragraph lengths; do not pad at the end."
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.65,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("draft", f"首稿生成失败：{exc}") from exc
        candidate = _clean_model_text(response.content)
        if not candidate:
            raise WritingPipelineError("draft", "首稿生成失败：模型返回为空。")
        if _contains_banned_meta(candidate):
            raise WritingPipelineError("draft", "首稿含有元分析/提示词话语，已拒绝交付。")
        return _light_trim_to_word_count(candidate, state.target_word_count)

    def _review_with_facet(
        self,
        state: WritingStreamState,
        facet: StoneWritingFacetContext,
        draft: str,
        analysis_bundle: StoneWritingAnalysisBundle,
        topic_translation: dict[str, Any],
        outline: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("review", "写作模型未配置，无法评审草稿。")
        anchors = _anchors_for_facet(analysis_bundle, facet)
        if not anchors:
            raise WritingPipelineError("review", f"{facet.label} 缺少可引用的 source anchor。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone 写作流水线中的 source-grounded reviewer。\n"
                            "你只负责当前分配到的单一维度。\n"
                            "所有问题、判断和修订建议都必须绑定 source anchor id。\n"
                            "只返回 JSON，不要输出额外解释。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"当前维度：\n{_build_single_facet_prompt(facet)}\n\n"
                            f"source anchors：\n{json.dumps(anchors, ensure_ascii=False, indent=2)}\n\n"
                            f"topic_translation：\n{json.dumps(topic_translation, ensure_ascii=False, indent=2)}\n\n"
                            f"outline：\n{json.dumps(outline, ensure_ascii=False, indent=2)}\n\n"
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"候选文章：\n{draft}\n\n"
                            "请返回 JSON，字段如下：\n"
                            "{\n"
                            '  "pass": boolean,\n'
                            '  "score": number,\n'
                            '  "anchor_ids": [source anchor id],\n'
                            '  "matched_signals": [中文字符串],\n'
                            '  "violations": [{"anchor_id": source anchor id, "span": 原文片段, "issue": 问题}],\n'
                            '  "must_keep_spans": [原文片段],\n'
                            '  "must_rewrite_spans": [{"anchor_id": source anchor id, "span": 原文片段, "instruction": 修改方向}],\n'
                            '  "revision_instructions": [{"anchor_id": source anchor id, "instruction": 中文字符串}]\n'
                            "}\n"
                            "只判断这一维，不要谈其他维度。没有 anchor_id 的问题无效。"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.15,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("review", f"{facet.label} 评审失败：{exc}") from exc
        payload = parse_json_response(response.content, fallback=True)
        review = _normalize_review_payload(payload, facet, anchors)
        if not _review_has_valid_anchors(review):
            raise WritingPipelineError("review", f"{facet.label} 评审缺少有效 source anchor。")
        return review

    def _synthesize_review_plan(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        reviews: list[dict[str, Any]],
        topic_translation: dict[str, Any],
        outline: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if not client:
            raise WritingPipelineError("review_synthesis", "写作模型未配置，无法合并评审。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone 写作流水线里的评审整合 agent。\n"
                            "请把 8 个 source-grounded 评审意见合并成一份可执行修订计划。\n"
                            "只修失败维度，明确哪些原句必须保留，哪些片段必须改写。\n"
                            "所有改写任务都必须保留 anchor id。\n"
                            "只返回 JSON。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"topic_translation：\n{json.dumps(topic_translation, ensure_ascii=False, indent=2)}\n\n"
                            f"outline：\n{json.dumps(outline, ensure_ascii=False, indent=2)}\n\n"
                            f"首稿：\n{draft}\n\n"
                            f"8 个评审 JSON：\n{json.dumps(reviews, ensure_ascii=False, indent=2)}\n\n"
                            "请返回 JSON，字段如下：\n"
                            "{\n"
                            '  "summary": 中文字符串,\n'
                            '  "must_keep_spans": [原文片段],\n'
                            '  "must_rewrite_spans": [{"anchor_id": source anchor id, "span": 原文片段, "instruction": 修改方向}],\n'
                            '  "revision_instructions": [{"anchor_id": source anchor id, "instruction": 中文字符串}],\n'
                            '  "risk_watch": [中文字符串]\n'
                            "}"
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.15,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("review_synthesis", f"评审合并失败：{exc}") from exc
        payload = parse_json_response(response.content, fallback=True)
        plan = _normalize_review_plan_payload(payload, reviews)
        if not _review_plan_has_valid_anchors(plan, reviews):
            plan = _heuristic_review_plan(reviews)
        if not _review_plan_has_valid_anchors(plan, reviews):
            raise WritingPipelineError("review_synthesis", "评审合并缺少有效 source anchor。")
        return plan

    def _revise_draft(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        reviews: list[dict[str, Any]],
        review_plan: dict[str, Any],
        topic_translation: dict[str, Any],
        outline: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> str:
        if not client:
            raise WritingPipelineError("revise", "写作模型未配置，无法修订正文。")
        try:
            response = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are the targeted Stone reviser.\n"
                            "Revise only the failed dimensions from the review plan.\n"
                            "Preserve must_keep_spans, passed rhythm, and the closing energy that already works.\n"
                            "Do not rewrite the whole article from scratch.\n"
                            "Return only the final article body."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"topic_translation JSON:\n{json.dumps(topic_translation, ensure_ascii=False, indent=2)}\n\n"
                            f"outline JSON:\n{json.dumps(outline, ensure_ascii=False, indent=2)}\n\n"
                            f"drafting_generation_packet JSON:\n{json.dumps(_build_drafting_packet(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                            f"First draft:\n{draft}\n\n"
                            f"Integrated review plan JSON:\n{json.dumps(review_plan, ensure_ascii=False, indent=2)}\n\n"
                            f"Reviewer outputs JSON:\n{json.dumps(reviews, ensure_ascii=False, indent=2)}\n\n"
                            "Hard rules:\n"
                            "- Preserve must_keep_spans unless grammar forces tiny edits.\n"
                            "- Rewrite only must_rewrite_spans and directly related connective tissue.\n"
                            "- Do not include DSM, diagnosis, pathology labels, analysis language, anchor ids, or prompt explanations.\n"
                            "- Return only the final article body."
                        ),
                    },
                ],
                model=client.config.model,
                temperature=0.45,
                max_tokens=None,
            )
        except Exception as exc:
            raise WritingPipelineError("revise", f"终稿修订失败：{exc}") from exc
        candidate = _clean_model_text(response.content)
        if not candidate:
            raise WritingPipelineError("revise", "终稿修订失败：模型返回为空。")
        if _contains_banned_meta(candidate):
            raise WritingPipelineError("revise", "终稿含有元分析/提示词话语，已拒绝交付。")
        return _light_trim_to_word_count(candidate, state.target_word_count)

    def _run_reviews_in_parallel(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        topic_translation: dict[str, Any],
        outline: dict[str, Any],
        config,
    ) -> list[dict[str, Any]]:
        facets = list(analysis_bundle.facets)
        if not facets:
            return []

        if self.run_inline:
            return [
                self._review_with_facet(
                    state,
                    facet,
                    draft,
                    analysis_bundle,
                    topic_translation,
                    outline,
                    self._build_client(config),
                )
                for facet in facets
            ]

        results: dict[str, dict[str, Any]] = {}
        max_workers = min(len(facets), 8)
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-reviewer") as executor:
            future_map = {
                executor.submit(
                    self._review_with_facet,
                    state,
                    facet,
                    draft,
                    analysis_bundle,
                    topic_translation,
                    outline,
                    self._build_client(config),
                ): facet
                for facet in facets
            }
            for future in as_completed(future_map):
                self._ensure_stream_active(state)
                facet = future_map[future]
                results[facet.key] = future.result()

        return [results[facet.key] for facet in facets if facet.key in results]

    def _ensure_stream_active(self, state: WritingStreamState) -> None:
        if state.cancelled.is_set():
            raise RuntimeError("Writing stream cancelled.")

    def _emit(self, state: WritingStreamState, event_type: str, payload: dict[str, Any]) -> None:
        self._ensure_stream_active(state)
        state.events.put({"type": event_type, "payload": payload})


def _derive_session_title(topic: str) -> str:
    clean = re.sub(r"\s+", " ", str(topic or "").strip())
    return clean[:48] or "New writing session"


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _build_facet_context(definition: FacetDefinition, facet_row) -> StoneWritingFacetContext:
    findings = dict(facet_row.findings_json or {})
    fewshots_source = findings.get("fewshots") or facet_row.evidence_json or []
    conflicts_source = findings.get("conflicts") or facet_row.conflicts_json or []
    evidence_source = facet_row.evidence_json or findings.get("evidence") or []
    return StoneWritingFacetContext(
        key=definition.key,
        label=definition.label,
        purpose=definition.purpose,
        confidence=round(float(facet_row.confidence or 0.0), 3),
        summary=str(findings.get("summary") or "").strip(),
        bullets=_normalize_string_list(findings.get("bullets"), limit=4),
        fewshots=_normalize_fewshots(fewshots_source),
        conflicts=_normalize_conflicts(conflicts_source),
        evidence=_normalize_evidence(evidence_source),
    )


def _build_analysis_prompt_text(bundle: StoneWritingAnalysisBundle) -> str:
    lines: list[str] = [
        "Stone multi-facet writing baseline",
        f"Analysis run: {bundle.run_id}",
    ]
    if bundle.target_role:
        lines.append(f"Target role: {bundle.target_role}")
    if bundle.analysis_context:
        lines.append(f"Analysis context: {bundle.analysis_context}")
    lines.append("")
    for index, facet in enumerate(bundle.facets, start=1):
        lines.append(f"{index}. {facet.label} ({facet.key})")
        lines.append(f"Purpose: {facet.purpose}")
        if facet.summary:
            lines.append(f"Summary: {facet.summary}")
        if facet.bullets:
            lines.append("Signals:")
            for bullet in facet.bullets[:4]:
                lines.append(f"- {bullet}")
        if facet.fewshots:
            lines.append("Quoted anchors:")
            for item in facet.fewshots[:2]:
                quote = str(item.get("quote") or "").strip()
                if quote:
                    lines.append(f"- {quote}")
        if facet.conflicts:
            lines.append("Conflict watch:")
            for item in facet.conflicts[:2]:
                detail = str(item.get("detail") or item.get("title") or "").strip()
                if detail:
                    lines.append(f"- {detail}")
        lines.append("")
    return "\n".join(lines).strip()


def _build_single_facet_prompt(facet: StoneWritingFacetContext) -> str:
    lines = [
        f"Facet: {facet.label} ({facet.key})",
        f"Purpose: {facet.purpose}",
    ]
    if facet.summary:
        lines.append(f"Summary: {facet.summary}")
    if facet.bullets:
        lines.append("Signals:")
        lines.extend(f"- {item}" for item in facet.bullets[:4])
    if facet.fewshots:
        lines.append("Quoted anchors:")
        for item in facet.fewshots[:2]:
            quote = str(item.get("quote") or "").strip()
            if quote:
                lines.append(f"- {quote}")
    if facet.conflicts:
        lines.append("Conflict watch:")
        for item in facet.conflicts[:2]:
            detail = str(item.get("detail") or item.get("title") or "").strip()
            if detail:
                lines.append(f"- {detail}")
    return "\n".join(lines).strip()


def _load_stone_profiles(session, project_id: str) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        metadata = dict(document.metadata_json or {})
        profile = metadata.get("stone_profile_v2")
        if not isinstance(profile, dict):
            continue
        expanded = expand_stone_profile_v2_for_analysis(
            profile,
            article_text=str(document.clean_text or document.raw_text or ""),
            title=document.title or document.filename,
        )
        profiles.append(
            {
                "document_id": document.id,
                "title": document.title or document.filename,
                **expanded,
            }
        )
    return profiles


def _load_writing_guide_payload(
    session,
    *,
    project_id: str,
    project_name: str,
    target_role: str,
    analysis_context: str,
    facet_rows: list[Any],
    stone_profiles: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    latest_version = repository.get_latest_asset_version(session, project_id, asset_kind="writing_guide")
    if latest_version and isinstance(latest_version.json_payload, dict):
        return dict(latest_version.json_payload), "published_writing_guide"
    latest_draft = repository.get_latest_asset_draft(session, project_id, asset_kind="writing_guide")
    if latest_draft and isinstance(latest_draft.json_payload, dict):
        return dict(latest_draft.json_payload), "draft_writing_guide"

    summary_by_key = {row.facet_key: dict(row.findings_json or {}) for row in facet_rows}
    evidence_by_key = {row.facet_key: list(row.evidence_json or []) for row in facet_rows}
    return (
        build_writing_guide_payload_from_facets(
            project_name=project_name,
            target_role=target_role or project_name,
            analysis_context=analysis_context or "",
            summary_by_key=summary_by_key,
            evidence_by_key=evidence_by_key,
            stone_profiles=stone_profiles,
        ),
        "derived_from_analysis",
    )


def _build_source_anchors(
    facets: list[StoneWritingFacetContext],
    writing_guide: dict[str, Any],
    stone_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen_quotes: set[str] = set()

    def add_anchor(
        *,
        anchor_id: str,
        source: str,
        quote: Any,
        title: str = "",
        facet_key: str | None = None,
        role: str | None = None,
        note: str = "",
    ) -> None:
        text = _trim_text(quote, 260)
        if not text:
            return
        dedupe_key = f"{source}:{facet_key or ''}:{text}"
        if dedupe_key in seen_quotes:
            return
        seen_quotes.add(dedupe_key)
        anchors.append(
            {
                "id": anchor_id,
                "source": source,
                "facet_key": facet_key,
                "title": title,
                "role": role,
                "quote": text,
                "note": note,
            }
        )

    for facet in facets:
        for index, item in enumerate(facet.fewshots, start=1):
            add_anchor(
                anchor_id=f"facet:{facet.key}:fewshot:{index}",
                source="facet_fewshot",
                facet_key=facet.key,
                title=facet.label,
                quote=item.get("quote"),
                note=item.get("reason") or item.get("expression") or "",
            )
        for index, item in enumerate(facet.evidence, start=1):
            add_anchor(
                anchor_id=f"facet:{facet.key}:evidence:{index}",
                source="facet_evidence",
                facet_key=facet.key,
                title=str(item.get("title") or item.get("document_title") or facet.label),
                quote=item.get("quote") or item.get("content") or item.get("text"),
                note=str(item.get("reason") or item.get("label") or ""),
            )

    for index, item in enumerate(writing_guide.get("fewshot_anchors") or [], start=1):
        if not isinstance(item, dict):
            continue
        add_anchor(
            anchor_id=f"guide:anchor:{index}",
            source="writing_guide",
            title=str(item.get("title") or "Writing Guide"),
            quote=item.get("quote") or item.get("line"),
            role="guide",
        )

    for profile_index, profile in enumerate(stone_profiles, start=1):
        document_id = str(profile.get("document_id") or profile_index)
        title = str(profile.get("title") or f"article {profile_index}")
        passages = [
            str(item).strip()
            for item in (profile.get("representative_lines") or profile.get("selected_passages") or [])
            if str(item).strip()
        ]
        if profile.get("content_summary"):
            passages.append(str(profile.get("content_summary")))
        for passage_index, passage in enumerate(passages[:4], start=1):
            role = "opening" if passage_index == 1 else "turn" if passage_index < len(passages[:4]) else "closing"
            add_anchor(
                anchor_id=f"profile:{document_id}:passage:{passage_index}",
                source="stone_profile_v2",
                title=title,
                quote=passage,
                role=role,
            )

    return anchors[:48]


def _attach_anchor_ids_to_facets(facets: list[StoneWritingFacetContext], anchors: list[dict[str, Any]]) -> None:
    global_anchor_ids = [str(item.get("id") or "") for item in anchors if item.get("id")][:6]
    for facet in facets:
        facet_anchor_ids = [
            str(item.get("id") or "")
            for item in anchors
            if item.get("facet_key") == facet.key and item.get("id")
        ]
        facet.anchor_ids = _unique_preserve_order([*facet_anchor_ids, *global_anchor_ids])[:8]


def _build_generation_packet(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    guide = dict(bundle.writing_guide or {})
    anchors = list(bundle.source_anchors or [])
    guide_slice = {
        "voice_dna": guide.get("voice_dna") or {},
        "sentence_mechanics": guide.get("sentence_mechanics") or {},
        "structure_patterns": guide.get("structure_patterns") or [],
        "do_and_dont": guide.get("do_and_dont") or {},
        "topic_translation_rules": guide.get("topic_translation_rules") or [],
        "revision_rubric": guide.get("revision_rubric") or [],
        "word_count_strategies": guide.get("word_count_strategies") or {},
    }
    return {
        "baseline": {
            "analysis_ready": True,
            "guide_available": bool(guide),
            "guide_source": bundle.guide_source,
            "exemplar_packet_ready": bool(anchors),
            "source": "analysis + guide + exemplars",
        },
        "analysis_run": {
            "run_id": bundle.run_id,
            "version_label": bundle.version_label,
            "target_role": bundle.target_role,
            "analysis_context": bundle.analysis_context,
        },
        "facets": {
            facet.key: {
                "label": facet.label,
                "purpose": facet.purpose,
                "confidence": facet.confidence,
                "summary": facet.summary,
                "bullets": facet.bullets,
                "anchor_ids": facet.anchor_ids,
            }
            for facet in bundle.facets
        },
        "writing_guide": guide_slice,
        "exemplars": {
            "anchors": anchors,
            "openings": [item for item in anchors if item.get("role") == "opening"][:6],
            "turns": [item for item in anchors if item.get("role") == "turn"][:6],
            "closings": [item for item in anchors if item.get("role") == "closing"][:6],
            "common_imagery": _normalize_string_list(guide.get("motif_theme_bank"), limit=8),
            "common_syntax": _normalize_string_list((guide.get("sentence_mechanics") or {}).get("cadence"), limit=8),
            "forbidden_expressions": _normalize_string_list((guide.get("do_and_dont") or {}).get("dont"), limit=8),
        },
        "review_constraints": {
            "nonclinical_psychodynamics": guide.get("nonclinical_psychodynamics") or [],
            "clinical_profile": ((guide.get("external_slots") or {}).get("clinical_profile") or {}),
            "rule": "Use only as reviewer/constraint input; never put DSM, diagnosis, or pathology labels into article prose.",
        },
    }


def _build_translation_packet(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    packet = bundle.generation_packet
    return {
        "topic_translation_rules": (packet.get("writing_guide") or {}).get("topic_translation_rules") or [],
        "do_and_dont": (packet.get("writing_guide") or {}).get("do_and_dont") or {},
        "motifs": (packet.get("exemplars") or {}).get("common_imagery") or [],
        "facets": {
            key: value
            for key, value in (packet.get("facets") or {}).items()
            if key in {"imagery_theme", "stance_values", "emotional_arc", "creative_constraints"}
        },
        "anchors": (packet.get("exemplars") or {}).get("anchors") or [],
        "review_constraints": packet.get("review_constraints") or {},
    }


def _build_outline_packet(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    packet = bundle.generation_packet
    return {
        "sentence_mechanics": (packet.get("writing_guide") or {}).get("sentence_mechanics") or {},
        "structure_patterns": (packet.get("writing_guide") or {}).get("structure_patterns") or [],
        "word_count_strategies": (packet.get("writing_guide") or {}).get("word_count_strategies") or {},
        "anchors": (packet.get("exemplars") or {}).get("anchors") or [],
        "facets": {
            key: value
            for key, value in (packet.get("facets") or {}).items()
            if key in {"structure_composition", "voice_signature", "lexicon_idiolect", "emotional_arc"}
        },
    }


def _build_drafting_packet(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    packet = bundle.generation_packet
    facets = {
        key: value
        for key, value in (packet.get("facets") or {}).items()
        if key != "nonclinical_psychodynamics"
    }
    return {
        "baseline": packet.get("baseline") or {},
        "facets": facets,
        "writing_guide": packet.get("writing_guide") or {},
        "exemplars": packet.get("exemplars") or {},
    }


def _normalize_topic_translation_payload(payload: dict[str, Any], bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    available_anchor_ids = _available_anchor_ids(bundle)
    anchor_ids = [item for item in _normalize_string_list(payload.get("anchor_ids"), limit=8) if item in available_anchor_ids]
    if not anchor_ids:
        anchor_ids = available_anchor_ids[:3]
    not_to_write = _normalize_string_list(payload.get("not_to_write"), limit=8)
    not_to_write = _unique_preserve_order(
        [
            *not_to_write,
            "不要写成诊断、DSM、病理标签或心理解释报告。",
            "不要写成提示词说明、分析结论复述或写作方案说明。",
        ]
    )
    return {
        "scene": _normalize_string_list(payload.get("scene"), limit=6),
        "imagery": _normalize_string_list(payload.get("imagery"), limit=6),
        "felt_cost": _normalize_string_list(payload.get("felt_cost") or payload.get("cost"), limit=6),
        "relationship_pressure": _normalize_string_list(payload.get("relationship_pressure"), limit=6),
        "stance": _normalize_string_list(payload.get("stance"), limit=6),
        "emotional_arc": _normalize_string_list(payload.get("emotional_arc"), limit=6),
        "not_to_write": not_to_write[:8],
        "anchor_ids": anchor_ids,
    }


def _normalize_outline_payload(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    target_word_count: int,
) -> dict[str, Any]:
    target = max(100, int(payload.get("target_word_count") or target_word_count or 0))
    available_anchor_ids = _available_anchor_ids(bundle)
    paragraphs_source = payload.get("paragraphs") if isinstance(payload.get("paragraphs"), list) else []
    desired_count = _clamp_int(payload.get("paragraph_count"), default=_default_paragraph_count(target), minimum=3, maximum=6)
    paragraphs: list[dict[str, Any]] = []
    for index, item in enumerate(paragraphs_source[:6], start=1):
        if not isinstance(item, dict):
            continue
        anchor_ids = [
            anchor_id
            for anchor_id in _normalize_string_list(item.get("anchor_ids") or item.get("anchor_id"), limit=4)
            if anchor_id in available_anchor_ids
        ]
        if not anchor_ids and available_anchor_ids:
            anchor_ids = [available_anchor_ids[(index - 1) % len(available_anchor_ids)]]
        paragraphs.append(
            {
                "index": int(item.get("index") or index),
                "function": str(item.get("function") or item.get("role") or "").strip(),
                "emotional_position": str(item.get("emotional_position") or item.get("emotion") or "").strip(),
                "anchor_ids": anchor_ids,
                "target_words": max(40, int(item.get("target_words") or round(target / max(desired_count, 1)))),
                "closing_move": str(item.get("closing_move") or item.get("ending") or "").strip(),
            }
        )
    if len(paragraphs) < 3:
        paragraphs = _skeleton_outline_from_anchors(available_anchor_ids, target, desired_count)
    paragraphs = paragraphs[:6]
    _rebalance_outline_words(paragraphs, target)
    return {
        "target_word_count": target,
        "paragraph_count": len(paragraphs),
        "word_count_strategy": str(payload.get("word_count_strategy") or f"用 {len(paragraphs)} 段控制篇幅，每段按计划推进，不在结尾补段。").strip(),
        "paragraphs": paragraphs,
        "not_to_write": _normalize_string_list(payload.get("not_to_write"), limit=8),
    }


def _skeleton_outline_from_anchors(anchor_ids: list[str], target: int, desired_count: int) -> list[dict[str, Any]]:
    count = max(3, min(6, desired_count))
    functions = ["起笔：从场景或物件进入，不解释题目", "推进：让关系压力和代价浮出", "转折：把情绪往回收", "再推进：补一层具体动作", "收口：留余味，不下结论", "余波：只留下一个未关紧的动作"]
    paragraphs = []
    for index in range(count):
        paragraphs.append(
            {
                "index": index + 1,
                "function": functions[index],
                "emotional_position": ["压低", "显影", "转折", "回收", "余波", "停住"][index],
                "anchor_ids": [anchor_ids[index % len(anchor_ids)]] if anchor_ids else [],
                "target_words": max(40, round(target / count)),
                "closing_move": "用动作或意象收束，不解释。",
            }
        )
    return paragraphs


def _rebalance_outline_words(paragraphs: list[dict[str, Any]], target: int) -> None:
    if not paragraphs:
        return
    current = sum(int(item.get("target_words") or 0) for item in paragraphs)
    if current <= 0:
        even = max(40, round(target / len(paragraphs)))
        for item in paragraphs:
            item["target_words"] = even
        return
    delta = target - current
    paragraphs[-1]["target_words"] = max(40, int(paragraphs[-1].get("target_words") or 0) + delta)


def _render_topic_translation(payload: dict[str, Any]) -> str:
    sections = [
        ("场景", payload.get("scene")),
        ("意象", payload.get("imagery")),
        ("代价", payload.get("felt_cost")),
        ("关系压力", payload.get("relationship_pressure")),
        ("立场", payload.get("stance")),
        ("情绪弧线", payload.get("emotional_arc")),
        ("不该写成", payload.get("not_to_write")),
        ("Anchors", payload.get("anchor_ids")),
    ]
    lines: list[str] = []
    for title, value in sections:
        items = _normalize_string_list(value, limit=8)
        if not items:
            continue
        lines.append(f"### {title}")
        lines.extend(f"- {item}" for item in items)
        lines.append("")
    return "\n".join(lines).strip()


def _render_outline(payload: dict[str, Any]) -> str:
    lines = [
        f"目标字数：{payload.get('target_word_count')}",
        f"段落数：{payload.get('paragraph_count')}",
        f"策略：{payload.get('word_count_strategy') or ''}",
        "",
    ]
    for item in payload.get("paragraphs") or []:
        lines.append(f"### P{item.get('index')} · {item.get('function')}")
        lines.append(f"- 情绪位置：{item.get('emotional_position')}")
        lines.append(f"- Anchor：{', '.join(item.get('anchor_ids') or [])}")
        lines.append(f"- 目标字数：{item.get('target_words')}")
        lines.append(f"- 收束方式：{item.get('closing_move')}")
        lines.append("")
    return "\n".join(lines).strip()


def _render_review_plan(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("summary") or "评审已合并。").strip(), ""]
    keep = _normalize_string_list(payload.get("must_keep_spans"), limit=6)
    rewrite = _normalize_review_items(payload.get("must_rewrite_spans"), limit=6)
    instructions = _normalize_review_items(payload.get("revision_instructions"), limit=6)
    if keep:
        lines.append("### 必须保留")
        lines.extend(f"- {item}" for item in keep)
        lines.append("")
    if rewrite:
        lines.append("### 必须改写")
        lines.extend(f"- [{item.get('anchor_id')}] {item.get('span')}: {item.get('instruction')}" for item in rewrite)
        lines.append("")
    if instructions:
        lines.append("### 修订指令")
        lines.extend(f"- [{item.get('anchor_id')}] {item.get('instruction') or item.get('issue')}" for item in instructions)
    return "\n".join(lines).strip()


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


def _build_reviewer_message_payload(review: dict[str, Any]) -> dict[str, Any]:
    key = str(review.get("dimension_key") or "reviewer").strip() or "reviewer"
    label = str(review.get("dimension_label") or review.get("dimension") or "Reviewer").strip() or "Reviewer"
    return {
        "stage": "reviewer",
        "label": f"{label} 评审",
        "actor_id": f"reviewer-{key}",
        "actor_name": label,
        "actor_role": "reviewer",
        "message_kind": "review",
        "body": _render_review_message(review),
        "detail": review,
        "created_at": _iso_now(),
    }


def _render_review_message(review: dict[str, Any]) -> str:
    lines = [
        f"结论：{'通过' if review.get('pass') else '需要修改'}",
        f"分数：{int(round(float(review.get('score') or 0.0) * 100))}/100",
    ]
    anchor_ids = _normalize_string_list(review.get("anchor_ids"), limit=6)
    signals = _normalize_string_list(review.get("matched_signals"), limit=4)
    violations = _normalize_review_items(review.get("violations"), limit=4)
    keep_spans = _normalize_string_list(review.get("must_keep_spans"), limit=4)
    rewrite_spans = _normalize_review_items(review.get("must_rewrite_spans"), limit=4)
    instructions = _normalize_review_items(review.get("revision_instructions"), limit=5)

    if anchor_ids:
        lines.append("")
        lines.append("Anchor：")
        lines.extend(f"- {item}" for item in anchor_ids)
    if signals:
        lines.append("")
        lines.append("命中信号：")
        lines.extend(f"- {item}" for item in signals)
    if keep_spans:
        lines.append("")
        lines.append("必须保留：")
        lines.extend(f"- {item}" for item in keep_spans)
    if violations:
        lines.append("")
        lines.append("问题：")
        lines.extend(f"- [{item.get('anchor_id')}] {item.get('issue') or item.get('instruction') or item.get('span')}" for item in violations)
    if rewrite_spans:
        lines.append("")
        lines.append("必须改写：")
        lines.extend(f"- [{item.get('anchor_id')}] {item.get('span')}: {item.get('instruction')}" for item in rewrite_spans)
    if instructions:
        lines.append("")
        lines.append("修改建议：")
        lines.extend(f"- [{item.get('anchor_id')}] {item.get('instruction') or item.get('issue')}" for item in instructions)
    return "\n".join(lines).strip()


def _heuristic_initial_draft(
    topic: str,
    target_word_count: int,
    analysis_bundle: StoneWritingAnalysisBundle,
    extra_requirements: str | None,
) -> str:
    voice_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "voice_signature"))[:3], fallback="压低声调、克制推进")
    imagery_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:3], fallback="夜色、旧物、余温")
    stance_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:3], fallback="代价、边界、判断")
    emotion_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:3], fallback="迟疑、压抑、回收")
    anchor_quote = _first_anchor_quote(analysis_bundle)

    paragraphs = [
        f"{topic}不是那种适合被大声宣告的题目，它更像一块压在心口的旧石头，白天不响，夜里才慢慢显出重量。",
        f"如果沿着{voice_hint}去写，叙述就不该急着解释一切，而是让人、物和动作先站到前面，再让情绪从缝隙里露出来。",
        f"场景里可以反复回到{imagery_hint}，因为真正支撑这篇文章的，不是结论本身，而是这些意象如何把主题一点点压回现实。",
        f"写到最后，仍然要落到{stance_hint}，以及情绪在{emotion_hint}之间的来回折返，让文章的收口留有余味，而不是把话说尽。",
    ]
    if anchor_quote:
        paragraphs.append(f"分析里最能充当锚点的一句原话是：{anchor_quote}")
    if extra_requirements:
        paragraphs.append(f"这次写作还要继续守住一个额外要求：{extra_requirements}。")
    return _fit_word_count("\n\n".join(paragraphs), target_word_count, analysis_bundle, topic, extra_requirements)


def _fit_word_count(
    text: str,
    target_word_count: int,
    analysis_bundle: StoneWritingAnalysisBundle,
    topic: str,
    extra_requirements: str | None,
) -> str:
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


def _heuristic_review_payload(
    facet: StoneWritingFacetContext,
    draft: str,
    analysis_bundle: StoneWritingAnalysisBundle,
    topic: str,
    target_word_count: int,
) -> dict[str, Any]:
    del analysis_bundle
    cue_terms = _facet_terms(facet)[:8]
    cue_hits = [item for item in cue_terms if item and item in draft]
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n+", draft) if item.strip()]
    word_count = estimate_word_count(draft)
    lower = int(max(100, int(target_word_count or 0)) * 0.9)
    upper = int(max(100, int(target_word_count or 0)) * 1.05)

    strengths: list[str] = []
    issues: list[str] = []
    instructions: list[str] = []
    score = 0.7

    if cue_hits:
        strengths.append("文章已经和该维度的基线信号建立了连接。")
        score += 0.1
    else:
        issues.append("这一维度的作者特征还不够明确。")
        instructions.append("回到该维度的分析结论，把对应的表达痕迹补进正文。")
        score -= 0.08

    if facet.key == "structure_composition":
        if len(paragraphs) < 3:
            issues.append("段落推进偏平，首稿的结构节拍还不够清楚。")
            instructions.append("把起笔、推进和收口拆成更明确的段落层次。")
            score -= 0.08
        else:
            strengths.append("段落层次基本成立。")
    elif facet.key == "stance_values":
        if topic and topic not in draft:
            issues.append("主题在正文里的可见度偏弱。")
            instructions.append("让主题直接进入正文，而不是只停留在命题层。")
            score -= 0.1
        else:
            strengths.append("主题与立场已经发生绑定。")
    elif facet.key == "creative_constraints":
        if not lower <= word_count <= upper:
            issues.append("字数还没有稳稳落在目标附近。")
            instructions.append("收紧或扩展篇幅，让终稿贴近目标字数。")
            score -= 0.1
        if _duplicate_sentence_ratio(draft) > 0.22:
            issues.append("部分句意有重复，容易把文风写平。")
            instructions.append("去掉重复表述，让每一段只保留必要动作。")
            score -= 0.06
    elif facet.key == "emotional_arc":
        if len(paragraphs) >= 3:
            strengths.append("情绪推进已经开始形成层次。")
        else:
            issues.append("情绪弧线还不够完整。")
            instructions.append("补出情绪从压低到显影再到回落的过程。")
            score -= 0.06

    if not strengths:
        strengths.append("首稿已经有一个可继续修的基础。")
    if not instructions:
        instructions = issues[:] or ["保持当前优势，只做必要收束。"]

    score = max(0.0, min(score, 0.95))
    passed = score >= 0.72 and not issues
    return {
        "dimension": facet.label,
        "dimension_key": facet.key,
        "dimension_label": facet.label,
        "pass": passed,
        "score": round(score, 3),
        "strengths": strengths[:4],
        "issues": issues[:4],
        "revision_instructions": instructions[:5],
        "supporting_signals": cue_hits[:4] or cue_terms[:4] or facet.bullets[:2],
    }


def _normalize_review_payload(
    payload: dict[str, Any],
    facet: StoneWritingFacetContext,
    source_anchors: list[dict[str, Any]],
) -> dict[str, Any]:
    score = _clamp_score(payload.get("score"), default=0.68)
    valid_anchor_ids = {str(item.get("id") or "").strip() for item in source_anchors if item.get("id")}
    anchor_ids = [
        item
        for item in _normalize_string_list(payload.get("anchor_ids") or payload.get("anchors"), limit=6)
        if item in valid_anchor_ids
    ]
    matched_signals = _normalize_string_list(
        payload.get("matched_signals") or payload.get("supporting_signals") or payload.get("evidence"),
        limit=4,
    )
    violations = [
        item
        for item in _normalize_review_items(payload.get("violations") or payload.get("issues"), limit=6)
        if str(item.get("anchor_id") or "").strip() in valid_anchor_ids
    ]
    must_keep_spans = _normalize_string_list(
        payload.get("must_keep_spans") or payload.get("strengths") or payload.get("keep"),
        limit=5,
    )
    must_rewrite_spans = [
        item
        for item in _normalize_review_items(payload.get("must_rewrite_spans") or payload.get("must_fix"), limit=6)
        if str(item.get("anchor_id") or "").strip() in valid_anchor_ids
    ]
    revision_instructions = [
        item
        for item in _normalize_review_items(payload.get("revision_instructions"), limit=6)
        if str(item.get("anchor_id") or "").strip() in valid_anchor_ids
    ]
    passed = bool(payload.get("pass")) if "pass" in payload else (score >= 0.72 and not violations and not must_rewrite_spans)
    return {
        "dimension": facet.label,
        "dimension_key": facet.key,
        "dimension_label": facet.label,
        "pass": passed,
        "score": round(score, 3),
        "anchor_ids": anchor_ids,
        "matched_signals": matched_signals,
        "violations": violations,
        "must_keep_spans": must_keep_spans,
        "must_rewrite_spans": must_rewrite_spans,
        "revision_instructions": revision_instructions,
        "strengths": must_keep_spans,
        "issues": [str(item.get("issue") or item.get("instruction") or "").strip() for item in violations if str(item.get("issue") or item.get("instruction") or "").strip()],
        "supporting_signals": matched_signals,
    }


def _heuristic_review_plan(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    keep = _unique_preserve_order(item for review in reviews for item in review.get("must_keep_spans", []))
    rewrite_spans = [
        item
        for review in reviews
        for item in _normalize_review_items(review.get("must_rewrite_spans"), limit=8)
    ]
    instructions = [
        item
        for review in reviews
        for item in _normalize_review_items(review.get("revision_instructions"), limit=8)
    ]
    risk_watch = _unique_preserve_order(
        f"{review.get('dimension_label')}: {item.get('issue') or item.get('instruction')}"
        for review in reviews
        for item in _normalize_review_items(review.get("violations"), limit=8)
    )
    pass_count = sum(1 for review in reviews if review.get("pass"))
    return {
        "summary": f"{pass_count}/8 个维度已经达标，优先修补最不稳的维度，不要平均用力。",
        "must_keep_spans": keep[:5],
        "must_rewrite_spans": rewrite_spans[:6],
        "revision_instructions": instructions[:6],
        "risk_watch": risk_watch[:4],
        "keep": keep[:5],
        "priorities": [str(item.get("instruction") or item.get("issue") or "").strip() for item in instructions[:6]],
        "revision_blueprint": [str(item.get("instruction") or item.get("issue") or "").strip() for item in instructions[:6]],
    }


def _normalize_review_plan_payload(payload: dict[str, Any], reviews: list[dict[str, Any]]) -> dict[str, Any]:
    heuristic = _heuristic_review_plan(reviews)
    summary = str(payload.get("summary") or heuristic["summary"]).strip()
    keep = _normalize_string_list(payload.get("must_keep_spans") or payload.get("keep"), limit=5) or heuristic["must_keep_spans"]
    rewrite_spans = _normalize_review_items(
        payload.get("must_rewrite_spans") or payload.get("rewrite_spans"),
        limit=6,
    ) or heuristic["must_rewrite_spans"]
    revision_instructions = _normalize_review_items(
        payload.get("revision_instructions") or payload.get("priorities") or payload.get("revision_blueprint"),
        limit=6,
    ) or heuristic["revision_instructions"]
    risk_watch = _normalize_string_list(payload.get("risk_watch"), limit=4) or heuristic["risk_watch"]
    plan = {
        "summary": summary,
        "must_keep_spans": keep,
        "must_rewrite_spans": rewrite_spans,
        "revision_instructions": revision_instructions,
        "risk_watch": risk_watch,
        "keep": keep,
        "priorities": [str(item.get("instruction") or item.get("issue") or "").strip() for item in revision_instructions],
        "revision_blueprint": [str(item.get("instruction") or item.get("issue") or "").strip() for item in revision_instructions],
    }
    return _repair_review_plan_anchors(plan, heuristic, reviews)


def _repair_review_plan_anchors(
    plan: dict[str, Any],
    heuristic: dict[str, Any],
    reviews: list[dict[str, Any]],
) -> dict[str, Any]:
    valid_anchor_ids = _valid_review_anchor_ids(reviews)
    if not valid_anchor_ids:
        return plan

    repaired = dict(plan)
    for key in ("must_rewrite_spans", "revision_instructions"):
        model_items = _normalize_review_items(repaired.get(key), limit=6)
        heuristic_items = _normalize_review_items(heuristic.get(key), limit=6)
        repaired[key] = _repair_review_items_anchors(model_items, heuristic_items, valid_anchor_ids)

    revision_instructions = _normalize_review_items(repaired.get("revision_instructions"), limit=6)
    repaired["priorities"] = [
        str(item.get("instruction") or item.get("issue") or "").strip()
        for item in revision_instructions
        if str(item.get("instruction") or item.get("issue") or "").strip()
    ]
    repaired["revision_blueprint"] = list(repaired["priorities"])
    return repaired


def _repair_review_items_anchors(
    model_items: list[dict[str, str]],
    heuristic_items: list[dict[str, str]],
    valid_anchor_ids: list[str],
) -> list[dict[str, str]]:
    repaired: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    def append_item(item: dict[str, str]) -> None:
        anchor_id = str(item.get("anchor_id") or "").strip()
        if anchor_id not in valid_anchor_ids:
            return
        key = (
            anchor_id,
            str(item.get("span") or "").strip(),
            str(item.get("instruction") or item.get("issue") or "").strip(),
        )
        if key in seen:
            return
        seen.add(key)
        repaired.append(dict(item))

    for item in model_items:
        append_item(item)
    if len(repaired) == len(model_items):
        return repaired
    for item in heuristic_items:
        append_item(item)
    return repaired


def _heuristic_revise_draft(
    draft: str,
    analysis_bundle: StoneWritingAnalysisBundle,
    reviews: list[dict[str, Any]],
    review_plan: dict[str, Any],
    topic: str,
    target_word_count: int,
    extra_requirements: str | None,
) -> str:
    revised = str(draft or "").strip()
    failed_keys = {str(review.get("dimension_key") or "") for review in reviews if not review.get("pass")}
    priorities = _normalize_string_list(review_plan.get("priorities"), limit=6)

    if topic and topic not in revised:
        revised = f"{topic}，真正难写的，从来不是表面的事情，而是它会把人重新拖回现实里。\n\n{revised}"

    additions: list[str] = []
    if "voice_signature" in failed_keys:
        voice_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "voice_signature"))[:2], fallback="更低、更稳的声调")
        additions.append(f"写到这里，口气还是要继续往{voice_hint}里收，不能突然拔高。")
    if "lexicon_idiolect" in failed_keys:
        lexicon_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "lexicon_idiolect"))[:2], fallback="作者惯用的转折和落点")
        additions.append(f"真正该留下来的说法，往往不是漂亮，而是{lexicon_hint}。")
    if "imagery_theme" in failed_keys:
        imagery_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:2], fallback="熟悉的意象和旧场景")
        additions.append(f"等到情绪再往里沉一点，场景还是会回到{imagery_hint}。")
    if "stance_values" in failed_keys:
        stance_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:2], fallback="代价、边界和判断")
        additions.append(f"归根到底，这件事还是要落到{stance_hint}上，而不是变成一个干净的结论。")
    if "emotional_arc" in failed_keys:
        emotion_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:2], fallback="迟疑和回落")
        additions.append(f"情绪不该一下子摊开，它应该先往里压，再沿着{emotion_hint}慢慢显出来。")
    if "nonclinical_psychodynamics" in failed_keys:
        psycho_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "nonclinical_psychodynamics"))[:2], fallback="退缩、防卫和自我回收")
        additions.append(f"人真正用来保护自己的，很多时候就是{psycho_hint}这些动作。")
    if "creative_constraints" in failed_keys:
        revised = _dedupe_sentences(revised)

    if priorities:
        additions.append(f"这次修订最重要的两件事是：{ '；'.join(priorities[:2]) }。")
    if extra_requirements:
        additions.append(f"同时继续守住这个附加要求：{extra_requirements}。")
    if additions:
        revised = f"{revised}\n\n" + "\n\n".join(additions)

    return _fit_word_count(revised, target_word_count, analysis_bundle, topic, extra_requirements)


def _build_final_assessment(
    final_text: str,
    reviews: list[dict[str, Any]],
    review_plan: dict[str, Any],
    topic: str,
    target_word_count: int,
) -> dict[str, Any]:
    word_count = estimate_word_count(final_text)
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.9)
    upper = int(target * 1.05)
    pass_count = sum(1 for review in reviews if review.get("pass"))
    remaining_risks = _normalize_string_list(review_plan.get("risk_watch"), limit=4)
    if not lower <= word_count <= upper:
        remaining_risks.append("字数仍然需要人工复核。")
    if topic and topic not in final_text:
        remaining_risks.append("主题在正文里的可见度仍然偏弱。")
    return {
        "reviewer_pass_count": pass_count,
        "reviewer_total": len(reviews),
        "length_ok": lower <= word_count <= upper,
        "topic_visible": bool(topic and topic in final_text),
        "remaining_risks": remaining_risks[:4],
    }


def _build_trace_blocks(
    analysis_bundle: StoneWritingAnalysisBundle,
    topic_translation: dict[str, Any],
    outline: dict[str, Any],
    reviews: list[dict[str, Any]],
    review_plan: dict[str, Any],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "generation_packet",
            "label": f"Generation packet ready ({analysis_bundle.version_label})",
            "baseline": analysis_bundle.generation_packet.get("baseline", {}),
        },
        {
            "type": "stage",
            "stage": "topic_translation",
            "label": "Topic translated into author world",
            "anchor_ids": topic_translation.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "outline",
            "label": "Paragraph outline planned",
            "paragraph_count": outline.get("paragraph_count"),
        },
        {
            "type": "stage",
            "stage": "draft",
            "label": "First draft completed",
        },
    ]
    for review in reviews:
        blocks.append(
            {
                "type": "review",
                "dimension": review["dimension_label"],
                "score": review["score"],
                "anchor_ids": review.get("anchor_ids") or [],
                "matched_signals": review.get("matched_signals") or [],
                "violations": review.get("violations") or [],
                "must_fix": review.get("revision_instructions") or review.get("must_rewrite_spans") or [],
                "keep": review.get("must_keep_spans") or [],
                "pass": review.get("pass"),
                "issues": review.get("violations") or review.get("issues") or [],
                "revision_instructions": review.get("revision_instructions") or [],
            }
        )
    blocks.append(
        {
            "type": "review_plan",
            "label": "Merged eight reviewer notes",
            "summary": review_plan.get("summary"),
            "priorities": review_plan.get("revision_instructions") or review_plan.get("priorities") or [],
            "keep": review_plan.get("must_keep_spans") or review_plan.get("keep") or [],
            "must_rewrite_spans": review_plan.get("must_rewrite_spans") or [],
        }
    )
    blocks.append(
        {
            "type": "stage",
            "stage": "final",
            "label": "Final revision completed",
        }
    )
    return blocks


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


def _normalize_review_items(value: Any, *, limit: int = 6) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, str):
        value = _normalize_string_list(value, limit=limit)
    for item in value or []:
        if isinstance(item, dict):
            anchor_id = _extract_review_item_anchor_id(item)
            span = str(item.get("span") or item.get("text") or "").strip()
            issue = str(item.get("issue") or item.get("reason") or "").strip()
            instruction = str(item.get("instruction") or item.get("revision") or item.get("fix") or "").strip()
        else:
            anchor_id = ""
            span = ""
            issue = str(item or "").strip()
            instruction = ""
        if not any((anchor_id, span, issue, instruction)):
            continue
        items.append(
            {
                "anchor_id": anchor_id,
                "span": span,
                "issue": issue,
                "instruction": instruction,
            }
        )
        if len(items) >= limit:
            break
    return items


def _extract_review_item_anchor_id(item: dict[str, Any]) -> str:
    value = (
        item.get("anchor_id")
        or item.get("source_anchor_id")
        or item.get("source_anchor")
        or item.get("anchor")
        or item.get("anchor_ids")
        or item.get("anchors")
    )
    if isinstance(value, dict):
        value = value.get("id") or value.get("anchor_id")
    if isinstance(value, (list, tuple, set)):
        value = next((entry for entry in value if str(entry or "").strip()), "")
        if isinstance(value, dict):
            value = value.get("id") or value.get("anchor_id")
    return str(value or "").strip()


def _normalize_evidence(value: Any) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for item in value or []:
        if isinstance(item, dict):
            quote = _trim_text(item.get("quote") or item.get("content") or item.get("text"), 240)
            if not quote:
                continue
            evidence.append(
                {
                    "quote": quote,
                    "title": _trim_text(item.get("title") or item.get("document_title") or item.get("filename"), 80),
                    "reason": _trim_text(item.get("reason") or item.get("label"), 120),
                }
            )
        else:
            quote = _trim_text(item, 240)
            if quote:
                evidence.append({"quote": quote, "title": "", "reason": ""})
        if len(evidence) >= 6:
            break
    return evidence


def _normalize_fewshots(value: Any) -> list[dict[str, str]]:
    fewshots: list[dict[str, str]] = []
    for item in value or []:
        if not isinstance(item, dict):
            continue
        quote = _trim_text(item.get("quote"), 160)
        if not quote:
            continue
        fewshots.append(
            {
                "quote": quote,
                "expression": _trim_text(item.get("expression"), 80),
                "situation": _trim_text(item.get("situation"), 100),
                "reason": _trim_text(item.get("reason"), 100),
            }
        )
        if len(fewshots) >= 3:
            break
    return fewshots


def _normalize_conflicts(value: Any) -> list[dict[str, str]]:
    conflicts: list[dict[str, str]] = []
    for item in value or []:
        if not isinstance(item, dict):
            continue
        detail = _trim_text(item.get("detail"), 160)
        title = _trim_text(item.get("title"), 80)
        if not detail and not title:
            continue
        conflicts.append({"title": title, "detail": detail})
        if len(conflicts) >= 3:
            break
    return conflicts


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


def _anchors_for_facet(bundle: StoneWritingAnalysisBundle, facet: StoneWritingFacetContext) -> list[dict[str, Any]]:
    ids = set(facet.anchor_ids or [])
    anchors = [item for item in bundle.source_anchors if item.get("id") in ids]
    if not anchors:
        anchors = list(bundle.source_anchors[:6])
    return anchors[:10]


def _review_has_valid_anchors(review: dict[str, Any]) -> bool:
    anchor_ids = {str(item or "").strip() for item in review.get("anchor_ids") or [] if str(item or "").strip()}
    if not anchor_ids:
        return False
    for item in _normalize_review_items(review.get("violations"), limit=12):
        if not item.get("anchor_id"):
            return False
    for item in _normalize_review_items(review.get("must_rewrite_spans"), limit=12):
        if not item.get("anchor_id"):
            return False
    for item in _normalize_review_items(review.get("revision_instructions"), limit=12):
        if not item.get("anchor_id"):
            return False
    return True


def _valid_review_anchor_ids(reviews: list[dict[str, Any]]) -> list[str]:
    values = [
        anchor_id
        for review in reviews
        for anchor_id in (review.get("anchor_ids") or [])
    ]
    return _unique_preserve_order(values)


def _review_plan_has_valid_anchors(plan: dict[str, Any], reviews: list[dict[str, Any]]) -> bool:
    valid_anchor_ids = set(_valid_review_anchor_ids(reviews))
    for key in ("must_rewrite_spans", "revision_instructions"):
        for item in _normalize_review_items(plan.get(key), limit=20):
            anchor_id = str(item.get("anchor_id") or "").strip()
            if anchor_id and anchor_id in valid_anchor_ids:
                continue
            return False
    return True


def _collect_trace_anchor_ids(
    bundle: StoneWritingAnalysisBundle,
    outline: dict[str, Any],
    reviews: list[dict[str, Any]],
) -> list[str]:
    values: list[str] = []
    values.extend(_available_anchor_ids(bundle)[:12])
    for paragraph in outline.get("paragraphs") or []:
        values.extend(paragraph.get("anchor_ids") or [])
    for review in reviews:
        values.extend(review.get("anchor_ids") or [])
    return _unique_preserve_order(values)


def _load_stone_profiles_v2(session, project_id: str) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        metadata = dict(document.metadata_json or {})
        profile = metadata.get("stone_profile_v2")
        if not isinstance(profile, dict):
            continue
        normalized = normalize_stone_profile_v2(
            profile,
            article_text=str(document.clean_text or document.raw_text or ""),
            fallback_title=document.title or document.filename,
        )
        normalized["document_id"] = document.id
        normalized["title"] = document.title or document.filename
        profiles.append(normalized)
    return profiles


def _load_stone_documents_v2(session, project_id: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        if document.ingest_status != "ready":
            continue
        documents.append(
            {
                "document_id": document.id,
                "title": document.title or document.filename,
                "text": str(document.clean_text or document.raw_text or ""),
                "clean_text": document.clean_text,
                "raw_text": document.raw_text,
            }
        )
    return documents


def _load_v2_asset_payload(session, project_id: str, *, asset_kind: str) -> dict[str, Any]:
    version = repository.get_latest_asset_version(session, project_id, asset_kind=asset_kind)
    if version and isinstance(version.json_payload, dict) and is_valid_stone_v2_asset_payload(asset_kind, version.json_payload):
        return dict(version.json_payload)
    draft = repository.get_latest_asset_draft(session, project_id, asset_kind=asset_kind)
    if draft and isinstance(draft.json_payload, dict) and is_valid_stone_v2_asset_payload(asset_kind, draft.json_payload):
        return dict(draft.json_payload)
    return {}


def _build_source_anchors_v2(prototype_index: dict[str, Any]) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_anchor(*, anchor_id: str, document_id: str, title: str, role: str, quote: Any, note: str = "") -> None:
        text = _trim_text(quote, 260)
        if not text or text in seen:
            return
        seen.add(text)
        anchors.append(
            {
                "id": anchor_id,
                "source": "stone_prototype_index_v2",
                "document_id": document_id,
                "title": title,
                "role": role,
                "quote": text,
                "note": note,
            }
        )

    for item in prototype_index.get("documents") or []:
        document_id = str(item.get("document_id") or "").strip()
        if not document_id:
            continue
        title = str(item.get("title") or "（未命名）").strip() or "（未命名）"
        windows = dict(item.get("windows") or {})
        add_anchor(
            anchor_id=f"prototype:{document_id}:opening",
            document_id=document_id,
            title=title,
            role="opening",
            quote=windows.get("opening"),
            note=item.get("prototype_family") or "",
        )
        if windows.get("pivot"):
            add_anchor(
                anchor_id=f"prototype:{document_id}:pivot",
                document_id=document_id,
                title=title,
                role="pivot",
                quote=windows.get("pivot"),
                note=item.get("prototype_family") or "",
            )
        add_anchor(
            anchor_id=f"prototype:{document_id}:closing",
            document_id=document_id,
            title=title,
            role="closing",
            quote=windows.get("closing"),
            note=item.get("prototype_family") or "",
        )
        for index, quote in enumerate(windows.get("signature_line") or [], start=1):
            add_anchor(
                anchor_id=f"prototype:{document_id}:signature:{index}",
                document_id=document_id,
                title=title,
                role="signature",
                quote=quote,
                note=item.get("prototype_family") or "",
            )
    return anchors[:48]


def _build_analysis_prompt_text_v2(bundle: StoneWritingAnalysisBundle) -> str:
    parts = [
        "Stone v2 writing baseline",
        f"Preprocess run: {bundle.run_id}",
        f"Target role: {bundle.target_role or ''}",
        f"Profile count: {len(bundle.stone_profiles)}",
        f"Prototype documents: {len((bundle.prototype_index or {}).get('documents') or [])}",
        "",
        "Voice / Form:",
        *[f"- {item}" for item in ((bundle.author_model.get("views") or {}).get("voice_form") or [])[:4]],
        "",
        "Motif / Worldview:",
        *[f"- {item}" for item in ((bundle.author_model.get("views") or {}).get("motif_worldview") or [])[:4]],
    ]
    return "\n".join(parts).strip()


def _build_generation_packet_v2(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    views = dict(bundle.author_model.get("views") or {})
    return {
        "baseline": {
            "stone_v2": True,
            "preprocess_ready": True,
            "corpus_ready": bool(bundle.stone_profiles),
            "profile_count": len(bundle.stone_profiles),
            "author_model_ready": bool(bundle.author_model),
            "prototype_index_ready": bool(bundle.prototype_index),
            "source_anchor_count": len(bundle.source_anchors),
            "source": "stone_profile_v2 + stone_author_model_v2 + stone_prototype_index_v2",
        },
        "analysis_run": {
            "run_id": bundle.run_id,
            "version_label": bundle.version_label,
            "target_role": bundle.target_role,
            "analysis_context": bundle.analysis_context,
        },
        "author_model": {
            "views": views,
            "topic_translation_map": list(bundle.author_model.get("topic_translation_map") or [])[:8],
            "anti_patterns": list(bundle.author_model.get("anti_patterns") or [])[:8],
            "length_behaviors": list(bundle.author_model.get("length_behaviors") or [])[:8],
        },
        "prototype_index": {
            "document_count": int(bundle.prototype_index.get("document_count") or 0),
            "prototype_families": [
                {
                    "family_key": item.get("family_key"),
                    "label": item.get("label"),
                    "member_count": item.get("member_count"),
                }
                for item in (bundle.author_model.get("prototype_families") or [])[:8]
            ],
        },
        "source_anchors": bundle.source_anchors[:24],
    }


def _build_evidence_planner_context_v2(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    return {
        "analysis_run": {
            "run_id": bundle.run_id,
            "version_label": bundle.version_label,
            "target_role": bundle.target_role,
            "analysis_context": bundle.analysis_context,
        },
        "author_model": {
            "views": dict(bundle.author_model.get("views") or {}),
            "topic_translation_map": list(bundle.author_model.get("topic_translation_map") or [])[:6],
            "prototype_families": list(bundle.author_model.get("prototype_families") or [])[:6],
            "anti_patterns": list(bundle.author_model.get("anti_patterns") or [])[:6],
        },
        "prototype_documents": [
            {
                "document_id": str(item.get("document_id") or ""),
                "title": str(item.get("title") or "").strip(),
                "prototype_family": str(item.get("prototype_family") or "").strip(),
                "length_band": str(item.get("length_band") or "").strip(),
                "surface_form": str(item.get("surface_form") or "").strip(),
                "motif_tags": list(item.get("motif_tags") or [])[:4],
                "windows": {
                    "opening": _trim_text((item.get("windows") or {}).get("opening"), 180),
                    "pivot": _trim_text((item.get("windows") or {}).get("pivot"), 180),
                    "closing": _trim_text((item.get("windows") or {}).get("closing"), 180),
                },
            }
            for item in (bundle.prototype_index.get("documents") or [])[:6]
        ],
        "source_anchors": [
            {
                "id": str(item.get("id") or ""),
                "source": str(item.get("source") or ""),
                "facet_key": str(item.get("facet_key") or ""),
                "title": str(item.get("title") or "").strip(),
                "role": str(item.get("role") or ""),
                "document_id": str(item.get("document_id") or ""),
                "quote": _trim_text(item.get("quote"), 220),
                "note": _trim_text(item.get("note"), 100),
            }
            for item in bundle.source_anchors[:12]
        ],
        "source_anchor_count": len(bundle.source_anchors),
        "prototype_document_count": int(bundle.prototype_index.get("document_count") or 0),
    }


def _build_topic_adapter_packet_v2(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    packet = bundle.generation_packet
    return {
        "views": ((packet.get("author_model") or {}).get("views") or {}),
        "topic_translation_map": (packet.get("author_model") or {}).get("topic_translation_map") or [],
        "anti_patterns": (packet.get("author_model") or {}).get("anti_patterns") or [],
        "prototype_families": (packet.get("prototype_index") or {}).get("prototype_families") or [],
        "source_anchors": packet.get("source_anchors") or [],
        "evidence_plan": packet.get("evidence_plan") or {},
    }


def _build_drafting_packet_v2(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    packet = bundle.generation_packet
    return {
        "baseline": packet.get("baseline") or {},
        "author_model": packet.get("author_model") or {},
        "prototype_index": {
            "document_count": (packet.get("prototype_index") or {}).get("document_count"),
            "prototype_families": (packet.get("prototype_index") or {}).get("prototype_families") or [],
        },
        "source_anchors": packet.get("source_anchors") or [],
        "evidence_plan": packet.get("evidence_plan") or {},
    }


def _build_evidence_planner_messages_v2(
    state: WritingStreamState,
    bundle: StoneWritingAnalysisBundle,
) -> list[dict[str, str]]:
    context = _build_evidence_planner_context_v2(bundle)
    return [
        {
            "role": "system",
            "content": (
                "你是 Stone v2 写作链路里的 evidence planner。\n"
                "你只能围绕当前写作任务做证据检索、片段选择和计划归纳，不要写正文。\n"
                "你可以自主调用工具去读取 source anchors、stone profiles 和 prototype documents 的更细切片。\n"
                "先检索，再归纳；先找原文片段，再决定开头、压力、转折和收口。\n"
                "只返回 JSON，不要输出解释。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                f"Stone v2 基线：\n{json.dumps(context, ensure_ascii=False, indent=2)}\n\n"
                "请返回 JSON，字段如下：\n"
                "{\n"
                '  "author_angle": "作者会从什么角度切入",\n'
                '  "entry_scene": "最适合的起笔场景或动作",\n'
                '  "felt_cost": "题目背后的代价",\n'
                '  "judgment_target": "作者会在判断谁/什么",\n'
                '  "value_lens": "代价|资格|体面|生存|虚假",\n'
                '  "desired_judgment": "厌恶|怜悯|自损|讥讽|悬置",\n'
                '  "desired_distance": "贴脸|回收|旁观|宣判",\n'
                '  "motif_path": ["建议使用的意象"],\n'
                '  "forbidden_drift": ["不要写偏成什么"],\n'
                '  "prototype_family_hints": ["优先命中的 family key 或 label"],\n'
                '  "search_terms": ["建议继续检索的关键词"],\n'
                '  "anchor_ids": ["可参考的 source anchor id"],\n'
                '  "evidence_windows": [{"anchor_id":"source anchor id","quote":"原文片段","reason":"为什么有用"}],\n'
                '  "plan_steps": ["按先后顺序的写作动作"],\n'
                '  "coverage_gaps": ["还缺什么证据"]\n'
                "}"
            ),
        },
    ]


def _compact_evidence_plan_for_prompt_v2(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "author_angle": str(payload.get("author_angle") or "").strip(),
        "entry_scene": str(payload.get("entry_scene") or "").strip(),
        "felt_cost": str(payload.get("felt_cost") or "").strip(),
        "judgment_target": str(payload.get("judgment_target") or "").strip(),
        "value_lens": str(payload.get("value_lens") or "").strip(),
        "desired_judgment": str(payload.get("desired_judgment") or "").strip(),
        "desired_distance": str(payload.get("desired_distance") or "").strip(),
        "motif_path": _normalize_string_list(payload.get("motif_path"), limit=6),
        "forbidden_drift": _normalize_string_list(payload.get("forbidden_drift"), limit=8),
        "prototype_family_hints": _normalize_string_list(payload.get("prototype_family_hints"), limit=6),
        "search_terms": _normalize_string_list(payload.get("search_terms"), limit=8),
        "anchor_ids": _normalize_string_list(payload.get("anchor_ids"), limit=8),
        "evidence_windows": [
            {
                "anchor_id": str(item.get("anchor_id") or "").strip(),
                "quote": _trim_text(item.get("quote"), 180),
                "reason": _trim_text(item.get("reason"), 100),
            }
            for item in (payload.get("evidence_windows") or [])[:6]
            if isinstance(item, dict)
        ],
        "plan_steps": _normalize_string_list(payload.get("plan_steps"), limit=6),
        "coverage_gaps": _normalize_string_list(payload.get("coverage_gaps"), limit=6),
    }


def _search_source_anchors_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    query_terms = _normalize_search_terms_v2(args.get("query"))
    source = normalize_whitespace(str(args.get("source") or ""))
    facet_key = normalize_whitespace(str(args.get("facet_key") or ""))
    role = normalize_whitespace(str(args.get("role") or ""))
    document_id = str(args.get("document_id") or "").strip()
    limit = _clamp_int(args.get("limit"), default=5, minimum=1, maximum=8)
    matched: list[dict[str, Any]] = []
    for anchor in bundle.source_anchors:
        if source and normalize_whitespace(str(anchor.get("source") or "")) != source:
            continue
        if facet_key and normalize_whitespace(str(anchor.get("facet_key") or "")) != facet_key:
            continue
        if role and normalize_whitespace(str(anchor.get("role") or "")) != role:
            continue
        if document_id and str(anchor.get("document_id") or "").strip() != document_id:
            continue
        haystack = _anchor_search_haystack_v2(anchor)
        if query_terms and not _matches_search_terms_v2(haystack, query_terms):
            continue
        matched.append(_compact_anchor_payload_v2(anchor))
        if len(matched) >= limit:
            break
    return (
        {
            "query": normalize_whitespace(str(args.get("query") or "")),
            "returned": len(matched),
            "matches": matched,
        },
        {
            "anchor_ids": {str(item.get("id") or "").strip() for item in matched if str(item.get("id") or "").strip()},
            "document_ids": {
                str(item.get("document_id") or "").strip()
                for item in matched
                if str(item.get("document_id") or "").strip()
            },
        },
    )


def _read_source_anchor_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    anchor_id = str(args.get("anchor_id") or "").strip()
    anchor = _anchor_lookup_v2(bundle).get(anchor_id)
    if not anchor:
        return {"error": f"未找到 source anchor: {anchor_id}"}, {"anchor_ids": set(), "document_ids": set()}
    document_id = str(anchor.get("document_id") or "").strip()
    related = [
        _compact_anchor_payload_v2(item)
        for item in bundle.source_anchors
        if str(item.get("document_id") or "").strip() == document_id and str(item.get("id") or "").strip() != anchor_id
    ][:4]
    return (
        {
            "anchor": _compact_anchor_payload_v2(anchor, limit=320),
            "related_anchors": related,
        },
        {
            "anchor_ids": {anchor_id, *(str(item.get("id") or "").strip() for item in related if str(item.get("id") or "").strip())},
            "document_ids": {document_id} if document_id else set(),
        },
    )


def _search_stone_profiles_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    query_terms = _normalize_search_terms_v2(args.get("query"))
    content_type = normalize_whitespace(str(args.get("content_type") or ""))
    length_band = normalize_whitespace(str(args.get("length_band") or ""))
    emotion_label = normalize_whitespace(str(args.get("emotion_label") or ""))
    limit = _clamp_int(args.get("limit"), default=4, minimum=1, maximum=6)
    matched: list[dict[str, Any]] = []
    document_ids: set[str] = set()
    anchor_ids: set[str] = set()
    for profile in bundle.stone_profiles:
        expanded = _expand_profile_for_tool_v2(profile)
        if content_type and normalize_whitespace(str(expanded.get("content_type") or "")) != content_type:
            continue
        if length_band and normalize_whitespace(str(expanded.get("length_label") or "")) != length_band:
            continue
        if emotion_label and emotion_label not in normalize_whitespace(str(expanded.get("emotion_label") or "")).lower():
            continue
        haystack = _profile_search_haystack_v2(profile, expanded)
        if query_terms and not _matches_search_terms_v2(haystack, query_terms):
            continue
        matched.append(_compact_profile_payload_v2(profile, expanded))
        document_id = str(profile.get("document_id") or "").strip()
        if document_id:
            document_ids.add(document_id)
            anchor_ids.update(_anchor_ids_for_document_v2(bundle, document_id))
        if len(matched) >= limit:
            break
    return (
        {
            "query": normalize_whitespace(str(args.get("query") or "")),
            "returned": len(matched),
            "matches": matched,
        },
        {
            "anchor_ids": anchor_ids,
            "document_ids": document_ids,
        },
    )


def _read_stone_profile_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    document_id = str(args.get("document_id") or "").strip()
    profile = next((item for item in bundle.stone_profiles if str(item.get("document_id") or "").strip() == document_id), None)
    if not profile:
        return {"error": f"未找到 stone profile: {document_id}"}, {"anchor_ids": set(), "document_ids": set()}
    expanded = _expand_profile_for_tool_v2(profile)
    anchor_ids = _anchor_ids_for_document_v2(bundle, document_id)
    return (
        {
            "profile": {
                **_compact_profile_payload_v2(profile, expanded, preview=False),
                "voice_mask": dict(profile.get("voice_mask") or {}),
                "stance_vector": dict(profile.get("stance_vector") or {}),
                "syntax_signature": dict(profile.get("syntax_signature") or {}),
                "segment_map": list(profile.get("segment_map") or [])[:4],
                "opening_move": str(profile.get("opening_move") or "").strip(),
                "turning_move": str(profile.get("turning_move") or "").strip(),
                "closure_move": str(profile.get("closure_move") or "").strip(),
                "anti_patterns": list(profile.get("anti_patterns") or [])[:6],
                "source_anchors": [
                    _compact_anchor_payload_v2(item)
                    for item in bundle.source_anchors
                    if str(item.get("document_id") or "").strip() == document_id
                ][:6],
            }
        },
        {
            "anchor_ids": anchor_ids,
            "document_ids": {document_id},
        },
    )


def _list_prototype_families_v2(
    bundle: StoneWritingAnalysisBundle,
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    families = [
        {
            "family_key": str(item.get("family_key") or "").strip(),
            "label": str(item.get("label") or item.get("family_key") or "").strip(),
            "member_count": int(item.get("member_count") or 0),
            "motif_tags": list(item.get("motif_tags") or [])[:4],
            "sample_titles": list(item.get("sample_titles") or [])[:3],
        }
        for item in (bundle.prototype_index.get("prototype_families") or [])[:12]
        if isinstance(item, dict)
    ]
    return {
        "family_count": len(families),
        "families": families,
    }, {"anchor_ids": set(), "document_ids": set()}


def _search_prototype_documents_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    query_terms = _normalize_search_terms_v2(args.get("query"))
    family_key = normalize_whitespace(str(args.get("family_key") or ""))
    length_band = normalize_whitespace(str(args.get("length_band") or ""))
    surface_form = normalize_whitespace(str(args.get("surface_form") or ""))
    limit = _clamp_int(args.get("limit"), default=4, minimum=1, maximum=6)
    matched: list[dict[str, Any]] = []
    document_ids: set[str] = set()
    anchor_ids: set[str] = set()
    for item in bundle.prototype_index.get("documents") or []:
        if family_key and family_key not in normalize_whitespace(str(item.get("prototype_family") or "")).lower():
            continue
        if length_band and normalize_whitespace(str(item.get("length_band") or "")) != length_band:
            continue
        if surface_form and normalize_whitespace(str(item.get("surface_form") or "")) != surface_form:
            continue
        haystack = _prototype_document_search_haystack_v2(item)
        if query_terms and not _matches_search_terms_v2(haystack, query_terms):
            continue
        matched.append(_compact_prototype_document_payload_v2(item))
        document_id = str(item.get("document_id") or "").strip()
        if document_id:
            document_ids.add(document_id)
            anchor_ids.update(_anchor_ids_for_document_v2(bundle, document_id))
        if len(matched) >= limit:
            break
    return (
        {
            "query": normalize_whitespace(str(args.get("query") or "")),
            "returned": len(matched),
            "matches": matched,
        },
        {
            "anchor_ids": anchor_ids,
            "document_ids": document_ids,
        },
    )


def _read_prototype_document_v2(
    bundle: StoneWritingAnalysisBundle,
    args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    document_id = str(args.get("document_id") or "").strip()
    item = next(
        (
            document
            for document in (bundle.prototype_index.get("documents") or [])
            if str(document.get("document_id") or "").strip() == document_id
        ),
        None,
    )
    if not item:
        return {"error": f"未找到 prototype document: {document_id}"}, {"anchor_ids": set(), "document_ids": set()}
    anchor_ids = _anchor_ids_for_document_v2(bundle, document_id)
    return (
        {
            "document": {
                **_compact_prototype_document_payload_v2(item, preview=False),
                "retrieval_facets": dict(item.get("retrieval_facets") or {}),
                "voice_mask": dict(item.get("voice_mask") or {}),
                "stance_vector": dict(item.get("stance_vector") or {}),
                "anchor_spans": dict(item.get("anchor_spans") or {}),
                "source_anchors": [
                    _compact_anchor_payload_v2(anchor)
                    for anchor in bundle.source_anchors
                    if str(anchor.get("document_id") or "").strip() == document_id
                ][:6],
            }
        },
        {
            "anchor_ids": anchor_ids,
            "document_ids": {document_id},
        },
    )


def _normalize_evidence_plan_payload_v2(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    *,
    query_trace: list[dict[str, Any]],
    queried_anchor_ids: list[str],
    queried_document_ids: list[str],
) -> dict[str, Any]:
    anchor_lookup = _anchor_lookup_v2(bundle)
    valid_anchor_ids = set(anchor_lookup)
    anchor_ids = _unique_preserve_order(
        [
            *_normalize_string_list(payload.get("anchor_ids"), limit=8),
            *[
                str(item.get("anchor_id") or "").strip()
                for item in (payload.get("evidence_windows") or [])
                if isinstance(item, dict)
            ],
            *[anchor_id for anchor_id in queried_anchor_ids if anchor_id in valid_anchor_ids],
        ]
    )
    anchor_ids = [anchor_id for anchor_id in anchor_ids if anchor_id in valid_anchor_ids][:8]
    if not anchor_ids:
        anchor_ids = _available_anchor_ids(bundle)[:4]
    evidence_windows = _normalize_evidence_windows_v2(
        payload.get("evidence_windows"),
        anchor_lookup=anchor_lookup,
        fallback_anchor_ids=anchor_ids,
    )
    family_hints = _normalize_string_list(payload.get("prototype_family_hints"), limit=6)
    if not family_hints:
        family_hints = _prototype_family_hints_from_documents_v2(bundle, queried_document_ids)
    if not family_hints:
        family_hints = [
            str(item.get("family_key") or item.get("label") or "").strip()
            for item in (bundle.prototype_index.get("prototype_families") or [])[:3]
            if str(item.get("family_key") or item.get("label") or "").strip()
        ]
    return {
        "author_angle": str(payload.get("author_angle") or "").strip() or "先找作者最会落地的动作，再把代价慢慢显出来。",
        "entry_scene": str(payload.get("entry_scene") or "").strip() or "从一个物件、动作或狭窄场景切入。",
        "felt_cost": str(payload.get("felt_cost") or "").strip() or "先让代价出现，再让判断自己浮出来。",
        "judgment_target": str(payload.get("judgment_target") or "").strip() or "关系处境",
        "value_lens": str(payload.get("value_lens") or "").strip() or "代价",
        "desired_judgment": str(payload.get("desired_judgment") or "").strip() or "悬置",
        "desired_distance": str(payload.get("desired_distance") or "").strip() or "贴脸",
        "motif_path": _normalize_string_list(payload.get("motif_path"), limit=6),
        "forbidden_drift": _normalize_string_list(payload.get("forbidden_drift"), limit=8)
        or ["不要写成分析说明", "不要写成诊断或自助建议"],
        "prototype_family_hints": family_hints[:6],
        "search_terms": _normalize_string_list(payload.get("search_terms"), limit=8),
        "anchor_ids": anchor_ids,
        "evidence_windows": evidence_windows[:6],
        "plan_steps": _normalize_string_list(payload.get("plan_steps"), limit=8)
        or ["先找起笔动作", "再沿代价推进", "最后回收到残响里"],
        "coverage_gaps": _normalize_string_list(payload.get("coverage_gaps"), limit=6),
        "query_trace": list(query_trace or [])[:8],
        "queried_anchor_ids": [anchor_id for anchor_id in queried_anchor_ids if anchor_id in valid_anchor_ids][:8],
        "queried_document_ids": _unique_preserve_order(queried_document_ids)[:8],
    }


def _render_evidence_plan_v2(payload: dict[str, Any]) -> str:
    lines = [
        f"切入角度：{payload.get('author_angle') or ''}",
        f"起笔场景：{payload.get('entry_scene') or ''}",
        f"代价焦点：{payload.get('felt_cost') or ''}",
        f"目标 family：{', '.join(payload.get('prototype_family_hints') or [])}",
        "",
        "检索词：",
        *[f"- {item}" for item in (payload.get("search_terms") or [])[:6]],
        "",
        "证据片段：",
    ]
    evidence_windows = list(payload.get("evidence_windows") or [])[:4]
    if evidence_windows:
        for item in evidence_windows:
            lines.append(
                f"- {item.get('anchor_id') or ''} | {item.get('quote') or ''}"
            )
            if item.get("reason"):
                lines.append(f"  {item.get('reason')}")
    else:
        lines.append("- 暂无明确片段，使用默认 anchors 兜底。")
    lines.extend(
        [
            "",
            "写作步骤：",
            *[f"- {item}" for item in (payload.get("plan_steps") or [])[:6]],
        ]
    )
    if payload.get("coverage_gaps"):
        lines.extend(["", "待补证据：", *[f"- {item}" for item in (payload.get("coverage_gaps") or [])[:4]]])
    if payload.get("fallback_reason"):
        lines.extend(["", f"回退原因：{payload.get('fallback_reason')}"])
    return "\n".join(lines).strip()


def _build_fallback_evidence_plan_v2(
    state: WritingStreamState,
    bundle: StoneWritingAnalysisBundle,
    *,
    reason: str,
) -> dict[str, Any]:
    topic_terms = _extract_topic_keywords_v2(normalize_whitespace(state.topic).lower())
    prototype_candidates = _rank_prototype_documents_for_fallback_v2(bundle, topic_terms)
    anchor_candidates = _rank_source_anchors_for_fallback_v2(bundle, topic_terms, prototype_candidates)
    chosen_anchors = anchor_candidates[:4] or list(bundle.source_anchors[:4])
    anchor_ids = [
        str(item.get("id") or "").strip()
        for item in chosen_anchors
        if str(item.get("id") or "").strip()
    ]
    motif_path = _unique_preserve_order(
        [
            tag
            for item in prototype_candidates[:2]
            for tag in (item.get("motif_tags") or [])[:3]
            if str(tag or "").strip()
        ]
    )[:6]
    family_hints = _unique_preserve_order(
        [
            str(item.get("prototype_family") or "").strip()
            for item in prototype_candidates[:3]
            if str(item.get("prototype_family") or "").strip()
        ]
    )[:6]
    evidence_windows = [
        {
            "anchor_id": str(item.get("id") or "").strip(),
            "document_id": str(item.get("document_id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "role": str(item.get("role") or "").strip(),
            "quote": _trim_text(item.get("quote"), 220),
            "reason": _fallback_anchor_reason_v2(item),
        }
        for item in chosen_anchors[:4]
        if str(item.get("id") or "").strip()
    ]
    first_anchor = chosen_anchors[0] if chosen_anchors else {}
    entry_scene = _infer_entry_scene_from_anchor_v2(first_anchor, topic_terms)
    search_terms = _unique_preserve_order(
        [
            *topic_terms[:4],
            *(motif_path[:2]),
            str(first_anchor.get("role") or "").strip(),
        ]
    )[:6]
    felt_cost = _fallback_felt_cost_v2(state.topic, prototype_candidates)
    return {
        "author_angle": "先用本地索引兜底，抓住最贴近题目的场景、代价和收口方式。",
        "entry_scene": entry_scene,
        "felt_cost": felt_cost,
        "judgment_target": "关系处境" if prototype_candidates else "眼前处境",
        "value_lens": _fallback_value_lens_v2(prototype_candidates),
        "desired_judgment": _fallback_desired_judgment_v2(prototype_candidates),
        "desired_distance": _fallback_desired_distance_v2(prototype_candidates),
        "motif_path": motif_path,
        "forbidden_drift": ["不要写成分析说明", "不要写成诊断、鸡汤或教程"],
        "prototype_family_hints": family_hints
        or [
            str(item.get("family_key") or item.get("label") or "").strip()
            for item in (bundle.prototype_index.get("prototype_families") or [])[:2]
            if str(item.get("family_key") or item.get("label") or "").strip()
        ],
        "search_terms": search_terms,
        "anchor_ids": anchor_ids,
        "evidence_windows": evidence_windows,
        "plan_steps": [
            "先从具体动作或物件起笔",
            "沿着代价和关系压力慢慢推进",
            "结尾收回到一个没说尽的残响",
        ],
        "coverage_gaps": [] if evidence_windows else ["本轮未拿到更细片段，只能用默认 anchors 兜底。"],
        "query_trace": [],
        "queried_anchor_ids": anchor_ids,
        "queried_document_ids": [
            str(item.get("document_id") or "").strip()
            for item in prototype_candidates[:3]
            if str(item.get("document_id") or "").strip()
        ],
        "planner_mode": "heuristic_fallback",
        "fallback_reason": _trim_text(reason, 180),
    }


def _anchor_lookup_v2(bundle: StoneWritingAnalysisBundle) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("id") or "").strip(): item
        for item in bundle.source_anchors
        if str(item.get("id") or "").strip()
    }


def _anchor_ids_for_document_v2(bundle: StoneWritingAnalysisBundle, document_id: str) -> set[str]:
    return {
        str(item.get("id") or "").strip()
        for item in bundle.source_anchors
        if str(item.get("document_id") or "").strip() == document_id and str(item.get("id") or "").strip()
    }


def _compact_anchor_payload_v2(anchor: dict[str, Any], *, limit: int = 220) -> dict[str, Any]:
    return {
        "id": str(anchor.get("id") or "").strip(),
        "source": str(anchor.get("source") or "").strip(),
        "title": str(anchor.get("title") or "").strip(),
        "role": str(anchor.get("role") or "").strip(),
        "document_id": str(anchor.get("document_id") or "").strip(),
        "quote": _trim_text(anchor.get("quote"), limit),
        "note": _trim_text(anchor.get("note"), 120),
    }


def _anchor_search_haystack_v2(anchor: dict[str, Any]) -> str:
    return normalize_whitespace(
        " ".join(
            [
                str(anchor.get("id") or ""),
                str(anchor.get("source") or ""),
                str(anchor.get("title") or ""),
                str(anchor.get("role") or ""),
                str(anchor.get("document_id") or ""),
                str(anchor.get("quote") or ""),
                str(anchor.get("note") or ""),
                str(anchor.get("facet_key") or ""),
            ]
        )
    ).lower()


def _expand_profile_for_tool_v2(profile: dict[str, Any]) -> dict[str, Any]:
    return expand_stone_profile_v2_for_analysis(
        profile,
        article_text=str(profile.get("article_text") or profile.get("source_text") or profile.get("raw_text") or ""),
        title=str(profile.get("title") or "").strip() or None,
    )


def _compact_profile_payload_v2(
    profile: dict[str, Any],
    expanded: dict[str, Any],
    *,
    preview: bool = True,
) -> dict[str, Any]:
    return {
        "document_id": str(profile.get("document_id") or "").strip(),
        "title": str(profile.get("title") or "").strip() or "（未命名）",
        "content_summary": _trim_text(expanded.get("content_summary"), 160 if preview else 320),
        "content_type": str(expanded.get("content_type") or "").strip(),
        "length_label": str(expanded.get("length_label") or "").strip(),
        "emotion_label": str(expanded.get("emotion_label") or "").strip(),
        "prototype_family": str(profile.get("prototype_family") or "").strip(),
        "motif_tags": list(profile.get("motif_tags") or [])[:4],
        "selected_passages": [
            _trim_text(item, 140 if preview else 260)
            for item in (expanded.get("selected_passages") or [])[:3]
        ],
    }


def _profile_search_haystack_v2(profile: dict[str, Any], expanded: dict[str, Any]) -> str:
    return normalize_whitespace(
        " ".join(
            [
                str(profile.get("document_id") or ""),
                str(profile.get("title") or ""),
                str(expanded.get("content_summary") or ""),
                str(expanded.get("content_type") or ""),
                str(expanded.get("length_label") or ""),
                str(expanded.get("emotion_label") or ""),
                str(profile.get("opening_move") or ""),
                str(profile.get("turning_move") or ""),
                str(profile.get("closure_move") or ""),
                str(profile.get("prototype_family") or ""),
                " ".join(profile.get("motif_tags") or []),
                " ".join(profile.get("lexicon_markers") or []),
                " ".join(profile.get("segment_map") or []),
                " ".join(expanded.get("selected_passages") or []),
                str(expanded.get("tone") or ""),
                str(expanded.get("structure_template") or ""),
            ]
        )
    ).lower()


def _compact_prototype_document_payload_v2(
    item: dict[str, Any],
    *,
    preview: bool = True,
) -> dict[str, Any]:
    windows = dict(item.get("windows") or {})
    return {
        "document_id": str(item.get("document_id") or "").strip(),
        "title": str(item.get("title") or "").strip() or "（未命名）",
        "prototype_family": str(item.get("prototype_family") or "").strip(),
        "length_band": str(item.get("length_band") or "").strip(),
        "surface_form": str(item.get("surface_form") or "").strip(),
        "motif_tags": list(item.get("motif_tags") or [])[:4],
        "retrieval_terms": list(item.get("retrieval_terms") or [])[:8],
        "exemplar_text": _trim_text(item.get("exemplar_text"), 180 if preview else 420),
        "windows": {
            "opening": _trim_text(windows.get("opening"), 140 if preview else 260),
            "pivot": _trim_text(windows.get("pivot"), 140 if preview else 260),
            "closing": _trim_text(windows.get("closing"), 140 if preview else 260),
            "signature_line": [_trim_text(value, 120 if preview else 220) for value in (windows.get("signature_line") or [])[:3]],
        },
    }


def _prototype_document_search_haystack_v2(item: dict[str, Any]) -> str:
    windows = dict(item.get("windows") or {})
    return normalize_whitespace(
        " ".join(
            [
                str(item.get("document_id") or ""),
                str(item.get("title") or ""),
                str(item.get("prototype_family") or ""),
                str(item.get("length_band") or ""),
                str(item.get("surface_form") or ""),
                " ".join(item.get("motif_tags") or []),
                " ".join(item.get("retrieval_terms") or []),
                str(item.get("exemplar_text") or ""),
                str(windows.get("opening") or ""),
                str(windows.get("pivot") or ""),
                str(windows.get("closing") or ""),
                " ".join(windows.get("signature_line") or []),
            ]
        )
    ).lower()


def _normalize_evidence_windows_v2(
    value: Any,
    *,
    anchor_lookup: dict[str, dict[str, Any]],
    fallback_anchor_ids: list[str],
) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value or []:
        if not isinstance(item, dict):
            continue
        anchor_id = str(item.get("anchor_id") or "").strip()
        if anchor_id not in anchor_lookup or anchor_id in seen:
            continue
        anchor = anchor_lookup[anchor_id]
        seen.add(anchor_id)
        windows.append(
            {
                "anchor_id": anchor_id,
                "document_id": str(anchor.get("document_id") or "").strip(),
                "title": str(anchor.get("title") or "").strip(),
                "role": str(anchor.get("role") or "").strip(),
                "quote": _trim_text(item.get("quote") or anchor.get("quote"), 220),
                "reason": _trim_text(item.get("reason") or anchor.get("note") or f"可用于{anchor.get('role') or '正文'}位置。", 120),
            }
        )
        if len(windows) >= 6:
            return windows
    for anchor_id in fallback_anchor_ids:
        if anchor_id not in anchor_lookup or anchor_id in seen:
            continue
        anchor = anchor_lookup[anchor_id]
        windows.append(
            {
                "anchor_id": anchor_id,
                "document_id": str(anchor.get("document_id") or "").strip(),
                "title": str(anchor.get("title") or "").strip(),
                "role": str(anchor.get("role") or "").strip(),
                "quote": _trim_text(anchor.get("quote"), 220),
                "reason": _trim_text(anchor.get("note") or f"可作为{anchor.get('role') or '正文'}的落点。", 120),
            }
        )
        seen.add(anchor_id)
        if len(windows) >= 6:
            break
    return windows


def _prototype_family_hints_from_documents_v2(
    bundle: StoneWritingAnalysisBundle,
    document_ids: list[str],
) -> list[str]:
    documents = {
        str(item.get("document_id") or "").strip(): item
        for item in (bundle.prototype_index.get("documents") or [])
        if str(item.get("document_id") or "").strip()
    }
    hints: list[str] = []
    for document_id in document_ids:
        item = documents.get(str(document_id or "").strip())
        if not item:
            continue
        family = str(item.get("prototype_family") or "").strip()
        if family:
            hints.append(family)
    return _unique_preserve_order(hints)[:6]


def _normalize_search_terms_v2(value: Any) -> list[str]:
    text = normalize_whitespace(str(value or "")).lower()
    if not text:
        return []
    raw_terms = re.findall(r"[a-z0-9]{2,}|[\u4e00-\u9fff]{2,}", text)
    return _unique_preserve_order(raw_terms)[:8]


def _matches_search_terms_v2(haystack: str, query_terms: list[str]) -> bool:
    if not query_terms:
        return True
    return all(term in haystack for term in query_terms)


def _rank_prototype_documents_for_fallback_v2(
    bundle: StoneWritingAnalysisBundle,
    topic_terms: list[str],
) -> list[dict[str, Any]]:
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in bundle.prototype_index.get("documents") or []:
        haystack = _prototype_document_search_haystack_v2(item)
        score = sum(4 if len(term) >= 4 else 2 for term in topic_terms if term in haystack)
        if str((item.get("windows") or {}).get("opening") or "").strip():
            score += 2
        if str((item.get("windows") or {}).get("closing") or "").strip():
            score += 1
        scored.append((score, item))
    scored.sort(key=lambda pair: (-pair[0], str(pair[1].get("title") or "")))
    ranked = [item for score, item in scored if score > 0]
    return ranked[:4] or [item for _, item in scored[:3]]


def _rank_source_anchors_for_fallback_v2(
    bundle: StoneWritingAnalysisBundle,
    topic_terms: list[str],
    prototype_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidate_document_ids = {
        str(item.get("document_id") or "").strip()
        for item in prototype_candidates[:3]
        if str(item.get("document_id") or "").strip()
    }
    scored: list[tuple[int, dict[str, Any]]] = []
    for anchor in bundle.source_anchors:
        haystack = _anchor_search_haystack_v2(anchor)
        score = sum(4 if len(term) >= 4 else 2 for term in topic_terms if term in haystack)
        role = str(anchor.get("role") or "").strip()
        if role == "opening":
            score += 3
        elif role == "closing":
            score += 2
        elif role == "signature":
            score += 1
        if candidate_document_ids and str(anchor.get("document_id") or "").strip() in candidate_document_ids:
            score += 3
        scored.append((score, anchor))
    scored.sort(key=lambda pair: (-pair[0], str(pair[1].get("id") or "")))
    ranked = [item for score, item in scored if score > 0]
    return ranked[:6] or [item for _, item in scored[:4]]


def _fallback_anchor_reason_v2(anchor: dict[str, Any]) -> str:
    role = str(anchor.get("role") or "").strip()
    if role == "opening":
        return "适合直接拿来做起笔动作和气氛落地。"
    if role == "closing":
        return "适合拿来约束收口，不把话说尽。"
    if role == "pivot":
        return "适合中段拧出压力或句意转折。"
    return "适合拿来补作者的句法和质感。"


def _infer_entry_scene_from_anchor_v2(anchor: dict[str, Any], topic_terms: list[str]) -> str:
    quote = str(anchor.get("quote") or "").strip()
    if quote:
        return _trim_text(quote, 28)
    if topic_terms:
        return f"从{_join_terms(topic_terms[:2], fallback='一个动作')}切入"
    return "从一个动作或物件切入"


def _fallback_felt_cost_v2(topic: str, prototype_candidates: list[dict[str, Any]]) -> str:
    for item in prototype_candidates:
        stance = dict(item.get("stance_vector") or {})
        value_lens = str(stance.get("value_lens") or "").strip()
        if value_lens:
            return f"先把{topic}背后的{value_lens}写出来，再让情绪显形。"
    return f"先把{topic}背后的代价写出来，再让情绪显形。"


def _fallback_value_lens_v2(prototype_candidates: list[dict[str, Any]]) -> str:
    for item in prototype_candidates:
        stance = dict(item.get("stance_vector") or {})
        value_lens = str(stance.get("value_lens") or "").strip()
        if value_lens:
            return value_lens
    return "代价"


def _fallback_desired_judgment_v2(prototype_candidates: list[dict[str, Any]]) -> str:
    for item in prototype_candidates:
        stance = dict(item.get("stance_vector") or {})
        judgment = str(stance.get("judgment") or "").strip()
        if judgment:
            return judgment
    return "悬置"


def _fallback_desired_distance_v2(prototype_candidates: list[dict[str, Any]]) -> str:
    for item in prototype_candidates:
        voice_mask = dict(item.get("voice_mask") or {})
        distance = str(voice_mask.get("distance") or "").strip()
        if distance:
            return distance
    return "贴脸"


def _normalize_topic_adapter_payload_v2(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    *,
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    available_anchor_ids = _available_anchor_ids(bundle)
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=8)
        if anchor_id in available_anchor_ids
    ]
    if not anchor_ids:
        anchor_ids = available_anchor_ids[:4]
    family_hints = _normalize_string_list(payload.get("prototype_family_hints"), limit=6)
    if not family_hints:
        family_hints = _normalize_string_list((evidence_plan or {}).get("prototype_family_hints"), limit=6)
    if not family_hints:
        family_hints = [
            str(item.get("family_key") or item.get("label") or "").strip()
            for item in (bundle.author_model.get("prototype_families") or [])[:3]
            if str(item.get("family_key") or item.get("label") or "").strip()
        ]
    forbidden = _unique_preserve_order(
        [
            *_normalize_string_list(payload.get("forbidden_drift"), limit=8),
            *_normalize_string_list((evidence_plan or {}).get("forbidden_drift"), limit=8),
            *((bundle.author_model.get("anti_patterns") or [])[:4]),
            "不要写成诊断、DSM、病理标签或心理解释报告。",
            "不要把题目直接贴在作者文风外面。",
        ]
    )
    return {
        "author_angle": str(payload.get("author_angle") or "").strip() or "把题目翻进作者一贯的具体代价与关系压力里",
        "entry_scene": str(payload.get("entry_scene") or "").strip() or "从一个动作或物件切入",
        "felt_cost": str(payload.get("felt_cost") or "").strip() or "先让代价出现，再让立场显影",
        "judgment_target": str(payload.get("judgment_target") or "").strip() or "关系处境",
        "value_lens": str(payload.get("value_lens") or "").strip() or "代价",
        "desired_judgment": str(payload.get("desired_judgment") or "").strip() or "悬置",
        "desired_distance": str(payload.get("desired_distance") or "").strip() or "回收",
        "motif_path": _normalize_string_list(payload.get("motif_path"), limit=6)
        or _normalize_string_list(((bundle.author_model.get("topic_translation_map") or [{}])[0]).get("motif_tags"), limit=4),
        "forbidden_drift": forbidden[:8],
        "prototype_family_hints": family_hints[:6],
        "anchor_ids": anchor_ids,
    }


def _select_prototypes_for_topic_v2(
    bundle: StoneWritingAnalysisBundle,
    topic_adapter: dict[str, Any],
    *,
    target_word_count: int,
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    desired_length_band = _default_length_band_from_target_v2(target_word_count)
    family_hints = _unique_preserve_order(
        [
            *_normalize_string_list(topic_adapter.get("prototype_family_hints"), limit=6),
            *_normalize_string_list((evidence_plan or {}).get("prototype_family_hints"), limit=6),
        ]
    )
    combined_adapter = dict(topic_adapter)
    combined_adapter["prototype_family_hints"] = family_hints
    if evidence_plan:
        combined_adapter["motif_path"] = _unique_preserve_order(
            [
                *_normalize_string_list(topic_adapter.get("motif_path"), limit=6),
                *_normalize_string_list(evidence_plan.get("motif_path"), limit=6),
            ]
        )[:6]
    scored: list[dict[str, Any]] = []
    for item in bundle.prototype_index.get("documents") or []:
        total, breakdown, reasons = _score_prototype_entry_v2(
            item,
            combined_adapter,
            desired_length_band=desired_length_band,
        )
        scored.append(
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "prototype_family": item.get("prototype_family"),
                "length_band": item.get("length_band"),
                "motif_tags": list(item.get("motif_tags") or [])[:4],
                "windows": dict(item.get("windows") or {}),
                "score": total,
                "score_breakdown": breakdown,
                "reasons": reasons,
            }
        )
    scored.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("title") or "")))
    selected_documents = scored[:3]
    selected_windows: list[dict[str, Any]] = []
    for item in selected_documents:
        document_id = str(item.get("document_id") or "")
        windows = dict(item.get("windows") or {})
        for role in ("opening", "pivot", "closing"):
            quote = str(windows.get(role) or "").strip()
            if not quote:
                continue
            selected_windows.append(
                {
                    "id": f"prototype:{document_id}:{role}",
                    "document_id": document_id,
                    "title": item.get("title"),
                    "role": role,
                    "quote": quote,
                    "score": item.get("score"),
                }
            )
        for index, quote in enumerate(windows.get("signature_line") or [], start=1):
            text = str(quote or "").strip()
            if not text:
                continue
            selected_windows.append(
                {
                    "id": f"prototype:{document_id}:signature:{index}",
                    "document_id": document_id,
                    "title": item.get("title"),
                    "role": "signature",
                    "quote": text,
                    "score": item.get("score"),
                }
            )
    selected_windows.sort(key=lambda item: (-float(item.get("score") or 0.0), item.get("role") != "opening"))
    selected_windows = selected_windows[:9]
    anchor_ids = _unique_preserve_order(
        [
            *(topic_adapter.get("anchor_ids") or []),
            *(item.get("id") for item in selected_windows),
        ]
    )[:12]
    return {
        "desired_length_band": desired_length_band,
        "selected_documents": selected_documents,
        "selected_windows": selected_windows,
        "family_hits": _unique_preserve_order(item.get("prototype_family") for item in selected_documents)[:4],
        "anchor_ids": anchor_ids,
    }


def _default_length_band_from_target_v2(target_word_count: int) -> str:
    target = max(100, int(target_word_count or 0))
    if target <= 160:
        return "micro"
    if target <= 320:
        return "short"
    if target <= 900:
        return "medium"
    return "long"


def _score_prototype_entry_v2(
    item: dict[str, Any],
    topic_adapter: dict[str, Any],
    *,
    desired_length_band: str,
) -> tuple[float, dict[str, float], list[str]]:
    family_text = " ".join(
        str(value or "")
        for value in (
            item.get("prototype_family"),
            item.get("title"),
            " ".join(item.get("motif_tags") or []),
        )
    ).lower()
    family_hints = [hint.lower() for hint in _normalize_string_list(topic_adapter.get("prototype_family_hints"), limit=6)]
    family_score = 35.0 if any(hint and hint in family_text for hint in family_hints) else 0.0

    entry_band = str(item.get("length_band") or "")
    if entry_band == desired_length_band:
        length_score = 25.0
    elif {entry_band, desired_length_band} <= {"micro", "short"} or {entry_band, desired_length_band} <= {"medium", "long"}:
        length_score = 12.0
    else:
        length_score = 0.0

    stance = dict(item.get("stance_vector") or {})
    desired_judgment = str(topic_adapter.get("desired_judgment") or "").strip()
    value_lens = str(topic_adapter.get("value_lens") or "").strip()
    stance_score = 0.0
    if desired_judgment and stance.get("judgment") == desired_judgment:
        stance_score += 12.0
    if value_lens and stance.get("value_lens") == value_lens:
        stance_score += 8.0

    desired_motifs = set(_normalize_string_list(topic_adapter.get("motif_path"), limit=6))
    entry_motifs = set(_normalize_string_list(item.get("motif_tags"), limit=4))
    motif_overlap = len(desired_motifs & entry_motifs)
    motif_score = min(10.0, float(motif_overlap * 5))

    voice = dict(item.get("voice_mask") or {})
    desired_distance = str(topic_adapter.get("desired_distance") or "").strip()
    voice_score = 10.0 if desired_distance and voice.get("distance") == desired_distance else 0.0

    breakdown = {
        "prototype_family": family_score,
        "length_band": length_score,
        "stance_vector": stance_score,
        "motif_tags": motif_score,
        "voice_mask": voice_score,
    }
    reasons = [
        text
        for text in (
            f"family={family_score:.0f}" if family_score else "",
            f"length={length_score:.0f}" if length_score else "",
            f"stance={stance_score:.0f}" if stance_score else "",
            f"motif={motif_score:.0f}" if motif_score else "",
            f"voice={voice_score:.0f}" if voice_score else "",
        )
        if text
    ]
    return sum(breakdown.values()), breakdown, reasons


def _compact_author_model_for_blueprint_v2(author_model: dict[str, Any]) -> dict[str, Any]:
    return {
        "views": dict(author_model.get("views") or {}),
        "topic_translation_map": list(author_model.get("topic_translation_map") or [])[:6],
        "anti_patterns": list(author_model.get("anti_patterns") or [])[:6],
        "length_behaviors": list(author_model.get("length_behaviors") or [])[:6],
    }


def _normalize_blueprint_payload_v2(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    target_word_count: int,
) -> dict[str, Any]:
    target = max(100, int(target_word_count or 0))
    available_anchor_ids = _available_anchor_ids(bundle)
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=12)
        if anchor_id in available_anchor_ids
    ]
    if not anchor_ids:
        anchor_ids = available_anchor_ids[:4]
    paragraph_count = _normalize_blueprint_paragraph_count_v2(payload.get("paragraph_count"), target)
    return {
        "paragraph_count": paragraph_count,
        "shape_note": str(payload.get("shape_note") or "").strip() or _default_shape_note_v2(target, paragraph_count),
        "entry_move": str(payload.get("entry_move") or "").strip() or "先从动作或物件落地",
        "development_move": str(payload.get("development_move") or "").strip() or "沿着代价和关系压力慢慢推进",
        "turning_device": str(payload.get("turning_device") or "").strip() or "none",
        "closure_residue": str(payload.get("closure_residue") or "").strip() or "留一层没说尽的残响",
        "keep_terms": _normalize_string_list(payload.get("keep_terms"), limit=8),
        "motif_obligations": _normalize_string_list(payload.get("motif_obligations"), limit=6),
        "steps": _normalize_string_list(payload.get("steps"), limit=8)
        or ["起笔落地", "沿压力推进", "在结尾收回去"],
        "do_not_do": _unique_preserve_order(
            [
                *_normalize_string_list(payload.get("do_not_do"), limit=8),
                "不要写成诊断或解释报告",
                "不要把题目硬贴在文风外面",
            ]
        )[:8],
        "anchor_ids": anchor_ids,
    }


def _normalize_blueprint_paragraph_count_v2(value: Any, target_word_count: int) -> int:
    if target_word_count <= 220:
        default = 1
    elif target_word_count <= 420:
        default = 2
    elif target_word_count <= 900:
        default = 3
    else:
        default = 4
    return _clamp_int(value, default=default, minimum=1, maximum=5)


def _default_shape_note_v2(target_word_count: int, paragraph_count: int) -> str:
    if target_word_count <= 320:
        return f"{paragraph_count}段内完成，直接进入状态，不展开完整情节。"
    return f"{paragraph_count}段推进，允许转折，但不要把结构写成模板说明。"


def _compact_prototype_selection_for_draft_v2(selection: dict[str, Any]) -> dict[str, Any]:
    return {
        "desired_length_band": selection.get("desired_length_band"),
        "family_hits": list(selection.get("family_hits") or [])[:4],
        "selected_documents": [
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "prototype_family": item.get("prototype_family"),
                "length_band": item.get("length_band"),
                "motif_tags": list(item.get("motif_tags") or [])[:4],
                "reasons": list(item.get("reasons") or [])[:5],
                "windows": {
                    "opening": _trim_text((item.get("windows") or {}).get("opening"), 220),
                    "pivot": _trim_text((item.get("windows") or {}).get("pivot"), 220),
                    "closing": _trim_text((item.get("windows") or {}).get("closing"), 220),
                },
            }
            for item in (selection.get("selected_documents") or [])[:3]
        ],
        "selected_windows": [
            {
                "id": item.get("id"),
                "document_id": item.get("document_id"),
                "role": item.get("role"),
                "quote": _trim_text(item.get("quote"), 220),
            }
            for item in (selection.get("selected_windows") or [])[:9]
        ],
    }


def _build_author_style_pack_v2(bundle: StoneWritingAnalysisBundle, selection: dict[str, Any]) -> dict[str, Any]:
    views = dict(bundle.author_model.get("views") or {})
    style_invariants = dict(bundle.author_model.get("style_invariants") or {})
    return {
        "voice_form": list(views.get("voice_form") or [])[:6],
        "motif_worldview": list(views.get("motif_worldview") or [])[:6],
        "lexicon_tics": list(style_invariants.get("lexicon_tics") or [])[:10],
        "rhetoric_preferences": list(style_invariants.get("rhetoric_preferences") or [])[:8],
        "opening_signatures": list(style_invariants.get("opening_signatures") or [])[:6],
        "closure_signatures": list(style_invariants.get("closure_signatures") or [])[:6],
        "prototype_windows": [
            {
                "role": item.get("role"),
                "quote": _trim_text(item.get("quote"), 240),
            }
            for item in (selection.get("selected_windows") or [])[:9]
        ],
        "evidence_windows": [
            {
                "family": item.get("prototype_family"),
                "opening": _trim_text(item.get("opening"), 180),
                "closing": _trim_text(item.get("closing"), 180),
            }
            for item in (bundle.author_model.get("evidence_windows") or [])[:6]
        ],
        "anti_patterns": list(bundle.author_model.get("anti_patterns") or [])[:8],
    }


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
            "focus": "只检查这篇是否像拼出来的仿写、像 checklist、像分析结论复述，是否有假味。",
        },
    }
    return mapping.get(critic_key, mapping["formal_fidelity"])


def _critic_packet_v2(bundle: StoneWritingAnalysisBundle, critic_key: str) -> dict[str, Any]:
    views = dict(bundle.author_model.get("views") or {})
    if critic_key == "formal_fidelity":
        return {
            "voice_form": views.get("voice_form") or [],
            "length_behaviors": bundle.author_model.get("length_behaviors") or [],
            "evidence_windows": bundle.author_model.get("evidence_windows") or [],
        }
    if critic_key == "worldview_translation":
        return {
            "motif_worldview": views.get("motif_worldview") or [],
            "topic_translation_map": bundle.author_model.get("topic_translation_map") or [],
            "anti_patterns": bundle.author_model.get("anti_patterns") or [],
        }
    return {
        "anti_patterns": bundle.author_model.get("anti_patterns") or [],
        "evidence_windows": bundle.author_model.get("evidence_windows") or [],
        "prototype_families": bundle.author_model.get("prototype_families") or [],
    }


def _normalize_critic_payload_v2(
    payload: dict[str, Any],
    critic_key: str,
    bundle: StoneWritingAnalysisBundle,
) -> dict[str, Any]:
    available_anchor_ids = set(_available_anchor_ids(bundle))
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=8)
        if anchor_id in available_anchor_ids
    ]
    if not anchor_ids:
        anchor_ids = list(available_anchor_ids)[:3]
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


def _build_critic_message_payload_v2(
    critic: dict[str, Any],
    *,
    stream_key: str | None = None,
    stream_state: str = "complete",
    render_mode: str = "markdown",
) -> dict[str, Any]:
    key = str(critic.get("critic_key") or "critic").strip() or "critic"
    label = str(critic.get("critic_label") or key).strip() or key
    return {
        "stage": "critic",
        "label": f"{label} critic",
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


def _resolve_critic_action_v2(
    critics: list[dict[str, Any]],
    *,
    draft_text: str,
    topic: str,
    target_word_count: int,
) -> str:
    verdicts = [str(item.get("verdict") or "approve") for item in critics]
    scores = [_clamp_score(item.get("score"), default=0.0) for item in critics]
    if any(verdict == "redraft" for verdict in verdicts):
        return "redraft"
    if any(score < 0.52 for score in scores):
        return "redraft"
    if any(verdict == "line_edit" for verdict in verdicts):
        return "line_edit"
    if any(score < 0.72 for score in scores):
        return "line_edit"
    task_assessment = _assess_task_compliance_v2(draft_text, topic, target_word_count)
    if not task_assessment["length_ok"] or not task_assessment["topic_visible"]:
        return "line_edit"
    return "approve"


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
    candidates: list[str] = []
    for token in re.findall(r"[a-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", topic):
        if token in stop_terms:
            continue
        candidates.append(token)
        if re.fullmatch(r"[\u4e00-\u9fff]{4,}", token):
            for size in (4, 3, 2):
                for index in range(0, len(token) - size + 1):
                    piece = token[index:index + size]
                    if piece not in stop_terms:
                        candidates.append(piece)
    return _unique_preserve_order(sorted(candidates, key=len, reverse=True))[:10]


def _collect_trace_anchor_ids_v2(
    bundle: StoneWritingAnalysisBundle,
    evidence_plan: dict[str, Any],
    topic_adapter: dict[str, Any],
    prototype_selection: dict[str, Any],
    blueprint: dict[str, Any],
    critics: list[dict[str, Any]],
) -> list[str]:
    values: list[str] = []
    values.extend(_available_anchor_ids(bundle)[:12])
    values.extend(evidence_plan.get("anchor_ids") or [])
    values.extend(topic_adapter.get("anchor_ids") or [])
    values.extend(prototype_selection.get("anchor_ids") or [])
    values.extend(blueprint.get("anchor_ids") or [])
    for critic in critics:
        values.extend(critic.get("anchor_ids") or [])
    return _unique_preserve_order(values)


def _build_trace_blocks_v2(
    analysis_bundle: StoneWritingAnalysisBundle,
    evidence_plan: dict[str, Any],
    topic_adapter: dict[str, Any],
    prototype_selection: dict[str, Any],
    blueprint: dict[str, Any],
    critics: list[dict[str, Any]],
    revision_action: str,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "generation_packet",
            "label": f"Stone v2 baseline ready ({analysis_bundle.version_label})",
            "baseline": analysis_bundle.generation_packet.get("baseline", {}),
        },
        {
            "type": "stage",
            "stage": "evidence_plan",
            "label": "Evidence plan completed",
            "anchor_ids": evidence_plan.get("anchor_ids") or [],
            "search_terms": evidence_plan.get("search_terms") or [],
        },
        {
            "type": "stage",
            "stage": "topic_adapter",
            "label": "Topic adapted into author angle",
            "anchor_ids": topic_adapter.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "prototype_selector",
            "label": "Prototype retrieval completed",
            "anchor_ids": prototype_selection.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "blueprint",
            "label": "Blueprint composed",
            "anchor_ids": blueprint.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "draft",
            "label": "First draft completed",
        },
    ]
    for critic in critics:
        blocks.append(
            {
                "type": "critic",
                "critic_key": critic.get("critic_key"),
                "verdict": critic.get("verdict"),
                "score": critic.get("score"),
                "anchor_ids": critic.get("anchor_ids") or [],
                "line_edits": critic.get("line_edits") or [],
                "redraft_reason": critic.get("redraft_reason"),
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


def _render_topic_adapter_v2(payload: dict[str, Any]) -> str:
    lines = [
        f"切入角度：{payload.get('author_angle') or ''}",
        f"起笔场景：{payload.get('entry_scene') or ''}",
        f"代价：{payload.get('felt_cost') or ''}",
        f"判断对象：{payload.get('judgment_target') or ''}",
        f"价值镜头：{payload.get('value_lens') or ''}",
        f"叙述距离：{payload.get('desired_distance') or ''}",
        "",
        "意象路径：",
        *[f"- {item}" for item in (payload.get("motif_path") or [])[:6]],
        "",
        "避免漂移：",
        *[f"- {item}" for item in (payload.get("forbidden_drift") or [])[:6]],
    ]
    return "\n".join(lines).strip()


def _render_prototype_selection_v2(payload: dict[str, Any]) -> str:
    lines = [
        f"目标长度带：{payload.get('desired_length_band') or ''}",
        f"命中 family：{', '.join(payload.get('family_hits') or [])}",
        "",
    ]
    for index, item in enumerate(payload.get("selected_documents") or [], start=1):
        lines.extend(
            [
                f"P{index} · {item.get('title') or '（未命名）'}",
                f"- family: {item.get('prototype_family') or ''}",
                f"- score: {round(float(item.get('score') or 0.0), 2)}",
                f"- reasons: {', '.join(item.get('reasons') or [])}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _render_blueprint_v2(payload: dict[str, Any]) -> str:
    lines = [
        f"段落数：{payload.get('paragraph_count')}",
        f"整体形状：{payload.get('shape_note') or ''}",
        f"起笔：{payload.get('entry_move') or ''}",
        f"推进：{payload.get('development_move') or ''}",
        f"转折：{payload.get('turning_device') or ''}",
        f"收口：{payload.get('closure_residue') or ''}",
        "",
        "步骤：",
        *[f"- {item}" for item in (payload.get("steps") or [])[:8]],
    ]
    return "\n".join(lines).strip()


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
        "anchor id",
        "Anchor ID",
        "DSM",
        "诊断",
        "病理标签",
    )
    return any(item in text for item in banned)


def _light_trim_to_word_count(text: str, target_word_count: int) -> str:
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


def _first_anchor_quote(bundle: StoneWritingAnalysisBundle) -> str:
    for facet in bundle.facets:
        for item in facet.fewshots:
            quote = str(item.get("quote") or "").strip()
            if quote:
                return quote
    return ""


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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
