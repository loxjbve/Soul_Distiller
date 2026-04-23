from __future__ import annotations

import json
import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, Lock
from typing import Any
from uuid import uuid4

from app.analysis.facets import FacetDefinition, get_facets_for_mode
from app.analysis.stone import estimate_word_count, render_writing_request
from app.db import Database
from app.llm.client import OpenAICompatibleClient, parse_json_response
from app.runtime_limits import background_task_slot
from app.storage import repository

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


@dataclass(slots=True)
class StoneWritingAnalysisBundle:
    run_id: str
    source: str
    version_label: str
    target_role: str | None
    analysis_context: str | None
    facets: list[StoneWritingFacetContext]
    prompt_text: str


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
                            "status": "failed",
                            "timeline": [],
                            "reviews": [],
                            "review_plan": None,
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
                "stage": "analysis_loaded",
                "label": "Loaded latest Stone analysis baseline",
                "baseline_source": analysis_bundle.source,
                "analysis_run_id": analysis_bundle.run_id,
                "analysis_version": analysis_bundle.version_label,
                "analysis_target_role": analysis_bundle.target_role,
            },
        )

        config = repository.get_service_config(session, "chat_service")
        client = self._build_client(config)

        initial_draft = self._generate_initial_draft(state, analysis_bundle, client)
        draft_payload = _build_writer_message_payload(
            message_kind="draft",
            label="首稿已完成",
            body=initial_draft,
            detail={"word_count": estimate_word_count(initial_draft)},
        )
        self._emit(state, "stage", draft_payload)

        reviews = self._run_reviews_in_parallel(
            state,
            analysis_bundle,
            initial_draft,
            repository.get_service_config(session, "chat_service"),
        )
        review_messages: list[dict[str, Any]] = []
        for review in reviews:
            review_payload = _build_reviewer_message_payload(review)
            review_messages.append(review_payload)
            self._emit(state, "stage", review_payload)

        review_plan = self._synthesize_review_plan(state, analysis_bundle, initial_draft, reviews, client)
        self._emit(
            state,
            "status",
            {
                "stage": "review_synthesis",
                "label": "Merged eight reviewer notes",
                "review_plan": review_plan,
            },
        )

        final_text = self._revise_draft(
            state,
            analysis_bundle,
            initial_draft,
            reviews,
            review_plan,
            client,
        )
        final_assessment = _build_final_assessment(
            final_text,
            reviews,
            review_plan,
            state.topic,
            state.target_word_count,
        )
        final_payload = _build_writer_message_payload(
            message_kind="final",
            label="终稿已完成",
            body=final_text,
            detail={
                "word_count": estimate_word_count(final_text),
                "review_plan": review_plan,
                "final_assessment": final_assessment,
            },
        )

        trace = {
            "kind": "writing_result",
            "status": "completed",
            "topic": state.topic,
            "target_word_count": state.target_word_count,
            "extra_requirements": state.extra_requirements,
            "raw_message": state.raw_message,
            "baseline_source": analysis_bundle.source,
            "analysis_run_id": analysis_bundle.run_id,
            "analysis_version": analysis_bundle.version_label,
            "analysis_target_role": analysis_bundle.target_role,
            "analysis_context": analysis_bundle.analysis_context,
            "analysis_facets": [facet.key for facet in analysis_bundle.facets],
            "blocks": _build_trace_blocks(analysis_bundle, reviews, review_plan),
            "reviews": reviews,
            "review_plan": review_plan,
            "draft": initial_draft,
            "final_text": final_text,
            "final_assessment": final_assessment,
            "timeline": [draft_payload, *review_messages, final_payload],
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
            "review_count": len(reviews),
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

    def _resolve_analysis_bundle(self, session, project_id: str) -> StoneWritingAnalysisBundle:
        run = repository.get_latest_analysis_run(session, project_id, load_facets=True, load_events=False)
        if not run:
            raise ValueError("No Stone analysis is available yet. Run analysis first.")
        if run.status in {"queued", "running"}:
            raise ValueError("Stone analysis is still running. Wait until it finishes.")

        facets_by_key = {
            facet.facet_key: facet
            for facet in (run.facets or [])
            if facet.status == "completed" and isinstance(facet.findings_json, dict)
        }
        missing = [definition.label for definition in STONE_WRITING_FACETS if definition.key not in facets_by_key]
        if missing:
            raise ValueError(f"Stone writing needs all 8 facet results first. Missing: {', '.join(missing)}")

        contexts = [
            _build_facet_context(definition, facets_by_key[definition.key])
            for definition in STONE_WRITING_FACETS
        ]
        summary = dict(run.summary_json or {})
        version_label = f"run {run.created_at.isoformat(timespec='minutes')}" if run.created_at else "latest"
        bundle = StoneWritingAnalysisBundle(
            run_id=run.id,
            source="analysis_run",
            version_label=version_label,
            target_role=str(summary.get("target_role") or "").strip() or None,
            analysis_context=str(summary.get("analysis_context") or "").strip() or None,
            facets=contexts,
            prompt_text="",
        )
        bundle.prompt_text = _build_analysis_prompt_text(bundle)
        return bundle

    def _generate_initial_draft(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient | None,
    ) -> str:
        if client:
            try:
                response = client.chat_completion_result(
                    [
                        {
                            "role": "system",
                            "content": (
                                "You are the Stone writing drafter.\n"
                                "Write the first draft only.\n"
                                "Follow the analysis baseline tightly, keep the voice coherent, and output only the article body."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"Stone multi-facet baseline:\n{analysis_bundle.prompt_text}\n\n"
                                "Requirements:\n"
                                "- Make the topic visible in the article body.\n"
                                "- Follow the analyzed voice, diction, imagery, stance, emotional arc, and constraints.\n"
                                "- This is round one, so focus on a strong but revisable first draft.\n"
                                "- Return only the article body."
                            ),
                        },
                    ],
                    model=client.config.model,
                    temperature=0.7,
                    max_tokens=None,
                )
                candidate = _clean_model_text(response.content)
                if candidate:
                    return _fit_word_count(
                        candidate,
                        state.target_word_count,
                        analysis_bundle,
                        state.topic,
                        state.extra_requirements,
                    )
            except Exception:
                pass
        return _heuristic_initial_draft(
            state.topic,
            state.target_word_count,
            analysis_bundle,
            state.extra_requirements,
        )

    def _review_with_facet(
        self,
        state: WritingStreamState,
        facet: StoneWritingFacetContext,
        draft: str,
        analysis_bundle: StoneWritingAnalysisBundle,
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if client:
            try:
                response = client.chat_completion_result(
                    [
                        {
                            "role": "system",
                            "content": (
                                "你是 Stone 写作流水线中的一个评审 agent。\n"
                                "你只负责当前分配到的单一维度。\n"
                                "请用中文给出严格、具体、可执行的判断。\n"
                                "只返回 JSON，不要输出额外解释。"
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"当前维度：\n{_build_single_facet_prompt(facet)}\n\n"
                                f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"候选文章：\n{draft}\n\n"
                                "请返回 JSON，字段如下：\n"
                                "{\n"
                                '  "pass": boolean,\n'
                                '  "score": number,\n'
                                '  "strengths": [中文字符串],\n'
                                '  "issues": [中文字符串],\n'
                                '  "revision_instructions": [中文字符串],\n'
                                '  "supporting_signals": [中文字符串]\n'
                                "}\n"
                                "只判断这一维，不要谈其他维度。"
                            ),
                        },
                    ],
                    model=client.config.model,
                    temperature=0.2,
                    max_tokens=None,
                )
                payload = parse_json_response(response.content, fallback=True)
                return _normalize_review_payload(payload, facet)
            except Exception:
                pass
        return _heuristic_review_payload(
            facet,
            draft,
            analysis_bundle,
            state.topic,
            state.target_word_count,
        )

    def _synthesize_review_plan(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        reviews: list[dict[str, Any]],
        client: OpenAICompatibleClient | None,
    ) -> dict[str, Any]:
        if client:
            try:
                response = client.chat_completion_result(
                    [
                        {
                            "role": "system",
                            "content": (
                                "你是 Stone 写作流水线里的评审整合 agent。\n"
                                "请把 8 个评审意见合并成一份中文修订提纲。\n"
                                "保留已经成立的部分，优先指出最值得修改的几件事。\n"
                                "只返回 JSON。"
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"写作任务：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"Stone 基线：\n{analysis_bundle.prompt_text}\n\n"
                                f"首稿：\n{draft}\n\n"
                                f"8 个评审 JSON：\n{json.dumps(reviews, ensure_ascii=False, indent=2)}\n\n"
                                "请返回 JSON，字段如下：\n"
                                "{\n"
                                '  "summary": 中文字符串,\n'
                                '  "keep": [中文字符串],\n'
                                '  "priorities": [中文字符串],\n'
                                '  "revision_blueprint": [中文字符串],\n'
                                '  "risk_watch": [中文字符串]\n'
                                "}"
                            ),
                        },
                    ],
                    model=client.config.model,
                    temperature=0.2,
                    max_tokens=None,
                )
                payload = parse_json_response(response.content, fallback=True)
                return _normalize_review_plan_payload(payload, reviews)
            except Exception:
                pass
        return _heuristic_review_plan(reviews)

    def _revise_draft(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
        reviews: list[dict[str, Any]],
        review_plan: dict[str, Any],
        client: OpenAICompatibleClient | None,
    ) -> str:
        if client:
            try:
                response = client.chat_completion_result(
                    [
                        {
                            "role": "system",
                            "content": (
                                "You are the Stone writing reviser.\n"
                                "Revise the first draft exactly once using the integrated review brief.\n"
                                "Preserve the original voice, sharpen the weak facets, and output only the final article body."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"Stone multi-facet baseline:\n{analysis_bundle.prompt_text}\n\n"
                                f"First draft:\n{draft}\n\n"
                                f"Integrated review plan JSON:\n{json.dumps(review_plan, ensure_ascii=False, indent=2)}\n\n"
                                f"Reviewer outputs JSON:\n{json.dumps(reviews, ensure_ascii=False, indent=2)}\n\n"
                                "Return only the final revised article body."
                            ),
                        },
                    ],
                    model=client.config.model,
                    temperature=0.55,
                    max_tokens=None,
                )
                candidate = _clean_model_text(response.content)
                if candidate:
                    return _fit_word_count(
                        candidate,
                        state.target_word_count,
                        analysis_bundle,
                        state.topic,
                        state.extra_requirements,
                    )
            except Exception:
                pass
        return _heuristic_revise_draft(
            draft,
            analysis_bundle,
            reviews,
            review_plan,
            state.topic,
            state.target_word_count,
            state.extra_requirements,
        )

    def _run_reviews_in_parallel(
        self,
        state: WritingStreamState,
        analysis_bundle: StoneWritingAnalysisBundle,
        draft: str,
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
    return StoneWritingFacetContext(
        key=definition.key,
        label=definition.label,
        purpose=definition.purpose,
        confidence=round(float(facet_row.confidence or 0.0), 3),
        summary=str(findings.get("summary") or "").strip(),
        bullets=_normalize_string_list(findings.get("bullets"), limit=4),
        fewshots=_normalize_fewshots(fewshots_source),
        conflicts=_normalize_conflicts(conflicts_source),
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


def _build_writer_message_payload(
    *,
    message_kind: str,
    label: str,
    body: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage": "writer",
        "label": label,
        "actor_id": f"writer-{message_kind}",
        "actor_name": WRITER_ACTOR_NAME,
        "actor_role": "writer",
        "message_kind": message_kind,
        "body": body,
        "detail": detail or {},
        "created_at": _iso_now(),
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
    strengths = _normalize_string_list(review.get("strengths"), limit=4)
    issues = _normalize_string_list(review.get("issues"), limit=4)
    instructions = _normalize_string_list(review.get("revision_instructions"), limit=5)
    signals = _normalize_string_list(review.get("supporting_signals"), limit=4)

    if strengths:
        lines.append("")
        lines.append("保留：")
        lines.extend(f"- {item}" for item in strengths)
    if issues:
        lines.append("")
        lines.append("问题：")
        lines.extend(f"- {item}" for item in issues)
    if instructions:
        lines.append("")
        lines.append("修改建议：")
        lines.extend(f"- {item}" for item in instructions)
    if signals:
        lines.append("")
        lines.append("参考信号：")
        lines.extend(f"- {item}" for item in signals)
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


def _normalize_review_payload(payload: dict[str, Any], facet: StoneWritingFacetContext) -> dict[str, Any]:
    score = _clamp_score(payload.get("score"), default=0.68)
    strengths = _normalize_string_list(payload.get("strengths"), limit=4)
    issues = _normalize_string_list(payload.get("issues"), limit=4)
    revision_instructions = _normalize_string_list(
        payload.get("revision_instructions") or payload.get("must_fix"),
        limit=5,
    )
    supporting_signals = _normalize_string_list(
        payload.get("supporting_signals") or payload.get("evidence"),
        limit=4,
    )
    if not strengths:
        strengths = ["首稿已经有一个可继续修的基础。"]
    if not revision_instructions:
        revision_instructions = issues[:] or ["保持当前优势，只做必要收束。"]
    passed = bool(payload.get("pass")) if "pass" in payload else (score >= 0.72 and not issues)
    return {
        "dimension": facet.label,
        "dimension_key": facet.key,
        "dimension_label": facet.label,
        "pass": passed,
        "score": round(score, 3),
        "strengths": strengths,
        "issues": issues,
        "revision_instructions": revision_instructions,
        "supporting_signals": supporting_signals or facet.bullets[:2],
    }


def _heuristic_review_plan(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    keep = _unique_preserve_order(item for review in reviews for item in review.get("strengths", []))
    priorities = _unique_preserve_order(item for review in reviews for item in review.get("revision_instructions", []))
    risk_watch = _unique_preserve_order(
        f"{review.get('dimension_label')}: {issue}"
        for review in reviews
        for issue in review.get("issues", [])
    )
    pass_count = sum(1 for review in reviews if review.get("pass"))
    return {
        "summary": f"{pass_count}/8 个维度已经达标，优先修补最不稳的维度，不要平均用力。",
        "keep": keep[:4] or ["保留首稿里已经成立的语气和收束感。"],
        "priorities": priorities[:6] or ["只做必要修订，让终稿更贴近分析基线。"],
        "revision_blueprint": [
            "先补最弱的 2-3 个维度，不平均摊开修改。",
            "保留首稿已经建立起来的语气与节奏。",
            "让主题在正文中更可见，但不要写成说明文。",
        ],
        "risk_watch": risk_watch[:4],
    }


def _normalize_review_plan_payload(payload: dict[str, Any], reviews: list[dict[str, Any]]) -> dict[str, Any]:
    heuristic = _heuristic_review_plan(reviews)
    summary = str(payload.get("summary") or heuristic["summary"]).strip()
    keep = _normalize_string_list(payload.get("keep"), limit=4) or heuristic["keep"]
    priorities = _normalize_string_list(payload.get("priorities"), limit=6) or heuristic["priorities"]
    revision_blueprint = _normalize_string_list(payload.get("revision_blueprint"), limit=6) or heuristic["revision_blueprint"]
    risk_watch = _normalize_string_list(payload.get("risk_watch"), limit=4) or heuristic["risk_watch"]
    return {
        "summary": summary,
        "keep": keep,
        "priorities": priorities,
        "revision_blueprint": revision_blueprint,
        "risk_watch": risk_watch,
    }


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
    reviews: list[dict[str, Any]],
    review_plan: dict[str, Any],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "analysis_loaded",
            "label": f"Analysis baseline loaded ({analysis_bundle.version_label})",
        },
        {
            "type": "stage",
            "stage": "drafter",
            "label": "First draft completed",
        },
    ]
    for review in reviews:
        blocks.append(
            {
                "type": "review",
                "dimension": review["dimension_label"],
                "score": review["score"],
                "must_fix": review.get("revision_instructions") or review.get("issues") or [],
                "keep": review.get("strengths") or [],
                "pass": review.get("pass"),
                "issues": review.get("issues") or [],
                "revision_instructions": review.get("revision_instructions") or [],
            }
        )
    blocks.append(
        {
            "type": "review_plan",
            "label": "Merged eight reviewer notes",
            "summary": review_plan.get("summary"),
            "priorities": review_plan.get("priorities") or [],
            "keep": review_plan.get("keep") or [],
        }
    )
    blocks.append(
        {
            "type": "stage",
            "stage": "reviser",
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
