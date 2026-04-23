from __future__ import annotations

import json
import re
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
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
                            "blocks": [{"type": "stage", "stage": "failed", "label": "Writing failed"}],
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
            "stage",
            {
                "stage": "analysis_loaded",
                "baseline_source": analysis_bundle.source,
                "analysis_run_id": analysis_bundle.run_id,
                "analysis_version": analysis_bundle.version_label,
                "label": "Loaded latest Stone analysis baseline",
            },
        )

        config = repository.get_service_config(session, "chat_service")
        client = self._build_client(config)

        initial_draft = self._generate_initial_draft(state, analysis_bundle, client)
        self._emit(
            state,
            "stage",
            {
                "stage": "drafter",
                "label": "First draft completed",
                "draft": initial_draft,
                "word_count": estimate_word_count(initial_draft),
            },
        )

        reviews = [
            self._review_with_facet(
                state,
                facet,
                initial_draft,
                analysis_bundle,
                client,
            )
            for facet in analysis_bundle.facets
        ]
        for review in reviews:
            self._emit(
                state,
                "stage",
                {
                    "stage": "reviewer",
                    "label": f"Reviewer {review['dimension_label']}",
                    "dimension": review["dimension_label"],
                    "review": review,
                },
            )

        review_plan = self._synthesize_review_plan(state, analysis_bundle, initial_draft, reviews, client)
        self._emit(
            state,
            "stage",
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

        self._emit(
            state,
            "stage",
            {
                "stage": "reviser",
                "label": "Final revision completed",
                "draft": final_text,
                "word_count": estimate_word_count(final_text),
                "final_assessment": final_assessment,
            },
        )

        trace = {
            "kind": "writing_result",
            "topic": state.topic,
            "target_word_count": state.target_word_count,
            "extra_requirements": state.extra_requirements,
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
                "baseline_source": analysis_bundle.source,
                "analysis_run_id": analysis_bundle.run_id,
                "review_count": len(reviews),
                "final_assessment": final_assessment,
            },
        )

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
                                "- Let the topic be explicit in the article body.\n"
                                "- Use the analyzed voice, diction, imagery, stance, emotional arc, and constraints.\n"
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
                                "You are one reviewer in the Stone writing pipeline.\n"
                                "Evaluate the draft only against the assigned facet.\n"
                                "Be strict, concrete, and actionable.\n"
                                "Return JSON only."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Assigned facet:\n{_build_single_facet_prompt(facet)}\n\n"
                                f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"Candidate article:\n{draft}\n\n"
                                "Return JSON with keys:\n"
                                "{\n"
                                '  "pass": boolean,\n'
                                '  "score": number,\n'
                                '  "strengths": [string],\n'
                                '  "issues": [string],\n'
                                '  "revision_instructions": [string],\n'
                                '  "supporting_signals": [string]\n'
                                "}\n"
                                "Only judge this single facet."
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
                                "You are the review integrator in the Stone writing pipeline.\n"
                                "Merge eight reviewer notes into one revision brief.\n"
                                "Preserve the strongest parts and prioritize the few changes that matter most.\n"
                                "Return JSON only."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Writing request:\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                                f"Stone baseline:\n{analysis_bundle.prompt_text}\n\n"
                                f"First draft:\n{draft}\n\n"
                                f"Reviewer outputs JSON:\n{json.dumps(reviews, ensure_ascii=False, indent=2)}\n\n"
                                "Return JSON with keys:\n"
                                "{\n"
                                '  "summary": string,\n'
                                '  "keep": [string],\n'
                                '  "priorities": [string],\n'
                                '  "revision_blueprint": [string],\n'
                                '  "risk_watch": [string]\n'
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


def _heuristic_initial_draft(
    topic: str,
    target_word_count: int,
    analysis_bundle: StoneWritingAnalysisBundle,
    extra_requirements: str | None,
) -> str:
    voice = _facet_lookup(analysis_bundle, "voice_signature")
    imagery = _facet_lookup(analysis_bundle, "imagery_theme")
    stance = _facet_lookup(analysis_bundle, "stance_values")
    emotion = _facet_lookup(analysis_bundle, "emotional_arc")
    constraints = _facet_lookup(analysis_bundle, "creative_constraints")

    voice_cues = _facet_terms(voice)[:3]
    imagery_cues = _facet_terms(imagery)[:3]
    stance_cues = _facet_terms(stance)[:3]
    emotion_cues = _facet_terms(emotion)[:3]
    constraint_cues = _facet_terms(constraints)[:3]
    anchor_quote = _first_anchor_quote(analysis_bundle)

    paragraphs = [
        (
            f"{topic}并不是一个适合被说成答案的题目。"
            f"它更像一块压在桌角的旧石头，平时不响，一碰就把白天没说完的话重新顶出来。"
        ),
        (
            f"如果沿着{_join_terms(voice_cues, fallback='那种压低声调、先收后放的说法')}往下写，"
            f"我不会急着把情绪摊平，反而会让它顺着"
            f"{_join_terms(emotion_cues, fallback='克制、迟疑和回收')}慢慢露出来。"
        ),
        (
            f"这件事最后还是会落到{_join_terms(stance_cues, fallback='代价、边界和立场')}上。"
            f"真正值得写的，不是表面的起因，而是一个人怎么在现实里把自己一点点收紧，"
            f"又怎么在必须开口的时候把那口气缓慢地放出来。"
        ),
        (
            f"所以文章里的场景，我更愿意让它沾着"
            f"{_join_terms(imagery_cues, fallback='夜色、旧物和没有收好的余温')}，"
            f"而不是把它写成一段整齐的解释。这样收口，才比较接近这个作者本来的路数。"
        ),
    ]
    if anchor_quote:
        paragraphs.append(f"有时候一句旧话就已经够重了：{anchor_quote}")
    if constraint_cues:
        paragraphs.append(
            f"写到最后，我会提醒自己别把它写得太满，尤其别偏离{_join_terms(constraint_cues[:2], fallback='作者本来的约束')}。"
        )
    if extra_requirements:
        paragraphs.append(f"附加要求我会留在句子内部处理：{extra_requirements}。")
    text = "\n\n".join(paragraphs)
    return _fit_word_count(text, target_word_count, analysis_bundle, topic, extra_requirements)


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

    imagery_terms = _facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:2]
    stance_terms = _facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:2]
    emotion_terms = _facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:2]

    while current < lower:
        addition = (
            f"\n\n说到底，{topic}之所以难写，不是因为它新，而是因为它总会重新碰到"
            f"{_join_terms(imagery_terms, fallback='那些旧场景')}"
            f"，又把{_join_terms(stance_terms, fallback='代价和立场')}重新照亮。"
            f"等视线再往里收一层，情绪也还是会回到{_join_terms(emotion_terms, fallback='克制和回落')}。"
        )
        if extra_requirements and current < lower - 60:
            addition += f" 我会把{extra_requirements}也顺手压进这层叙述里。"
        text += addition
        current = estimate_word_count(text)
        if len(addition) < 20:
            break

    if current > upper:
        trimmed = text
        while estimate_word_count(trimmed) > upper and len(trimmed) > 40:
            trimmed = trimmed[:-20].rstrip("，。！？；： ")
        if trimmed and trimmed[-1] not in "。！？":
            trimmed = f"{trimmed}。"
        text = trimmed
    return text.strip()


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
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.85)
    upper = int(target * 1.15)

    strengths: list[str] = []
    issues: list[str] = []
    revision_instructions: list[str] = []
    score = 0.66

    if facet.key == "voice_signature":
        if cue_hits:
            strengths.append("声音底色已经开始稳定下来。")
            score = 0.8
        else:
            issues.append("还需要把语气压得更像作者本人，而不是通用抒情写法。")
            revision_instructions.append("加强叙述口吻的收束感和第一反应式表达。")
            score = 0.62
    elif facet.key == "lexicon_idiolect":
        if len(cue_hits) >= 2:
            strengths.append("已经出现了一部分作者自己的词汇气味。")
            score = 0.79
        else:
            issues.append("作者私方言还不够明显，词面太平。")
            revision_instructions.append("补回分析里提到的词汇偏好和惯用转折。")
            score = 0.61
    elif facet.key == "structure_composition":
        if len(paragraphs) < 3:
            issues.append("结构推进偏平，需要更清楚的段落层次。")
            revision_instructions.append("把开头、推进和收口分成更明确的节拍。")
            score = 0.6
        elif len(paragraphs) > 6:
            issues.append("段落太散，影响整体构图。")
            revision_instructions.append("压缩段落数量，避免解释过多。")
            score = 0.66
        else:
            strengths.append("段落节拍基本成立。")
            score = 0.8
    elif facet.key == "imagery_theme":
        if cue_hits:
            strengths.append("意象和母题已经开始回到作者熟悉的场域。")
            score = 0.78
        else:
            issues.append("还没把作者常用的意象母题真正写进来。")
            revision_instructions.append("让场景、物件或反复出现的母题承担更多表达。")
            score = 0.6
    elif facet.key == "stance_values":
        if topic not in draft:
            issues.append("题目在正文里还不够可见。")
            revision_instructions.append("让主题直接进入正文，而不是只停留在外围。")
            score = 0.58
        elif cue_hits:
            strengths.append("主题和立场已经发生了绑定。")
            score = 0.81
        else:
            issues.append("立场判断还不够像作者自己的价值排序。")
            revision_instructions.append("把主题翻译成作者熟悉的代价、边界或判断逻辑。")
            score = 0.63
    elif facet.key == "emotional_arc":
        if len(paragraphs) >= 3:
            strengths.append("情绪推进开始出现层次。")
            score = 0.77
        else:
            issues.append("情绪弧线还太短，缺少转折后的回落。")
            revision_instructions.append("补出情绪从压低到显露再到收回的过程。")
            score = 0.62
    elif facet.key == "nonclinical_psychodynamics":
        introspection_markers = ("我", "自己", "不愿意", "害怕", "忍", "收回", "边界")
        if any(marker in draft for marker in introspection_markers):
            strengths.append("文本里有了向内收的心理动力。")
            score = 0.76
        else:
            issues.append("心理动力还偏表层，没有把防御、拉扯和自我收缩写出来。")
            revision_instructions.append("增加一层内在回收或自我防守的动作。")
            score = 0.61
    elif facet.key == "creative_constraints":
        if _duplicate_sentence_ratio(draft) > 0.28:
            issues.append("重复句式偏多，容易把文风写平。")
            revision_instructions.append("去掉重叠句意，让每一段只留必要动作。")
            score = 0.58
        if not lower <= word_count <= upper:
            issues.append("字数开始偏离目标范围。")
            revision_instructions.append("把字数收回到目标附近。")
            score = min(score, 0.6)
        if not issues:
            strengths.append("基本守住了写作约束。")
            score = 0.8

    if not strengths:
        strengths.append("当前稿子已经有一个可修的基础。")
    if not revision_instructions:
        revision_instructions = list(issues) or ["保持当前优势，只做细部收束。"]

    passed = score >= 0.72 and not issues
    return {
        "dimension": facet.label,
        "dimension_key": facet.key,
        "dimension_label": facet.label,
        "pass": passed,
        "score": round(score, 3),
        "strengths": strengths[:4],
        "issues": issues[:4],
        "revision_instructions": revision_instructions[:5],
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
        strengths = ["当前稿子已经有一个可修的基础。"]
    if not revision_instructions:
        revision_instructions = issues[:] or ["保持当前优势，只做细部收束。"]
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
    priorities = _unique_preserve_order(
        item for review in reviews for item in review.get("revision_instructions", [])
    )
    risk_watch = _unique_preserve_order(
        f"{review.get('dimension_label')}: {issue}"
        for review in reviews
        for issue in review.get("issues", [])
    )
    pass_count = sum(1 for review in reviews if review.get("pass"))
    return {
        "summary": f"{pass_count}/8 个维度初步达标，优先修补最不稳的声音、结构和约束问题。",
        "keep": keep[:4] or ["保留当前稿子里已经形成的压低声调和收束感。"],
        "priorities": priorities[:6] or ["保持声音稳定，只做必要收束。"],
        "revision_blueprint": [
            "先补足最弱的 2-3 个维度，不要平均用力。",
            "保留首稿已经成立的语气和收口方式。",
            "让主题在正文中更可见，但不要写成说明文。",
        ],
        "risk_watch": risk_watch[:4],
    }


def _normalize_review_plan_payload(payload: dict[str, Any], reviews: list[dict[str, Any]]) -> dict[str, Any]:
    heuristic = _heuristic_review_plan(reviews)
    summary = str(payload.get("summary") or heuristic["summary"]).strip()
    keep = _normalize_string_list(payload.get("keep"), limit=4) or heuristic["keep"]
    priorities = _normalize_string_list(payload.get("priorities"), limit=6) or heuristic["priorities"]
    revision_blueprint = _normalize_string_list(
        payload.get("revision_blueprint"),
        limit=6,
    ) or heuristic["revision_blueprint"]
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
    priorities = _normalize_string_list(review_plan.get("priorities"), limit=6)
    failed_keys = {str(review.get("dimension_key") or "") for review in reviews if not review.get("pass")}

    if topic and topic not in revised:
        revised = f"{topic}，这件事真正难写的，不是表面，而是它会把人重新拖回现实里。\n\n{revised}"

    if "structure_composition" in failed_keys:
        sentences = [item.strip() for item in re.split(r"(?<=[。！？])", revised) if item.strip()]
        if len(sentences) >= 5:
            revised = "\n\n".join(
                [
                    "".join(sentences[:2]).strip(),
                    "".join(sentences[2:4]).strip(),
                    "".join(sentences[4:]).strip(),
                ]
            ).strip()

    if "voice_signature" in failed_keys:
        voice_terms = _facet_terms(_facet_lookup(analysis_bundle, "voice_signature"))[:2]
        revised += f"\n\n我还是愿意把口气收回到{_join_terms(voice_terms, fallback='更低、更稳的声音')}里。"

    if "lexicon_idiolect" in failed_keys:
        lexicon_terms = _facet_terms(_facet_lookup(analysis_bundle, "lexicon_idiolect"))[:2]
        revised += f"\n\n真正留下来的说法，往往不是漂亮，而是{_join_terms(lexicon_terms, fallback='作者自己惯用的那种转折和落点')}。"

    if "imagery_theme" in failed_keys:
        imagery_terms = _facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:2]
        revised += f"\n\n等到情绪沉下来，场景还是会回到{_join_terms(imagery_terms, fallback='那些熟悉的意象和旧场景')}里。"

    if "stance_values" in failed_keys:
        stance_terms = _facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:2]
        revised += f"\n\n归根到底，我更愿意把它理解成{_join_terms(stance_terms, fallback='代价、边界和个人立场')}，而不是一个干净的结论。"

    if "emotional_arc" in failed_keys:
        emotion_terms = _facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:2]
        revised += f"\n\n情绪也不该一下子摊开，它应该先往里压，再沿着{_join_terms(emotion_terms, fallback='迟疑和回落')}慢慢显出来。"

    if "nonclinical_psychodynamics" in failed_keys:
        psycho_terms = _facet_terms(_facet_lookup(analysis_bundle, "nonclinical_psychodynamics"))[:2]
        revised += f"\n\n人真正用来保护自己的，很多时候就是{_join_terms(psycho_terms, fallback='那点退缩、防御和回身的动作')}。"

    if "creative_constraints" in failed_keys:
        revised = _dedupe_sentences(revised)

    if extra_requirements and extra_requirements not in revised:
        revised += f"\n\n附加要求我也会继续守住：{extra_requirements}。"

    if priorities:
        revised += "\n\n" + " ".join(f"我会继续记住：{item}" for item in priorities[:2])

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
    lower = int(target * 0.85)
    upper = int(target * 1.15)
    pass_count = sum(1 for review in reviews if review.get("pass"))
    remaining_risks = _normalize_string_list(review_plan.get("risk_watch"), limit=4)
    if not lower <= word_count <= upper:
        remaining_risks = remaining_risks + ["字数仍然需要人工再确认。"]
    if topic and topic not in final_text:
        remaining_risks = remaining_risks + ["主题在正文中的可见度仍然偏弱。"]
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
        pieces = re.split(r"[\n;；]+", value)
        return [piece.strip() for piece in pieces if piece.strip()][:limit]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_normalize_string_list(item, limit=limit))
        return _unique_preserve_order(flattened)[:limit]
    if isinstance(value, (list, tuple)):
        flattened = []
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
    pieces = re.split(r"[\s,，。！？；：、()\[\]{}<>\"'|/\\-]+", text)
    cleaned = [piece.strip() for piece in pieces if len(piece.strip()) >= 2]
    return cleaned[:12]


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
    sentences = [item.strip() for item in re.split(r"[。！？]+", text) if item.strip()]
    if not sentences:
        return 0.0
    unique_count = len(set(sentences))
    return max(0.0, 1.0 - (unique_count / len(sentences)))


def _dedupe_sentences(text: str) -> str:
    sentences = [item.strip() for item in re.split(r"(?<=[。！？])", text) if item.strip()]
    unique: list[str] = []
    for sentence in sentences:
        if sentence in unique:
            continue
        unique.append(sentence)
    return "".join(unique).strip()
