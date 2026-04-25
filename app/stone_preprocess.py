import asyncio
import json
import logging
import traceback
from collections.abc import AsyncGenerator
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analysis.stone_v3 import (
    STONE_V3_PROFILE_KEY,
    STONE_V3_PROFILE_CHUNK_TOKEN_BUDGET,
    STONE_V3_PROMPT_TOKEN_BUDGET,
    StoneV3BaselineSynthesizer,
    build_stone_profile_v3_messages,
    build_stone_profile_v3_merge_messages,
    estimate_stone_prompt_tokens,
    normalize_stone_profile_v3,
    render_stone_author_model_v3_markdown,
    render_stone_prototype_index_v3_markdown,
    split_text_for_stone_budget,
)
from app.db import Database
from app.llm import OpenAICompatibleClient, parse_json_response
from app.models import StonePreprocessRun
from app.runtime_limits import background_task_slot
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.storage import repository

logger = logging.getLogger(__name__)
STONE_MIN_ANALYSIS_READY_RATIO = 0.5


@dataclass(slots=True)
class StoneDocumentSnapshot:
    id: str
    title: str | None
    filename: str
    source_type: str | None
    created_at_guess: str | None
    clean_text: str | None
    raw_text: str | None
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class StoneProfileResult:
    profile: dict[str, Any]
    usage: dict[str, int]


class StonePreprocessWorker:
    def __init__(self, db: Database, stream_hub: "StonePreprocessStreamHub", llm_log_path: str | None = None) -> None:
        self.db = db
        self.stream_hub = stream_hub
        self.llm_log_path = llm_log_path
        self._baseline_synthesizer = StoneV3BaselineSynthesizer(log_path=llm_log_path)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._futures: dict[str, Future[None]] = {}
        self._project_by_future: dict[str, str] = {}
        self._lock = Lock()

    @staticmethod
    def _build_summary(summary: dict[str, Any] | None, *, concurrency: int) -> dict[str, Any]:
        updated = dict(summary or {})
        updated["concurrency"] = max(1, int(concurrency or DEFAULT_ANALYSIS_CONCURRENCY))
        updated["stone_profile_total"] = int(updated.get("stone_profile_total") or 0)
        updated["stone_profile_completed"] = 0
        updated["stone_profile_failed"] = int(updated.get("stone_profile_failed") or 0)
        updated["current_stage"] = "queued"
        updated["progress_percent"] = 0
        updated["profile_version"] = "v3"
        updated["baseline_version"] = "v3"
        updated["stage_trace"] = list(updated.get("stage_trace") or [])
        return updated

    @staticmethod
    def _meets_analysis_ready_threshold(profile_count: int, total_count: int) -> bool:
        total = max(0, int(total_count or 0))
        completed = max(0, int(profile_count or 0))
        if total <= 0:
            return False
        return completed * 2 > total

    @staticmethod
    def _append_stage_trace(
        summary: dict[str, Any],
        *,
        stage: str,
        attempt: int = 1,
        status: str,
        model: str | None = None,
        usage: dict[str, Any] | None = None,
        output_preview: str | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        trace = list(summary.get("stage_trace") or [])
        trace.append(
            {
                "stage": stage,
                "attempt": int(attempt or 1),
                "status": status,
                "model": model,
                "usage": dict(usage or {}),
                "output_preview": str(output_preview or "")[:320],
                "failure_reason": str(failure_reason or "")[:240],
            }
        )
        summary["stage_trace"] = trace[-48:]
        return summary

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def resume_interrupted_runs(self) -> None:
        interrupted_at = datetime.now(UTC).replace(tzinfo=None)
        with self.db.session() as session:
            active_runs = list(
                session.scalars(
                    select(StonePreprocessRun).where(StonePreprocessRun.status.in_(("queued", "running")))
                )
            )
            for run in active_runs:
                summary = dict(run.summary_json or {})
                summary["current_stage"] = "interrupted"
                summary["progress_percent"] = int(summary.get("progress_percent") or run.progress_percent or 0)
                run.status = "failed"
                run.finished_at = interrupted_at
                run.error_message = (
                    "Service restarted while Stone preprocess was still running. "
                    "Start preprocess again to resume from saved document profiles."
                )
                run.current_stage = "Interrupted"
                run.summary_json = summary

    def submit(self, project_id: str, *, concurrency: int = DEFAULT_ANALYSIS_CONCURRENCY) -> StonePreprocessRun:
        normalized_concurrency = max(1, int(concurrency or DEFAULT_ANALYSIS_CONCURRENCY))
        with self.db.session() as session:
            project = repository.get_project(session, project_id)
            if not project or project.mode != "stone":
                raise ValueError("Stone project not found.")
            chat_config = repository.get_service_config(session, "chat_service")
            active_run = repository.get_active_stone_preprocess_run(session, project_id)
            if active_run and self.is_tracking(active_run.id):
                run_id = active_run.id
            else:
                resumable_run = active_run or repository.get_latest_resumable_stone_preprocess_run(session, project_id)
                if resumable_run:
                    resumable_run.status = "queued"
                    resumable_run.started_at = None
                    resumable_run.finished_at = None
                    resumable_run.error_message = None
                    resumable_run.current_stage = "queued"
                    resumable_run.progress_percent = 0
                    resumable_run.llm_model = chat_config.model if chat_config else resumable_run.llm_model
                    resumable_run.summary_json = self._build_summary(
                        resumable_run.summary_json,
                        concurrency=normalized_concurrency,
                    )
                    run_id = resumable_run.id
                else:
                    run = repository.create_stone_preprocess_run(
                        session,
                        project_id=project_id,
                        llm_model=chat_config.model if chat_config else None,
                        summary_json=self._build_summary({}, concurrency=normalized_concurrency),
                    )
                    run_id = run.id
        if not self.is_tracking(run_id):
            self.process(run_id, project_id)
        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if not run:
                raise ValueError("Stone preprocess run not found after submit.")
            return run

    def process(self, run_id: str, project_id: str) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            raise RuntimeError("Stone preprocess event loop is not ready.")
        future = asyncio.run_coroutine_threadsafe(self._process(run_id, project_id), loop)
        with self._lock:
            self._futures[run_id] = future
            self._project_by_future[run_id] = project_id
        future.add_done_callback(lambda _: self._finish_future(run_id))

    def is_tracking(self, run_id: str) -> bool:
        with self._lock:
            future = self._futures.get(run_id)
        return future is not None and not future.done()

    async def shutdown(self) -> None:
        with self._lock:
            futures = list(self._futures.values())
        for future in futures:
            future.cancel()
        if not futures:
            return
        await asyncio.gather(
            *(asyncio.wrap_future(future) for future in futures),
            return_exceptions=True,
        )

    async def _process(self, run_id: str, project_id: str) -> None:
        try:
            with background_task_slot():
                await self._run(run_id, project_id)
        except asyncio.CancelledError:
            logger.info("Stone preprocess %s cancelled.", run_id)
            self._mark_failed(run_id, "Cancelled.")
            raise
        except Exception as e:
            logger.exception("Stone preprocess %s failed.", run_id)
            self._mark_failed(run_id, f"Internal error: {e}\n{traceback.format_exc()}")

    async def _run(self, run_id: str, project_id: str) -> None:
        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if not run:
                return
            project = repository.get_project(session, project_id)
            if not project:
                self._mark_failed(run_id, "Project not found.")
                return

            if run.status == "cancelled":
                return
            if run.status == "queued":
                run.status = "running"
                run.started_at = datetime.now(UTC).replace(tzinfo=None)
                session.commit()

            self._trace(run, "Starting Stone preprocess run.")

            documents = [
                StoneDocumentSnapshot(
                    id=document.id,
                    title=document.title,
                    filename=document.filename,
                    source_type=document.source_type,
                    created_at_guess=document.created_at_guess,
                    clean_text=document.clean_text,
                    raw_text=document.raw_text,
                    metadata_json=dict(document.metadata_json or {}),
                )
                for document in repository.list_project_documents(session, project.id)
                if document.ingest_status == "ready"
            ]

            chat_config = repository.get_service_config(session, "chat_service")
            if not chat_config:
                self._append_stage_trace(
                    summary := dict(run.summary_json or {}),
                    stage="document_profile_v3",
                    status="failed",
                    failure_reason="Stone v3 preprocess requires a configured chat model.",
                )
                run.summary_json = summary
                session.commit()
                self._mark_failed(run_id, "Stone v3 preprocess requires a configured chat model.")
                return

            summary = dict(run.summary_json or {})
            concurrency = max(1, int(summary.get("concurrency") or DEFAULT_ANALYSIS_CONCURRENCY))
            summary["stone_profile_total"] = len(documents)
            summary["stone_profile_completed"] = 0
            summary["current_stage"] = "document_profile_v3"
            summary["progress_percent"] = 0
            summary["profile_version"] = "v3"
            summary["baseline_version"] = "v3"
            run.summary_json = summary
            run.current_stage = "document_profile_v3"
            run.progress_percent = 0
            session.commit()
            self._progress(run)

        semaphore = asyncio.Semaphore(concurrency)
        completed_count = 0
        total_docs = len(documents)
        profile_errors: list[str] = []

        async def _process_document(document: StoneDocumentSnapshot) -> None:
            nonlocal completed_count
            async with semaphore:
                with self.db.session() as session:
                    run = repository.get_stone_preprocess_run(session, run_id)
                    if not run or run.status == "cancelled":
                        return

                    metadata = dict(document.metadata_json or {})
                    if STONE_V3_PROFILE_KEY in metadata and isinstance(metadata[STONE_V3_PROFILE_KEY], dict):
                        completed_count += 1
                        self._update_progress(session, run, completed_count, total_docs, "document_profile_v3")
                        return

                    self._trace(
                        run,
                        f"Stone v3 profiling document {completed_count + 1}/{total_docs}: {document.title or document.filename}",
                    )

                try:
                    profile_result = await asyncio.to_thread(
                        self._build_stone_profile_payload,
                        document,
                        project_name=project.name,
                        chat_config=chat_config,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Failed to build Stone v3 profile for document %s", document.id)
                    profile_errors.append(f"{document.title or document.filename}: {exc}")
                    return

                with self.db.session() as session:
                    doc = repository.get_document(session, document.id)
                    if doc:
                        metadata = dict(doc.metadata_json or {})
                        metadata[STONE_V3_PROFILE_KEY] = profile_result.profile
                        metadata["stone_profile_version"] = "v3"
                        metadata.pop("stone_profile", None)
                        doc.metadata_json = metadata
                        session.add(doc)

                    run = repository.get_stone_preprocess_run(session, run_id)
                    if run:
                        usage = dict(profile_result.usage or {})
                        run.prompt_tokens = int(run.prompt_tokens or 0) + int(usage.get("prompt_tokens") or 0)
                        run.completion_tokens = int(run.completion_tokens or 0) + int(usage.get("completion_tokens") or 0)
                        run.total_tokens = int(run.total_tokens or 0) + int(usage.get("total_tokens") or 0)
                        completed_count += 1
                        self._update_progress(session, run, completed_count, total_docs, "document_profile_v3")

        tasks = [_process_document(doc) for doc in documents]
        if tasks:
            await asyncio.gather(*tasks)

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if not run or run.status != "running":
                return
            summary = dict(run.summary_json or {})
            summary["stone_profile_failed"] = len(profile_errors)
            summary = self._append_stage_trace(
                summary,
                stage="document_profile_v3",
                status=(
                    "completed"
                    if not profile_errors
                    else "partial_failed"
                    if self._meets_analysis_ready_threshold(completed_count, total_docs)
                    else "failed"
                ),
                model=chat_config.model,
                usage={
                    "prompt_tokens": int(run.prompt_tokens or 0),
                    "completion_tokens": int(run.completion_tokens or 0),
                    "total_tokens": int(run.total_tokens or 0),
                },
                output_preview=f"profiled {completed_count}/{total_docs} documents",
                failure_reason="; ".join(profile_errors[:3]),
            )
            run.summary_json = summary
            session.commit()

        if profile_errors:
            if self._meets_analysis_ready_threshold(completed_count, total_docs):
                logger.warning(
                    "Stone v3 preprocess continuing with partial corpus coverage for project %s: %s/%s profiles ready.",
                    project_id,
                    completed_count,
                    total_docs,
                )
            else:
                self._mark_failed(
                    run_id,
                    "Stone v3 document profiling failed: " + "; ".join(profile_errors[:5]),
                )
                return

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status == "running":
                summary = dict(run.summary_json or {})
                summary["stone_profile_completed"] = completed_count
                summary["stone_profile_total"] = total_docs
                summary["stone_profile_failed"] = len(profile_errors)
                summary["analysis_ready"] = self._meets_analysis_ready_threshold(completed_count, total_docs)
                run.summary_json = summary
                session.commit()

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if not run or run.status != "running":
                return
            summary = dict(run.summary_json or {})
            summary["current_stage"] = "family_induction_v3"
            run.current_stage = "family_induction_v3"
            run.summary_json = summary
            session.commit()
            self._progress(run)

        try:
            synthesis_result = await asyncio.to_thread(
                self._build_stone_v3_assets,
                project_id,
                chat_config=chat_config,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to build Stone v3 baseline assets for project %s", project_id)
            if self._meets_analysis_ready_threshold(completed_count, total_docs):
                self._mark_partial_failed(
                    run_id,
                    f"Stone v3 baseline synthesis failed after profiling {completed_count}/{total_docs} documents: {exc}",
                )
            else:
                self._mark_failed(run_id, f"Stone v3 baseline synthesis failed: {exc}")
            return

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status == "running":
                summary = dict(run.summary_json or {})
                summary["stone_author_model_v3_draft_id"] = synthesis_result.get("stone_author_model_v3")
                summary["stone_prototype_index_v3_draft_id"] = synthesis_result.get("stone_prototype_index_v3")
                summary["baseline_review_v3"] = dict(synthesis_result.get("critic_review") or {})
                summary["stage_trace"] = list(summary.get("stage_trace") or []) + list(synthesis_result.get("stage_trace") or [])
                summary["baseline_version"] = "v3"
                summary["profile_version"] = "v3"
                summary["stone_profile_completed"] = completed_count
                summary["stone_profile_total"] = total_docs
                summary["stone_profile_failed"] = len(profile_errors)
                summary["analysis_ready"] = self._meets_analysis_ready_threshold(completed_count, total_docs)
                run.summary_json = summary
                run.status = "partial_failed" if profile_errors else "completed"
                run.finished_at = datetime.now(UTC).replace(tzinfo=None)
                run.error_message = (
                    f"Stone v3 preprocess completed with {len(profile_errors)} document profiling failures."
                    if profile_errors
                    else None
                )
                run.current_stage = "Completed with gaps" if profile_errors else "Completed"
                run.progress_percent = 100
                session.commit()
                self._progress(run)
                self._trace(run, "Stone v3 preprocess run completed successfully.")

    def _update_progress(self, session: Session, run: StonePreprocessRun, index: int, total: int, stage: str) -> None:
        summary = dict(run.summary_json or {})
        summary["stone_profile_completed"] = index
        summary["stone_profile_total"] = total
        summary["current_stage"] = stage
        summary["progress_percent"] = int(index / total * 100) if total > 0 else 100
        run.summary_json = summary
        run.current_stage = f"{stage} ({index}/{total})"
        run.progress_percent = int(index / total * 100) if total > 0 else 100
        session.commit()
        self._progress(run)

    def _build_stone_profile_payload(
        self,
        document: StoneDocumentSnapshot,
        *,
        project_name: str,
        chat_config: "ServiceConfig | None",
    ) -> StoneProfileResult:
        text = str(document.clean_text or document.raw_text or "").strip()
        if not text:
            raise ValueError(f"Document {document.id} has no text to profile.")
        if not chat_config:
            raise ValueError("Stone v3 document profiling requires a configured chat model.")

        client = OpenAICompatibleClient(chat_config, log_path=self.llm_log_path)

        def _profile_once(messages: list[dict[str, Any]], *, stage_label: str) -> StoneProfileResult:
            last_error: Exception | None = None
            for attempt in range(1, 4):
                try:
                    response = client.chat_completion_result(
                        messages,
                        model=chat_config.model,
                        temperature=0.2,
                        max_tokens=1600,
                    )
                    parsed = parse_json_response(response.content, fallback=True)
                    return StoneProfileResult(
                        profile=dict(parsed or {}),
                        usage=dict(response.usage or {}),
                    )
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    logger.exception(
                        "Stone v3 %s attempt %s failed for document %s",
                        stage_label,
                        attempt,
                        document.id,
                    )
            raise RuntimeError(f"Stone v3 {stage_label} failed after retries: {last_error}")

        if estimate_stone_prompt_tokens(text) > STONE_V3_PROMPT_TOKEN_BUDGET:
            chunks = split_text_for_stone_budget(
                text,
                token_budget=STONE_V3_PROFILE_CHUNK_TOKEN_BUDGET,
            )
            logger.info(
                "Stone v3 document %s exceeded prompt budget; profiling %s chunks before merge.",
                document.id,
                len(chunks),
            )
            chunk_profiles: list[dict[str, Any]] = []
            total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            for index, chunk in enumerate(chunks, start=1):
                chunk_result = _profile_once(
                    build_stone_profile_v3_messages(
                        project_name,
                        f"{document.title or document.filename} [chunk {index}/{len(chunks)}]",
                        chunk,
                    ),
                    stage_label=f"document_profile_chunk_{index}",
                )
                chunk_profiles.append(
                    normalize_stone_profile_v3(
                        chunk_result.profile,
                        article_text=chunk,
                        fallback_title=document.title or document.filename,
                        document_id=document.id,
                        source_meta={
                            "chunk_index": index,
                            "chunk_total": len(chunks),
                            "created_at_guess": document.created_at_guess,
                            "source_type": document.source_type,
                        },
                    )
                )
                for usage_key in total_usage:
                    total_usage[usage_key] += int(chunk_result.usage.get(usage_key) or 0)
            merge_result = _profile_once(
                build_stone_profile_v3_merge_messages(
                    project_name,
                    document.title or document.filename,
                    chunk_profiles,
                ),
                stage_label="document_profile_merge",
            )
            for usage_key in total_usage:
                total_usage[usage_key] += int(merge_result.usage.get(usage_key) or 0)
            return StoneProfileResult(
                profile=normalize_stone_profile_v3(
                    merge_result.profile,
                    article_text=text,
                    fallback_title=document.title or document.filename,
                    document_id=document.id,
                    source_meta={
                        "created_at_guess": document.created_at_guess,
                        "source_type": document.source_type,
                        "chunked_profile": True,
                        "chunk_total": len(chunks),
                    },
                ),
                usage=total_usage,
            )

        messages = build_stone_profile_v3_messages(
            project_name,
            document.title or document.filename,
            text,
        )
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = client.chat_completion_result(
                    messages,
                    model=chat_config.model,
                    temperature=0.2,
                    max_tokens=1600,
                )
                parsed = parse_json_response(response.content, fallback=True)
                return StoneProfileResult(
                    profile=normalize_stone_profile_v3(
                        dict(parsed or {}),
                        article_text=text,
                        fallback_title=document.title or document.filename,
                        document_id=document.id,
                        source_meta={
                            "created_at_guess": document.created_at_guess,
                            "source_type": document.source_type,
                        },
                    ),
                    usage=dict(response.usage or {}),
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.exception(
                    "Stone v3 profile attempt %s failed for document %s",
                    attempt,
                    document.id,
                )
        raise RuntimeError(f"Stone v3 profile generation failed after retries: {last_error}")

    def _build_stone_v3_assets(
        self,
        project_id: str,
        *,
        chat_config: "ServiceConfig | None",
    ) -> dict[str, Any]:
        if not chat_config:
            raise ValueError("Stone v3 baseline synthesis requires a configured chat model.")
        with self.db.session() as session:
            project = repository.get_project(session, project_id)
            if not project:
                raise ValueError("Project not found.")

            profiles: list[dict[str, Any]] = []
            documents: list[dict[str, Any]] = []
            for document in repository.list_project_documents(session, project_id):
                metadata = dict(document.metadata_json or {})
                profile = metadata.get(STONE_V3_PROFILE_KEY)
                if not isinstance(profile, dict):
                    continue
                normalized = normalize_stone_profile_v3(
                    dict(profile or {}),
                    article_text=str(document.clean_text or document.raw_text or ""),
                    fallback_title=document.title or document.filename,
                    document_id=document.id,
                    source_meta={
                        "created_at_guess": document.created_at_guess,
                        "source_type": document.source_type,
                    },
                )
                profiles.append(normalized)
                documents.append(
                    {
                        "document_id": document.id,
                        "title": document.title or document.filename,
                        "filename": document.filename,
                        "source_type": document.source_type,
                        "created_at_guess": document.created_at_guess,
                        "clean_text": document.clean_text,
                        "raw_text": document.raw_text,
                        "text": str(document.clean_text or document.raw_text or ""),
                    }
                )

            if not profiles:
                raise ValueError("No stone_profile_v3 documents are available for Stone v3 baseline synthesis.")

            synthesis = self._baseline_synthesizer.build(
                project_name=project.name,
                profiles=profiles,
                documents=documents,
                config=chat_config,
            )
            author_model = synthesis["author_model"]
            prototype_index = synthesis["prototype_index"]

            author_draft = repository.create_asset_draft(
                session,
                project_id=project_id,
                run_id=None,
                asset_kind="stone_author_model_v3",
                markdown_text=render_stone_author_model_v3_markdown(author_model),
                json_payload=author_model,
                prompt_text=json.dumps(author_model, ensure_ascii=False, indent=2),
                notes="Stone v3 preprocess auto-generated author model baseline draft.",
            )
            prototype_draft = repository.create_asset_draft(
                session,
                project_id=project_id,
                run_id=None,
                asset_kind="stone_prototype_index_v3",
                markdown_text=render_stone_prototype_index_v3_markdown(prototype_index),
                json_payload=prototype_index,
                prompt_text=json.dumps(prototype_index, ensure_ascii=False, indent=2),
                notes="Stone v3 preprocess auto-generated prototype index baseline draft.",
            )
            return {
                "stone_author_model_v3": author_draft.id,
                "stone_prototype_index_v3": prototype_draft.id,
                "stage_trace": list(synthesis.get("stage_trace") or []),
                "critic_review": dict(synthesis.get("critic_review") or {}),
            }

    def _mark_failed(self, run_id: str, error_message: str) -> None:
        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status in ("queued", "running"):
                summary = dict(run.summary_json or {})
                summary["current_stage"] = "failed"
                summary["progress_percent"] = int(summary.get("progress_percent") or run.progress_percent or 0)
                run.status = "failed"
                run.finished_at = datetime.now(UTC).replace(tzinfo=None)
                run.error_message = error_message
                run.current_stage = "Failed"
                run.summary_json = summary
                session.commit()
                self._progress(run)

    def _mark_partial_failed(self, run_id: str, error_message: str) -> None:
        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status in ("queued", "running"):
                summary = dict(run.summary_json or {})
                summary["current_stage"] = "partial_failed"
                summary["progress_percent"] = 100 if int(summary.get("stone_profile_completed") or 0) > 0 else int(
                    summary.get("progress_percent") or run.progress_percent or 0
                )
                summary["analysis_ready"] = self._meets_analysis_ready_threshold(
                    int(summary.get("stone_profile_completed") or 0),
                    int(summary.get("stone_profile_total") or 0),
                )
                run.status = "partial_failed"
                run.finished_at = datetime.now(UTC).replace(tzinfo=None)
                run.error_message = error_message
                run.current_stage = "Partial failed"
                run.progress_percent = int(summary.get("progress_percent") or 100)
                run.summary_json = summary
                session.commit()
                self._progress(run)

    def _progress(self, run: StonePreprocessRun) -> None:
        self.stream_hub.broadcast_progress(run.id, run.project_id)

    def _trace(self, run: StonePreprocessRun, message: str) -> None:
        self.stream_hub.broadcast_trace(run.id, run.project_id, message)

    def _finish_future(self, run_id: str) -> None:
        with self._lock:
            self._futures.pop(run_id, None)
            self._project_by_future.pop(run_id, None)


class StonePreprocessStreamHub:
    def __init__(self) -> None:
        self._queues: dict[str, list[asyncio.Queue]] = {}

    def subscribe(self, run_id: str) -> asyncio.Queue:
        q = asyncio.Queue()
        if run_id not in self._queues:
            self._queues[run_id] = []
        self._queues[run_id].append(q)
        return q

    def unsubscribe(self, run_id: str, q: asyncio.Queue) -> None:
        if run_id in self._queues:
            self._queues[run_id].remove(q)
            if not self._queues[run_id]:
                del self._queues[run_id]

    def broadcast_progress(self, run_id: str, project_id: str) -> None:
        self._broadcast(run_id, {"type": "progress", "run_id": run_id, "project_id": project_id})

    def broadcast_trace(self, run_id: str, project_id: str, message: str) -> None:
        self._broadcast(run_id, {"type": "trace", "run_id": run_id, "project_id": project_id, "message": message})

    def _broadcast(self, run_id: str, payload: dict[str, str]) -> None:
        for q in self._queues.get(run_id, []):
            q.put_nowait(payload)

    async def stream_events(self, run_id: str) -> AsyncGenerator[str, None]:
        q = self.subscribe(run_id)
        try:
            while True:
                payload = await q.get()
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            self.unsubscribe(run_id, q)
