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

from app.analysis.stone_v2 import (
    build_short_text_clusters,
    build_stone_author_model_v2,
    build_stone_profile_v2,
    build_stone_profile_v2_messages,
    build_stone_prototype_index_v2,
    normalize_stone_profile_v2,
    render_stone_author_model_markdown,
    render_stone_prototype_index_markdown,
)
from app.db import Database
from app.llm import OpenAICompatibleClient, parse_json_response
from app.models import StonePreprocessRun
from app.runtime_limits import background_task_slot
from app.storage import repository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StoneDocumentSnapshot:
    id: str
    title: str | None
    filename: str
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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._futures: dict[str, Future[None]] = {}
        self._project_by_future: dict[str, str] = {}
        self._lock = Lock()

    @staticmethod
    def _build_summary(summary: dict[str, Any] | None, *, concurrency: int) -> dict[str, Any]:
        updated = dict(summary or {})
        updated["concurrency"] = max(1, int(concurrency or 1))
        updated["stone_profile_total"] = int(updated.get("stone_profile_total") or 0)
        updated["stone_profile_completed"] = 0
        updated["current_stage"] = "queued"
        updated["progress_percent"] = 0
        return updated

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

    def submit(self, project_id: str, *, concurrency: int = 1) -> StonePreprocessRun:
        normalized_concurrency = max(1, int(concurrency or 1))
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
                    clean_text=document.clean_text,
                    raw_text=document.raw_text,
                    metadata_json=dict(document.metadata_json or {}),
                )
                for document in repository.list_project_documents(session, project.id)
                if document.ingest_status == "ready"
            ]

            chat_config = repository.get_service_config(session, "chat_service")

            summary = dict(run.summary_json or {})
            concurrency = max(1, int(summary.get("concurrency", 1)))
            summary["stone_profile_total"] = len(documents)
            summary["stone_profile_completed"] = 0
            summary["current_stage"] = "checking_documents"
            summary["progress_percent"] = 0
            run.summary_json = summary
            run.current_stage = "Checking documents"
            run.progress_percent = 0
            session.commit()
            self._progress(run)

        semaphore = asyncio.Semaphore(concurrency)
        completed_count = 0
        total_docs = len(documents)

        async def _process_document(document: StoneDocumentSnapshot) -> None:
            nonlocal completed_count
            async with semaphore:
                with self.db.session() as session:
                    run = repository.get_stone_preprocess_run(session, run_id)
                    if not run or run.status == "cancelled":
                        return

                    metadata = dict(document.metadata_json or {})
                    if "stone_profile_v2" in metadata and isinstance(metadata["stone_profile_v2"], dict):
                        completed_count += 1
                        self._update_progress(session, run, completed_count, total_docs, "Profiling documents")
                        return

                    self._trace(
                        run,
                        f"Profiling document {completed_count + 1}/{total_docs}: {document.title or document.filename}",
                    )

                try:
                    profile_result = await asyncio.to_thread(
                        self._build_stone_profile_payload,
                        document,
                        project_name=project.name,
                        chat_config=chat_config,
                    )
                except Exception as e:
                    logger.exception("Failed to build stone profile for document %s", document.id)
                    profile_result = StoneProfileResult(profile=build_stone_profile_v2(document), usage={})

                with self.db.session() as session:
                    doc = repository.get_document(session, document.id)
                    if doc:
                        metadata = dict(doc.metadata_json or {})
                        metadata["stone_profile_v2"] = profile_result.profile
                        metadata["stone_profile_version"] = "v2"
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
                        self._update_progress(session, run, completed_count, total_docs, "Profiling documents")

        tasks = [_process_document(doc) for doc in documents]
        if tasks:
            await asyncio.gather(*tasks)

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status == "running":
                asset_ids = self._refresh_stone_v2_assets(session, project_id)
                summary = dict(run.summary_json or {})
                summary["stone_author_model_v2_draft_id"] = asset_ids.get("stone_author_model_v2")
                summary["stone_prototype_index_v2_draft_id"] = asset_ids.get("stone_prototype_index_v2")
                run.summary_json = summary
                run.status = "completed"
                run.finished_at = datetime.now(UTC).replace(tzinfo=None)
                run.current_stage = "Completed"
                run.progress_percent = 100
                session.commit()
                self._progress(run)
                self._trace(run, "Stone preprocess run completed successfully.")

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
            return StoneProfileResult(profile=build_stone_profile_v2(document), usage={})
        if not chat_config:
            return StoneProfileResult(profile=build_stone_profile_v2(document), usage={})
            
        client = OpenAICompatibleClient(chat_config, log_path=self.llm_log_path)
        messages = build_stone_profile_v2_messages(
            project_name,
            document.title or document.filename,
            text,
        )
        try:
            response = client.chat_completion_result(
                messages,
                model=chat_config.model,
                temperature=0.2,
                max_tokens=1200,
            )
            parsed = parse_json_response(response.content, fallback=True)
            return StoneProfileResult(
                profile=normalize_stone_profile_v2(
                    parsed,
                    article_text=text,
                    fallback_title=document.title or document.filename,
                ),
                usage=dict(response.usage or {}),
            )
        except Exception as e:
            logger.exception("Failed to build stone profile for document %s", document.id)
            return StoneProfileResult(profile=build_stone_profile_v2(document), usage={})

    def _refresh_stone_v2_assets(self, session: Session, project_id: str) -> dict[str, str]:
        project = repository.get_project(session, project_id)
        if not project:
            return {}

        profiles: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
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
            documents.append(
                {
                    "document_id": document.id,
                    "title": document.title or document.filename,
                    "clean_text": document.clean_text,
                    "raw_text": document.raw_text,
                    "text": str(document.clean_text or document.raw_text or ""),
                }
            )

        if not profiles:
            return {}

        clusters = build_short_text_clusters(profiles)
        author_model = build_stone_author_model_v2(
            project_name=project.name,
            profiles=profiles,
            short_text_clusters=clusters,
        )
        prototype_index = build_stone_prototype_index_v2(
            project_name=project.name,
            profiles=profiles,
            documents=documents,
        )

        author_draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_author_model_v2",
            markdown_text=render_stone_author_model_markdown(author_model),
            json_payload=author_model,
            prompt_text=json.dumps(author_model, ensure_ascii=False, indent=2),
            notes="Stone v2 preprocess auto-generated baseline draft.",
        )
        prototype_draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=None,
            asset_kind="stone_prototype_index_v2",
            markdown_text=render_stone_prototype_index_markdown(prototype_index),
            json_payload=prototype_index,
            prompt_text=json.dumps(prototype_index, ensure_ascii=False, indent=2),
            notes="Stone v2 preprocess auto-generated prototype draft.",
        )
        return {
            "stone_author_model_v2": author_draft.id,
            "stone_prototype_index_v2": prototype_draft.id,
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
