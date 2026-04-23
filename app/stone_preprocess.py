import asyncio
import json
import logging
import traceback
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.analysis.stone import build_stone_profile, build_stone_profile_messages, normalize_stone_profile
from app.llm import OpenAICompatibleClient, parse_json_response
from app.models import DocumentRecord, Project, StonePreprocessRun
from app.storage import repository
from app.db import Database

logger = logging.getLogger(__name__)


class StonePreprocessWorker:
    def __init__(self, db: Database, stream_hub: "StonePreprocessStreamHub", llm_log_path: str | None = None) -> None:
        self.db = db
        self.stream_hub = stream_hub
        self.llm_log_path = llm_log_path

    def process(self, run_id: str, project_id: str) -> None:
        loop = asyncio.get_running_loop()
        loop.create_task(self._process(run_id, project_id))

    async def _process(self, run_id: str, project_id: str) -> None:
        try:
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
                document
                for document in repository.list_project_documents(session, project.id)
                if document.ingest_status == "ready"
            ]

            chat_config = repository.get_service_config(session, "chat_service")

            summary = dict(run.summary_json or {})
            concurrency = max(1, int(summary.get("concurrency", 1)))
            summary["stone_profile_total"] = len(documents)
            summary["stone_profile_completed"] = 0
            run.summary_json = summary
            run.current_stage = "Checking documents"
            run.progress_percent = 0
            session.commit()
            self._progress(run)

        semaphore = asyncio.Semaphore(concurrency)
        completed_count = 0
        total_docs = len(documents)

        async def _process_document(document: DocumentRecord) -> None:
            nonlocal completed_count
            async with semaphore:
                with self.db.session() as session:
                    run = repository.get_stone_preprocess_run(session, run_id)
                    if not run or run.status == "cancelled":
                        return

                    metadata = dict(document.metadata_json or {})
                    if "stone_profile" in metadata and isinstance(metadata["stone_profile"], dict):
                        completed_count += 1
                        self._update_progress(session, run, completed_count, total_docs, "Profiling documents")
                        return

                    self._trace(run, f"Profiling document {completed_count + 1}/{total_docs}: {document.title}")

                try:
                    profile_payload = await asyncio.to_thread(
                        self._build_stone_profile_payload,
                        document,
                        project_name=project.name,
                        chat_config=chat_config,
                    )
                except Exception as e:
                    logger.exception("Failed to build stone profile for document %s", document.id)
                    profile_payload = build_stone_profile(document)

                with self.db.session() as session:
                    doc = repository.get_document(session, document.id)
                    if doc:
                        metadata = dict(doc.metadata_json or {})
                        metadata["stone_profile"] = profile_payload
                        doc.metadata_json = metadata
                        session.add(doc)

                    run = repository.get_stone_preprocess_run(session, run_id)
                    if run:
                        completed_count += 1
                        self._update_progress(session, run, completed_count, total_docs, "Profiling documents")

        tasks = [_process_document(doc) for doc in documents]
        if tasks:
            await asyncio.gather(*tasks)

        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status == "running":
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
        run.summary_json = summary
        run.current_stage = f"{stage} ({index}/{total})"
        run.progress_percent = int(index / total * 100) if total > 0 else 100
        session.commit()
        self._progress(run)

    def _build_stone_profile_payload(
        self,
        document: DocumentRecord,
        *,
        project_name: str,
        chat_config: "ServiceConfig | None",
    ) -> dict[str, str]:
        text = str(document.clean_text or document.raw_text or "").strip()
        if not text:
            return build_stone_profile(document)
        if not chat_config:
            return build_stone_profile(document)
            
        client = OpenAICompatibleClient(chat_config, log_path=self.llm_log_path)
        messages = build_stone_profile_messages(
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
            # We don't update token usage in DB here yet to keep it simple, 
            # but we could update run.prompt_tokens etc.
            parsed = parse_json_response(response.content, fallback=True)
            return normalize_stone_profile(parsed)
        except Exception as e:
            logger.exception("Failed to build stone profile for document %s", document.id)
            return build_stone_profile(document)

    def _mark_failed(self, run_id: str, error_message: str) -> None:
        with self.db.session() as session:
            run = repository.get_stone_preprocess_run(session, run_id)
            if run and run.status in ("queued", "running"):
                run.status = "failed"
                run.finished_at = datetime.now(UTC).replace(tzinfo=None)
                run.error_message = error_message
                session.commit()
                self._progress(run)

    def _progress(self, run: StonePreprocessRun) -> None:
        self.stream_hub.broadcast_progress(run.id, run.project_id)

    def _trace(self, run: StonePreprocessRun, message: str) -> None:
        self.stream_hub.broadcast_trace(run.id, run.project_id, message)


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
