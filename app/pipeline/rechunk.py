from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Lock
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select

from app.db import Database
from app.llm.client import OpenAICompatibleClient
from app.models import DocumentRecord, TextChunk, utcnow
from app.pipeline.chunking import chunk_segments
from app.schemas import ExtractedSegment, ServiceConfig
from app.storage import repository

DEFAULT_CHUNK_SIZE = 1800
DEFAULT_CHUNK_OVERLAP = 300
EMBEDDING_BATCH_SIZE = 32


class RechunkTaskManager:
    def __init__(
        self,
        db: Database,
        *,
        llm_log_path: str | None = None,
        max_workers: int = 1,
    ) -> None:
        self.db = db
        self.llm_log_path = llm_log_path
        self.executor = ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="rechunk")
        self._tasks: dict[str, dict[str, Any]] = {}
        self._active_by_project: dict[str, str] = {}
        self._lock = Lock()

    def submit(
        self,
        *,
        project_id: str,
        embedding_config: ServiceConfig | None,
    ) -> dict[str, Any]:
        task_id = str(uuid4())
        now = utcnow().isoformat()
        with self._lock:
            existing = self._active_by_project.get(project_id)
            if existing:
                existing_task = self._tasks.get(existing) or {}
                if existing_task.get("status") in {"queued", "running"}:
                    raise ValueError(existing)
            self._tasks[task_id] = {
                "task_id": task_id,
                "project_id": project_id,
                "status": "queued",
                "stage": "queued",
                "progress_percent": 0,
                "created_at": now,
                "started_at": None,
                "finished_at": None,
                "error": None,
                "chunk_size": DEFAULT_CHUNK_SIZE,
                "chunk_overlap": DEFAULT_CHUNK_OVERLAP,
                "document_total": 0,
                "document_processed": 0,
                "chunk_total": 0,
                "chunk_processed": 0,
                "embedding_enabled": bool(embedding_config),
                "embedding_model": embedding_config.model if embedding_config else None,
                "embedding_total": 0,
                "embedding_processed": 0,
                "embedding_batches": 0,
                "embedding_errors": 0,
            }
            self._active_by_project[project_id] = task_id
        self.executor.submit(self._run_task, task_id, project_id, embedding_config)
        return self.get(task_id) or {}

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return deepcopy(task) if task else None

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)

    def _run_task(self, task_id: str, project_id: str, embedding_config: ServiceConfig | None) -> None:
        self._update(
            task_id,
            status="running",
            stage="rechunk_documents",
            started_at=utcnow().isoformat(),
            progress_percent=1,
        )
        try:
            with self.db.session() as session:
                document_ids = session.scalars(
                    select(DocumentRecord.id)
                    .where(DocumentRecord.project_id == project_id, DocumentRecord.ingest_status == "ready")
                ).all()
                self._update(task_id, document_total=len(document_ids))
                total_chunks = 0
                for index, doc_id in enumerate(document_ids, start=1):
                    document = session.get(DocumentRecord, doc_id)
                    if not document:
                        continue
                    chunks = self._build_document_chunks(document)
                    total_chunks += len(chunks)
                    repository.replace_document_chunks(
                        session,
                        document.id,
                        [
                            {
                                "project_id": project_id,
                                "chunk_index": chunk["chunk_index"],
                                "content": chunk["content"],
                                "start_offset": chunk["start_offset"],
                                "end_offset": chunk["end_offset"],
                                "page_number": chunk["page_number"],
                                "token_count": chunk["token_count"],
                                "metadata_json": chunk["metadata_json"],
                            }
                            for chunk in chunks
                        ],
                    )
                    progress = 10
                    if document_ids:
                        progress = min(60, 10 + int((index / len(document_ids)) * 50))
                    self._update(
                        task_id,
                        document_processed=index,
                        chunk_processed=total_chunks,
                        chunk_total=total_chunks,
                        progress_percent=progress,
                    )

            if embedding_config:
                self._update(task_id, stage="rebuild_embeddings", progress_percent=65)
                self._rebuild_embeddings(task_id, project_id, embedding_config)

            self._update(
                task_id,
                status="completed",
                stage="completed",
                progress_percent=100,
                finished_at=utcnow().isoformat(),
            )
        except Exception as exc:
            self._update(
                task_id,
                status="failed",
                stage="failed",
                error=str(exc),
                finished_at=utcnow().isoformat(),
            )
        finally:
            with self._lock:
                active_task_id = self._active_by_project.get(project_id)
                if active_task_id == task_id:
                    self._active_by_project.pop(project_id, None)

    def _rebuild_embeddings(self, task_id: str, project_id: str, config: ServiceConfig) -> None:
        client = OpenAICompatibleClient(config, log_path=self.llm_log_path)
        resolved_model = config.model or client.resolve_model()
        with self.db.session() as session:
            total = int(
                session.scalar(
                    select(func.count())
                    .select_from(TextChunk)
                    .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
                    .where(
                        TextChunk.project_id == project_id,
                        DocumentRecord.ingest_status == "ready",
                    )
                )
                or 0
            )
            self._update(task_id, embedding_total=total, embedding_model=resolved_model)
            if total <= 0:
                return

            stmt = (
                select(TextChunk.id, TextChunk.content)
                .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
                .where(
                    TextChunk.project_id == project_id,
                    DocumentRecord.ingest_status == "ready",
                )
                .order_by(TextChunk.document_id.asc(), TextChunk.chunk_index.asc())
            )
            result = session.execute(stmt)
            processed = 0
            batches = 0
            while True:
                rows = result.fetchmany(EMBEDDING_BATCH_SIZE)
                if not rows:
                    break
                chunk_ids = [str(row.id) for row in rows]
                chunk_texts = [str(row.content or "") for row in rows]
                vectors = client.embeddings(chunk_texts, model=resolved_model)
                chunks = list(session.scalars(select(TextChunk).where(TextChunk.id.in_(chunk_ids))))
                chunk_map = {chunk.id: chunk for chunk in chunks}
                for chunk_id, vector in zip(chunk_ids, vectors):
                    chunk = chunk_map.get(chunk_id)
                    if not chunk:
                        continue
                    chunk.embedding_vector = vector
                    chunk.embedding_model = resolved_model
                    processed += 1
                batches += 1
                session.flush()
                progress = min(99, 65 + int((processed / max(total, 1)) * 34))
                self._update(
                    task_id,
                    embedding_processed=processed,
                    embedding_batches=batches,
                    progress_percent=progress,
                )

    @staticmethod
    def _build_document_chunks(document: DocumentRecord) -> list[dict[str, Any]]:
        clean_text = (document.clean_text or "").strip()
        if not clean_text:
            return []
        segments = [ExtractedSegment(text=clean_text, metadata={"source_type": document.source_type})]
        chunk_payloads = chunk_segments(
            segments,
            chunk_size=DEFAULT_CHUNK_SIZE,
            overlap=DEFAULT_CHUNK_OVERLAP,
        )
        return [
            {
                "chunk_index": chunk.chunk_index,
                "content": chunk.content,
                "start_offset": chunk.start_offset,
                "end_offset": chunk.end_offset,
                "page_number": chunk.page_number,
                "token_count": chunk.token_count,
                "metadata_json": chunk.metadata,
            }
            for chunk in chunk_payloads
        ]

    def _update(self, task_id: str, **fields: Any) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.update(fields)
