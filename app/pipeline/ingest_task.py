from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select

from app.db import Database
from app.llm.client import OpenAICompatibleClient
from app.models import DocumentRecord, TextChunk, utcnow
from app.pipeline.chunking import chunk_segments
from app.pipeline.extractors import ExtractionError, extract_text
from app.retrieval.vector_store import VectorStoreManager
from app.schemas import ServiceConfig
from app.storage import repository


class TaskStage(str, Enum):
    QUEUED = "queued"
    PARSING = "parsing"
    EXTRACTING = "extracting"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    STORING = "storing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class IngestTask:
    task_id: str
    project_id: str
    document_id: str
    filename: str
    status: TaskStage = TaskStage.QUEUED
    progress_percent: int = 0
    error: str | None = None
    retry_count: int = 0
    created_at: str = field(default_factory=lambda: utcnow().isoformat())
    started_at: str | None = None
    finished_at: str | None = None
    stages: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "project_id": self.project_id,
            "document_id": self.document_id,
            "filename": self.filename,
            "status": self.status.value if isinstance(self.status, TaskStage) else self.status,
            "progress_percent": self.progress_percent,
            "error": self.error,
            "retry_count": self.retry_count,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stages": self.stages,
        }


class IngestTaskManager:
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0
    EMBEDDING_BATCH_SIZE = 32

    def __init__(
        self,
        db: Database,
        vector_store_manager: VectorStoreManager,
        *,
        max_workers: int = 4,
        llm_log_path: str | None = None,
    ) -> None:
        self.db = db
        self.vector_store_manager = vector_store_manager
        self.executor = ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="ingest")
        self._tasks: dict[str, IngestTask] = {}
        self._tasks_by_project: dict[str, set[str]] = {}
        self._tasks_by_document: dict[str, str] = {}
        self._lock = Lock()
        self.llm_log_path = llm_log_path
        self._embedding_config: ServiceConfig | None = None

    def set_embedding_config(self, config: ServiceConfig | None) -> None:
        self._embedding_config = config

    def submit(
        self,
        project_id: str,
        document_id: str,
        filename: str,
        content: bytes,
        mime_type: str | None = None,
    ) -> dict[str, Any]:
        task_id = str(uuid4())
        with self._lock:
            existing = self._tasks_by_document.get(document_id)
            if existing and existing in self._tasks:
                task = self._tasks[existing]
                if task.status in (TaskStage.QUEUED, TaskStage.PARSING, TaskStage.EXTRACTING, TaskStage.CHUNKING, TaskStage.EMBEDDING, TaskStage.STORING):
                    return task.to_dict()
            task = IngestTask(
                task_id=task_id,
                project_id=project_id,
                document_id=document_id,
                filename=filename,
            )
            self._tasks[task_id] = task
            self._tasks_by_project.setdefault(project_id, set()).add(task_id)
            self._tasks_by_document[document_id] = task_id
        self.executor.submit(self._run_task, task_id, content, mime_type)
        return task.to_dict()

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return deepcopy(task.to_dict()) if task else None

    def get_by_document(self, document_id: str) -> dict[str, Any] | None:
        with self._lock:
            task_id = self._tasks_by_document.get(document_id)
            if task_id:
                task = self._tasks.get(task_id)
                return deepcopy(task.to_dict()) if task else None
        return None

    def get_by_project(self, project_id: str) -> list[dict[str, Any]]:
        with self._lock:
            task_ids = self._tasks_by_project.get(project_id, set())
            return [deepcopy(self._tasks[tid].to_dict()) for tid in task_ids if tid in self._tasks]

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)

    def _run_task(self, task_id: str, content: bytes, mime_type: str | None) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        self._update_task(task, status=TaskStage.PARSING, progress_percent=5, started_at=utcnow().isoformat())
        try:
            self._update_task(task, status=TaskStage.EXTRACTING, progress_percent=15)
            result = self._extract_with_retry(task, content)
            if result is None:
                return
            self._update_task(task, status=TaskStage.CHUNKING, progress_percent=40)
            self._process_chunks(task, result)
            self._update_task(task, status=TaskStage.EMBEDDING, progress_percent=70)
            self._process_embeddings(task)
            self._update_task(task, status=TaskStage.STORING, progress_percent=90)
            self._finalize_document(task)
            self._update_task(task, status=TaskStage.COMPLETED, progress_percent=100, finished_at=utcnow().isoformat())
        except Exception as exc:
            self._handle_failure(task, exc)

    def _extract_with_retry(self, task: IngestTask, content: bytes) -> Any | None:
        last_error: Exception | None = None
        for attempt in range(self.MAX_RETRIES):
            try:
                return extract_text(task.filename, content)
            except ExtractionError as exc:
                last_error = exc
                if attempt < self.MAX_RETRIES - 1:
                    self._update_task(task, status=TaskStage.RETRYING, retry_count=attempt + 1)
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
        error_msg = f"Extraction failed after {self.MAX_RETRIES} attempts: {last_error}"
        self._update_task(task, status=TaskStage.FAILED, error=error_msg, finished_at=utcnow().isoformat())
        self._mark_document_failed(task, error_msg)
        return None

    def _process_chunks(self, task: IngestTask, extraction_result: Any) -> None:
        chunks = chunk_segments(extraction_result.segments)
        self._update_task(task, stages={"chunk_count": len(chunks)})
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.title = extraction_result.title or task.filename
                document.author_guess = extraction_result.author_guess
                document.created_at_guess = extraction_result.created_at_guess
                document.raw_text = extraction_result.raw_text
                document.clean_text = extraction_result.clean_text
                document.language = extraction_result.language
                document.metadata_json = extraction_result.metadata
                document.ingest_status = "ready"
                session.flush()
            repository.replace_document_chunks(
                session,
                document_id=task.document_id,
                chunks=[
                    {
                        "project_id": task.project_id,
                        "chunk_index": chunk.chunk_index,
                        "content": chunk.content,
                        "start_offset": chunk.start_offset,
                        "end_offset": chunk.end_offset,
                        "page_number": chunk.page_number,
                        "token_count": chunk.token_count,
                        "metadata_json": chunk.metadata,
                    }
                    for chunk in chunks
                ],
            )
            session.commit()

    def _process_embeddings(self, task: IngestTask) -> None:
        if not self._embedding_config:
            return
        client = OpenAICompatibleClient(self._embedding_config, log_path=self.llm_log_path)
        resolved_model = self._embedding_config.model or client.resolve_model()
        with self.db.session() as session:
            total = session.scalar(
                select(func.count()).select_from(TextChunk).where(TextChunk.document_id == task.document_id)
            ) or 0
            if total <= 0:
                return
            self._update_task(task, stages={"embedding_total": total})
            offset = 0
            processed = 0
            while offset < total:
                rows = session.execute(
                    select(TextChunk.id, TextChunk.content)
                    .where(TextChunk.document_id == task.document_id)
                    .order_by(TextChunk.chunk_index)
                    .offset(offset)
                    .limit(self.EMBEDDING_BATCH_SIZE)
                ).fetchall()
                if not rows:
                    break
                chunk_ids = [str(row.id) for row in rows]
                chunk_texts = [str(row.content or "") for row in rows]
                vectors = client.embeddings(chunk_texts, model=resolved_model)
                chunks = list(session.scalars(select(TextChunk).where(TextChunk.id.in_(chunk_ids))))
                chunk_map = {chunk.id: chunk for chunk in chunks}
                for chunk_id, vector in zip(chunk_ids, vectors):
                    chunk_obj = chunk_map.get(chunk_id)
                    if chunk_obj:
                        chunk_obj.embedding_vector = vector
                        chunk_obj.embedding_model = resolved_model
                        processed += 1
                session.flush()
                offset += len(rows)
                progress = 70 + int((offset / max(total, 1)) * 20)
                self._update_task(task, progress_percent=min(89, progress), stages={"embedding_processed": processed})
            session.commit()
        store = self.vector_store_manager.get_store(task.project_id)
        with self.db.session() as session:
            chunks = list(session.scalars(select(TextChunk).where(TextChunk.document_id == task.document_id)))
            chunk_ids = [c.id for c in chunks]
            vectors = [c.embedding_vector for c in chunks if c.embedding_vector]
            if vectors and chunk_ids:
                payloads = [{"content": c.content, "filename": task.filename, "chunk_index": c.chunk_index} for c in chunks]
                try:
                    store.add(ids=chunk_ids, vectors=vectors, payloads=payloads)
                    store.save()
                except Exception:
                    pass

    def _finalize_document(self, task: IngestTask) -> None:
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "ready"
                session.commit()

    def _mark_document_failed(self, task: IngestTask, error_message: str) -> None:
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "failed"
                document.error_message = error_message
                session.commit()

    def _handle_failure(self, task: IngestTask, exc: Exception) -> None:
        self._update_task(task, status=TaskStage.FAILED, error=str(exc), finished_at=utcnow().isoformat())
        self._mark_document_failed(task, str(exc))

    def _update_task(self, task: IngestTask, **fields: Any) -> None:
        with self._lock:
            for key, value in fields.items():
                if hasattr(task, key):
                    setattr(task, key, value)
