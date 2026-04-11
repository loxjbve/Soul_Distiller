from __future__ import annotations

import asyncio
import concurrent.futures
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
from app.retrieval.vector_store import VectorStoreManager
from app.schemas import ServiceConfig, ExtractionResult, ExtractedSegment
from app.storage import repository

EMBEDDING_DIMENSION_DEFAULT = 1024


class TaskStage(str, Enum):
    QUEUED = "queued"
    PARSING = "parsing"
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
    is_cancelled: bool = False

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
    EMBEDDING_BATCH_SIZE = 16
    EMBEDDING_CONCURRENCY = 4

    def __init__(
        self,
        db: Database,
        vector_store_manager: VectorStoreManager,
        *,
        max_workers: int = 16,
        llm_log_path: str | None = None,
    ) -> None:
        self.db = db
        self.vector_store_manager = vector_store_manager
        self.executor = ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="ingest")
        self._embedding_executor = ThreadPoolExecutor(max_workers=self.EMBEDDING_CONCURRENCY, thread_name_prefix="embedding")
        self._tasks: dict[str, IngestTask] = {}
        self._tasks_by_project: dict[str, set[str]] = {}
        self._tasks_by_document: dict[str, str] = {}
        self._futures = {}
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
        storage_path: str,
        mime_type: str | None = None,
    ) -> dict[str, Any]:
        task_id = str(uuid4())
        with self._lock:
            existing = self._tasks_by_document.get(document_id)
            if existing and existing in self._tasks:
                task = self._tasks[existing]
                if task.status in (TaskStage.QUEUED, TaskStage.PARSING, TaskStage.CHUNKING, TaskStage.EMBEDDING, TaskStage.STORING):
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
        future = self.executor.submit(self._run_task, task_id, storage_path, mime_type)
        self._futures[task_id] = future
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

    def stop_project_tasks(self, project_id: str) -> None:
        with self._lock:
            task_ids = list(self._tasks_by_project.get(project_id, set()))
        for task_id in task_ids:
            with self._lock:
                task = self._tasks.get(task_id)
                future = self._futures.get(task_id)
            if not task or task.status in (TaskStage.COMPLETED, TaskStage.FAILED):
                continue
            task.is_cancelled = True
            if future:
                future.cancel()
            
            with self.db.session() as session:
                doc = repository.get_document(session, task.document_id)
                if doc and doc.ingest_status in ("pending", "processing", "queued"):
                    doc.ingest_status = "pending"
                    session.commit()
            
            with self._lock:
                if task_id in self._tasks:
                    del self._tasks[task_id]
                if task_id in self._futures:
                    del self._futures[task_id]
                if task.document_id in self._tasks_by_document:
                    del self._tasks_by_document[task.document_id]
                if project_id in self._tasks_by_project and task_id in self._tasks_by_project[project_id]:
                    self._tasks_by_project[project_id].remove(task_id)

    def get_by_project(self, project_id: str) -> list[dict[str, Any]]:
        with self._lock:
            task_ids = self._tasks_by_project.get(project_id, set())
            return [deepcopy(self._tasks[tid].to_dict()) for tid in task_ids if tid in self._tasks]

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
        self._embedding_executor.shutdown(wait=False, cancel_futures=True)

    def _run_task(self, task_id: str, storage_path: str, mime_type: str | None) -> None:
        task = self._tasks.get(task_id)
        if not task or task.is_cancelled:
            return
        try:
            with open(storage_path, "rb") as f:
                content = f.read()
        except Exception as exc:
            self._mark_document_failed(task, f"Failed to read file: {exc}")
            return
        self._update_task(task, status=TaskStage.PARSING, progress_percent=5, started_at=utcnow().isoformat())
        try:
            if task.is_cancelled: return
            text = content.decode("utf-8", errors="ignore")
            result = ExtractionResult(
                raw_text=text,
                clean_text=text,
                title=task.filename,
                author_guess=None,
                created_at_guess=None,
                language="unknown",
                metadata={"format": "raw_text"},
                segments=[ExtractedSegment(text=text, metadata={})]
            )
            self._update_task(task, status=TaskStage.CHUNKING, progress_percent=40)
            if task.is_cancelled: return
            self._process_chunks(task, result)
            self._update_task(task, status=TaskStage.EMBEDDING, progress_percent=70)
            if task.is_cancelled: return
            self._process_embeddings_concurrent(task)
            self._update_task(task, status=TaskStage.STORING, progress_percent=90)
            if task.is_cancelled: return
            self._store_to_vector_db(task)
            self._finalize_document(task)
            self._update_task(task, status=TaskStage.COMPLETED, progress_percent=100, finished_at=utcnow().isoformat())
        except Exception as exc:
            self._handle_failure(task, exc)

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
                document.ingest_status = "processing"
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

    def _process_embeddings_concurrent(self, task: IngestTask) -> None:
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

            query = select(TextChunk.id, TextChunk.content).where(TextChunk.document_id == task.document_id).order_by(TextChunk.chunk_index)
            # Load all chunks at once; 13MB is ~40MB in RAM which is perfectly safe
            rows = session.execute(query).fetchall()

            batch_ids = []
            batch_texts = []
            for i in range(0, len(rows), self.EMBEDDING_BATCH_SIZE):
                batch = rows[i:i + self.EMBEDDING_BATCH_SIZE]
                batch_ids.append([str(row.id) for row in batch])
                batch_texts.append([str(row.content or "") for row in batch])

            def _fetch_batch(texts: list[str]) -> list[list[float]]:
                if not texts or task.is_cancelled:
                    return []
                import time
                for attempt in range(3):
                    if task.is_cancelled:
                        return []
                    try:
                        return client.embeddings(texts, model=resolved_model, timeout=180.0)
                    except Exception as exc:
                        if attempt == 2 or task.is_cancelled:
                            raise
                        time.sleep(2.0 * (attempt + 1))
                return []

            from concurrent.futures import as_completed
            futures = []
            for texts, ids in zip(batch_texts, batch_ids):
                if texts:
                    futures.append(self._embedding_executor.submit(_fetch_batch, texts))

            future_to_ids = {future: ids for future, ids in zip(futures, batch_ids)}
            processed_count = 0

            for future in as_completed(futures):
                if task.is_cancelled: return
                ids = future_to_ids[future]
                vectors = future.result()
                
                mappings = []
                for idx, id_ in enumerate(ids):
                    if idx < len(vectors):
                        mappings.append({
                            "id": id_,
                            "embedding_vector": vectors[idx],
                            "embedding_model": resolved_model
                        })
                
                if mappings:
                    with self.db.session() as update_session:
                        update_session.bulk_update_mappings(TextChunk, mappings)
                        update_session.commit()
                
                processed_count += len(ids)
                self._update_task(task, stages={"embedding_processed": processed_count})

    def _store_to_vector_db(self, task: IngestTask) -> None:
        store = self.vector_store_manager.get_store(task.project_id)
        with self.db.session() as session:
            chunks = list(session.scalars(
                select(TextChunk).where(TextChunk.document_id == task.document_id)
            ))
            # Only use chunks that have valid embeddings
            valid_chunks = [c for c in chunks if c.embedding_vector]
            chunk_ids = [str(c.id) for c in valid_chunks]
            vectors = [c.embedding_vector for c in valid_chunks]
            if vectors and chunk_ids:
                payloads = [
                    {"content": c.content, "filename": task.filename, "chunk_index": c.chunk_index}
                    for c in valid_chunks
                ]
                # Insert to vector db in safe batches to prevent payload limits / OOM
                batch_size = 1000
                for i in range(0, len(chunk_ids), batch_size):
                    store.add(
                        ids=chunk_ids[i:i + batch_size],
                        vectors=vectors[i:i + batch_size],
                        payloads=payloads[i:i + batch_size]
                    )
                store.save()

    def _finalize_document(self, task: IngestTask) -> None:
        with self._lock:
            if task.task_id in self._tasks: del self._tasks[task.task_id]
            if task.task_id in self._futures: del self._futures[task.task_id]
            if task.document_id in self._tasks_by_document: del self._tasks_by_document[task.document_id]
            if task.project_id in self._tasks_by_project and task.task_id in self._tasks_by_project[task.project_id]:
                self._tasks_by_project[task.project_id].remove(task.task_id)
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "ready"
                session.commit()

    def _mark_document_failed(self, task: IngestTask, error_message: str) -> None:
        with self._lock:
            if task.task_id in self._tasks: del self._tasks[task.task_id]
            if task.task_id in self._futures: del self._futures[task.task_id]
            if task.document_id in self._tasks_by_document: del self._tasks_by_document[task.document_id]
            if task.project_id in self._tasks_by_project and task.task_id in self._tasks_by_project[task.project_id]:
                self._tasks_by_project[task.project_id].remove(task.task_id)
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "failed"
                document.error_message = error_message
                session.commit()

    def _handle_failure(self, task: IngestTask, exc: Exception) -> None:
        import traceback
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"IngestTask {task.task_id} for doc {task.document_id} failed: {exc}")
        logger.error(traceback.format_exc())
        
        task.is_cancelled = True
        self._update_task(task, status=TaskStage.FAILED, error=str(exc), finished_at=utcnow().isoformat())
        self._mark_document_failed(task, str(exc))

    def _update_task(self, task: IngestTask, **fields: Any) -> None:
        with self._lock:
            for key, value in fields.items():
                if hasattr(task, key):
                    setattr(task, key, value)
