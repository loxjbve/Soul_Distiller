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
from app.pipeline.extractors import ExtractionError, extract_text
from app.retrieval.vector_store import VectorStoreBatch, VectorStoreManager
from app.schemas import ServiceConfig
from app.storage import repository
from app.telegram_support import parse_telegram_export

import json

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

    def stop_project_tasks(
        self,
        project_id: str,
        *,
        wait: bool = False,
        timeout_s: float = 30.0,
        reset_documents: bool = True,
    ) -> bool:
        with self._lock:
            task_ids = list(self._tasks_by_project.get(project_id, set()))
        for task_id in task_ids:
            with self._lock:
                task = self._tasks.get(task_id)
                future = self._futures.get(task_id)
            if not task or task.status in (TaskStage.COMPLETED, TaskStage.FAILED):
                continue
            task.is_cancelled = True
            cancelled_before_start = bool(future and future.cancel())
            if cancelled_before_start:
                self._discard_task(task)
            if reset_documents:
                with self.db.session() as session:
                    doc = repository.get_document(session, task.document_id)
                    if doc and doc.ingest_status in ("pending", "processing", "queued"):
                        doc.ingest_status = "pending"
                        session.commit()
        if wait:
            deadline = time.time() + max(timeout_s, 0.0)
            while self.has_project_activity(project_id):
                if time.time() >= deadline:
                    return False
                time.sleep(0.05)
        return not self.has_project_activity(project_id)

    def has_project_activity(self, project_id: str) -> bool:
        with self._lock:
            task_ids = list(self._tasks_by_project.get(project_id, set()))
            if task_ids:
                return True
            return any(
                task.project_id == project_id and task.status not in (TaskStage.COMPLETED, TaskStage.FAILED)
                for task in self._tasks.values()
            )

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
            if task and task.is_cancelled:
                self._discard_task(task)
            return
        try:
            with open(storage_path, "rb") as f:
                content = f.read()
        except Exception as exc:
            self._mark_document_failed(task, f"Failed to read file: {exc}")
            return
        self._update_task(task, status=TaskStage.PARSING, progress_percent=5, started_at=utcnow().isoformat())
        try:
            with self.db.session() as session:
                project = repository.get_project(session, task.project_id)
            if project and project.mode == "telegram":
                self._process_telegram_document(task, content)
                self._update_task(task, status=TaskStage.STORING, progress_percent=90)
                if task.is_cancelled: return
                self._finalize_document(task)
                self._update_task(task, status=TaskStage.COMPLETED, progress_percent=100, finished_at=utcnow().isoformat())
                return
            if project and project.mode == "stone":
                self._process_stone_document(task, content)
                self._update_task(task, status=TaskStage.STORING, progress_percent=90)
                if task.is_cancelled: return
                self._finalize_document(task)
                self._update_task(task, status=TaskStage.COMPLETED, progress_percent=100, finished_at=utcnow().isoformat())
                return
            if task.is_cancelled: return
            result = extract_text(task.filename, content)
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
        except ExtractionError as exc:
            self._mark_document_failed(task, str(exc))
        except Exception as exc:
            self._handle_failure(task, exc)
        finally:
            if task.is_cancelled:
                self._discard_task(task)

    def _process_telegram_document(self, task: IngestTask, content: bytes) -> None:
        try:
            payload = json.loads(content.decode("utf-8-sig"))
        except UnicodeDecodeError:
            payload = json.loads(content.decode("utf-8"))
        bundle = parse_telegram_export(payload)
        self._update_task(
            task,
            status=TaskStage.CHUNKING,
            progress_percent=45,
            stages={
                "telegram_message_count": len(bundle.messages),
                "telegram_participant_count": len(bundle.participants),
            },
        )
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.title = bundle.chat.get("title") or task.filename
                document.author_guess = None
                document.created_at_guess = None
                document.raw_text = bundle.preview_text
                document.clean_text = bundle.preview_text
                document.language = "unknown"
                document.source_type = "telegram_export"
                document.metadata_json = {
                    **bundle.metadata,
                    "telegram_chat": bundle.chat,
                }
                document.ingest_status = "processing"
                session.flush()
            repository.replace_document_chunks(session, document_id=task.document_id, chunks=[])
            repository.replace_document_telegram_export(
                session,
                project_id=task.project_id,
                document_id=task.document_id,
                chat_payload=bundle.chat,
                participants=bundle.participants,
                messages=bundle.messages,
            )
            session.commit()

    def _process_stone_document(self, task: IngestTask, content: bytes) -> None:
        result = extract_text(task.filename, content)
        self._update_task(
            task,
            status=TaskStage.CHUNKING,
            progress_percent=45,
            stages={
                "chunk_count": 0,
                "stone_direct_text": True,
                "paragraph_count": int((result.metadata or {}).get("paragraph_count") or len(result.segments or [])),
            },
        )
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                preserved_metadata = dict(document.metadata_json or {})
                document.title = document.title or result.title or None
                document.author_guess = result.author_guess
                document.created_at_guess = result.created_at_guess
                document.raw_text = result.raw_text
                document.clean_text = result.clean_text
                document.language = result.language
                metadata = dict(result.metadata or {})
                for key in ("user_note", "stone_text_entry", "stone_json_import"):
                    if key in preserved_metadata:
                        metadata[key] = preserved_metadata[key]
                document.metadata_json = metadata
                document.ingest_status = "processing"
                session.flush()
            repository.replace_document_chunks(session, document_id=task.document_id, chunks=[])
            session.commit()

    def _process_chunks(self, task: IngestTask, extraction_result: Any) -> None:
        chunks = chunk_segments(extraction_result.segments)
        self._update_task(task, stages={"chunk_count": len(chunks)})
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                preserved_metadata = dict(document.metadata_json or {})
                document.title = extraction_result.title or task.filename
                document.author_guess = extraction_result.author_guess
                document.created_at_guess = extraction_result.created_at_guess
                document.raw_text = extraction_result.raw_text
                document.clean_text = extraction_result.clean_text
                document.language = extraction_result.language
                metadata = dict(extraction_result.metadata or {})
                for key in ("user_note", "stone_text_entry"):
                    if key in preserved_metadata:
                        metadata[key] = preserved_metadata[key]
                document.metadata_json = metadata
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
            self._update_task(task, stages={"embedding_total": total, "embedding_model": resolved_model})

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
                new_progress = 70 + int((processed_count / max(total, 1)) * 20)
                self._update_task(task, stages={"embedding_processed": processed_count}, progress_percent=new_progress)

    def _store_to_vector_db(self, task: IngestTask) -> None:
        self._update_task(task, stages={"vector_store_sync": "pending"})

    def _finalize_document(self, task: IngestTask) -> None:
        should_flush_project = self._discard_task(task)
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "ready"
                session.commit()
        if should_flush_project:
            self._sync_project_vector_store(task.project_id)

    def _mark_document_failed(self, task: IngestTask, error_message: str) -> None:
        should_flush_project = self._discard_task(task)
        with self.db.session() as session:
            document = repository.get_document(session, task.document_id)
            if document:
                document.ingest_status = "failed"
                document.error_message = error_message
                session.commit()
        if should_flush_project and not task.is_cancelled:
            try:
                self._sync_project_vector_store(task.project_id)
            except Exception:
                import logging

                logging.getLogger(__name__).exception(
                    "Failed to sync vector store for project %s after document failure.",
                    task.project_id,
                )

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
                if key == "stages" and isinstance(value, dict):
                    task.stages.update(value)
                elif hasattr(task, key):
                    setattr(task, key, value)

    def _discard_task(self, task: IngestTask) -> bool:
        with self._lock:
            self._tasks.pop(task.task_id, None)
            self._futures.pop(task.task_id, None)
            if self._tasks_by_document.get(task.document_id) == task.task_id:
                self._tasks_by_document.pop(task.document_id, None)
            project_tasks = self._tasks_by_project.get(task.project_id)
            if project_tasks and task.task_id in project_tasks:
                project_tasks.remove(task.task_id)
                if not project_tasks:
                    self._tasks_by_project.pop(task.project_id, None)
                    return True
            return not bool(self._tasks_by_project.get(task.project_id))

    def _sync_project_vector_store(self, project_id: str) -> None:
        batches_by_model: dict[str, VectorStoreBatch] = {}
        with self.db.session() as session:
            stmt = (
                select(
                    TextChunk.id.label("chunk_id"),
                    TextChunk.embedding_vector.label("embedding_vector"),
                    TextChunk.embedding_model.label("embedding_model"),
                )
                .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
                .where(
                    TextChunk.project_id == project_id,
                    DocumentRecord.ingest_status == "ready",
                    TextChunk.embedding_vector.is_not(None),
                    TextChunk.embedding_model.is_not(None),
                )
                .order_by(TextChunk.embedding_model.asc(), TextChunk.document_id.asc(), TextChunk.chunk_index.asc())
            )
            rows = session.execute(stmt).all()
        for row in rows:
            model = str(row.embedding_model or "").strip()
            vector = row.embedding_vector
            if not model or not isinstance(vector, (list, tuple)):
                continue
            batch = batches_by_model.setdefault(model, VectorStoreBatch(ids=[], vectors=[]))
            batch.ids.append(str(row.chunk_id))
            batch.vectors.append(list(vector))
        self.vector_store_manager.rebuild_project(project_id, batches_by_model)
