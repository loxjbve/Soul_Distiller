from __future__ import annotations

import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from app.config import AppConfig
from app.db import Database
from app.models import (
    AnalysisRun,
    DocumentRecord,
    GeneratedArtifact,
    SkillDraft,
    SkillVersion,
    TelegramPreprocessActiveUser,
    TelegramPreprocessRun,
    TelegramPreprocessTopic,
    TelegramPreprocessTopicParticipant,
    TextChunk,
    utcnow,
)
from app.pipeline.ingest_task import IngestTaskManager
from app.pipeline.rechunk import RechunkTaskManager
from app.preprocess.service import PreprocessAgentService
from app.retrieval.vector_store import VectorStoreManager
from app.storage import repository

logger = logging.getLogger(__name__)

ACTIVE_TASK_STATUSES = {"queued", "stopping_tasks", "deleting_db", "deleting_files"}


class ProjectDeletionManager:
    def __init__(
        self,
        *,
        db: Database,
        config: AppConfig,
        vector_store_manager: VectorStoreManager,
        ingest_task_manager: IngestTaskManager,
        rechunk_manager: RechunkTaskManager,
        analysis_runner,
        preprocess_service: PreprocessAgentService,
        telegram_preprocess_manager=None,
        max_workers: int = 1,
        batch_size: int = 1000,
        stop_timeout_s: float = 180.0,
    ) -> None:
        self.db = db
        self.config = config
        self.vector_store_manager = vector_store_manager
        self.ingest_task_manager = ingest_task_manager
        self.rechunk_manager = rechunk_manager
        self.analysis_runner = analysis_runner
        self.preprocess_service = preprocess_service
        self.telegram_preprocess_manager = telegram_preprocess_manager
        self.batch_size = max(1, batch_size)
        self.stop_timeout_s = max(1.0, stop_timeout_s)
        self.executor = ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="project-delete")
        self._tasks: dict[str, dict[str, Any]] = {}
        self._project_to_task: dict[str, str] = {}
        self._lock = Lock()

    def submit(self, root_project_id: str, *, project_ids: list[str] | None = None) -> dict[str, Any]:
        with self.db.session() as session:
            resolved_project_ids = list(project_ids or repository.get_project_tree_ids(session, root_project_id))
        if not resolved_project_ids:
            raise ValueError("Project tree is empty.")

        existing = self.get_by_project(root_project_id)
        if existing and existing.get("status") in ACTIVE_TASK_STATUSES:
            return existing

        task_id = str(uuid4())
        now = utcnow().isoformat()
        task = {
            "task_id": task_id,
            "root_project_id": root_project_id,
            "project_ids": resolved_project_ids,
            "status": "queued",
            "progress_percent": 0,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "error": None,
            "warnings": [],
        }
        with self._lock:
            self._tasks[task_id] = task
            for project_id in resolved_project_ids:
                self._project_to_task[project_id] = task_id
        self.executor.submit(self._run_task, task_id)
        return self.get(task_id) or task

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return deepcopy(task) if task else None

    def get_by_project(self, project_id: str) -> dict[str, Any] | None:
        with self._lock:
            task_id = self._project_to_task.get(project_id)
            task = self._tasks.get(task_id) if task_id else None
            return deepcopy(task) if task else None

    def resume_pending_deletions(self) -> None:
        with self.db.session() as session:
            deleting_projects = repository.list_projects_by_lifecycle(session, repository.PROJECT_LIFECYCLE_DELETING)
            deleting_ids = {project.id for project in deleting_projects}
            root_ids = [project.id for project in deleting_projects if project.parent_id not in deleting_ids]

        for root_project_id in root_ids:
            try:
                self.submit(root_project_id)
            except Exception:
                logger.exception("Failed to resume project deletion for %s", root_project_id)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)

    def _run_task(self, task_id: str) -> None:
        task = self.get(task_id)
        if not task:
            return
        project_ids = [str(item) for item in task.get("project_ids", [])]
        try:
            self._update_task(
                task_id,
                status="stopping_tasks",
                started_at=utcnow().isoformat(),
                progress_percent=5,
            )
            self._stop_project_activity(project_ids)
            self._wait_for_quiet(project_ids)

            for project_id in project_ids:
                self.vector_store_manager.delete_store(project_id)

            self._update_task(task_id, status="deleting_db", progress_percent=25)
            self._delete_database_rows(project_ids, task_id)

            self._update_task(task_id, status="deleting_files", progress_percent=92)
            warnings = self._delete_project_files(project_ids)
            if warnings:
                self._extend_warnings(task_id, warnings)

            self._update_task(
                task_id,
                status="completed",
                progress_percent=100,
                finished_at=utcnow().isoformat(),
            )
        except Exception as exc:
            error_text = str(exc).strip() or exc.__class__.__name__
            logger.exception("Project deletion failed for %s", task.get("root_project_id"))
            self._mark_delete_failed(project_ids, error_text)
            self._update_task(
                task_id,
                status="failed",
                error=error_text,
                finished_at=utcnow().isoformat(),
            )
        finally:
            for project_id in project_ids:
                try:
                    self.analysis_runner.clear_project_cancel(project_id)
                except Exception:
                    logger.exception("Failed to clear analysis cancel flag for %s", project_id)

    def _stop_project_activity(self, project_ids: list[str]) -> None:
        for project_id in project_ids:
            self.ingest_task_manager.stop_project_tasks(project_id, wait=False, reset_documents=True)
            self.rechunk_manager.cancel_project(project_id)
            self.analysis_runner.cancel_project(project_id)
            self.preprocess_service.cancel_project(project_id)
            if self.telegram_preprocess_manager:
                self.telegram_preprocess_manager.cancel_project(project_id)

    def _wait_for_quiet(self, project_ids: list[str]) -> None:
        deadline = time.time() + self.stop_timeout_s
        while time.time() < deadline:
            if not any(self._project_has_activity(project_id) for project_id in project_ids):
                return
            time.sleep(0.1)
        raise TimeoutError("Timed out while waiting for project tasks to stop.")

    def _project_has_activity(self, project_id: str) -> bool:
        return any(
            (
                self.ingest_task_manager.has_project_activity(project_id),
                self.rechunk_manager.has_project_activity(project_id),
                self.analysis_runner.has_project_activity(project_id),
                self.preprocess_service.has_project_activity(project_id),
                self.telegram_preprocess_manager.has_project_activity(project_id)
                if self.telegram_preprocess_manager
                else False,
            )
        )

    def _delete_database_rows(self, project_ids: list[str], task_id: str) -> None:
        ordered_project_ids = [str(item) for item in project_ids]
        reversed_project_ids = list(reversed(ordered_project_ids))
        total_steps = 10
        completed_steps = 0

        def advance_progress() -> None:
            progress = 25 + int((completed_steps / total_steps) * 60)
            self._update_task(task_id, progress_percent=min(progress, 90))

        self._delete_telegram_preprocess_tree(ordered_project_ids)
        completed_steps += 1
        advance_progress()

        self._delete_analysis_tree(ordered_project_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_model_rows(ordered_project_ids, GeneratedArtifact, repository.delete_generated_artifacts_by_ids)
        completed_steps += 1
        advance_progress()

        self._delete_chat_tree(ordered_project_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_model_rows(ordered_project_ids, SkillVersion, repository.delete_skill_versions_by_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_model_rows(ordered_project_ids, SkillDraft, repository.delete_skill_drafts_by_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_model_rows(ordered_project_ids, TextChunk, repository.delete_text_chunks_by_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_model_rows(ordered_project_ids, DocumentRecord, repository.delete_documents_by_ids)
        completed_steps += 1
        advance_progress()

        self._delete_project_runs_only(ordered_project_ids)
        completed_steps += 1
        advance_progress()

        self._delete_projects(reversed_project_ids)
        completed_steps += 1
        advance_progress()

    def _delete_analysis_tree(self, project_ids: list[str]) -> None:
        with self.db.session() as session:
            run_ids = [str(item) for item in session.scalars(select(AnalysisRun.id).where(AnalysisRun.project_id.in_(project_ids))).all()]
        if not run_ids:
            return
        for index in range(0, len(run_ids), self.batch_size):
            run_id_batch = run_ids[index:index + self.batch_size]
            while True:
                with self.db.session() as session:
                    facet_ids = repository.list_analysis_facet_ids_for_run_ids(session, run_id_batch, limit=self.batch_size)
                    if not facet_ids:
                        break
                    repository.delete_analysis_facets_by_ids(session, facet_ids)
                    session.commit()
            while True:
                with self.db.session() as session:
                    event_ids = repository.list_analysis_event_ids_for_run_ids(session, run_id_batch, limit=self.batch_size)
                    if not event_ids:
                        break
                    repository.delete_analysis_events_by_ids(session, event_ids)
                    session.commit()

    def _delete_telegram_preprocess_tree(self, project_ids: list[str]) -> None:
        with self.db.session() as session:
            run_ids = [
                str(item)
                for item in session.scalars(select(TelegramPreprocessRun.id).where(TelegramPreprocessRun.project_id.in_(project_ids))).all()
            ]
        if not run_ids:
            return
        for index in range(0, len(run_ids), self.batch_size):
            run_id_batch = run_ids[index:index + self.batch_size]
            with self.db.session() as session:
                topic_ids = [
                    str(item)
                    for item in session.scalars(select(TelegramPreprocessTopic.id).where(TelegramPreprocessTopic.run_id.in_(run_id_batch))).all()
                ]
                if topic_ids:
                    session.execute(delete(TelegramPreprocessTopicParticipant).where(TelegramPreprocessTopicParticipant.topic_id.in_(topic_ids)))
                    session.commit()
                session.execute(delete(TelegramPreprocessActiveUser).where(TelegramPreprocessActiveUser.run_id.in_(run_id_batch)))
                session.execute(delete(TelegramPreprocessTopic).where(TelegramPreprocessTopic.run_id.in_(run_id_batch)))
                session.execute(delete(TelegramPreprocessRun).where(TelegramPreprocessRun.id.in_(run_id_batch)))
                session.commit()

    def _delete_chat_tree(self, project_ids: list[str]) -> None:
        while True:
            with self.db.session() as session:
                session_ids = repository.list_chat_session_ids_for_projects(session, project_ids, limit=self.batch_size)
                if not session_ids:
                    return
                while True:
                    turn_ids = repository.list_chat_turn_ids_for_session_ids(session, session_ids, limit=self.batch_size)
                    if not turn_ids:
                        break
                    repository.delete_chat_turns_by_ids(session, turn_ids)
                    session.commit()
                repository.delete_chat_sessions_by_ids(session, session_ids)
                session.commit()

    def _delete_project_runs_only(self, project_ids: list[str]) -> None:
        while True:
            with self.db.session() as session:
                run_ids = repository.list_analysis_run_ids_for_projects(session, project_ids, limit=self.batch_size)
                if not run_ids:
                    return
                repository.delete_analysis_runs_by_ids(session, run_ids)
                session.commit()

    def _delete_project_model_rows(self, project_ids: list[str], model, delete_by_ids) -> None:
        while True:
            with self.db.session() as session:
                ids = repository.list_project_model_ids(session, model, project_ids, limit=self.batch_size)
                if not ids:
                    return
                delete_by_ids(session, ids)
                session.commit()

    def _delete_projects(self, project_ids: list[str]) -> None:
        for index in range(0, len(project_ids), self.batch_size):
            batch_ids = project_ids[index:index + self.batch_size]
            with self.db.session() as session:
                repository.delete_projects_by_ids(session, batch_ids)
                session.commit()

    def _delete_project_files(self, project_ids: list[str]) -> list[str]:
        warnings: list[str] = []
        for project_id in project_ids:
            for directory in self._project_directories(project_id):
                try:
                    if directory.exists():
                        shutil.rmtree(directory, ignore_errors=False)
                except Exception as exc:
                    warning = f"Failed to delete {directory}: {exc}"
                    warnings.append(warning)
                    logger.warning(warning)
        return warnings

    def _project_directories(self, project_id: str) -> tuple[Path, ...]:
        return (
            self.config.upload_dir / project_id,
            self.config.assets_dir / project_id,
            self.config.output_dir / project_id,
            self.config.skill_dir / project_id,
            self.config.data_dir / "vectors" / project_id,
        )

    def _mark_delete_failed(self, project_ids: list[str], error: str) -> None:
        with self.db.session() as session:
            existing_ids = [project_id for project_id in project_ids if repository.get_project(session, project_id)]
            if not existing_ids:
                return
            repository.mark_projects_delete_failed(session, existing_ids, error=error)
            session.commit()

    def _update_task(self, task_id: str, **fields: Any) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            for key, value in fields.items():
                task[key] = value

    def _extend_warnings(self, task_id: str, warnings: list[str]) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            current = list(task.get("warnings") or [])
            current.extend(warnings)
            task["warnings"] = current
