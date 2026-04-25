from __future__ import annotations

import json
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from app.service.common.pipeline import AnalysisCancelledError, AnalysisEngine
from app.db import Database
from app.models import utcnow
from app.runtime_limits import background_task_slot
from app.storage import repository

_RUNNER_LOG_LOCK = Lock()


class AnalysisTaskRunner:
    def __init__(
        self,
        db: Database,
        engine: AnalysisEngine,
        *,
        max_workers: int = 4,
        run_inline: bool = False,
        error_log_path: str | None = None,
    ) -> None:
        self.db = db
        self.engine = engine
        self.run_inline = run_inline
        self.error_log_path = Path(error_log_path) if error_log_path else None
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="analysis-runner")
        self.futures: dict[str, Future[None]] = {}
        self._project_by_future: dict[str, str] = {}
        self._cancelled_projects: set[str] = set()
        self._lock = Lock()
        self.engine.set_cancel_checker(self._is_cancelled)

    def submit(self, run_id: str) -> None:
        with self.db.session() as session:
            run = repository.get_analysis_run(session, run_id)
        if not run:
            raise ValueError("Analysis run not found.")
        project_id = run.project_id
        if self.run_inline:
            self._execute(run_id)
            return
        future = self.executor.submit(self._execute, run_id)
        with self._lock:
            self.futures[run_id] = future
            self._project_by_future[run_id] = project_id

    def submit_facet_rerun(self, project_id: str, facet_key: str) -> None:
        future_key = f"facet:{project_id}:{facet_key}"
        if self.run_inline:
            self._execute_facet_rerun(project_id, facet_key)
            return
        future = self.executor.submit(self._execute_facet_rerun, project_id, facet_key)
        with self._lock:
            self.futures[future_key] = future
            self._project_by_future[future_key] = project_id

    def _execute(self, run_id: str) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self.engine.execute_run(session, run_id)
        except AnalysisCancelledError:
            with self.db.session() as session:
                run = repository.get_analysis_run(session, run_id)
                if run:
                    summary = dict(run.summary_json or {})
                    summary["current_stage"] = "Analysis cancelled while project deletion was in progress."
                    summary["current_phase"] = "failed"
                    summary["current_facet"] = None
                    summary["finished_at"] = utcnow().isoformat()
                    run.summary_json = summary
                    run.status = "failed"
                    run.finished_at = utcnow()
                    repository.add_analysis_event(
                        session,
                        run_id,
                        event_type="lifecycle",
                        level="warning",
                        message="Analysis cancelled because the project is being deleted.",
                        payload_json={"run_id": run_id, "cancelled_for_project_deletion": True},
                    )
        except Exception as exc:
            error_traceback = traceback.format_exc()
            self._append_error_log(
                {
                    "kind": "analysis_run",
                    "run_id": run_id,
                    "error": str(exc),
                    "traceback": error_traceback,
                }
            )
            with self.db.session() as session:
                run = repository.get_analysis_run(session, run_id)
                if run:
                    summary = dict(run.summary_json or {})
                    summary["current_stage"] = "后台任务异常终止"
                    summary["finished_at"] = utcnow().isoformat()
                    run.summary_json = summary
                    run.status = "failed"
                    run.finished_at = utcnow()
                    repository.add_analysis_event(
                        session,
                        run_id,
                        event_type="lifecycle",
                        level="error",
                        message=f"后台任务异常终止：{exc}",
                        payload_json={
                            "run_id": run_id,
                            "error": str(exc),
                            "traceback": error_traceback,
                            "log_path": str(self.error_log_path) if self.error_log_path else None,
                        },
                    )
            raise
        finally:
            self._finish_future(run_id)

    def _execute_facet_rerun(self, project_id: str, facet_key: str) -> None:
        future_key = f"facet:{project_id}:{facet_key}"
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self.engine.rerun_facet(session, project_id, facet_key)
        except AnalysisCancelledError:
            with self.db.session() as session:
                run = repository.get_latest_analysis_run(session, project_id)
                if run:
                    repository.add_analysis_event(
                        session,
                        run.id,
                        event_type="facet",
                        level="warning",
                        message=f"Facet rerun cancelled because project {project_id} is being deleted.",
                        payload_json={
                            "facet_key": facet_key,
                            "cancelled_for_project_deletion": True,
                        },
                    )
        except Exception as exc:
            error_traceback = traceback.format_exc()
            self._append_error_log(
                {
                    "kind": "facet_rerun",
                    "project_id": project_id,
                    "facet_key": facet_key,
                    "error": str(exc),
                    "traceback": error_traceback,
                }
            )
            with self.db.session() as session:
                run = repository.get_latest_analysis_run(session, project_id)
                if run:
                    repository.add_analysis_event(
                        session,
                        run.id,
                        event_type="facet",
                        level="error",
                        message=f"Facet rerun crashed: {exc}",
                        payload_json={
                            "facet_key": facet_key,
                            "error": str(exc),
                            "traceback": error_traceback,
                            "log_path": str(self.error_log_path) if self.error_log_path else None,
                        },
                    )
            raise
        finally:
            self._finish_future(future_key)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def is_tracking(self, run_id: str) -> bool:
        with self._lock:
            future = self.futures.get(run_id)
        return future is not None and not future.done()

    def cancel_project(self, project_id: str) -> bool:
        with self._lock:
            self._cancelled_projects.add(project_id)
            future_items = [
                (future_key, self.futures.get(future_key))
                for future_key, mapped_project_id in self._project_by_future.items()
                if mapped_project_id == project_id
            ]
        all_cancelled = True
        for future_key, future in future_items:
            if future is None:
                continue
            if not future.cancel():
                all_cancelled = False
                continue
            self._finish_future(future_key)
        return all_cancelled or not self.has_project_activity(project_id)

    def has_project_activity(self, project_id: str) -> bool:
        with self._lock:
            for future_key, mapped_project_id in self._project_by_future.items():
                if mapped_project_id != project_id:
                    continue
                future = self.futures.get(future_key)
                if future is not None and not future.done():
                    return True
            return False

    def clear_project_cancel(self, project_id: str) -> None:
        if self.has_project_activity(project_id):
            return
        with self._lock:
            self._cancelled_projects.discard(project_id)

    def _append_error_log(self, record: dict[str, object]) -> None:
        if not self.error_log_path:
            return
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"timestamp": utcnow().isoformat(), **record}
        with _RUNNER_LOG_LOCK:
            with self.error_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _finish_future(self, future_key: str) -> None:
        project_id: str | None
        with self._lock:
            self.futures.pop(future_key, None)
            project_id = self._project_by_future.pop(future_key, None)
            if project_id is None:
                return
            if any(
                mapped_project_id == project_id and self.futures.get(other_key) is not None and not self.futures[other_key].done()
                for other_key, mapped_project_id in self._project_by_future.items()
            ):
                return
            self._cancelled_projects.discard(project_id)

    def _is_cancelled(self, run_id: str, project_id: str) -> bool:
        del run_id
        with self._lock:
            return project_id in self._cancelled_projects
