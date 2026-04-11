from __future__ import annotations

import json
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from app.analysis.engine import AnalysisEngine
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

    def submit(self, run_id: str) -> None:
        if self.run_inline:
            self._execute(run_id)
            return
        future = self.executor.submit(self._execute, run_id)
        self.futures[run_id] = future

    def submit_facet_rerun(self, project_id: str, facet_key: str) -> None:
        future_key = f"facet:{project_id}:{facet_key}"
        if self.run_inline:
            self._execute_facet_rerun(project_id, facet_key)
            return
        future = self.executor.submit(self._execute_facet_rerun, project_id, facet_key)
        self.futures[future_key] = future

    def _execute(self, run_id: str) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self.engine.execute_run(session, run_id)
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
            self.futures.pop(run_id, None)

    def _execute_facet_rerun(self, project_id: str, facet_key: str) -> None:
        future_key = f"facet:{project_id}:{facet_key}"
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self.engine.rerun_facet(session, project_id, facet_key)
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
            self.futures.pop(future_key, None)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def is_tracking(self, run_id: str) -> bool:
        future = self.futures.get(run_id)
        return future is not None and not future.done()

    def _append_error_log(self, record: dict[str, object]) -> None:
        if not self.error_log_path:
            return
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"timestamp": utcnow().isoformat(), **record}
        with _RUNNER_LOG_LOCK:
            with self.error_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
