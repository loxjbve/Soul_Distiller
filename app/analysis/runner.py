from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

from app.analysis.engine import AnalysisEngine
from app.db import Database
from app.models import utcnow
from app.storage import repository


class AnalysisTaskRunner:
    def __init__(
        self,
        db: Database,
        engine: AnalysisEngine,
        *,
        max_workers: int = 4,
        run_inline: bool = False,
    ) -> None:
        self.db = db
        self.engine = engine
        self.run_inline = run_inline
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
            with self.db.session() as session:
                self.engine.execute_run(session, run_id)
        except Exception as exc:
            with self.db.session() as session:
                run = repository.get_analysis_run(session, run_id)
                if run:
                    summary = dict(run.summary_json or {})
                    summary["current_stage"] = "后台任务异常结束"
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
                    )
            raise
        finally:
            self.futures.pop(run_id, None)

    def _execute_facet_rerun(self, project_id: str, facet_key: str) -> None:
        future_key = f"facet:{project_id}:{facet_key}"
        try:
            with self.db.session() as session:
                self.engine.rerun_facet(session, project_id, facet_key)
        except Exception as exc:
            with self.db.session() as session:
                run = repository.get_latest_analysis_run(session, project_id)
                if run:
                    repository.add_analysis_event(
                        session,
                        run.id,
                        event_type="facet",
                        level="error",
                        message=f"Facet rerun crashed: {exc}",
                        payload_json={"facet_key": facet_key},
                    )
            raise
        finally:
            self.futures.pop(future_key, None)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
