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

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
