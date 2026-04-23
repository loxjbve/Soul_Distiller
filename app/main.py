from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from app.analysis.engine import AnalysisEngine
from app.analysis.runner import AnalysisTaskRunner
from app.analysis.streaming import AnalysisStreamHub
from app.analysis.synthesizer import AssetSynthesizer
from app.config import AppConfig, default_config
from app.db import Database
from app.models import utcnow
from app.pipeline.ingest import DocumentIngestService
from app.pipeline.ingest_task import IngestTaskManager
from app.pipeline.project_deletion import ProjectDeletionManager
from app.pipeline.rechunk import RechunkTaskManager
from app.preprocess.service import PreprocessAgentService
from app.retrieval.service import RetrievalService
from app.retrieval.vector_store import VectorStoreManager
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.stone_preprocess import StonePreprocessStreamHub, StonePreprocessWorker
from app.storage import repository
from app.telegram_preprocess import TelegramPreprocessManager
from app.web.routes import router
from app.writing.service import WritingAgentService


def _recover_interrupted_analysis_runs(database: Database) -> None:
    with database.session() as session:
        active_runs = repository.list_active_analysis_runs(session)
        for run in active_runs:
            summary = dict(run.summary_json or {})
            summary["current_stage"] = "服务重启，旧的后台任务已终止"
            summary["current_facet"] = None
            summary["finished_at"] = utcnow().isoformat()
            run.summary_json = summary
            run.status = "failed"
            run.finished_at = utcnow()
            repository.add_analysis_event(
                session,
                run.id,
                event_type="lifecycle",
                level="warning",
                message="检测到服务重启，未完成的分析任务已被标记为失败。",
                payload_json={"recovered_after_restart": True},
            )


def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or default_config()
    config.ensure_dirs()
    database = Database(config)
    database.create_all()

    vector_store_manager = VectorStoreManager(config.data_dir)
    retrieval = RetrievalService(vector_store=vector_store_manager)
    analysis_stream_hub = AnalysisStreamHub()
    telegram_preprocess_stream_hub = AnalysisStreamHub()
    stone_preprocess_stream_hub = StonePreprocessStreamHub()
    analysis_engine = AnalysisEngine(
        retrieval,
        db=database,
        llm_log_path=str(config.llm_log_path),
        use_processes=False,
        facet_max_workers=DEFAULT_ANALYSIS_CONCURRENCY,
        stream_hub=analysis_stream_hub,
    )
    analysis_runner = AnalysisTaskRunner(
        database,
        analysis_engine,
        max_workers=4,
        error_log_path=str(config.analysis_error_log_path),
    )
    ingest_service = DocumentIngestService(config)
    ingest_task_manager = IngestTaskManager(
        database,
        vector_store_manager,
        max_workers=4,
        llm_log_path=str(config.llm_log_path),
    )
    rechunk_manager = RechunkTaskManager(
        database,
        vector_store_manager,
        llm_log_path=str(config.llm_log_path),
        max_workers=1,
    )
    asset_synthesizer = AssetSynthesizer(log_path=str(config.llm_log_path))
    preprocess_service = PreprocessAgentService(database, config, retrieval, max_workers=4)
    writing_service = WritingAgentService(database, config, max_workers=4)
    telegram_preprocess_manager = TelegramPreprocessManager(
        database,
        llm_log_path=str(config.llm_log_path),
        max_workers=2,
        stream_hub=telegram_preprocess_stream_hub,
    )
    stone_preprocess_worker = StonePreprocessWorker(
        database,
        stream_hub=stone_preprocess_stream_hub,
        llm_log_path=str(config.llm_log_path),
    )
    project_deletion_manager = ProjectDeletionManager(
        db=database,
        config=config,
        vector_store_manager=vector_store_manager,
        ingest_task_manager=ingest_task_manager,
        rechunk_manager=rechunk_manager,
        analysis_runner=analysis_runner,
        preprocess_service=preprocess_service,
        writing_service=writing_service,
        telegram_preprocess_manager=telegram_preprocess_manager,
    )

    _recover_interrupted_analysis_runs(database)
    telegram_preprocess_manager.resume_interrupted_runs()
    project_deletion_manager.resume_pending_deletions()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stone_preprocess_worker.bind_loop(asyncio.get_running_loop())
        stone_preprocess_worker.resume_interrupted_runs()
        try:
            yield
        finally:
            project_deletion_manager.shutdown()
            analysis_runner.shutdown()
            preprocess_service.shutdown()
            writing_service.shutdown()
            telegram_preprocess_manager.shutdown()
            await stone_preprocess_worker.shutdown()
            rechunk_manager.shutdown()
            ingest_task_manager.shutdown()
            vector_store_manager.save_all()
            database.close()

    app = FastAPI(title="Persona Distiller", lifespan=lifespan)
    app.state.config = config
    app.state.db = database
    app.state.retrieval = retrieval
    app.state.vector_store_manager = vector_store_manager
    app.state.analysis_stream_hub = analysis_stream_hub
    app.state.telegram_preprocess_stream_hub = telegram_preprocess_stream_hub
    app.state.analysis_engine = analysis_engine
    app.state.analysis_runner = analysis_runner
    app.state.ingest_service = ingest_service
    app.state.ingest_task_manager = ingest_task_manager
    app.state.rechunk_manager = rechunk_manager
    app.state.asset_synthesizer = asset_synthesizer
    app.state.skill_synthesizer = asset_synthesizer
    app.state.preprocess_service = preprocess_service
    app.state.writing_service = writing_service
    app.state.telegram_preprocess_manager = telegram_preprocess_manager
    app.state.project_deletion_manager = project_deletion_manager
    app.state.stone_preprocess_stream_hub = stone_preprocess_stream_hub
    app.state.stone_preprocess_worker = stone_preprocess_worker

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.include_router(router)
    return app


app = create_app()


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def run() -> None:
    host = os.getenv("UVICORN_HOST", "127.0.0.1")
    port = int(os.getenv("UVICORN_PORT", "8000"))
    reload = _env_flag("UVICORN_RELOAD", default=False)
    target = "app.main:app" if reload else app
    uvicorn.run(target, host=host, port=port, reload=reload)


if __name__ == "__main__":
    run()
