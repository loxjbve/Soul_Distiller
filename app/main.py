from __future__ import annotations

from contextlib import asynccontextmanager
import os
from pathlib import Path
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from app.analysis.engine import AnalysisEngine
from app.analysis.runner import AnalysisTaskRunner
from app.analysis.synthesizer import AssetSynthesizer
from app.config import AppConfig, default_config
from app.db import Database
from app.pipeline.ingest import DocumentIngestService
from app.preprocess.service import PreprocessAgentService
from app.retrieval.service import RetrievalService
from app.web.routes import router


def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or default_config()
    config.ensure_dirs()
    database = Database(config)
    database.create_all()
    retrieval = RetrievalService()
    analysis_engine = AnalysisEngine(retrieval, llm_log_path=str(config.llm_log_path), use_processes=os.name != "nt")
    analysis_runner = AnalysisTaskRunner(database, analysis_engine, max_workers=4)
    ingest_service = DocumentIngestService(config)
    asset_synthesizer = AssetSynthesizer(log_path=str(config.llm_log_path))
    preprocess_service = PreprocessAgentService(database, config, retrieval, max_workers=4)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            analysis_runner.shutdown()
            preprocess_service.shutdown()
            database.close()

    app = FastAPI(title="Persona Distiller", lifespan=lifespan)
    app.state.config = config
    app.state.db = database
    app.state.retrieval = retrieval
    app.state.analysis_engine = analysis_engine
    app.state.analysis_runner = analysis_runner
    app.state.ingest_service = ingest_service
    app.state.asset_synthesizer = asset_synthesizer
    app.state.skill_synthesizer = asset_synthesizer
    app.state.preprocess_service = preprocess_service
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
