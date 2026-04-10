from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.analysis.engine import AnalysisEngine
from app.analysis.runner import AnalysisTaskRunner
from app.analysis.synthesizer import SkillSynthesizer
from app.config import AppConfig, default_config
from app.db import Database
from app.pipeline.ingest import DocumentIngestService
from app.retrieval.service import RetrievalService
from app.web.routes import router


def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or default_config()
    config.ensure_dirs()
    database = Database(config)
    database.create_all()

    app = FastAPI(title="Persona Distiller")
    app.state.config = config
    app.state.db = database
    app.state.retrieval = RetrievalService()
    app.state.analysis_engine = AnalysisEngine(app.state.retrieval, use_processes=os.name != "nt")
    app.state.analysis_runner = AnalysisTaskRunner(database, app.state.analysis_engine)
    app.state.ingest_service = DocumentIngestService(config)
    app.state.skill_synthesizer = SkillSynthesizer()
    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.include_router(router)
    return app


app = create_app()
