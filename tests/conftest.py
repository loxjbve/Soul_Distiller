from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig
from app.main import create_app


@pytest.fixture()
def app():
    workspace_root = Path("e:\\Dev\\--\\.test-workspaces")
    workspace_root.mkdir(parents=True, exist_ok=True)
    root_dir = workspace_root / f"persona-distiller-tests-{uuid4().hex}"
    root_dir.mkdir(parents=True, exist_ok=False)
    config = AppConfig(root_dir=root_dir)
    application = create_app(config)
    application.state.services.analysis_engine.use_processes = False
    application.state.services.analysis_runner.run_inline = True
    application.state.services.preprocess_service.run_inline = True
    application.state.services.writing_service.run_inline = True
    application.state.services.telegram_preprocess_manager.run_inline = True
    try:
        yield application
    finally:
        application.state.services.analysis_runner.shutdown()
        application.state.services.preprocess_service.shutdown()
        application.state.services.writing_service.shutdown()
        application.state.services.telegram_preprocess_manager.shutdown()
        application.state.services.rechunk_manager.shutdown()
        application.state.db.close()
        shutil.rmtree(root_dir, ignore_errors=True)


@pytest.fixture()
def client(app):
    with TestClient(app) as test_client:
        yield test_client
