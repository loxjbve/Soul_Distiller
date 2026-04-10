from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig
from app.main import create_app


@pytest.fixture()
def app(tmp_path: Path):
    config = AppConfig(root_dir=tmp_path)
    application = create_app(config)
    application.state.analysis_engine.use_processes = False
    application.state.analysis_runner.run_inline = True
    try:
        yield application
    finally:
        application.state.analysis_runner.shutdown()


@pytest.fixture()
def client(app):
    return TestClient(app)
