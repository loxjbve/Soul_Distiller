from __future__ import annotations

import os
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from app.api.router import router as api_router
from app.core import AppConfig, AppContainer, default_config
from app.web.routes import router as web_router


def create_app(config: AppConfig | None = None) -> FastAPI:
    container = AppContainer.build(config or default_config())
    app = FastAPI(title="Persona Distiller", lifespan=container.lifespan)
    container.attach_to_app(app)

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.include_router(web_router)
    app.include_router(api_router)
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
