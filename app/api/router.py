from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.analysis import router as analysis_router
from app.api.routes.assets import router as assets_router
from app.api.routes.playground import router as playground_router
from app.api.routes.preprocess import router as preprocess_router
from app.api.routes.projects import router as projects_router
from app.api.routes.settings import router as settings_router
from app.api.routes.streams import router as streams_router
from app.api.routes.telegram import router as telegram_router
from app.api.routes.writing import router as writing_router

_SPLIT_ROUTERS = (
    projects_router,
    analysis_router,
    preprocess_router,
    telegram_router,
    settings_router,
    streams_router,
    assets_router,
    playground_router,
    writing_router,
)


router = APIRouter()

for split_router in _SPLIT_ROUTERS:
    router.include_router(split_router)
