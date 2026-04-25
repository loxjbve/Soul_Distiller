from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.analysis import router as analysis_router
from app.api.routes.assets import router as assets_router
from app.api.routes.preprocess import router as preprocess_router
from app.api.routes.projects import router as projects_router
from app.api.routes.settings import router as settings_router
from app.api.routes.streams import router as streams_router
from app.api.routes.telegram import router as telegram_router
from app.api.routes.writing import router as writing_router
from app.web import routes as legacy_routes

_SPLIT_ROUTERS = (
    projects_router,
    analysis_router,
    preprocess_router,
    telegram_router,
    settings_router,
    streams_router,
    assets_router,
    writing_router,
)

_MOVED_PATHS = {
    getattr(route, "path", "")
    for split_router in _SPLIT_ROUTERS
    for route in split_router.routes
    if getattr(route, "path", "")
}


router = APIRouter()

for route in legacy_routes.router.routes:
    path = getattr(route, "path", "")
    if path in _MOVED_PATHS:
        continue
    router.routes.append(route)

for split_router in _SPLIT_ROUTERS:
    router.include_router(split_router)
