from app.api.routes.analysis import router as analysis_router
from app.api.routes.assets import router as assets_router
from app.api.routes.preprocess import router as preprocess_router
from app.api.routes.projects import router as projects_router
from app.api.routes.settings import router as settings_router
from app.api.routes.streams import router as streams_router
from app.api.routes.telegram import router as telegram_router
from app.api.routes.writing import router as writing_router

__all__ = [
    "analysis_router",
    "assets_router",
    "preprocess_router",
    "projects_router",
    "settings_router",
    "streams_router",
    "telegram_router",
    "writing_router",
]
