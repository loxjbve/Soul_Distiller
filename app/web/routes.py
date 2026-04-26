from __future__ import annotations

import sys
from types import ModuleType

from fastapi import APIRouter

from app.web import runtime


router = APIRouter()

for route in runtime.router.routes:
    path = getattr(route, "path", "")
    if path.startswith("/api/"):
        continue
    router.routes.append(route)


_COMPAT_EXPORTS = {
    "ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS": runtime.ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS,
    "ASSET_STREAM_QUEUE_POLL_SECONDS": runtime.ASSET_STREAM_QUEUE_POLL_SECONDS,
    "_generate_asset_draft": runtime._generate_asset_draft,
    "_resolve_stone_writing_status": runtime._resolve_stone_writing_status,
}


def __getattr__(name: str):
    if name in _COMPAT_EXPORTS:
        return _COMPAT_EXPORTS[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _CompatModule(ModuleType):
    def __getattr__(self, name: str):
        return __getattr__(name)

    def __setattr__(self, name: str, value):
        if name in _COMPAT_EXPORTS:
            setattr(runtime, name, value)
            _COMPAT_EXPORTS[name] = value
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _CompatModule


__all__ = ["router", *_COMPAT_EXPORTS.keys()]
