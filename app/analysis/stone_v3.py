from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType

runtime = import_module("app.analysis.stone_v3_runtime")

_COMPAT_EXPORTS = {
    name: value
    for name, value in runtime.__dict__.items()
    if not name.startswith("__")
}


def __getattr__(name: str):
    if name in _COMPAT_EXPORTS:
        return getattr(runtime, name)
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

__all__ = ["runtime", *_COMPAT_EXPORTS.keys()]
