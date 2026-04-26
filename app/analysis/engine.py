from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType

runtime = import_module("app.analysis.engine_runtime")

_COMPAT_EXPORTS = {
    "AnalysisCancelledError": runtime.AnalysisCancelledError,
    "AnalysisEngine": runtime.AnalysisEngine,
    "FACET_BULLET_LIMIT": runtime.FACET_BULLET_LIMIT,
    "FACET_EVIDENCE_LIMIT": runtime.FACET_EVIDENCE_LIMIT,
    "GLOBAL_PERSONA_CARD_LABELS": runtime.GLOBAL_PERSONA_CARD_LABELS,
    "RAW_TEXT_PREVIEW_LIMIT": runtime.RAW_TEXT_PREVIEW_LIMIT,
    "analyze_facet_worker": runtime.analyze_facet_worker,
    "_analyze_heuristically": runtime._analyze_heuristically,
    "_normalize_concurrency": runtime._normalize_concurrency,
    "_normalize_facet_payload": runtime._normalize_facet_payload,
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
