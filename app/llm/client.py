from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType

runtime = import_module("app.llm.client_runtime")

_COMPAT_EXPORTS = {
    "LLMError": runtime.LLMError,
    "MAX_CONCURRENT_LLM_REQUESTS": runtime.MAX_CONCURRENT_LLM_REQUESTS,
    "OFFICIAL_PROVIDER_BASE_URLS": runtime.OFFICIAL_PROVIDER_BASE_URLS,
    "OpenAICompatibleClient": runtime.OpenAICompatibleClient,
    "normalize_api_mode": runtime.normalize_api_mode,
    "normalize_provider_kind": runtime.normalize_provider_kind,
    "parse_json_response": runtime.parse_json_response,
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
