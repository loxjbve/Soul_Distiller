from __future__ import annotations

"""Shared pipeline facade.

This module is the single public entrypoint for shared service pipeline
behavior. The implementation modules now live in `pipeline_impl/`, while the
non-flow helpers live in `pipeline_support/`.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_IMPL_PREFIX = "app.service.common.pipeline"
_IMPL_DIR = Path(__file__).with_name("pipeline_impl")
_SUPPORT_DIR = Path(__file__).with_name("pipeline_support")

# Make this module behave like a package so legacy imports such as
# `app.service.common.pipeline.analysis_runtime` keep working.
__path__ = [str(_IMPL_DIR), str(_SUPPORT_DIR)]
if __spec__ is not None:
    __spec__.submodule_search_locations = list(__path__)

_IMPL_MODULES = (
    "mode",
    "playground_runtime",
    "preprocess_runtime",
    "telegram_analysis_runtime",
    "stone_assets_runtime",
    "stone_analysis_runtime",
    "asset_runtime",
    "analysis_runtime",
    "telegram_runtime",
    "stone_preprocess_runtime",
    "stone_writing_runtime",
)


def _load_impl_module(name: str) -> ModuleType:
    full_name = f"{_IMPL_PREFIX}.{name}"
    existing = sys.modules.get(full_name)
    if existing is not None:
        return existing

    path = _IMPL_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(full_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load pipeline implementation module: {full_name}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return module


_modules = {name: _load_impl_module(name) for name in _IMPL_MODULES}

mode = _modules["mode"]
playground_runtime = _modules["playground_runtime"]
preprocess_runtime = _modules["preprocess_runtime"]
telegram_analysis_runtime = _modules["telegram_analysis_runtime"]
stone_assets_runtime = _modules["stone_assets_runtime"]
stone_analysis_runtime = _modules["stone_analysis_runtime"]
asset_runtime = _modules["asset_runtime"]
analysis_runtime = _modules["analysis_runtime"]
telegram_runtime = _modules["telegram_runtime"]
stone_preprocess_runtime = _modules["stone_preprocess_runtime"]
stone_writing_runtime = _modules["stone_writing_runtime"]

AnalysisCancelledError = analysis_runtime.AnalysisCancelledError
AnalysisEngine = analysis_runtime.AnalysisEngine
AssetSynthesizer = asset_runtime.AssetSynthesizer
BaseModePipeline = mode.BaseModePipeline
ModePipeline = mode.ModePipeline
PreprocessAgentService = preprocess_runtime.PreprocessAgentService
SkillSynthesizer = getattr(asset_runtime, "SkillSynthesizer", AssetSynthesizer)
UnsupportedModeCapability = mode.UnsupportedModeCapability
WritingRequest = mode.WritingRequest
playground_chat = playground_runtime.playground_chat

__all__ = [
    "AnalysisCancelledError",
    "AnalysisEngine",
    "AssetSynthesizer",
    "BaseModePipeline",
    "ModePipeline",
    "PreprocessAgentService",
    "SkillSynthesizer",
    "UnsupportedModeCapability",
    "WritingRequest",
    "playground_chat",
]
