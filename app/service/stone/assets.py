"""Stone 模式资产生成入口。

Stone 的 baseline author-model / prototype-index 是模式专属能力，
因此从本目录暴露统一入口，避免调用方直接依赖 support 模块细节。
"""

from app.service.stone.assets_support import StoneV3BaselineSynthesizer as StoneAssetService

__all__ = ["StoneAssetService"]
