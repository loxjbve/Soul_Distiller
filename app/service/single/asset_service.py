"""单人模式资产入口。"""

from app.service.common.workspace_assets import AssetSynthesizer


class SingleAssetService(AssetSynthesizer):
    """单人模式资产生成目前沿用公共 synthesizer。"""


__all__ = ["SingleAssetService"]
