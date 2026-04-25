"""单人模式 service。

这里只放单人模式的总流程编排入口。具体公共逻辑下沉到 common，
不要再把 telegram / stone 的分支塞回这个目录。
"""

from app.service.single.analysis import SingleAnalysisService
from app.service.single.assets import SingleAssetService
from app.service.single.preprocess import SinglePreprocessService

__all__ = ["SingleAnalysisService", "SingleAssetService", "SinglePreprocessService"]
