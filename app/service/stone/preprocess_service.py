"""Stone 预处理流程入口。"""

from app.service.stone.preprocess import StonePreprocessWorker


class StonePreprocessService(StonePreprocessWorker):
    """为 service registry 提供稳定命名的 Stone preprocess 入口。"""


__all__ = ["StonePreprocessService"]
