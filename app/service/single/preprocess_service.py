"""单人模式预处理入口。"""

from app.service.common.workspace_preprocess import PreprocessAgentService


class SinglePreprocessService(PreprocessAgentService):
    """单人模式当前沿用通用工作区预处理实现。"""


__all__ = ["SinglePreprocessService"]
