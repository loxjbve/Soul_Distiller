"""群聊模式预处理入口。"""

from app.service.common.workspace_preprocess import PreprocessAgentService


class GroupPreprocessService(PreprocessAgentService):
    """群聊模式当前沿用通用工作区预处理实现。"""


__all__ = ["GroupPreprocessService"]
