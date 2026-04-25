"""单人模式分析入口。

单人模式沿用 retrieval-based 多维分析主流程；
这里只保留单人模式入口名，具体公共实现留在 common，避免与 group 重复分叉。
"""

from app.service.common.workspace_analysis import AnalysisEngine as SingleAnalysisService

__all__ = ["SingleAnalysisService"]
