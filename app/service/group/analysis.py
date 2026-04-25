"""群聊模式分析入口。

群聊模式与单人模式共享 retrieval-based 分析底座，
但路由和 service registry 会通过这个模块表达“群聊模式”语义边界。
"""

from app.service.common.workspace_analysis import AnalysisEngine as GroupAnalysisService

__all__ = ["GroupAnalysisService"]
