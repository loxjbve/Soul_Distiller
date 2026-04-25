"""Telegram 模式分析入口。

Telegram 的 facet 分析依赖 SQL-only 证据获取和 Telegram 专用 tool loop，
具体执行器放在本目录，公共 runner 只负责调度。
"""

from app.service.telegram.analysis_agent import TelegramAnalysisAgent as TelegramAnalysisService

__all__ = ["TelegramAnalysisService"]
