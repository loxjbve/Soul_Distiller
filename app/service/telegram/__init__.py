"""Telegram 模式 service。"""

from app.service.telegram.analysis import TelegramAnalysisService
from app.service.telegram.assets import TelegramAssetService
from app.service.telegram.preprocess import TelegramPreprocessManager

TelegramPreprocessService = TelegramPreprocessManager

__all__ = ["TelegramAnalysisService", "TelegramAssetService", "TelegramPreprocessManager", "TelegramPreprocessService"]
