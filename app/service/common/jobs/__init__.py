"""后台任务与恢复逻辑。

这里放会跨模式复用的 runner / manager。
复杂的恢复和取消协议需要在这里集中维护，避免不同模式重复实现。
"""

from app.service.common.jobs.analysis_runner import AnalysisTaskRunner

__all__ = ["AnalysisTaskRunner"]
