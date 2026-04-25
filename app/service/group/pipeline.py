"""群聊模式总编排入口。

群聊模式和单人模式共用同一套分析、资产和试聊逻辑。
预处理 agent 已经从这两个模式里移除，因此这里不再保留任何预处理相关实现。
"""

from __future__ import annotations

from app.service.single.pipeline import SingleModePipeline


class GroupModePipeline(SingleModePipeline):
    mode = "group"
