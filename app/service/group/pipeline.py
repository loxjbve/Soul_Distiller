"""群聊模式总流程编排。

群聊模式和单人模式共用通用 runtime，这里只保留 mode 级别的总入口。
需要 target_role / 子画像差异时，由 pipeline 负责把参数传给 common runtime。
"""

from __future__ import annotations

from app.service.single.pipeline import SingleModePipeline


class GroupModePipeline(SingleModePipeline):
    mode = "group"

    def shutdown(self) -> None:
        # group 与 single 共享同一个 preprocess runtime，真正的关闭由 single pipeline 负责。
        return None
