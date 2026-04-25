"""Service registry。

容器只装配一次公共依赖，然后把不同模式的总流程入口注册到这里。
路由层只应通过 registry 取 mode bundle，不再直接 import 分散模块。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ModeServiceBundle:
    preprocess: Any | None = None
    analysis: Any | None = None
    assets: Any | None = None
    writing: Any | None = None


class ServiceRegistry:
    """按 mode 暴露总流程入口。

    这里是路由层能看到的唯一模式分发点。
    新模式应在这里注册，不要继续在上层写 project.mode 的大分支。
    """

    def __init__(self, *, single: ModeServiceBundle, group: ModeServiceBundle, telegram: ModeServiceBundle, stone: ModeServiceBundle) -> None:
        self.single = single
        self.group = group
        self.telegram = telegram
        self.stone = stone

    def for_mode(self, mode: str | None) -> ModeServiceBundle:
        normalized = str(mode or "").strip().lower()
        if normalized == "telegram":
            return self.telegram
        if normalized == "stone":
            return self.stone
        if normalized == "single":
            return self.single
        return self.group

    @classmethod
    def build(
        cls,
        *,
        single_preprocess: Any,
        group_preprocess: Any,
        telegram_preprocess: Any,
        stone_preprocess: Any,
        single_analysis: Any,
        group_analysis: Any,
        telegram_analysis: Any,
        stone_analysis: Any,
        single_assets: Any,
        group_assets: Any,
        telegram_assets: Any,
        stone_assets: Any,
        stone_writing: Any,
    ) -> "ServiceRegistry":
        return cls(
            single=ModeServiceBundle(
                preprocess=single_preprocess,
                analysis=single_analysis,
                assets=single_assets,
            ),
            group=ModeServiceBundle(
                preprocess=group_preprocess,
                analysis=group_analysis,
                assets=group_assets,
            ),
            telegram=ModeServiceBundle(
                preprocess=telegram_preprocess,
                analysis=telegram_analysis,
                assets=telegram_assets,
            ),
            stone=ModeServiceBundle(
                preprocess=stone_preprocess,
                analysis=stone_analysis,
                assets=stone_assets,
                writing=stone_writing,
            ),
        )


@dataclass(slots=True)
class AppServices:
    """挂到 `app.state.services` 的服务集合。

    这里保留高频共享依赖的稳定属性名，方便大体量路由层机械迁移到 `app.state.services.*`。
    与 mode 强相关的入口统一走 `registry.for_mode(...)`。
    """

    registry: ServiceRegistry
    retrieval: Any
    vector_store_manager: Any
    analysis_stream_hub: Any
    telegram_preprocess_stream_hub: Any
    stone_preprocess_stream_hub: Any
    analysis_engine: Any
    analysis_runner: Any
    ingest_service: Any
    ingest_task_manager: Any
    rechunk_manager: Any
    asset_synthesizer: Any
    stone_v3_synthesizer: Any
    preprocess_service: Any
    writing_service: Any
    telegram_preprocess_manager: Any
    stone_preprocess_worker: Any
    project_deletion_manager: Any
    stone_agents: Any
    telegram_agents: Any
    preprocess_agents: Any

    def for_mode(self, mode: str | None) -> ModeServiceBundle:
        return self.registry.for_mode(mode)
