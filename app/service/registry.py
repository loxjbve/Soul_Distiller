"""service registry。

容器只在启动时装配一次公共依赖，然后把四种 mode 的总流程 pipeline 注册到这里。
路由层应该只经由 registry 找 mode 入口，不要再直接 import 细碎模块。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.service.common.pipeline import ModePipeline


class ServiceRegistry:
    """按 mode 暴露总流程 pipeline。"""

    def __init__(self, *, single: ModePipeline, group: ModePipeline, telegram: ModePipeline, stone: ModePipeline) -> None:
        self.single = single
        self.group = group
        self.telegram = telegram
        self.stone = stone

    def for_mode(self, mode: str | None) -> ModePipeline:
        normalized = str(mode or "").strip().lower()
        if normalized == "telegram":
            return self.telegram
        if normalized == "stone":
            return self.stone
        if normalized == "single":
            return self.single
        return self.group

    def all_pipelines(self) -> tuple[ModePipeline, ...]:
        return (self.single, self.group, self.telegram, self.stone)

    @classmethod
    def build(
        cls,
        *,
        single: ModePipeline,
        group: ModePipeline,
        telegram: ModePipeline,
        stone: ModePipeline,
    ) -> "ServiceRegistry":
        return cls(single=single, group=group, telegram=telegram, stone=stone)


@dataclass(slots=True)
class AppServices:
    """挂到 `app.state.services` 的服务集合。

    这里保留真正通用的基础设施；mode 级行为统一从 `registry.for_mode(...)` 进入。
    """

    registry: ServiceRegistry
    retrieval: Any
    vector_store_manager: Any
    analysis_stream_hub: Any
    telegram_preprocess_stream_hub: Any
    stone_preprocess_stream_hub: Any
    analysis_runner: Any
    ingest_service: Any
    ingest_task_manager: Any
    rechunk_manager: Any
    project_deletion_manager: Any

    def for_mode(self, mode: str | None) -> ModePipeline:
        return self.registry.for_mode(mode)

    def shutdown_mode_pipelines(self) -> list[Any]:
        seen: set[int] = set()
        results: list[Any] = []
        for pipeline in self.registry.all_pipelines():
            identity = id(pipeline)
            if identity in seen:
                continue
            seen.add(identity)
            results.append(pipeline.shutdown())
        return results
