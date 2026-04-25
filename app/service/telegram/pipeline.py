"""Telegram 模式总流程编排。

这里负责把 Telegram 预处理、十维分析、资产生成和试聊统一收口为一个入口。
SQL-only 约束仍由 common tools/runtime 承担，路由层不应再直接访问具体 worker。
"""

from __future__ import annotations

from typing import Any

from app.service.common.pipeline.mode import BaseModePipeline
from app.service.common.pipeline.playground_runtime import playground_chat


class TelegramModePipeline(BaseModePipeline):
    mode = "telegram"

    def __init__(
        self,
        *,
        preprocess_manager: Any,
        analysis_engine: Any,
        analysis_runner: Any,
        asset_synthesizer: Any,
    ) -> None:
        self.preprocess_manager = preprocess_manager
        self.analysis_engine = analysis_engine
        self.analysis_runner = analysis_runner
        self.asset_synthesizer = asset_synthesizer

    def set_run_inline(self, enabled: bool) -> None:
        self.preprocess_manager.run_inline = enabled

    def shutdown(self) -> None:
        self.preprocess_manager.shutdown()

    def cancel_project(self, project_id: str) -> bool:
        return self.preprocess_manager.cancel_project(project_id)

    def has_project_activity(self, project_id: str) -> bool:
        return self.preprocess_manager.has_project_activity(project_id)

    def is_preprocess_tracking(self, run_id: str) -> bool:
        return self.preprocess_manager.is_tracking(run_id)

    def submit_preprocess_run(
        self,
        project_id: str,
        *,
        concurrency: int | None = None,
        weekly_summary_concurrency: int | None = None,
    ) -> Any:
        del concurrency
        return self.preprocess_manager.submit(
            project_id,
            weekly_summary_concurrency=weekly_summary_concurrency,
        )

    def create_analysis_run(
        self,
        session: Any,
        project_id: str,
        *,
        target_role: str | None = None,
        target_user_query: str | None = None,
        participant_id: str | None = None,
        analysis_context: str | None = None,
        concurrency: int | None = None,
    ) -> Any:
        del target_role
        return self.analysis_engine.create_run(
            session,
            project_id,
            target_role=None,
            target_user_query=target_user_query,
            participant_id=participant_id,
            analysis_context=analysis_context,
            concurrency=concurrency,
        )

    def submit_analysis_run(self, run_id: str) -> None:
        self.analysis_runner.submit(run_id)

    def build_asset_bundle(
        self,
        asset_kind: str,
        project: Any,
        facets: list[Any],
        config: Any,
        *,
        target_role: str | None = None,
        analysis_context: str | None = None,
        stream_callback: Any | None = None,
        progress_callback: Any | None = None,
        session: Any | None = None,
        retrieval_service: Any | None = None,
    ) -> Any:
        return self.asset_synthesizer.build(
            asset_kind,
            project,
            facets,
            config,
            target_role=target_role,
            analysis_context=analysis_context,
            stream_callback=stream_callback,
            progress_callback=progress_callback,
            session=session,
            retrieval_service=retrieval_service,
        )

    def playground_chat(
        self,
        request: Any,
        session: Any,
        project_id: str,
        *,
        message: str,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        return playground_chat(request, session, project_id, message=message, session_id=session_id)
