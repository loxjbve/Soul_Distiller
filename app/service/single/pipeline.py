"""单人模式总编排入口。

这个文件现在只负责单人模式的分析、资产生成和试聊编排。
单人/群聊的预处理 agent 已经移除，相关能力会在 common base 里直接视为不支持。
"""

from __future__ import annotations

from typing import Any

from app.service.common.pipeline import BaseModePipeline, playground_chat


class SingleModePipeline(BaseModePipeline):
    mode = "single"

    def __init__(
        self,
        *,
        analysis_engine: Any,
        analysis_runner: Any,
        asset_synthesizer: Any,
    ) -> None:
        self.analysis_engine = analysis_engine
        self.analysis_runner = analysis_runner
        self.asset_synthesizer = asset_synthesizer

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
        return self.analysis_engine.create_run(
            session,
            project_id,
            target_role=target_role,
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
