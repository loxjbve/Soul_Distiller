"""单人模式总流程编排。

这个文件只保留单人模式的编排入口：预处理会话、十维分析、资产生成、试聊。
底层 LLM、tool loop、SSE、具体 tool 实现都应继续留在 common。
"""

from __future__ import annotations

from typing import Any

from app.service.common.pipeline.mode import BaseModePipeline
from app.service.common.pipeline.playground_runtime import playground_chat


class SingleModePipeline(BaseModePipeline):
    mode = "single"

    def __init__(
        self,
        *,
        preprocess_service: Any,
        analysis_engine: Any,
        analysis_runner: Any,
        asset_synthesizer: Any,
    ) -> None:
        self.preprocess_service = preprocess_service
        self.analysis_engine = analysis_engine
        self.analysis_runner = analysis_runner
        self.asset_synthesizer = asset_synthesizer

    def set_run_inline(self, enabled: bool) -> None:
        self.preprocess_service.run_inline = enabled

    def shutdown(self) -> None:
        self.preprocess_service.shutdown()

    def cancel_project(self, project_id: str) -> bool:
        self.preprocess_service.cancel_project(project_id)
        return self.preprocess_service.has_project_activity(project_id) is False

    def has_project_activity(self, project_id: str) -> bool:
        return self.preprocess_service.has_project_activity(project_id)

    def list_mentions(self, session: Any, project_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        return self.preprocess_service.list_mentions(session, project_id, query, limit=limit)

    def start_preprocess_session_stream(self, *, project_id: str, session_id: str, message: str) -> dict[str, str]:
        return self.preprocess_service.start_stream(project_id=project_id, session_id=session_id, message=message)

    def stream_preprocess_session_events(self, stream_id: str):
        return self.preprocess_service.stream_events(stream_id)

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
