"""Stone 模式总流程编排。

Stone 的预处理、v3 基线资产、分析、写作和试聊都只从这个文件进入。
token budget、checkpoint、恢复逻辑仍留在 common runtime；这里只做总流程编排。
"""

from __future__ import annotations

from typing import Any

from app.service.common.pipeline import BaseModePipeline, WritingRequest, playground_chat
from app.service.common.pipeline.stone_analysis_runtime import StoneAnalysisAgent as _StoneAnalysisAgent
from app.service.common.pipeline.stone_assets_runtime import StoneV3BaselineSynthesizer as _StoneV3BaselineSynthesizer
from app.service.common.pipeline.stone_preprocess_runtime import (
    StonePreprocessStreamHub as _StonePreprocessStreamHub,
    StonePreprocessWorker as _StonePreprocessWorker,
)
from app.service.common.pipeline.stone_writing_runtime import WritingAgentService as _WritingAgentService


class StonePreprocessWorker(_StonePreprocessWorker):
    pass


class StonePreprocessStreamHub(_StonePreprocessStreamHub):
    pass


class StoneAnalysisAgent(_StoneAnalysisAgent):
    pass


class StoneV3BaselineSynthesizer(_StoneV3BaselineSynthesizer):
    pass


class WritingAgentService(_WritingAgentService):
    pass


class StoneModePipeline(BaseModePipeline):
    mode = "stone"
    preprocess_worker_class = StonePreprocessWorker
    preprocess_stream_hub_class = StonePreprocessStreamHub
    analysis_agent_class = StoneAnalysisAgent
    baseline_synthesizer_class = StoneV3BaselineSynthesizer
    writing_service_class = WritingAgentService

    def __init__(
        self,
        *,
        preprocess_worker: Any,
        analysis_engine: Any,
        analysis_runner: Any,
        asset_synthesizer: Any,
        writing_service: Any,
    ) -> None:
        self.preprocess_worker = preprocess_worker
        self.analysis_engine = analysis_engine
        self.analysis_runner = analysis_runner
        self.asset_synthesizer = asset_synthesizer
        self.writing_service = writing_service

    def set_run_inline(self, enabled: bool) -> None:
        # Stone 预处理当前仍绑定事件循环运行；这里只开放写作 inline，便于测试稳定。
        self.writing_service.run_inline = enabled

    def shutdown(self) -> Any:
        self.writing_service.shutdown()

    def cancel_project(self, project_id: str) -> bool:
        self.writing_service.cancel_project(project_id)
        self.preprocess_worker.cancel_project(project_id)
        return self.has_project_activity(project_id) is False

    def has_project_activity(self, project_id: str) -> bool:
        return any(
            (
                self.writing_service.has_project_activity(project_id),
                self.preprocess_worker.has_project_activity(project_id),
            )
        )

    def is_preprocess_tracking(self, run_id: str) -> bool:
        return self.preprocess_worker.is_tracking(run_id)

    def submit_preprocess_run(
        self,
        project_id: str,
        *,
        concurrency: int | None = None,
        weekly_summary_concurrency: int | None = None,
    ) -> Any:
        del weekly_summary_concurrency
        return self.preprocess_worker.submit(project_id, concurrency=concurrency or 0)

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
        del asset_kind, project, facets, config, target_role, analysis_context, stream_callback, progress_callback, session, retrieval_service
        raise RuntimeError("Stone assets should be built from preprocess profiles, not analysis facets.")

    def build_stone_v3_baseline(
        self,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
        config: Any,
        progress_callback: Any | None = None,
        cancel_requested: Any | None = None,
        checkpoint_callback: Any | None = None,
        resume_from: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Stone 基线合成是多阶段长链路，checkpoint/recovery 继续留在 common runtime。
        return self.asset_synthesizer.build(
            project_name=project_name,
            profiles=profiles,
            documents=documents,
            config=config,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
            checkpoint_callback=checkpoint_callback,
            resume_from=resume_from,
        )

    def start_writing_stream(
        self,
        *,
        project_id: str,
        session_id: str,
        request: WritingRequest,
    ) -> dict[str, str]:
        return self.writing_service.start_stream(
            project_id=project_id,
            session_id=session_id,
            topic=request.topic,
            target_word_count=request.target_word_count,
            extra_requirements=request.extra_requirements,
            raw_message=request.message,
        )

    def stream_writing_events(self, stream_id: str):
        return self.writing_service.stream_events(stream_id)

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
