"""模式 pipeline 协议。

这里定义四种 mode 共享的总流程入口。路由层只应该依赖这些方法，
不要再直接摸 mode 专属 runtime 或重新按 project.mode 分叉装配。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class UnsupportedModeCapability(RuntimeError):
    """某个 mode 不支持的能力入口。"""


@dataclass(slots=True)
class WritingRequest:
    topic: str
    target_word_count: int
    extra_requirements: str | None = None
    message: str | None = None


class ModePipeline(Protocol):
    mode: str

    def supports(self, capability: str) -> bool:
        ...

    def set_run_inline(self, enabled: bool) -> None:
        ...

    def shutdown(self) -> Any:
        ...

    def cancel_project(self, project_id: str) -> bool:
        ...

    def has_project_activity(self, project_id: str) -> bool:
        ...

    def is_preprocess_tracking(self, run_id: str) -> bool:
        ...

    def list_mentions(self, session: Any, project_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        ...

    def start_preprocess_session_stream(self, *, project_id: str, session_id: str, message: str) -> dict[str, str]:
        ...

    def stream_preprocess_session_events(self, stream_id: str):
        ...

    def submit_preprocess_run(
        self,
        project_id: str,
        *,
        concurrency: int | None = None,
        weekly_summary_concurrency: int | None = None,
    ) -> Any:
        ...

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
        ...

    def submit_analysis_run(self, run_id: str) -> None:
        ...

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
        ...

    def start_writing_stream(
        self,
        *,
        project_id: str,
        session_id: str,
        request: WritingRequest,
    ) -> dict[str, str]:
        ...

    def stream_writing_events(self, stream_id: str):
        ...

    def playground_chat(
        self,
        request: Any,
        session: Any,
        project_id: str,
        *,
        message: str,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        ...


class BaseModePipeline:
    """四种 mode 共享的默认拒绝实现。"""

    mode = "base"

    def supports(self, capability: str) -> bool:
        return hasattr(self, capability)

    def set_run_inline(self, enabled: bool) -> None:
        del enabled

    def shutdown(self) -> Any:
        return None

    def cancel_project(self, project_id: str) -> bool:
        del project_id
        return False

    def has_project_activity(self, project_id: str) -> bool:
        del project_id
        return False

    def is_preprocess_tracking(self, run_id: str) -> bool:
        del run_id
        return False

    def list_mentions(self, session: Any, project_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        del session, project_id, query, limit
        raise UnsupportedModeCapability(f"{self.mode} does not support preprocess mentions.")

    def start_preprocess_session_stream(self, *, project_id: str, session_id: str, message: str) -> dict[str, str]:
        del project_id, session_id, message
        raise UnsupportedModeCapability(f"{self.mode} does not support preprocess sessions.")

    def stream_preprocess_session_events(self, stream_id: str):
        del stream_id
        raise UnsupportedModeCapability(f"{self.mode} does not support preprocess sessions.")

    def submit_preprocess_run(
        self,
        project_id: str,
        *,
        concurrency: int | None = None,
        weekly_summary_concurrency: int | None = None,
    ) -> Any:
        del project_id, concurrency, weekly_summary_concurrency
        raise UnsupportedModeCapability(f"{self.mode} does not support preprocess runs.")

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
        del session, project_id, target_role, target_user_query, participant_id, analysis_context, concurrency
        raise UnsupportedModeCapability(f"{self.mode} does not support analysis.")

    def submit_analysis_run(self, run_id: str) -> None:
        del run_id
        raise UnsupportedModeCapability(f"{self.mode} does not support analysis.")

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
        del (
            asset_kind,
            project,
            facets,
            config,
            target_role,
            analysis_context,
            stream_callback,
            progress_callback,
            session,
            retrieval_service,
        )
        raise UnsupportedModeCapability(f"{self.mode} does not support asset generation.")

    def start_writing_stream(
        self,
        *,
        project_id: str,
        session_id: str,
        request: WritingRequest,
    ) -> dict[str, str]:
        del project_id, session_id, request
        raise UnsupportedModeCapability(f"{self.mode} does not support writing.")

    def stream_writing_events(self, stream_id: str):
        del stream_id
        raise UnsupportedModeCapability(f"{self.mode} does not support writing.")

    def playground_chat(
        self,
        request: Any,
        session: Any,
        project_id: str,
        *,
        message: str,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        del request, session, project_id, message, session_id
        raise UnsupportedModeCapability(f"{self.mode} does not support playground chat.")
