from pathlib import Path
from typing import Any

from app.agents.base import AgentResult, AgentRunContext
from app.agents.markdown_runtime import MarkdownBehaviorRegistry, load_markdown_subagents
from app.agents.orchestrator import AgentOrchestrator
from app.agents.registry import ToolBinding
from app.agents.stone.behaviors import register_stone_behaviors
from app.retrieval.service import RetrievalService


def _profile_slices_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("profile_slices")
    if isinstance(candidates, list) and candidates:
        return [dict(item) for item in candidates if isinstance(item, dict)]
    candidates = payload.get("profiles")
    if isinstance(candidates, list):
        return [dict(item) for item in candidates if isinstance(item, dict)]
    return []


def _documents_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("documents")
    if isinstance(candidates, list) and candidates:
        return [dict(item) for item in candidates if isinstance(item, dict)]
    prototype_index = payload.get("prototype_index")
    if isinstance(prototype_index, dict):
        return [dict(item) for item in list(prototype_index.get("documents") or []) if isinstance(item, dict)]
    return []


class StoneAgentOrchestrator(AgentOrchestrator):
    def __init__(self, *, retrieval_service: RetrievalService | None = None) -> None:
        super().__init__()
        self.retrieval_service = retrieval_service
        self._register_tools()
        self.behaviors = MarkdownBehaviorRegistry()
        register_stone_behaviors(self.behaviors)
        self.subagents = load_markdown_subagents(
            Path(__file__).resolve().parent / "subagents",
            self.behaviors,
        )

    def _register_tools(self) -> None:
        self.registry.register(
            ToolBinding(
                name="list_profile_slices",
                description="返回当前运行可用的 Stone 画像切片样本。优先使用这些切片，而不是全量画像。",
                handler=lambda args: _profile_slices_from_payload(dict(args)),
            )
        )
        self.registry.register(
            ToolBinding(
                name="read_profile_slice",
                description="按 document_id 读取一条 Stone 画像切片样本。",
                handler=lambda args: next(
                    (
                        item
                        for item in _profile_slices_from_payload(dict(args))
                        if item.get("document_id") == args.get("document_id")
                    ),
                    None,
                ),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_profile_index",
                description="返回 Stone 画像索引，包含语料规模、原型家族覆盖和稀疏采样说明。",
                handler=lambda args: dict(args.get("profile_index") or {}),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_analysis_facets",
                description="返回最近一次可用 Stone 分析运行的紧凑 facet 数据包。",
                handler=lambda args: list(
                    ((args.get("analysis_summary") or {}).get("facet_packets") or args.get("analysis_facets") or [])
                ),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_author_model",
                description="返回当前项目的 Stone Author Model V3。",
                handler=lambda args: dict(args.get("author_model") or {}),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_prototype_index",
                description="返回用于 shortlist 和 anchor 规划的 Stone Prototype Index V3。",
                handler=lambda args: dict(args.get("prototype_index") or {}),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_writing_guide",
                description="返回由分析 facets 和代表性画像切片合并得到的 Stone 写作指南。",
                handler=lambda args: dict(args.get("writing_guide") or {}),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_writing_packet",
                description="返回当前 writing packet 壳，或优先返回最新 packet_composer 产出的 writing_packet_v3。",
                handler=lambda args: (
                    dict((((args.get("pipeline_results") or {}).get("packet_composer") or {}).get("writing_packet_v3") or {}))
                    or dict(args.get("writing_packet") or {})
                ),
            )
        )
        self.registry.register(
            ToolBinding(
                name="get_pipeline_result",
                description="按 agent_name 读取本次 orchestrator 运行里前序 subagent 的输出。",
                handler=lambda args: dict(((args.get("pipeline_results") or {}).get(str(args.get("agent_name") or "")) or {})),
            )
        )
        self.registry.register(
            ToolBinding(
                name="list_documents",
                description="返回上下文里预加载的 Stone 文档，或 prototype index 里的文档条目。",
                handler=lambda args: _documents_from_payload(dict(args)),
            )
        )
        self.registry.register(
            ToolBinding(
                name="search_retrieval",
                description="当 retrieval service 可用时，代理执行检索搜索。",
                handler=self._search_retrieval,
            )
        )

    def _search_retrieval(self, args: dict[str, Any]) -> Any:
        if not self.retrieval_service:
            return []
        query = str(args.get("query") or "").strip()
        if not query:
            return []
        project_id = str(args.get("project_id") or "").strip()
        if not project_id:
            return []
        limit = int(args.get("limit") or 5)
        return [item.__dict__ for item in self.retrieval_service.search(project_id, query=query, limit=limit)]

    def run_pipeline(self, context: AgentRunContext) -> list[AgentResult]:
        results: list[AgentResult] = []
        pipeline_results: dict[str, dict[str, Any]] = {}
        rolling_payload = dict(context.payload)
        for agent in self.subagents:
            stage_context = AgentRunContext(
                project_id=context.project_id,
                session_id=context.session_id,
                user_id=context.user_id,
                payload={
                    **rolling_payload,
                    "pipeline_results": dict(pipeline_results),
                },
                metadata=dict(context.metadata),
            )
            result = self.run(agent, stage_context)
            results.append(result)
            pipeline_results[agent.name] = dict(result.payload or {})
            rolling_payload[agent.name] = dict(result.payload or {})
        return results
