from __future__ import annotations

from pathlib import Path
from typing import Any

from app.service.common.subagents.base import AgentResult, AgentRunContext
from app.service.common.subagents.markdown_runtime import MarkdownBehaviorRegistry, load_markdown_subagents
from app.service.common.subagents.runner import AgentOrchestrator
from app.service.common.subagents.registry import ToolBinding
from app.service.stone.subagent_behaviors import register_stone_behaviors
from app.retrieval.service import RetrievalService


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
                name="list_profiles",
                description="Return normalized Stone v3 profiles already loaded into the context payload.",
                handler=lambda args: list(args.get("profiles") or []),
            )
        )
        self.registry.register(
            ToolBinding(
                name="read_profile",
                description="Look up one Stone v3 profile by document_id from the context payload.",
                handler=lambda args: next(
                    (item for item in list(args.get("profiles") or []) if item.get("document_id") == args.get("document_id")),
                    None,
                ),
            )
        )
        self.registry.register(
            ToolBinding(
                name="list_documents",
                description="Return Stone documents preloaded into the context payload.",
                handler=lambda args: list(args.get("documents") or []),
            )
        )
        self.registry.register(
            ToolBinding(
                name="search_retrieval",
                description="Proxy retrieval search when the retrieval service is available.",
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
        for agent in self.subagents:
            results.append(self.run(agent, context))
        return results
