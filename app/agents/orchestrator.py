from __future__ import annotations

from app.agents.base import AgentResult, AgentRunContext, SubAgent
from app.agents.registry import ToolRegistry


class AgentOrchestrator:
    def __init__(self, *, registry: ToolRegistry | None = None) -> None:
        self.registry = registry or ToolRegistry()

    def run(self, agent: SubAgent, context: AgentRunContext) -> AgentResult:
        tools = self.registry.resolve_many(agent.tool_names)
        return agent.run(context, tools)
