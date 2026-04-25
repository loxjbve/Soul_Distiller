"""Subagent 执行器。

这里统一串起 tool registry、markdown spec loader 和 mode 行为注册表。
如果某个模式要新增 subagent，优先改 md 和 normalizer，不要再新增 Python agent class。
"""

from __future__ import annotations

from pathlib import Path

from app.service.common.subagents.base import AgentResult, AgentRunContext, SubAgent
from app.service.common.subagents.markdown_runtime import MarkdownBehaviorRegistry, MarkdownSubAgent, load_markdown_subagents
from app.service.common.subagents.registry import ToolRegistry


class AgentOrchestrator:
    def __init__(self, *, registry: ToolRegistry | None = None) -> None:
        self.registry = registry or ToolRegistry()

    def run(self, agent: SubAgent, context: AgentRunContext) -> AgentResult:
        tools = self.registry.resolve_many(agent.tool_names)
        return agent.run(context, tools)


class SubagentRunner:
    """统一 subagent 装配入口。"""

    def __init__(
        self,
        *,
        root_dir: Path,
        behavior_registry: MarkdownBehaviorRegistry,
        registry: ToolRegistry | None = None,
    ) -> None:
        self.root_dir = root_dir
        self.behavior_registry = behavior_registry
        self.registry = registry or ToolRegistry()
        self.orchestrator = AgentOrchestrator(registry=self.registry)
        self.subagents: list[MarkdownSubAgent] = load_markdown_subagents(root_dir, behavior_registry)

    def run_all(self, context: AgentRunContext) -> list[AgentResult]:
        return [self.orchestrator.run(agent, context) for agent in self.subagents]
