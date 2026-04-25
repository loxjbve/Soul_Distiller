"""Markdown subagent 运行时。

这里负责统一加载 agent.md、解析 frontmatter、拼接工具和执行结果归一化。
模式目录只提供 md 规范和 normalizer，不再定义模式专属 Python agent 类。
"""

from app.service.common.subagents.base import AgentResult, AgentRunContext, SubAgent
from app.service.common.subagents.markdown_runtime import (
    MarkdownAgentSpec,
    MarkdownBehaviorRegistry,
    MarkdownSubAgent,
    load_markdown_agent_spec,
    load_markdown_subagents,
)
from app.service.common.subagents.runner import AgentOrchestrator, SubagentRunner
from app.service.common.subagents.registry import ToolBinding, ToolRegistry

__all__ = [
    "AgentOrchestrator",
    "AgentResult",
    "AgentRunContext",
    "MarkdownAgentSpec",
    "MarkdownBehaviorRegistry",
    "MarkdownSubAgent",
    "SubAgent",
    "SubagentRunner",
    "ToolBinding",
    "ToolRegistry",
    "load_markdown_agent_spec",
    "load_markdown_subagents",
]
