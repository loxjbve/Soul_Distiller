from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from app.service.common.subagents.registry import ToolBinding


@dataclass(slots=True)
class AgentRunContext:
    project_id: str | None = None
    session_id: str | None = None
    user_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentResult:
    agent_name: str
    payload: dict[str, Any]
    trace: list[dict[str, Any]] = field(default_factory=list)


class SubAgent(Protocol):
    name: str
    tool_names: tuple[str, ...]

    def run(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> AgentResult:
        ...

