from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from app.agents.base import AgentResult, AgentRunContext
from app.agents.registry import ToolBinding


MarkdownBehaviorHandler = Callable[["MarkdownAgentSpec", AgentRunContext, dict[str, ToolBinding]], AgentResult]

_PLACEHOLDER_PATTERN = re.compile(r"{{\s*([^{}]+?)\s*}}")


@dataclass(slots=True)
class MarkdownSection:
    title: str
    slug: str
    body: str


@dataclass(slots=True)
class MarkdownRenderResult:
    text: str
    missing_placeholders: tuple[str, ...] = ()


@dataclass(slots=True)
class MarkdownAgentSpec:
    name: str
    behavior: str
    tools: tuple[str, ...] = ()
    order: int = 0
    summary: str = ""
    task: str = ""
    body: str = ""
    path: Path | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    sections: tuple[MarkdownSection, ...] = ()

    def render_text(self, text: str, context: AgentRunContext, tools: dict[str, ToolBinding]) -> MarkdownRenderResult:
        render_context = self.build_render_context(context, tools)
        return _render_template(text, render_context)

    def render_summary(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> MarkdownRenderResult:
        return self.render_text(self.summary, context, tools)

    def render_task(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> MarkdownRenderResult:
        return self.render_text(self.task, context, tools)

    def render_document(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> MarkdownRenderResult:
        return self.render_text(self.body, context, tools)

    def build_render_context(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> dict[str, Any]:
        profiles = list(context.payload.get("profiles") or [])
        documents = list(context.payload.get("documents") or [])
        tool_names = [binding.name for binding in tools.values()]
        tool_catalog = "\n".join(
            f"- {binding.name}: {binding.description}"
            for binding in tools.values()
        )
        return {
            "agent": {
                "name": self.name,
                "behavior": self.behavior,
                "summary": self.summary,
                "task": self.task,
                "tool_names": tool_names,
                "path": str(self.path) if self.path else "",
            },
            "context": {
                "project_id": context.project_id or "",
                "session_id": context.session_id or "",
                "user_id": context.user_id or "",
                "payload": dict(context.payload),
                "metadata": dict(context.metadata),
            },
            "payload": dict(context.payload),
            "metadata": dict(context.metadata),
            "tools": {
                binding.name: {
                    "name": binding.name,
                    "description": binding.description,
                }
                for binding in tools.values()
            },
            "runtime": {
                "project_id": context.project_id or "",
                "session_id": context.session_id or "",
                "user_id": context.user_id or "",
                "profile_count": len(profiles),
                "document_count": len(documents),
                "profile_document_ids": [item.get("document_id") for item in profiles if item.get("document_id")],
                "document_titles": [item.get("title") for item in documents if item.get("title")],
                "payload_keys": sorted(context.payload),
                "metadata_keys": sorted(context.metadata),
                "tool_names": tool_names,
                "tool_catalog": tool_catalog,
            },
            "project_id": context.project_id or "",
            "session_id": context.session_id or "",
            "user_id": context.user_id or "",
        }


class MarkdownBehaviorRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, MarkdownBehaviorHandler] = {}

    def register(self, behavior: str, handler: MarkdownBehaviorHandler) -> None:
        self._handlers[behavior] = handler

    def run(
        self,
        spec: MarkdownAgentSpec,
        context: AgentRunContext,
        tools: dict[str, ToolBinding],
    ) -> AgentResult:
        try:
            handler = self._handlers[spec.behavior]
        except KeyError as exc:
            raise ValueError(f"Unknown markdown agent behavior: {spec.behavior}") from exc
        return handler(spec, context, tools)


class MarkdownSubAgent:
    def __init__(self, spec: MarkdownAgentSpec, behaviors: MarkdownBehaviorRegistry) -> None:
        self.spec = spec
        self.name = spec.name
        self.tool_names = spec.tools
        self._behaviors = behaviors

    def run(self, context: AgentRunContext, tools: dict[str, ToolBinding]) -> AgentResult:
        result = self._behaviors.run(self.spec, context, tools)
        rendered = self.spec.render_document(context, tools)
        result.trace.append(
            {
                "agent": self.spec.name,
                "behavior": self.spec.behavior,
                "tools": list(self.spec.tools),
                "spec_path": str(self.spec.path) if self.spec.path else None,
                "sections": [section.slug for section in self.spec.sections],
                "prompt_document": rendered.text,
                "prompt_missing_placeholders": list(rendered.missing_placeholders),
            }
        )
        return result


def load_markdown_subagents(root_dir: Path, behaviors: MarkdownBehaviorRegistry) -> list[MarkdownSubAgent]:
    subagents: list[MarkdownSubAgent] = []
    for spec_path in sorted(root_dir.glob("*/agent.md")):
        spec = load_markdown_agent_spec(spec_path)
        subagents.append(MarkdownSubAgent(spec, behaviors))
    return sorted(subagents, key=lambda item: item.spec.order)


def load_markdown_agent_spec(path: Path) -> MarkdownAgentSpec:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = _split_frontmatter(text)
    data = _parse_frontmatter(frontmatter)
    cleaned_body = body.strip()
    return MarkdownAgentSpec(
        name=str(data.pop("name", path.parent.name)).strip() or path.parent.name,
        behavior=str(data.pop("behavior", path.parent.name)).strip() or path.parent.name,
        tools=tuple(_coerce_string_list(data.pop("tools", ()))),
        order=int(data.pop("order", 0) or 0),
        summary=str(data.pop("summary", "") or "").strip(),
        task=str(data.pop("task", "") or "").strip(),
        body=cleaned_body,
        path=path,
        metadata=data,
        sections=_parse_sections(cleaned_body),
    )


def _split_frontmatter(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return "", text
    try:
        end_index = next(index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---")
    except StopIteration:
        return "", text
    return "\n".join(lines[1:end_index]), "\n".join(lines[end_index + 1 :])


def _parse_frontmatter(frontmatter: str) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for raw_line in frontmatter.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = _parse_frontmatter_value(value.strip())
    return data


def _parse_frontmatter_value(raw_value: str) -> Any:
    if not raw_value:
        return ""
    if raw_value.startswith("[") and raw_value.endswith("]"):
        try:
            value = ast.literal_eval(raw_value)
        except Exception:
            return [item.strip() for item in raw_value[1:-1].split(",") if item.strip()]
        return value
    if raw_value.isdigit():
        return int(raw_value)
    lowered = raw_value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    return raw_value.strip("\"'")


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _parse_sections(body: str) -> tuple[MarkdownSection, ...]:
    if not body.strip():
        return ()
    sections: list[MarkdownSection] = []
    current_title: str | None = None
    current_lines: list[str] = []
    preface_lines: list[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip()
            if current_title is None and preface_lines:
                sections.append(
                    MarkdownSection(
                        title="Overview",
                        slug="overview",
                        body="\n".join(preface_lines).strip(),
                    )
                )
                preface_lines = []
            if current_title is not None:
                sections.append(
                    MarkdownSection(
                        title=current_title,
                        slug=_slugify_heading(current_title),
                        body="\n".join(current_lines).strip(),
                    )
                )
            current_title = heading or "Section"
            current_lines = []
            continue
        if current_title is None:
            preface_lines.append(line)
        else:
            current_lines.append(line)
    if current_title is None:
        return (
            MarkdownSection(
                title="Overview",
                slug="overview",
                body="\n".join(preface_lines).strip(),
            ),
        )
    sections.append(
        MarkdownSection(
            title=current_title,
            slug=_slugify_heading(current_title),
            body="\n".join(current_lines).strip(),
        )
    )
    return tuple(section for section in sections if section.body)


def _slugify_heading(title: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", title.strip().lower()).strip("-")
    return slug or "section"


def _render_template(text: str, data: dict[str, Any]) -> MarkdownRenderResult:
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        path = match.group(1).strip()
        resolved, found = _lookup_path(data, path)
        if not found:
            missing.append(path)
            return match.group(0)
        return _stringify_value(resolved)

    rendered = _PLACEHOLDER_PATTERN.sub(replace, text)
    return MarkdownRenderResult(
        text=rendered,
        missing_placeholders=tuple(sorted(set(missing))),
    )


def _lookup_path(data: Any, path: str) -> tuple[Any, bool]:
    current = data
    for part in [item for item in path.split(".") if item]:
        if isinstance(current, dict):
            if part not in current:
                return None, False
            current = current[part]
            continue
        if isinstance(current, (list, tuple)):
            if not part.isdigit():
                return None, False
            index = int(part)
            if index < 0 or index >= len(current):
                return None, False
            current = current[index]
            continue
        return None, False
    return current, True


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, indent=2)
