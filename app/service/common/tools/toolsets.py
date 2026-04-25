"""命名 toolset 注册表。

这个文件只负责把已有工具 schema 组合成稳定命名的 toolset，
让 subagent frontmatter 可以声明 `toolset`，而不是在代码里到处拼散装 schema。
"""

from __future__ import annotations

from typing import Any

from app.service.common.tools.workspace import build_tool_schemas


def _workspace_docs() -> list[dict[str, Any]]:
    return build_tool_schemas()[:3]


def _workspace_artifacts() -> list[dict[str, Any]]:
    return build_tool_schemas()[4:]


def _python_transform() -> list[dict[str, Any]]:
    return [build_tool_schemas()[3]]


NAMED_TOOLSETS: dict[str, tuple[dict[str, Any], ...]] = {
    "workspace_docs": tuple(_workspace_docs()),
    "workspace_artifacts": tuple(_workspace_artifacts()),
    "python_transform": tuple(_python_transform()),
    # Telegram / Stone 目前仍由各模式 workflow 提供实际处理逻辑；
    # 这里先固定命名，避免上层继续写散装字符串。
    "retrieval_search": (),
    "telegram_sql": (),
    "stone_corpus": (),
}


def build_toolset_schemas(toolset_names: list[str] | tuple[str, ...]) -> list[dict[str, Any]]:
    schemas: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for toolset_name in toolset_names:
        for schema in NAMED_TOOLSETS.get(toolset_name, ()):
            name = str(((schema.get("function") or {}).get("name")) or "")
            if name and name in seen_names:
                continue
            if name:
                seen_names.add(name)
            schemas.append(schema)
    return schemas
