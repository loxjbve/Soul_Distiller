from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


ToolHandler = Callable[[dict[str, Any]], Any]


@dataclass(slots=True)
class ToolBinding:
    name: str
    description: str
    handler: ToolHandler

    def invoke(self, arguments: dict[str, Any] | None = None) -> Any:
        return self.handler(dict(arguments or {}))


class ToolRegistry:
    def __init__(self) -> None:
        self._bindings: dict[str, ToolBinding] = {}

    def register(self, binding: ToolBinding) -> None:
        self._bindings[binding.name] = binding

    def get(self, name: str) -> ToolBinding:
        return self._bindings[name]

    def resolve_many(self, names: tuple[str, ...] | list[str]) -> dict[str, ToolBinding]:
        return {name: self.get(name) for name in names}

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._bindings))
