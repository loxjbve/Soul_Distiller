from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RetrievalFilters:
    source_types: list[str] | None = None
    date_from: str | None = None
    date_to: str | None = None
