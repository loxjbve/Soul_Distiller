from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ServiceConfig:
    base_url: str | None
    api_key: str
    model: str | None = None
    provider_kind: str = "openai-compatible"


@dataclass(slots=True)
class ExtractedSegment:
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExtractionResult:
    raw_text: str
    clean_text: str
    title: str | None
    author_guess: str | None
    created_at_guess: str | None
    language: str
    metadata: dict[str, Any]
    segments: list[ExtractedSegment]


@dataclass(slots=True)
class ChunkPayload:
    chunk_index: int
    content: str
    start_offset: int
    end_offset: int
    page_number: int | None
    token_count: int
    metadata: dict[str, Any]


@dataclass(slots=True)
class RetrievedChunk:
    chunk_id: str
    document_id: str
    document_title: str
    filename: str
    source_type: str
    content: str
    score: float
    page_number: int | None
    metadata: dict[str, Any]


@dataclass(slots=True)
class FacetResult:
    facet_key: str
    status: str
    confidence: float
    summary: str
    bullets: list[str]
    evidence: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]
    notes: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SkillBundle:
    markdown_text: str
    json_payload: dict[str, Any]
    system_prompt: str


@dataclass(slots=True)
class ChatCompletionResult:
    content: str
    model: str
    usage: dict[str, int]
