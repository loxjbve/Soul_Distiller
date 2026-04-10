from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ASSET_KINDS: tuple[str, str] = ("skill", "profile_report")


@dataclass(slots=True)
class ServiceConfig:
    base_url: str | None
    api_key: str
    model: str | None = None
    provider_kind: str = "openai-compatible"
    api_mode: str = "responses"


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
class AssetBundle:
    asset_kind: str
    markdown_text: str
    json_payload: dict[str, Any]
    prompt_text: str

    @property
    def system_prompt(self) -> str:
        return self.prompt_text


SkillBundle = AssetBundle


@dataclass(slots=True)
class ChatCompletionResult:
    content: str
    model: str
    usage: dict[str, int]
    request_url: str | None = None
    request_payload: dict[str, Any] | None = None
    raw_response_text: str | None = None
    response_id: str | None = None


@dataclass(slots=True)
class LLMToolCall:
    id: str
    name: str
    arguments_json: str
    arguments: dict[str, Any]


@dataclass(slots=True)
class ToolRoundResult:
    content: str
    model: str
    usage: dict[str, int]
    tool_calls: list[LLMToolCall] = field(default_factory=list)
    provider_response_id: str | None = None
