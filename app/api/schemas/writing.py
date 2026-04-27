from __future__ import annotations

from pydantic import BaseModel, Field


class WritingMessagePayload(BaseModel):
    message: str | None = None
    topic: str | None = None
    target_word_count: int | None = Field(default=None, ge=100)
    extra_requirements: str | None = None
    max_concurrency: int | None = Field(default=None, ge=1, le=8)


class StoneWritingSettingsPayload(BaseModel):
    max_concurrency: int = Field(default=4, ge=1, le=8)
