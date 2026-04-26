from __future__ import annotations

from pydantic import BaseModel, Field


class WritingMessagePayload(BaseModel):
    message: str | None = None
    topic: str | None = None
    target_word_count: int | None = Field(default=None, ge=100)
    extra_requirements: str | None = None
