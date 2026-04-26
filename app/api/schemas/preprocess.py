from __future__ import annotations

from pydantic import BaseModel, Field


class PreprocessSessionCreatePayload(BaseModel):
    title: str | None = None


class PreprocessSessionUpdatePayload(BaseModel):
    title: str | None = None


class TelegramPreprocessRunCreatePayload(BaseModel):
    weekly_summary_concurrency: int | None = Field(default=None, ge=1)
