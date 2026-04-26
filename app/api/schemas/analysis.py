from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas import MIN_ANALYSIS_CONCURRENCY


class AnalysisRequestPayload(BaseModel):
    target_role: str | None = None
    target_user_query: str | None = None
    participant_id: str | None = None
    analysis_context: str | None = None
    concurrency: int | None = Field(default=None, ge=MIN_ANALYSIS_CONCURRENCY)
