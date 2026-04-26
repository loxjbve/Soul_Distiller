from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, utcnow


class AnalysisRun(Base, TimestampMixin):
    __tablename__ = "analysis_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="analysis_runs")
    facets: Mapped[list["AnalysisFacet"]] = relationship(back_populates="run")
    events: Mapped[list["AnalysisEvent"]] = relationship(back_populates="run")
    skill_drafts: Mapped[list["SkillDraft"]] = relationship(back_populates="analysis_run")


class AnalysisFacet(Base, TimestampMixin):
    __tablename__ = "analysis_facets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("analysis_runs.id"), index=True)
    facet_key: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    accepted: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    findings_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    evidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    conflicts_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped[AnalysisRun] = relationship(back_populates="facets")


class AnalysisEvent(Base):
    __tablename__ = "analysis_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("analysis_runs.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(64), default="info")
    level: Mapped[str] = mapped_column(String(32), default="info")
    message: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    run: Mapped[AnalysisRun] = relationship(back_populates="events")


__all__ = ["AnalysisEvent", "AnalysisFacet", "AnalysisRun"]
