from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, JSON, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, utcnow


class SkillDraft(Base, TimestampMixin):
    __tablename__ = "skill_drafts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("analysis_runs.id"), nullable=True)
    asset_kind: Mapped[str] = mapped_column(String(32), default="skill", index=True)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    markdown_text: Mapped[str] = mapped_column(Text)
    json_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    system_prompt: Mapped[str] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="skill_drafts")
    analysis_run: Mapped["AnalysisRun | None"] = relationship(back_populates="skill_drafts")
    versions: Mapped[list["SkillVersion"]] = relationship(back_populates="draft")


class SkillVersion(Base):
    __tablename__ = "skill_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    draft_id: Mapped[str] = mapped_column(ForeignKey("skill_drafts.id"), index=True)
    asset_kind: Mapped[str] = mapped_column(String(32), default="skill", index=True)
    version_number: Mapped[int] = mapped_column(Integer)
    markdown_text: Mapped[str] = mapped_column(Text)
    json_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    system_prompt: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    project: Mapped["Project"] = relationship(back_populates="skill_versions")
    draft: Mapped[SkillDraft] = relationship(back_populates="versions")


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    session_kind: Mapped[str] = mapped_column(String(32), default="playground", index=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    project: Mapped["Project"] = relationship(back_populates="chat_sessions")
    turns: Mapped[list["ChatTurn"]] = relationship(back_populates="session")


class ChatTurn(Base):
    __tablename__ = "chat_turns"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(ForeignKey("chat_sessions.id"), index=True)
    role: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text)
    trace_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    session: Mapped[ChatSession] = relationship(back_populates="turns")


class GeneratedArtifact(Base):
    __tablename__ = "generated_artifacts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    session_id: Mapped[str] = mapped_column(ForeignKey("chat_sessions.id"), index=True)
    turn_id: Mapped[str | None] = mapped_column(ForeignKey("chat_turns.id"), index=True, nullable=True)
    filename: Mapped[str] = mapped_column(String(512))
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    storage_path: Mapped[str] = mapped_column(String(1024))
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

__all__ = ["ChatSession", "ChatTurn", "GeneratedArtifact", "SkillDraft", "SkillVersion"]
