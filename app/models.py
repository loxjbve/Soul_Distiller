from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=utcnow,
        onupdate=utcnow,
    )


class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    documents: Mapped[list["DocumentRecord"]] = relationship(back_populates="project")
    analysis_runs: Mapped[list["AnalysisRun"]] = relationship(back_populates="project")
    skill_drafts: Mapped[list["SkillDraft"]] = relationship(back_populates="project")
    skill_versions: Mapped[list["SkillVersion"]] = relationship(back_populates="project")
    chat_sessions: Mapped[list["ChatSession"]] = relationship(back_populates="project")


class DocumentRecord(Base, TimestampMixin):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    filename: Mapped[str] = mapped_column(String(512))
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    extension: Mapped[str] = mapped_column(String(32))
    source_type: Mapped[str] = mapped_column(String(64))
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    author_guess: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at_guess: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text)
    clean_text: Mapped[str] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String(32), default="unknown")
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    ingest_status: Mapped[str] = mapped_column(String(32), default="ready")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_path: Mapped[str] = mapped_column(String(1024))

    project: Mapped[Project] = relationship(back_populates="documents")
    chunks: Mapped[list["TextChunk"]] = relationship(back_populates="document")


class TextChunk(Base):
    __tablename__ = "chunks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("documents.id"), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(Text)
    start_offset: Mapped[int] = mapped_column(Integer)
    end_offset: Mapped[int] = mapped_column(Integer)
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    token_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    embedding_vector: Mapped[list[float] | None] = mapped_column(JSON, nullable=True)
    embedding_model: Mapped[str | None] = mapped_column(String(255), nullable=True)

    document: Mapped[DocumentRecord] = relationship(back_populates="chunks")


class AnalysisRun(Base, TimestampMixin):
    __tablename__ = "analysis_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped[Project] = relationship(back_populates="analysis_runs")
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


class SkillDraft(Base, TimestampMixin):
    __tablename__ = "skill_drafts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("analysis_runs.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    markdown_text: Mapped[str] = mapped_column(Text)
    json_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    system_prompt: Mapped[str] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    project: Mapped[Project] = relationship(back_populates="skill_drafts")
    analysis_run: Mapped[AnalysisRun | None] = relationship(back_populates="skill_drafts")
    versions: Mapped[list["SkillVersion"]] = relationship(back_populates="draft")


class SkillVersion(Base):
    __tablename__ = "skill_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    draft_id: Mapped[str] = mapped_column(ForeignKey("skill_drafts.id"), index=True)
    version_number: Mapped[int] = mapped_column(Integer)
    markdown_text: Mapped[str] = mapped_column(Text)
    json_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    system_prompt: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    project: Mapped[Project] = relationship(back_populates="skill_versions")
    draft: Mapped[SkillDraft] = relationship(back_populates="versions")


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    project: Mapped[Project] = relationship(back_populates="chat_sessions")
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


class AppSetting(Base, TimestampMixin):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[dict[str, Any]] = mapped_column(JSON)
