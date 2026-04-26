from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin


class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(String(32), default="group")
    parent_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), nullable=True)
    lifecycle_state: Mapped[str] = mapped_column(String(32), default="active", index=True)
    delete_requested_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    deletion_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    documents: Mapped[list["DocumentRecord"]] = relationship(back_populates="project")
    analysis_runs: Mapped[list["AnalysisRun"]] = relationship(back_populates="project")
    skill_drafts: Mapped[list["SkillDraft"]] = relationship(back_populates="project")
    skill_versions: Mapped[list["SkillVersion"]] = relationship(back_populates="project")
    chat_sessions: Mapped[list["ChatSession"]] = relationship(back_populates="project")
    telegram_chats: Mapped[list["TelegramChat"]] = relationship(back_populates="project")
    telegram_participants: Mapped[list["TelegramParticipant"]] = relationship(back_populates="project")
    telegram_messages: Mapped[list["TelegramMessage"]] = relationship(back_populates="project")
    telegram_reports: Mapped[list["TelegramTopicReport"]] = relationship(back_populates="project")
    telegram_preprocess_runs: Mapped[list["TelegramPreprocessRun"]] = relationship(back_populates="project")
    stone_preprocess_runs: Mapped[list["StonePreprocessRun"]] = relationship(back_populates="project")
    telegram_preprocess_top_users: Mapped[list["TelegramPreprocessTopUser"]] = relationship(back_populates="project")
    telegram_preprocess_weekly_topic_candidates: Mapped[list["TelegramPreprocessWeeklyTopicCandidate"]] = relationship(back_populates="project")
    telegram_preprocess_topics: Mapped[list["TelegramPreprocessTopic"]] = relationship(back_populates="project")
    telegram_preprocess_topic_quotes: Mapped[list["TelegramPreprocessTopicQuote"]] = relationship(back_populates="project")
    telegram_preprocess_active_users: Mapped[list["TelegramPreprocessActiveUser"]] = relationship(back_populates="project")
    telegram_relationship_snapshots: Mapped[list["TelegramRelationshipSnapshot"]] = relationship(back_populates="project")
    telegram_relationship_edges: Mapped[list["TelegramRelationshipEdge"]] = relationship(back_populates="project")


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
    telegram_chats: Mapped[list["TelegramChat"]] = relationship(back_populates="document")


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


__all__ = ["DocumentRecord", "Project", "TextChunk"]
