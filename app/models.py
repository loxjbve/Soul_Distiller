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
    generated_artifacts: Mapped[list["GeneratedArtifact"]] = relationship(back_populates="project")
    telegram_chats: Mapped[list["TelegramChat"]] = relationship(back_populates="project")
    telegram_participants: Mapped[list["TelegramParticipant"]] = relationship(back_populates="project")
    telegram_messages: Mapped[list["TelegramMessage"]] = relationship(back_populates="project")
    telegram_reports: Mapped[list["TelegramTopicReport"]] = relationship(back_populates="project")
    telegram_preprocess_runs: Mapped[list["TelegramPreprocessRun"]] = relationship(back_populates="project")
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
    asset_kind: Mapped[str] = mapped_column(String(32), default="skill", index=True)
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
    asset_kind: Mapped[str] = mapped_column(String(32), default="skill", index=True)
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
    session_kind: Mapped[str] = mapped_column(String(32), default="playground", index=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    project: Mapped[Project] = relationship(back_populates="chat_sessions")
    turns: Mapped[list["ChatTurn"]] = relationship(back_populates="session")
    artifacts: Mapped[list["GeneratedArtifact"]] = relationship(back_populates="session")


class ChatTurn(Base):
    __tablename__ = "chat_turns"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(ForeignKey("chat_sessions.id"), index=True)
    role: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text)
    trace_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    session: Mapped[ChatSession] = relationship(back_populates="turns")
    artifacts: Mapped[list["GeneratedArtifact"]] = relationship(back_populates="turn")


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

    project: Mapped[Project] = relationship(back_populates="generated_artifacts")
    session: Mapped[ChatSession] = relationship(back_populates="artifacts")
    turn: Mapped[ChatTurn | None] = relationship(back_populates="artifacts")


class TelegramChat(Base, TimestampMixin):
    __tablename__ = "telegram_chats"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("documents.id"), index=True)
    telegram_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    chat_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped[Project] = relationship(back_populates="telegram_chats")
    document: Mapped[DocumentRecord] = relationship(back_populates="telegram_chats")
    participants: Mapped[list["TelegramParticipant"]] = relationship(back_populates="chat")
    messages: Mapped[list["TelegramMessage"]] = relationship(back_populates="chat")
    reports: Mapped[list["TelegramTopicReport"]] = relationship(back_populates="chat")
    preprocess_runs: Mapped[list["TelegramPreprocessRun"]] = relationship(back_populates="chat")
    preprocess_top_users: Mapped[list["TelegramPreprocessTopUser"]] = relationship(back_populates="chat")
    preprocess_weekly_topic_candidates: Mapped[list["TelegramPreprocessWeeklyTopicCandidate"]] = relationship(back_populates="chat")
    preprocess_topics: Mapped[list["TelegramPreprocessTopic"]] = relationship(back_populates="chat")
    preprocess_active_users: Mapped[list["TelegramPreprocessActiveUser"]] = relationship(back_populates="chat")
    relationship_snapshots: Mapped[list["TelegramRelationshipSnapshot"]] = relationship(back_populates="chat")


class TelegramParticipant(Base, TimestampMixin):
    __tablename__ = "telegram_participants"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str] = mapped_column(ForeignKey("telegram_chats.id"), index=True)
    participant_key: Mapped[str] = mapped_column(String(255), index=True)
    telegram_user_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    service_event_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped[Project] = relationship(back_populates="telegram_participants")
    chat: Mapped[TelegramChat] = relationship(back_populates="participants")
    messages: Mapped[list["TelegramMessage"]] = relationship(back_populates="participant")
    preprocess_topic_links: Mapped[list["TelegramPreprocessTopicParticipant"]] = relationship(back_populates="participant")
    preprocess_topic_quotes: Mapped[list["TelegramPreprocessTopicQuote"]] = relationship(back_populates="participant")
    preprocess_active_users: Mapped[list["TelegramPreprocessActiveUser"]] = relationship(back_populates="participant")


class TelegramMessage(Base):
    __tablename__ = "telegram_messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str] = mapped_column(ForeignKey("telegram_chats.id"), index=True)
    participant_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_participants.id"), nullable=True, index=True)
    telegram_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    message_type: Mapped[str] = mapped_column(String(32), default="message", index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    sent_at_text: Mapped[str | None] = mapped_column(String(64), nullable=True)
    unix_ts: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    sender_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sender_ref: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    reply_to_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    reply_to_peer_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    media_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    action_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    file_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forwarded_from: Mapped[str | None] = mapped_column(String(255), nullable=True)
    forwarded_from_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    text_normalized: Mapped[str] = mapped_column(Text)
    text_raw_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSON, nullable=True)
    reactions_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSON, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped[Project] = relationship(back_populates="telegram_messages")
    chat: Mapped[TelegramChat] = relationship(back_populates="messages")
    participant: Mapped[TelegramParticipant | None] = relationship(back_populates="messages")


class TelegramTopicReport(Base, TimestampMixin):
    __tablename__ = "telegram_topic_reports"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str] = mapped_column(ForeignKey("telegram_chats.id"), index=True)
    stage_index: Mapped[int] = mapped_column(Integer, index=True)
    status: Mapped[str] = mapped_column(String(32), default="completed", index=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    summary: Mapped[str] = mapped_column(Text)
    time_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    end_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    start_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    topics_json: Mapped[list[dict[str, Any]] | list[str] | None] = mapped_column(JSON, nullable=True)
    participants_json: Mapped[list[dict[str, Any]] | list[str] | None] = mapped_column(JSON, nullable=True)
    evidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    llm_model: Mapped[str | None] = mapped_column(String(255), nullable=True)

    project: Mapped[Project] = relationship(back_populates="telegram_reports")
    chat: Mapped[TelegramChat] = relationship(back_populates="reports")


class TelegramPreprocessRun(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    llm_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    progress_percent: Mapped[int] = mapped_column(Integer, default=0)
    current_stage: Mapped[str | None] = mapped_column(String(128), nullable=True)
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cache_creation_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cache_read_tokens: Mapped[int] = mapped_column(Integer, default=0)
    window_count: Mapped[int] = mapped_column(Integer, default=0)
    topic_count: Mapped[int] = mapped_column(Integer, default=0)
    active_user_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_runs")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="preprocess_runs")
    top_users: Mapped[list["TelegramPreprocessTopUser"]] = relationship(back_populates="run")
    weekly_topic_candidates: Mapped[list["TelegramPreprocessWeeklyTopicCandidate"]] = relationship(back_populates="run")
    topics: Mapped[list["TelegramPreprocessTopic"]] = relationship(back_populates="run")
    topic_quotes: Mapped[list["TelegramPreprocessTopicQuote"]] = relationship(back_populates="run")
    active_users: Mapped[list["TelegramPreprocessActiveUser"]] = relationship(back_populates="run")
    relationship_snapshot: Mapped["TelegramRelationshipSnapshot | None"] = relationship(
        back_populates="run",
        uselist=False,
    )


class TelegramPreprocessTopUser(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_top_users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    rank: Mapped[int] = mapped_column(Integer, default=0)
    participant_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="top_users")
    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_top_users")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="preprocess_top_users")
    participant: Mapped[TelegramParticipant] = relationship()


class TelegramPreprocessWeeklyTopicCandidate(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_weekly_topic_candidates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    week_key: Mapped[str] = mapped_column(String(32), index=True)
    window_index: Mapped[int] = mapped_column(Integer, default=1)
    start_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    start_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    end_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    top_participants_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    sample_messages_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="weekly_topic_candidates")
    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_weekly_topic_candidates")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="preprocess_weekly_topic_candidates")


class TelegramPreprocessTopic(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_topics"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    week_key: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    topic_index: Mapped[int] = mapped_column(Integer, default=0)
    week_topic_index: Mapped[int] = mapped_column(Integer, default=0)
    title: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(Text)
    start_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    start_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    end_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    keywords_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    evidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="topics")
    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_topics")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="preprocess_topics")
    participants: Mapped[list["TelegramPreprocessTopicParticipant"]] = relationship(back_populates="topic")
    quotes: Mapped[list["TelegramPreprocessTopicQuote"]] = relationship(back_populates="topic")


class TelegramPreprocessTopicParticipant(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_topic_participants"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_topics.id"), index=True)
    participant_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    role_hint: Mapped[str | None] = mapped_column(String(128), nullable=True)
    stance_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    mention_count: Mapped[int] = mapped_column(Integer, default=0)

    topic: Mapped[TelegramPreprocessTopic] = relationship(back_populates="participants")
    participant: Mapped[TelegramParticipant] = relationship(back_populates="preprocess_topic_links")

class TelegramPreprocessTopicQuote(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_topic_quotes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_topics.id"), index=True)
    participant_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    rank: Mapped[int] = mapped_column(Integer, default=1)
    telegram_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    quote: Mapped[str] = mapped_column(Text)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="topic_quotes")
    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_topic_quotes")
    topic: Mapped[TelegramPreprocessTopic] = relationship(back_populates="quotes")
    participant: Mapped[TelegramParticipant] = relationship(back_populates="preprocess_topic_quotes")


class TelegramPreprocessActiveUser(Base, TimestampMixin):
    __tablename__ = "telegram_preprocess_active_users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    participant_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    rank: Mapped[int] = mapped_column(Integer, default=0)
    uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    aliases_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    evidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="active_users")
    project: Mapped[Project] = relationship(back_populates="telegram_preprocess_active_users")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="preprocess_active_users")
    participant: Mapped[TelegramParticipant] = relationship(back_populates="preprocess_active_users")


class TelegramRelationshipSnapshot(Base, TimestampMixin):
    __tablename__ = "telegram_relationship_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(ForeignKey("telegram_preprocess_runs.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    chat_id: Mapped[str | None] = mapped_column(ForeignKey("telegram_chats.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    analyzed_user_count: Mapped[int] = mapped_column(Integer, default=0)
    candidate_pair_count: Mapped[int] = mapped_column(Integer, default=0)
    llm_pair_count: Mapped[int] = mapped_column(Integer, default=0)
    label_scheme: Mapped[str] = mapped_column(String(64), default="friendly|neutral|tense|unclear")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[TelegramPreprocessRun] = relationship(back_populates="relationship_snapshot")
    project: Mapped[Project] = relationship(back_populates="telegram_relationship_snapshots")
    chat: Mapped[TelegramChat | None] = relationship(back_populates="relationship_snapshots")
    edges: Mapped[list["TelegramRelationshipEdge"]] = relationship(back_populates="snapshot")


class TelegramRelationshipEdge(Base, TimestampMixin):
    __tablename__ = "telegram_relationship_edges"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    snapshot_id: Mapped[str] = mapped_column(ForeignKey("telegram_relationship_snapshots.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    participant_a_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    participant_b_id: Mapped[str] = mapped_column(ForeignKey("telegram_participants.id"), index=True)
    interaction_strength: Mapped[float] = mapped_column(Float, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    relation_label: Mapped[str] = mapped_column(String(32), default="unclear", index=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    counterevidence_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    snapshot: Mapped[TelegramRelationshipSnapshot] = relationship(back_populates="edges")
    project: Mapped[Project] = relationship(back_populates="telegram_relationship_edges")


class AppSetting(Base, TimestampMixin):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[dict[str, Any]] = mapped_column(JSON)
