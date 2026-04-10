from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session, selectinload

from app.llm.client import normalize_provider_kind
from app.models import (
    AnalysisFacet,
    AnalysisEvent,
    AnalysisRun,
    AppSetting,
    ChatSession,
    ChatTurn,
    DocumentRecord,
    Project,
    SkillDraft,
    SkillVersion,
    TextChunk,
)
from app.schemas import ServiceConfig


def list_projects(session: Session) -> list[Project]:
    stmt = select(Project).order_by(desc(Project.updated_at))
    return list(session.scalars(stmt))


def create_project(session: Session, name: str, description: str | None = None) -> Project:
    project = Project(name=name.strip(), description=(description or "").strip() or None)
    session.add(project)
    session.flush()
    return project


def get_project(session: Session, project_id: str) -> Project | None:
    stmt = select(Project).where(Project.id == project_id)
    return session.scalar(stmt)


def delete_project(session: Session, project_id: str) -> None:
    session.execute(delete(Project).where(Project.id == project_id))


def create_document(session: Session, **kwargs: Any) -> DocumentRecord:
    document = DocumentRecord(**kwargs)
    session.add(document)
    session.flush()
    return document


def replace_document_chunks(session: Session, document_id: str, chunks: list[dict[str, Any]]) -> list[TextChunk]:
    session.execute(delete(TextChunk).where(TextChunk.document_id == document_id))
    created: list[TextChunk] = []
    for payload in chunks:
        chunk = TextChunk(document_id=document_id, **payload)
        session.add(chunk)
        created.append(chunk)
    session.flush()
    return created


def list_project_documents(session: Session, project_id: str) -> list[DocumentRecord]:
    stmt = (
        select(DocumentRecord)
        .where(DocumentRecord.project_id == project_id)
        .order_by(desc(DocumentRecord.created_at))
    )
    return list(session.scalars(stmt))


def get_document(session: Session, document_id: str) -> DocumentRecord | None:
    stmt = select(DocumentRecord).where(DocumentRecord.id == document_id)
    return session.scalar(stmt)


def update_document(
    session: Session,
    document: DocumentRecord,
    *,
    title: str | None = None,
    source_type: str | None = None,
    user_note: str | None = None,
) -> DocumentRecord:
    document.title = (title or "").strip() or document.filename
    if source_type:
        document.source_type = source_type.strip()
    metadata = dict(document.metadata_json or {})
    metadata["user_note"] = (user_note or "").strip()
    document.metadata_json = metadata
    session.flush()
    return document


def delete_document(session: Session, document: DocumentRecord) -> None:
    session.execute(delete(TextChunk).where(TextChunk.document_id == document.id))
    session.execute(delete(DocumentRecord).where(DocumentRecord.id == document.id))


def create_analysis_run(
    session: Session,
    project_id: str,
    status: str = "running",
    summary_json: dict[str, Any] | None = None,
) -> AnalysisRun:
    run = AnalysisRun(project_id=project_id, status=status, summary_json=summary_json)
    session.add(run)
    session.flush()
    return run


def get_latest_analysis_run(session: Session, project_id: str) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.project_id == project_id)
        .options(selectinload(AnalysisRun.facets), selectinload(AnalysisRun.events))
        .order_by(desc(AnalysisRun.created_at))
    )
    return session.scalars(stmt).first()


def get_active_analysis_run(session: Session, project_id: str) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(
            AnalysisRun.project_id == project_id,
            AnalysisRun.status.in_(("queued", "running")),
        )
        .options(selectinload(AnalysisRun.facets), selectinload(AnalysisRun.events))
        .order_by(desc(AnalysisRun.created_at))
    )
    return session.scalars(stmt).first()


def get_analysis_run(session: Session, run_id: str) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.id == run_id)
        .options(selectinload(AnalysisRun.facets), selectinload(AnalysisRun.events))
    )
    return session.scalar(stmt)


def list_analysis_runs(session: Session, project_id: str) -> list[AnalysisRun]:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.project_id == project_id)
        .options(selectinload(AnalysisRun.facets), selectinload(AnalysisRun.events))
        .order_by(desc(AnalysisRun.created_at))
    )
    return list(session.scalars(stmt))


def get_facet(session: Session, run_id: str, facet_key: str) -> AnalysisFacet | None:
    stmt = (
        select(AnalysisFacet)
        .where(AnalysisFacet.run_id == run_id, AnalysisFacet.facet_key == facet_key)
    )
    return session.scalar(stmt)


def add_analysis_event(
    session: Session,
    run_id: str,
    *,
    event_type: str,
    message: str,
    level: str = "info",
    payload_json: dict[str, Any] | None = None,
) -> AnalysisEvent:
    event = AnalysisEvent(
        run_id=run_id,
        event_type=event_type,
        level=level,
        message=message,
        payload_json=payload_json,
    )
    session.add(event)
    session.flush()
    return event


def upsert_facet(
    session: Session,
    run_id: str,
    facet_key: str,
    *,
    status: str,
    confidence: float,
    findings_json: dict[str, Any],
    evidence_json: list[dict[str, Any]],
    conflicts_json: list[dict[str, Any]],
    error_message: str | None = None,
) -> AnalysisFacet:
    facet = get_facet(session, run_id, facet_key)
    if not facet:
        facet = AnalysisFacet(run_id=run_id, facet_key=facet_key)
        session.add(facet)
    facet.status = status
    facet.confidence = confidence
    facet.findings_json = findings_json
    facet.evidence_json = evidence_json
    facet.conflicts_json = conflicts_json
    facet.error_message = error_message
    session.flush()
    return facet


def create_skill_draft(
    session: Session,
    *,
    project_id: str,
    run_id: str | None,
    markdown_text: str,
    json_payload: dict[str, Any],
    system_prompt: str,
    notes: str | None = None,
) -> SkillDraft:
    draft = SkillDraft(
        project_id=project_id,
        run_id=run_id,
        markdown_text=markdown_text,
        json_payload=json_payload,
        system_prompt=system_prompt,
        notes=notes,
    )
    session.add(draft)
    session.flush()
    return draft


def get_latest_skill_draft(session: Session, project_id: str) -> SkillDraft | None:
    stmt = (
        select(SkillDraft)
        .where(SkillDraft.project_id == project_id)
        .order_by(desc(SkillDraft.created_at))
    )
    return session.scalars(stmt).first()


def get_skill_draft(session: Session, draft_id: str) -> SkillDraft | None:
    stmt = select(SkillDraft).where(SkillDraft.id == draft_id)
    return session.scalar(stmt)


def list_skill_versions(session: Session, project_id: str) -> list[SkillVersion]:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id)
        .order_by(desc(SkillVersion.version_number))
    )
    return list(session.scalars(stmt))


def get_latest_skill_version(session: Session, project_id: str) -> SkillVersion | None:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id)
        .order_by(desc(SkillVersion.version_number))
    )
    return session.scalars(stmt).first()


def publish_skill_draft(session: Session, project_id: str, draft: SkillDraft) -> SkillVersion:
    latest = get_latest_skill_version(session, project_id)
    next_version = (latest.version_number if latest else 0) + 1
    version = SkillVersion(
        project_id=project_id,
        draft_id=draft.id,
        version_number=next_version,
        markdown_text=draft.markdown_text,
        json_payload=draft.json_payload,
        system_prompt=draft.system_prompt,
    )
    session.add(version)
    session.flush()
    return version


def get_or_create_chat_session(session: Session, project_id: str) -> ChatSession:
    stmt = (
        select(ChatSession)
        .where(ChatSession.project_id == project_id)
        .options(selectinload(ChatSession.turns))
        .order_by(desc(ChatSession.created_at))
    )
    chat_session = session.scalars(stmt).first()
    if chat_session:
        return chat_session
    chat_session = ChatSession(project_id=project_id)
    session.add(chat_session)
    session.flush()
    return chat_session


def add_chat_turn(
    session: Session,
    *,
    session_id: str,
    role: str,
    content: str,
    trace_json: dict[str, Any] | None = None,
) -> ChatTurn:
    turn = ChatTurn(session_id=session_id, role=role, content=content, trace_json=trace_json)
    session.add(turn)
    session.flush()
    return turn


def get_chat_session(session: Session, session_id: str) -> ChatSession | None:
    stmt = (
        select(ChatSession)
        .where(ChatSession.id == session_id)
        .options(selectinload(ChatSession.turns))
    )
    return session.scalar(stmt)


def upsert_setting(session: Session, key: str, value_json: dict[str, Any]) -> AppSetting:
    setting = session.get(AppSetting, key)
    if not setting:
        setting = AppSetting(key=key, value_json=value_json)
        session.add(setting)
    else:
        setting.value_json = value_json
    session.flush()
    return setting


def get_setting(session: Session, key: str) -> AppSetting | None:
    return session.get(AppSetting, key)


def get_service_config(session: Session, key: str) -> ServiceConfig | None:
    setting = get_setting(session, key)
    if not setting:
        return None
    payload = setting.value_json or {}
    api_key = (payload.get("api_key") or "").strip()
    if not api_key:
        return None
    base_url = (payload.get("base_url") or "").strip() or None
    provider_kind = normalize_provider_kind(
        payload.get("provider_kind") or ("openai-compatible" if base_url else "openai")
    )
    if provider_kind == "openai-compatible" and not base_url:
        return None
    return ServiceConfig(
        base_url=base_url,
        api_key=api_key,
        model=payload.get("model") or None,
        provider_kind=provider_kind,
    )
