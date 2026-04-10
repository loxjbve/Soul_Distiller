from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, or_, select
from sqlalchemy.orm import Session, selectinload

from app.llm.client import normalize_api_mode, normalize_provider_kind
from app.models import (
    AnalysisFacet,
    AnalysisEvent,
    AnalysisRun,
    AppSetting,
    ChatSession,
    ChatTurn,
    DocumentRecord,
    GeneratedArtifact,
    Project,
    SkillDraft,
    SkillVersion,
    TextChunk,
    utcnow,
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


def list_project_documents_by_ids(
    session: Session,
    project_id: str,
    document_ids: list[str],
) -> list[DocumentRecord]:
    if not document_ids:
        return []
    stmt = (
        select(DocumentRecord)
        .where(
            DocumentRecord.project_id == project_id,
            DocumentRecord.id.in_(document_ids),
        )
        .order_by(desc(DocumentRecord.created_at))
    )
    return list(session.scalars(stmt))


def search_project_documents(session: Session, project_id: str, query: str, *, limit: int = 8) -> list[DocumentRecord]:
    needle = f"%{query.strip()}%"
    stmt = (
        select(DocumentRecord)
        .where(
            DocumentRecord.project_id == project_id,
            or_(
                DocumentRecord.filename.ilike(needle),
                DocumentRecord.title.ilike(needle),
            ),
        )
        .order_by(desc(DocumentRecord.created_at))
        .limit(limit)
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
    stmt = select(AnalysisFacet).where(AnalysisFacet.run_id == run_id, AnalysisFacet.facet_key == facet_key)
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


def create_asset_draft(
    session: Session,
    *,
    project_id: str,
    run_id: str | None,
    asset_kind: str,
    markdown_text: str,
    json_payload: dict[str, Any],
    prompt_text: str,
    notes: str | None = None,
) -> SkillDraft:
    draft = SkillDraft(
        project_id=project_id,
        run_id=run_id,
        asset_kind=asset_kind,
        markdown_text=markdown_text,
        json_payload=json_payload,
        system_prompt=prompt_text,
        notes=notes,
    )
    session.add(draft)
    session.flush()
    return draft


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
    return create_asset_draft(
        session,
        project_id=project_id,
        run_id=run_id,
        asset_kind="skill",
        markdown_text=markdown_text,
        json_payload=json_payload,
        prompt_text=system_prompt,
        notes=notes,
    )


def get_latest_asset_draft(session: Session, project_id: str, *, asset_kind: str) -> SkillDraft | None:
    stmt = (
        select(SkillDraft)
        .where(SkillDraft.project_id == project_id, SkillDraft.asset_kind == asset_kind)
        .order_by(desc(SkillDraft.created_at))
    )
    return session.scalars(stmt).first()


def get_latest_skill_draft(session: Session, project_id: str) -> SkillDraft | None:
    return get_latest_asset_draft(session, project_id, asset_kind="skill")


def get_asset_draft(session: Session, draft_id: str, *, asset_kind: str | None = None) -> SkillDraft | None:
    stmt = select(SkillDraft).where(SkillDraft.id == draft_id)
    if asset_kind:
        stmt = stmt.where(SkillDraft.asset_kind == asset_kind)
    return session.scalar(stmt)


def get_skill_draft(session: Session, draft_id: str) -> SkillDraft | None:
    return get_asset_draft(session, draft_id, asset_kind="skill")


def list_asset_versions(session: Session, project_id: str, *, asset_kind: str) -> list[SkillVersion]:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return list(session.scalars(stmt))


def list_skill_versions(session: Session, project_id: str) -> list[SkillVersion]:
    return list_asset_versions(session, project_id, asset_kind="skill")


def get_latest_asset_version(session: Session, project_id: str, *, asset_kind: str) -> SkillVersion | None:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return session.scalars(stmt).first()


def get_latest_skill_version(session: Session, project_id: str) -> SkillVersion | None:
    return get_latest_asset_version(session, project_id, asset_kind="skill")


def publish_asset_draft(session: Session, project_id: str, draft: SkillDraft) -> SkillVersion:
    latest = get_latest_asset_version(session, project_id, asset_kind=draft.asset_kind)
    next_version = (latest.version_number if latest else 0) + 1
    version = SkillVersion(
        project_id=project_id,
        draft_id=draft.id,
        asset_kind=draft.asset_kind,
        version_number=next_version,
        markdown_text=draft.markdown_text,
        json_payload=draft.json_payload,
        system_prompt=draft.system_prompt,
    )
    session.add(version)
    session.flush()
    return version


def publish_skill_draft(session: Session, project_id: str, draft: SkillDraft) -> SkillVersion:
    if draft.asset_kind != "skill":
        raise ValueError("Draft is not a skill asset.")
    return publish_asset_draft(session, project_id, draft)


def create_chat_session(
    session: Session,
    *,
    project_id: str,
    session_kind: str,
    title: str | None = None,
) -> ChatSession:
    chat_session = ChatSession(
        project_id=project_id,
        session_kind=session_kind,
        title=(title or "").strip() or None,
        last_active_at=utcnow(),
    )
    session.add(chat_session)
    session.flush()
    return chat_session


def list_chat_sessions(session: Session, project_id: str, *, session_kind: str) -> list[ChatSession]:
    stmt = (
        select(ChatSession)
        .where(ChatSession.project_id == project_id, ChatSession.session_kind == session_kind)
        .order_by(desc(ChatSession.last_active_at), desc(ChatSession.created_at))
    )
    return list(session.scalars(stmt))


def get_or_create_chat_session(session: Session, project_id: str, *, session_kind: str = "playground") -> ChatSession:
    stmt = (
        select(ChatSession)
        .where(ChatSession.project_id == project_id, ChatSession.session_kind == session_kind)
        .options(selectinload(ChatSession.turns), selectinload(ChatSession.artifacts))
        .order_by(desc(ChatSession.last_active_at), desc(ChatSession.created_at))
    )
    chat_session = session.scalars(stmt).first()
    if chat_session:
        return chat_session
    return create_chat_session(session, project_id=project_id, session_kind=session_kind)


def get_chat_session(session: Session, session_id: str, *, session_kind: str | None = None) -> ChatSession | None:
    stmt = (
        select(ChatSession)
        .where(ChatSession.id == session_id)
        .options(selectinload(ChatSession.turns), selectinload(ChatSession.artifacts))
    )
    if session_kind:
        stmt = stmt.where(ChatSession.session_kind == session_kind)
    return session.scalar(stmt)


def rename_chat_session(session: Session, chat_session: ChatSession, *, title: str | None) -> ChatSession:
    chat_session.title = (title or "").strip() or None
    session.flush()
    return chat_session


def delete_chat_session(session: Session, chat_session: ChatSession) -> None:
    session.execute(delete(GeneratedArtifact).where(GeneratedArtifact.session_id == chat_session.id))
    session.execute(delete(ChatTurn).where(ChatTurn.session_id == chat_session.id))
    session.execute(delete(ChatSession).where(ChatSession.id == chat_session.id))


def add_chat_turn(
    session: Session,
    *,
    session_id: str,
    role: str,
    content: str,
    trace_json: dict[str, Any] | None = None,
) -> ChatTurn:
    chat_session = get_chat_session(session, session_id)
    if chat_session:
        chat_session.last_active_at = utcnow()
    turn = ChatTurn(session_id=session_id, role=role, content=content, trace_json=trace_json)
    session.add(turn)
    session.flush()
    return turn


def list_chat_turns(session: Session, session_id: str) -> list[ChatTurn]:
    stmt = select(ChatTurn).where(ChatTurn.session_id == session_id).order_by(ChatTurn.created_at)
    return list(session.scalars(stmt))


def create_generated_artifact(
    session: Session,
    *,
    project_id: str,
    session_id: str,
    turn_id: str | None,
    filename: str,
    mime_type: str | None,
    storage_path: str,
    summary: str | None = None,
) -> GeneratedArtifact:
    artifact = GeneratedArtifact(
        project_id=project_id,
        session_id=session_id,
        turn_id=turn_id,
        filename=filename,
        mime_type=mime_type,
        storage_path=storage_path,
        summary=summary,
    )
    session.add(artifact)
    session.flush()
    return artifact


def list_session_artifacts(session: Session, session_id: str, *, limit: int | None = None) -> list[GeneratedArtifact]:
    stmt = (
        select(GeneratedArtifact)
        .where(GeneratedArtifact.session_id == session_id)
        .order_by(desc(GeneratedArtifact.created_at))
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def get_generated_artifact(session: Session, artifact_id: str) -> GeneratedArtifact | None:
    stmt = select(GeneratedArtifact).where(GeneratedArtifact.id == artifact_id)
    return session.scalar(stmt)


def attach_artifacts_to_turn(session: Session, artifact_ids: list[str], *, turn_id: str) -> None:
    if not artifact_ids:
        return
    artifacts = list(
        session.scalars(select(GeneratedArtifact).where(GeneratedArtifact.id.in_(artifact_ids)))
    )
    for artifact in artifacts:
        artifact.turn_id = turn_id
    session.flush()


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
        model=(payload.get("model") or "").strip() or None,
        provider_kind=provider_kind,
        api_mode=normalize_api_mode(payload.get("api_mode")),
    )
