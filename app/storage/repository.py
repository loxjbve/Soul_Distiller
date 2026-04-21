from __future__ import annotations

from typing import Any
from uuid import uuid4

from sqlalchemy import delete, desc, func, or_, select
from sqlalchemy.orm import Session, defer, selectinload

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
    TelegramChat,
    TelegramMessage,
    TelegramParticipant,
    TelegramPreprocessTopUser,
    TelegramPreprocessActiveUser,
    TelegramPreprocessRun,
    TelegramPreprocessWeeklyTopicCandidate,
    TelegramPreprocessTopic,
    TelegramPreprocessTopicParticipant,
    TelegramTopicReport,
    TextChunk,
    utcnow,
)
from app.schemas import ServiceConfig

PROJECT_LIFECYCLE_ACTIVE = "active"
PROJECT_LIFECYCLE_DELETING = "deleting"
PROJECT_LIFECYCLE_DELETE_FAILED = "delete_failed"


def list_projects(session: Session) -> list[Project]:
    stmt = select(Project).where(Project.parent_id.is_(None)).order_by(desc(Project.updated_at))
    return list(session.scalars(stmt))


def list_child_projects(session: Session, parent_id: str) -> list[Project]:
    stmt = select(Project).where(Project.parent_id == parent_id).order_by(desc(Project.updated_at))
    return list(session.scalars(stmt))


def create_project(
    session: Session,
    name: str,
    description: str | None = None,
    mode: str = "group",
    parent_id: str | None = None,
) -> Project:
    project = Project(
        name=name.strip(),
        description=(description or "").strip() or None,
        mode=mode,
        parent_id=parent_id,
    )
    session.add(project)
    session.flush()
    return project


def get_target_project_id(session: Session, project_id: str) -> str:
    project = get_project(session, project_id)
    if project and project.parent_id:
        return project.parent_id
    return project_id





def get_project(session: Session, project_id: str) -> Project | None:
    stmt = select(Project).where(Project.id == project_id)
    return session.scalar(stmt)


def get_projects(session: Session, project_ids: list[str]) -> list[Project]:
    if not project_ids:
        return []
    stmt = select(Project).where(Project.id.in_(project_ids))
    return list(session.scalars(stmt))


def list_projects_by_lifecycle(session: Session, lifecycle_state: str) -> list[Project]:
    stmt = (
        select(Project)
        .where(Project.lifecycle_state == lifecycle_state)
        .order_by(desc(Project.updated_at))
    )
    return list(session.scalars(stmt))


def get_project_tree_ids(session: Session, project_id: str) -> list[str]:
    ordered_ids: list[str] = []
    seen: set[str] = set()
    frontier = [project_id]
    while frontier:
        next_frontier: list[str] = []
        for current_id in frontier:
            if current_id in seen:
                continue
            seen.add(current_id)
            ordered_ids.append(current_id)
        child_rows = session.scalars(select(Project.id).where(Project.parent_id.in_(frontier))).all()
        for child_id in child_rows:
            if child_id not in seen:
                next_frontier.append(child_id)
        frontier = next_frontier
    return ordered_ids


def mark_projects_for_deletion(session: Session, project_ids: list[str]) -> None:
    if not project_ids:
        return
    requested_at = utcnow()
    for project in get_projects(session, project_ids):
        project.lifecycle_state = PROJECT_LIFECYCLE_DELETING
        project.delete_requested_at = requested_at
        project.deletion_error = None
    session.flush()


def mark_projects_delete_failed(session: Session, project_ids: list[str], *, error: str) -> None:
    if not project_ids:
        return
    for project in get_projects(session, project_ids):
        project.lifecycle_state = PROJECT_LIFECYCLE_DELETE_FAILED
        project.deletion_error = error
    session.flush()


def restore_projects_active(session: Session, project_ids: list[str]) -> None:
    if not project_ids:
        return
    for project in get_projects(session, project_ids):
        project.lifecycle_state = PROJECT_LIFECYCLE_ACTIVE
        project.deletion_error = None
        project.delete_requested_at = None
    session.flush()


def delete_project(session: Session, project_id: str) -> None:
    session.execute(delete(Project).where(Project.id == project_id))


def delete_project_cascade(session: Session, project_id: str) -> None:
    child_ids = session.scalars(select(Project.id).where(Project.parent_id == project_id)).all()
    for cid in child_ids:
        delete_project_cascade(session, cid)

    session.execute(delete(TelegramPreprocessTopicParticipant).where(TelegramPreprocessTopicParticipant.run_id.in_(select(TelegramPreprocessRun.id).where(TelegramPreprocessRun.project_id == project_id))))
    session.execute(delete(TelegramPreprocessWeeklyTopicCandidate).where(TelegramPreprocessWeeklyTopicCandidate.project_id == project_id))
    session.execute(delete(TelegramPreprocessTopUser).where(TelegramPreprocessTopUser.project_id == project_id))
    session.execute(delete(TelegramPreprocessTopic).where(TelegramPreprocessTopic.project_id == project_id))
    session.execute(delete(TelegramPreprocessActiveUser).where(TelegramPreprocessActiveUser.project_id == project_id))
    session.execute(delete(TelegramPreprocessRun).where(TelegramPreprocessRun.project_id == project_id))
    session.execute(delete(TelegramTopicReport).where(TelegramTopicReport.project_id == project_id))
    session.execute(delete(TelegramMessage).where(TelegramMessage.project_id == project_id))
    session.execute(delete(TelegramParticipant).where(TelegramParticipant.project_id == project_id))
    session.execute(delete(TelegramChat).where(TelegramChat.project_id == project_id))
    session.execute(delete(TextChunk).where(TextChunk.project_id == project_id))
    session.execute(delete(DocumentRecord).where(DocumentRecord.project_id == project_id))
    session.execute(delete(AnalysisRun).where(AnalysisRun.project_id == project_id))
    session.execute(delete(SkillDraft).where(SkillDraft.project_id == project_id))
    session.execute(delete(SkillVersion).where(SkillVersion.project_id == project_id))
    session.execute(delete(ChatSession).where(ChatSession.project_id == project_id))
    session.execute(delete(Project).where(Project.id == project_id))


def list_analysis_run_ids_for_projects(session: Session, project_ids: list[str], *, limit: int) -> list[str]:
    if not project_ids or limit <= 0:
        return []
    stmt = select(AnalysisRun.id).where(AnalysisRun.project_id.in_(project_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def list_analysis_facet_ids_for_run_ids(session: Session, run_ids: list[str], *, limit: int) -> list[str]:
    if not run_ids or limit <= 0:
        return []
    stmt = select(AnalysisFacet.id).where(AnalysisFacet.run_id.in_(run_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def list_analysis_event_ids_for_run_ids(session: Session, run_ids: list[str], *, limit: int) -> list[str]:
    if not run_ids or limit <= 0:
        return []
    stmt = select(AnalysisEvent.id).where(AnalysisEvent.run_id.in_(run_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def list_chat_session_ids_for_projects(session: Session, project_ids: list[str], *, limit: int) -> list[str]:
    if not project_ids or limit <= 0:
        return []
    stmt = select(ChatSession.id).where(ChatSession.project_id.in_(project_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def list_chat_turn_ids_for_session_ids(session: Session, session_ids: list[str], *, limit: int) -> list[str]:
    if not session_ids or limit <= 0:
        return []
    stmt = select(ChatTurn.id).where(ChatTurn.session_id.in_(session_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def list_project_model_ids(session: Session, model, project_ids: list[str], *, limit: int) -> list[str]:
    if not project_ids or limit <= 0:
        return []
    stmt = select(model.id).where(model.project_id.in_(project_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def delete_analysis_facets_by_ids(session: Session, facet_ids: list[str]) -> int:
    if not facet_ids:
        return 0
    return session.execute(delete(AnalysisFacet).where(AnalysisFacet.id.in_(facet_ids))).rowcount or 0


def delete_analysis_events_by_ids(session: Session, event_ids: list[str]) -> int:
    if not event_ids:
        return 0
    return session.execute(delete(AnalysisEvent).where(AnalysisEvent.id.in_(event_ids))).rowcount or 0


def delete_chat_turns_by_ids(session: Session, turn_ids: list[str]) -> int:
    if not turn_ids:
        return 0
    return session.execute(delete(ChatTurn).where(ChatTurn.id.in_(turn_ids))).rowcount or 0


def delete_generated_artifacts_by_ids(session: Session, artifact_ids: list[str]) -> int:
    if not artifact_ids:
        return 0
    return session.execute(delete(GeneratedArtifact).where(GeneratedArtifact.id.in_(artifact_ids))).rowcount or 0


def delete_skill_versions_by_ids(session: Session, version_ids: list[str]) -> int:
    if not version_ids:
        return 0
    return session.execute(delete(SkillVersion).where(SkillVersion.id.in_(version_ids))).rowcount or 0


def delete_skill_drafts_by_ids(session: Session, draft_ids: list[str]) -> int:
    if not draft_ids:
        return 0
    return session.execute(delete(SkillDraft).where(SkillDraft.id.in_(draft_ids))).rowcount or 0


def delete_chat_sessions_by_ids(session: Session, session_ids: list[str]) -> int:
    if not session_ids:
        return 0
    return session.execute(delete(ChatSession).where(ChatSession.id.in_(session_ids))).rowcount or 0


def delete_text_chunks_by_ids(session: Session, chunk_ids: list[str]) -> int:
    if not chunk_ids:
        return 0
    return session.execute(delete(TextChunk).where(TextChunk.id.in_(chunk_ids))).rowcount or 0


def delete_documents_by_ids(session: Session, document_ids: list[str]) -> int:
    if not document_ids:
        return 0
    return session.execute(delete(DocumentRecord).where(DocumentRecord.id.in_(document_ids))).rowcount or 0


def delete_analysis_runs_by_ids(session: Session, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    return session.execute(delete(AnalysisRun).where(AnalysisRun.id.in_(run_ids))).rowcount or 0


def delete_projects_by_ids(session: Session, project_ids: list[str]) -> int:
    if not project_ids:
        return 0
    return session.execute(delete(Project).where(Project.id.in_(project_ids))).rowcount or 0


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


def list_project_documents(session: Session, project_id: str, *, limit: int | None = None, offset: int = 0) -> list[DocumentRecord]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(DocumentRecord)
        .where(DocumentRecord.project_id == target_project_id)
        .order_by(desc(DocumentRecord.created_at))
        .options(defer(DocumentRecord.raw_text), defer(DocumentRecord.clean_text))
    )
    if offset:
        stmt = stmt.offset(offset)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def count_project_documents(session: Session, project_id: str) -> dict[str, int]:
    target_project_id = get_target_project_id(session, project_id)
    total = session.scalar(
        select(func.count()).select_from(DocumentRecord).where(DocumentRecord.project_id == target_project_id)
    ) or 0
    grouped = session.execute(
        select(DocumentRecord.ingest_status, func.count())
        .where(DocumentRecord.project_id == target_project_id)
        .group_by(DocumentRecord.ingest_status)
    ).all()
    counts = {str(status or ""): int(count or 0) for status, count in grouped}
    ready = counts.get("ready", 0)
    failed = counts.get("failed", 0)
    queued = counts.get("queued", 0)
    processing = counts.get("processing", 0)
    pending = max(total - ready - failed - queued - processing, 0)
    return {
        "total": total,
        "ready": ready,
        "failed": failed,
        "queued": queued,
        "processing": processing,
        "pending": pending,
    }


def list_project_documents_by_ids(
    session: Session,
    project_id: str,
    document_ids: list[str],
) -> list[DocumentRecord]:
    target_project_id = get_target_project_id(session, project_id)
    if not document_ids:
        return []
    stmt = (
        select(DocumentRecord)
        .where(
            DocumentRecord.project_id == target_project_id,
            DocumentRecord.id.in_(document_ids),
        )
        .order_by(desc(DocumentRecord.created_at))
        .options(defer(DocumentRecord.raw_text), defer(DocumentRecord.clean_text))
    )
    return list(session.scalars(stmt))


def search_project_documents(session: Session, project_id: str, query: str, *, limit: int = 8) -> list[DocumentRecord]:
    target_project_id = get_target_project_id(session, project_id)
    needle = f"%{query.strip()}%"
    stmt = (
        select(DocumentRecord)
        .where(
            DocumentRecord.project_id == target_project_id,
            or_(
                DocumentRecord.filename.ilike(needle),
                DocumentRecord.title.ilike(needle),
            ),
        )
        .order_by(desc(DocumentRecord.created_at))
        .limit(limit)
        .options(defer(DocumentRecord.raw_text), defer(DocumentRecord.clean_text))
    )
    return list(session.scalars(stmt))


def get_document(session: Session, document_id: str) -> DocumentRecord | None:
    stmt = select(DocumentRecord).where(DocumentRecord.id == document_id)
    return session.scalar(stmt)


def list_telegram_chats(session: Session, project_id: str) -> list[TelegramChat]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramChat)
        .where(TelegramChat.project_id == target_project_id)
        .order_by(desc(TelegramChat.created_at))
    )
    return list(session.scalars(stmt))


def get_telegram_chat(session: Session, chat_id: str) -> TelegramChat | None:
    stmt = select(TelegramChat).where(TelegramChat.id == chat_id)
    return session.scalar(stmt)


def get_latest_telegram_chat(session: Session, project_id: str) -> TelegramChat | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramChat)
        .where(TelegramChat.project_id == target_project_id)
        .order_by(desc(TelegramChat.created_at))
    )
    return session.scalars(stmt).first()


def delete_telegram_export_by_document(session: Session, document_id: str) -> None:
    chat_ids = list(
        session.scalars(select(TelegramChat.id).where(TelegramChat.document_id == document_id))
    )
    if not chat_ids:
        return
    run_ids = list(
        session.scalars(select(TelegramPreprocessRun.id).where(TelegramPreprocessRun.chat_id.in_(chat_ids)))
    )
    if run_ids:
        session.execute(delete(TelegramPreprocessTopicParticipant).where(TelegramPreprocessTopicParticipant.run_id.in_(run_ids)))
        session.execute(delete(TelegramPreprocessWeeklyTopicCandidate).where(TelegramPreprocessWeeklyTopicCandidate.run_id.in_(run_ids)))
        session.execute(delete(TelegramPreprocessTopUser).where(TelegramPreprocessTopUser.run_id.in_(run_ids)))
        session.execute(delete(TelegramPreprocessTopic).where(TelegramPreprocessTopic.run_id.in_(run_ids)))
        session.execute(delete(TelegramPreprocessActiveUser).where(TelegramPreprocessActiveUser.run_id.in_(run_ids)))
        session.execute(delete(TelegramPreprocessRun).where(TelegramPreprocessRun.id.in_(run_ids)))
    session.execute(delete(TelegramTopicReport).where(TelegramTopicReport.chat_id.in_(chat_ids)))
    session.execute(delete(TelegramMessage).where(TelegramMessage.chat_id.in_(chat_ids)))
    session.execute(delete(TelegramParticipant).where(TelegramParticipant.chat_id.in_(chat_ids)))
    session.execute(delete(TelegramChat).where(TelegramChat.id.in_(chat_ids)))
    session.flush()


def replace_document_telegram_export(
    session: Session,
    *,
    project_id: str,
    document_id: str,
    chat_payload: dict[str, Any],
    participants: list[dict[str, Any]],
    messages: list[dict[str, Any]],
) -> TelegramChat:
    delete_telegram_export_by_document(session, document_id)

    chat = TelegramChat(
        project_id=project_id,
        document_id=document_id,
        telegram_chat_id=str(chat_payload.get("telegram_chat_id") or "") or None,
        chat_type=str(chat_payload.get("chat_type") or "") or None,
        title=str(chat_payload.get("title") or "") or None,
        message_count=int(chat_payload.get("message_count") or 0),
        participant_count=int(chat_payload.get("participant_count") or 0),
        metadata_json=chat_payload.get("metadata_json"),
    )
    session.add(chat)
    session.flush()

    participant_id_by_key: dict[str, str] = {}
    created_participants: list[TelegramParticipant] = []
    for payload in participants:
        participant = TelegramParticipant(
            project_id=project_id,
            chat_id=chat.id,
            participant_key=str(payload.get("participant_key") or ""),
            telegram_user_id=str(payload.get("telegram_user_id") or "") or None,
            display_name=str(payload.get("display_name") or "") or None,
            username=str(payload.get("username") or "") or None,
            first_seen_at=payload.get("first_seen_at"),
            last_seen_at=payload.get("last_seen_at"),
            message_count=int(payload.get("message_count") or 0),
            service_event_count=int(payload.get("service_event_count") or 0),
            metadata_json=payload.get("metadata_json"),
        )
        session.add(participant)
        created_participants.append(participant)
    session.flush()

    for participant in created_participants:
        participant_id_by_key[participant.participant_key] = participant.id

    for payload in messages:
        participant_key = str(payload.get("participant_key") or "")
        session.add(
            TelegramMessage(
                project_id=project_id,
                chat_id=chat.id,
                participant_id=participant_id_by_key.get(participant_key) if participant_key else None,
                telegram_message_id=payload.get("telegram_message_id"),
                message_type=str(payload.get("message_type") or "message"),
                sent_at=payload.get("sent_at"),
                sent_at_text=str(payload.get("sent_at_text") or "") or None,
                unix_ts=payload.get("unix_ts"),
                sender_name=str(payload.get("sender_name") or "") or None,
                sender_ref=str(payload.get("sender_ref") or "") or None,
                reply_to_message_id=payload.get("reply_to_message_id"),
                reply_to_peer_id=str(payload.get("reply_to_peer_id") or "") or None,
                media_type=str(payload.get("media_type") or "") or None,
                action_type=str(payload.get("action_type") or "") or None,
                file_path=str(payload.get("file_path") or "") or None,
                file_name=str(payload.get("file_name") or "") or None,
                mime_type=str(payload.get("mime_type") or "") or None,
                width=payload.get("width"),
                height=payload.get("height"),
                duration_seconds=payload.get("duration_seconds"),
                forwarded_from=str(payload.get("forwarded_from") or "") or None,
                forwarded_from_id=str(payload.get("forwarded_from_id") or "") or None,
                text_normalized=str(payload.get("text_normalized") or ""),
                text_raw_json=payload.get("text_raw_json"),
                reactions_json=payload.get("reactions_json"),
                metadata_json=payload.get("metadata_json"),
            )
        )
    session.flush()
    return chat


def list_telegram_participants(
    session: Session,
    project_id: str,
    *,
    chat_id: str | None = None,
    limit: int | None = None,
) -> list[TelegramParticipant]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramParticipant)
        .where(TelegramParticipant.project_id == target_project_id)
        .order_by(TelegramParticipant.message_count.desc(), TelegramParticipant.display_name.asc())
    )
    if chat_id:
        stmt = stmt.where(TelegramParticipant.chat_id == chat_id)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def list_telegram_messages(
    session: Session,
    project_id: str,
    *,
    chat_id: str | None = None,
    participant_ids: list[str] | None = None,
    text_query: str | None = None,
    message_id_start: int | None = None,
    message_id_end: int | None = None,
    limit: int | None = None,
    ascending: bool = True,
) -> list[TelegramMessage]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = select(TelegramMessage).where(TelegramMessage.project_id == target_project_id)
    if chat_id:
        stmt = stmt.where(TelegramMessage.chat_id == chat_id)
    if participant_ids:
        stmt = stmt.where(TelegramMessage.participant_id.in_(participant_ids))
    if text_query:
        needle = f"%{text_query.strip()}%"
        stmt = stmt.where(TelegramMessage.text_normalized.ilike(needle))
    if message_id_start is not None:
        stmt = stmt.where(TelegramMessage.telegram_message_id >= int(message_id_start))
    if message_id_end is not None:
        stmt = stmt.where(TelegramMessage.telegram_message_id <= int(message_id_end))
    order_column = TelegramMessage.telegram_message_id.asc() if ascending else TelegramMessage.telegram_message_id.desc()
    stmt = stmt.order_by(order_column)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def get_telegram_message_by_telegram_id(
    session: Session,
    project_id: str,
    telegram_message_id: int,
) -> TelegramMessage | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramMessage)
        .where(
            TelegramMessage.project_id == target_project_id,
            TelegramMessage.telegram_message_id == telegram_message_id,
        )
        .order_by(desc(TelegramMessage.sent_at))
    )
    return session.scalars(stmt).first()


def get_telegram_message_context(
    session: Session,
    project_id: str,
    telegram_message_id: int,
    *,
    before: int = 3,
    after: int = 3,
) -> list[TelegramMessage]:
    message = get_telegram_message_by_telegram_id(session, project_id, telegram_message_id)
    if not message or message.telegram_message_id is None:
        return []
    return list_telegram_messages(
        session,
        project_id,
        chat_id=message.chat_id,
        message_id_start=message.telegram_message_id - max(before, 0),
        message_id_end=message.telegram_message_id + max(after, 0),
        limit=max(before, 0) + max(after, 0) + 1,
        ascending=True,
    )


def replace_telegram_topic_reports(
    session: Session,
    *,
    project_id: str,
    chat_id: str,
    reports: list[dict[str, Any]],
) -> list[TelegramTopicReport]:
    session.execute(delete(TelegramTopicReport).where(TelegramTopicReport.chat_id == chat_id))
    created: list[TelegramTopicReport] = []
    for payload in reports:
        report = TelegramTopicReport(
            project_id=project_id,
            chat_id=chat_id,
            stage_index=int(payload.get("stage_index") or 0),
            status=str(payload.get("status") or "completed"),
            title=str(payload.get("title") or "") or None,
            summary=str(payload.get("summary") or ""),
            time_summary=str(payload.get("time_summary") or "") or None,
            start_message_id=payload.get("start_message_id"),
            end_message_id=payload.get("end_message_id"),
            start_at=payload.get("start_at"),
            end_at=payload.get("end_at"),
            message_count=int(payload.get("message_count") or 0),
            participant_count=int(payload.get("participant_count") or 0),
            topics_json=payload.get("topics_json"),
            participants_json=payload.get("participants_json"),
            evidence_json=payload.get("evidence_json"),
            metadata_json=payload.get("metadata_json"),
            llm_model=str(payload.get("llm_model") or "") or None,
        )
        session.add(report)
        created.append(report)
    session.flush()
    return created


def list_telegram_topic_reports(
    session: Session,
    project_id: str,
    *,
    chat_id: str | None = None,
    limit: int | None = None,
) -> list[TelegramTopicReport]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramTopicReport)
        .where(TelegramTopicReport.project_id == target_project_id)
        .order_by(TelegramTopicReport.stage_index.asc(), TelegramTopicReport.created_at.asc())
    )
    if chat_id:
        stmt = stmt.where(TelegramTopicReport.chat_id == chat_id)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def create_telegram_preprocess_run(
    session: Session,
    *,
    project_id: str,
    chat_id: str | None,
    status: str = "queued",
    llm_model: str | None = None,
    summary_json: dict[str, Any] | None = None,
) -> TelegramPreprocessRun:
    run = TelegramPreprocessRun(
        project_id=project_id,
        chat_id=chat_id,
        status=status,
        llm_model=llm_model,
        summary_json=summary_json,
    )
    session.add(run)
    session.flush()
    return run


def get_telegram_preprocess_run(session: Session, run_id: str) -> TelegramPreprocessRun | None:
    stmt = (
        select(TelegramPreprocessRun)
        .where(TelegramPreprocessRun.id == run_id)
        .options(
            selectinload(TelegramPreprocessRun.top_users),
            selectinload(TelegramPreprocessRun.weekly_topic_candidates),
            selectinload(TelegramPreprocessRun.topics).selectinload(TelegramPreprocessTopic.participants),
            selectinload(TelegramPreprocessRun.active_users),
        )
    )
    return session.scalar(stmt)


def list_telegram_preprocess_runs(session: Session, project_id: str, *, limit: int | None = None) -> list[TelegramPreprocessRun]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessRun)
        .where(TelegramPreprocessRun.project_id == target_project_id)
        .order_by(desc(TelegramPreprocessRun.created_at))
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def get_latest_telegram_preprocess_run(
    session: Session,
    project_id: str,
    *,
    status: str | None = None,
) -> TelegramPreprocessRun | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessRun)
        .where(TelegramPreprocessRun.project_id == target_project_id)
        .order_by(desc(TelegramPreprocessRun.created_at))
    )
    if status:
        stmt = stmt.where(TelegramPreprocessRun.status == status)
    return session.scalars(stmt).first()


def get_latest_successful_telegram_preprocess_run(session: Session, project_id: str) -> TelegramPreprocessRun | None:
    return get_latest_telegram_preprocess_run(session, project_id, status="completed")


def get_active_telegram_preprocess_run(session: Session, project_id: str) -> TelegramPreprocessRun | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessRun)
        .where(
            TelegramPreprocessRun.project_id == target_project_id,
            TelegramPreprocessRun.status.in_(("queued", "running")),
        )
        .order_by(desc(TelegramPreprocessRun.created_at))
    )
    return session.scalars(stmt).first()


def get_latest_resumable_telegram_preprocess_run(session: Session, project_id: str) -> TelegramPreprocessRun | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessRun)
        .where(
            TelegramPreprocessRun.project_id == target_project_id,
            TelegramPreprocessRun.status.in_(("failed", "cancelled", "queued", "running")),
        )
        .order_by(desc(TelegramPreprocessRun.created_at))
    )
    return session.scalars(stmt).first()


def replace_telegram_preprocess_top_users(
    session: Session,
    *,
    run_id: str,
    project_id: str,
    chat_id: str | None,
    top_users: list[dict[str, Any]],
) -> list[TelegramPreprocessTopUser]:
    session.execute(delete(TelegramPreprocessTopUser).where(TelegramPreprocessTopUser.run_id == run_id))
    created: list[TelegramPreprocessTopUser] = []
    for payload in top_users:
        participant_id = str(payload.get("participant_id") or "").strip()
        if not participant_id:
            continue
        item = TelegramPreprocessTopUser(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            rank=int(payload.get("rank") or len(created) + 1),
            participant_id=participant_id,
            uid=str(payload.get("uid") or "") or None,
            username=str(payload.get("username") or "") or None,
            display_name=str(payload.get("display_name") or "") or None,
            message_count=int(payload.get("message_count") or 0),
            first_seen_at=payload.get("first_seen_at"),
            last_seen_at=payload.get("last_seen_at"),
            metadata_json=dict(payload.get("metadata_json") or {}),
        )
        session.add(item)
        created.append(item)
    session.flush()
    return created


def list_telegram_preprocess_top_users(
    session: Session,
    project_id: str,
    *,
    run_id: str,
) -> list[TelegramPreprocessTopUser]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessTopUser)
        .where(
            TelegramPreprocessTopUser.project_id == target_project_id,
            TelegramPreprocessTopUser.run_id == run_id,
        )
        .options(selectinload(TelegramPreprocessTopUser.participant))
        .order_by(TelegramPreprocessTopUser.rank.asc(), TelegramPreprocessTopUser.created_at.asc())
    )
    return list(session.scalars(stmt))


def get_telegram_preprocess_top_user(session: Session, top_user_id: str) -> TelegramPreprocessTopUser | None:
    stmt = (
        select(TelegramPreprocessTopUser)
        .where(TelegramPreprocessTopUser.id == top_user_id)
        .options(selectinload(TelegramPreprocessTopUser.participant))
    )
    return session.scalar(stmt)


def replace_telegram_preprocess_weekly_topic_candidates(
    session: Session,
    *,
    run_id: str,
    project_id: str,
    chat_id: str | None,
    weekly_candidates: list[dict[str, Any]],
) -> list[TelegramPreprocessWeeklyTopicCandidate]:
    session.execute(delete(TelegramPreprocessWeeklyTopicCandidate).where(TelegramPreprocessWeeklyTopicCandidate.run_id == run_id))
    created: list[TelegramPreprocessWeeklyTopicCandidate] = []
    for payload in weekly_candidates:
        item = TelegramPreprocessWeeklyTopicCandidate(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            week_key=str(payload.get("week_key") or "").strip(),
            start_at=payload.get("start_at"),
            end_at=payload.get("end_at"),
            start_message_id=payload.get("start_message_id"),
            end_message_id=payload.get("end_message_id"),
            message_count=int(payload.get("message_count") or 0),
            participant_count=int(payload.get("participant_count") or 0),
            top_participants_json=list(payload.get("top_participants_json") or []),
            sample_messages_json=list(payload.get("sample_messages_json") or []),
            metadata_json=dict(payload.get("metadata_json") or {}),
        )
        session.add(item)
        created.append(item)
    session.flush()
    return created


def list_telegram_preprocess_weekly_topic_candidates(
    session: Session,
    project_id: str,
    *,
    run_id: str,
) -> list[TelegramPreprocessWeeklyTopicCandidate]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessWeeklyTopicCandidate)
        .where(
            TelegramPreprocessWeeklyTopicCandidate.project_id == target_project_id,
            TelegramPreprocessWeeklyTopicCandidate.run_id == run_id,
        )
        .order_by(
            TelegramPreprocessWeeklyTopicCandidate.week_key.asc(),
            TelegramPreprocessWeeklyTopicCandidate.start_at.asc(),
            TelegramPreprocessWeeklyTopicCandidate.created_at.asc(),
        )
    )
    return list(session.scalars(stmt))


def get_telegram_preprocess_weekly_topic_candidate(
    session: Session,
    candidate_id: str,
) -> TelegramPreprocessWeeklyTopicCandidate | None:
    stmt = select(TelegramPreprocessWeeklyTopicCandidate).where(TelegramPreprocessWeeklyTopicCandidate.id == candidate_id)
    return session.scalar(stmt)


def replace_telegram_preprocess_topics(
    session: Session,
    *,
    run_id: str,
    project_id: str,
    chat_id: str | None,
    topics: list[dict[str, Any]],
) -> list[TelegramPreprocessTopic]:
    existing_topic_ids = list(
        session.scalars(select(TelegramPreprocessTopic.id).where(TelegramPreprocessTopic.run_id == run_id))
    )
    if existing_topic_ids:
        session.execute(delete(TelegramPreprocessTopicParticipant).where(TelegramPreprocessTopicParticipant.topic_id.in_(existing_topic_ids)))
    session.execute(delete(TelegramPreprocessTopic).where(TelegramPreprocessTopic.run_id == run_id))

    created: list[TelegramPreprocessTopic] = []
    for payload in topics:
        topic = TelegramPreprocessTopic(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            topic_index=int(payload.get("topic_index") or len(created) + 1),
            title=str(payload.get("title") or "").strip() or f"Topic {len(created) + 1}",
            summary=str(payload.get("summary") or "").strip(),
            start_at=payload.get("start_at"),
            end_at=payload.get("end_at"),
            start_message_id=payload.get("start_message_id"),
            end_message_id=payload.get("end_message_id"),
            message_count=int(payload.get("message_count") or 0),
            participant_count=int(payload.get("participant_count") or 0),
            keywords_json=list(payload.get("keywords_json") or payload.get("keywords") or []),
            evidence_json=list(payload.get("evidence_json") or []),
            metadata_json=dict(payload.get("metadata_json") or {}),
        )
        session.add(topic)
        created.append(topic)
    session.flush()

    topic_id_by_index = {topic.topic_index: topic.id for topic in created}
    topic_id_by_title = {topic.title: topic.id for topic in created}
    for payload in topics:
        topic_id = payload.get("topic_id")
        if not topic_id:
            topic_id = topic_id_by_index.get(int(payload.get("topic_index") or 0))
        if not topic_id:
            topic_id = topic_id_by_title.get(str(payload.get("title") or "").strip())
        if not topic_id:
            continue
        for participant_payload in payload.get("participants") or []:
            participant_id = str(participant_payload.get("participant_id") or "").strip()
            if not participant_id:
                continue
            session.add(
                TelegramPreprocessTopicParticipant(
                    run_id=run_id,
                    topic_id=topic_id,
                    participant_id=participant_id,
                    role_hint=str(participant_payload.get("role_hint") or "").strip() or None,
                    message_count=int(participant_payload.get("message_count") or 0),
                    mention_count=int(participant_payload.get("mention_count") or 0),
                )
            )
    session.flush()
    return created


def replace_telegram_preprocess_active_users(
    session: Session,
    *,
    run_id: str,
    project_id: str,
    chat_id: str | None,
    active_users: list[dict[str, Any]],
) -> list[TelegramPreprocessActiveUser]:
    session.execute(delete(TelegramPreprocessActiveUser).where(TelegramPreprocessActiveUser.run_id == run_id))
    created: list[TelegramPreprocessActiveUser] = []
    for payload in active_users:
        participant_id = str(payload.get("participant_id") or "").strip()
        if not participant_id:
            continue
        item = TelegramPreprocessActiveUser(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            participant_id=participant_id,
            rank=int(payload.get("rank") or len(created) + 1),
            uid=str(payload.get("uid") or "") or None,
            username=str(payload.get("username") or "") or None,
            display_name=str(payload.get("display_name") or "") or None,
            primary_alias=str(payload.get("primary_alias") or "") or None,
            aliases_json=list(payload.get("aliases_json") or []),
            message_count=int(payload.get("message_count") or 0),
            first_seen_at=payload.get("first_seen_at"),
            last_seen_at=payload.get("last_seen_at"),
            evidence_json=list(payload.get("evidence_json") or []),
        )
        session.add(item)
        created.append(item)
    session.flush()
    return created


def list_telegram_preprocess_topics(
    session: Session,
    project_id: str,
    *,
    run_id: str,
) -> list[TelegramPreprocessTopic]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessTopic)
        .where(
            TelegramPreprocessTopic.project_id == target_project_id,
            TelegramPreprocessTopic.run_id == run_id,
        )
        .options(
            selectinload(TelegramPreprocessTopic.participants).selectinload(TelegramPreprocessTopicParticipant.participant)
        )
        .order_by(TelegramPreprocessTopic.topic_index.asc(), TelegramPreprocessTopic.start_at.asc())
    )
    return list(session.scalars(stmt))


def list_telegram_preprocess_active_users(
    session: Session,
    project_id: str,
    *,
    run_id: str,
) -> list[TelegramPreprocessActiveUser]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessActiveUser)
        .where(
            TelegramPreprocessActiveUser.project_id == target_project_id,
            TelegramPreprocessActiveUser.run_id == run_id,
        )
        .options(selectinload(TelegramPreprocessActiveUser.participant))
        .order_by(TelegramPreprocessActiveUser.rank.asc(), TelegramPreprocessActiveUser.created_at.asc())
    )
    return list(session.scalars(stmt))


def search_telegram_participants(
    session: Session,
    project_id: str,
    query: str,
    *,
    limit: int = 20,
) -> list[TelegramParticipant]:
    target_project_id = get_target_project_id(session, project_id)
    needle = f"%{query.strip()}%"
    stmt = (
        select(TelegramParticipant)
        .where(
            TelegramParticipant.project_id == target_project_id,
            or_(
                TelegramParticipant.display_name.ilike(needle),
                TelegramParticipant.username.ilike(needle),
                TelegramParticipant.telegram_user_id.ilike(needle),
                TelegramParticipant.participant_key.ilike(needle),
            ),
        )
        .order_by(TelegramParticipant.message_count.desc(), TelegramParticipant.display_name.asc())
        .limit(limit)
    )
    return list(session.scalars(stmt))


def get_telegram_participant(session: Session, participant_id: str) -> TelegramParticipant | None:
    return session.get(TelegramParticipant, participant_id)


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
    delete_telegram_export_by_document(session, document.id)
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


def get_latest_analysis_run(session: Session, project_id: str, *, load_facets: bool = True, load_events: bool = True) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.project_id == project_id)
        .order_by(desc(AnalysisRun.created_at))
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalars(stmt).first()


def get_active_analysis_run(session: Session, project_id: str, *, load_facets: bool = True, load_events: bool = True) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(
            AnalysisRun.project_id == project_id,
            AnalysisRun.status.in_(("queued", "running")),
        )
        .order_by(desc(AnalysisRun.created_at))
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalars(stmt).first()


def list_active_analysis_runs(session: Session, *, load_facets: bool = True, load_events: bool = True) -> list[AnalysisRun]:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.status.in_(("queued", "running")))
        .order_by(desc(AnalysisRun.created_at))
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return list(session.scalars(stmt))


def get_analysis_run(session: Session, run_id: str, *, load_facets: bool = True, load_events: bool = True) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.id == run_id)
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalar(stmt)


def list_analysis_runs(session: Session, project_id: str, *, load_facets: bool = True, load_events: bool = True) -> list[AnalysisRun]:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.project_id == project_id)
        .order_by(desc(AnalysisRun.created_at))
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
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
    return get_latest_asset_draft(session, project_id, asset_kind="cc_skill")


def get_asset_draft(session: Session, draft_id: str, *, asset_kind: str | None = None) -> SkillDraft | None:
    stmt = select(SkillDraft).where(SkillDraft.id == draft_id)
    if asset_kind:
        stmt = stmt.where(SkillDraft.asset_kind == asset_kind)
    return session.scalar(stmt)


def get_skill_draft(session: Session, draft_id: str) -> SkillDraft | None:
    return get_asset_draft(session, draft_id, asset_kind="cc_skill")


def get_asset_version(session: Session, version_id: str, *, asset_kind: str | None = None) -> SkillVersion | None:
    stmt = select(SkillVersion).where(SkillVersion.id == version_id)
    if asset_kind:
        stmt = stmt.where(SkillVersion.asset_kind == asset_kind)
    return session.scalar(stmt)


def list_asset_versions(session: Session, project_id: str, *, asset_kind: str) -> list[SkillVersion]:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return list(session.scalars(stmt))


def list_skill_versions(session: Session, project_id: str) -> list[SkillVersion]:
    return list_asset_versions(session, project_id, asset_kind="cc_skill")


def get_latest_asset_version(session: Session, project_id: str, *, asset_kind: str) -> SkillVersion | None:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return session.scalars(stmt).first()


def delete_asset_version(session: Session, version: SkillVersion) -> None:
    session.execute(delete(SkillVersion).where(SkillVersion.id == version.id))


def get_latest_skill_version(session: Session, project_id: str) -> SkillVersion | None:
    return get_latest_asset_version(session, project_id, asset_kind="cc_skill")


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
    if draft.asset_kind not in {"skill", "cc_skill"}:
        raise ValueError("Draft is not a Claude Code Skill asset.")
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


def get_service_setting_bundle(
    session: Session,
    key: str,
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> dict[str, Any]:
    setting = get_setting(session, key)
    return _normalize_service_setting_bundle(
        setting.value_json if setting else {},
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )


def upsert_service_setting_bundle(
    session: Session,
    key: str,
    bundle: dict[str, Any],
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> AppSetting:
    normalized_bundle = _normalize_service_setting_bundle(
        bundle,
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )
    return upsert_setting(session, key, normalized_bundle)


def get_service_setting_config(
    session: Session,
    key: str,
    config_id: str,
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> dict[str, Any] | None:
    bundle = get_service_setting_bundle(
        session,
        key,
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )
    target_id = str(config_id or "").strip()
    if not target_id:
        return None
    for config in bundle["configs"]:
        if config["id"] == target_id:
            return dict(config)
    return None


def get_service_config(session: Session, key: str) -> ServiceConfig | None:
    bundle = get_service_setting_bundle(session, key)
    ordered_configs = _ordered_service_setting_configs(bundle)
    resolved_configs = [config for config in (_build_service_config(item) for item in ordered_configs) if config]
    if not resolved_configs:
        return None
    primary, *fallbacks = resolved_configs
    primary.fallbacks = fallbacks
    return primary


def _normalize_service_setting_bundle(
    payload: dict[str, Any] | None,
    *,
    default_provider: str,
    default_api_mode: str,
) -> dict[str, Any]:
    source = dict(payload or {})
    raw_configs = source.get("configs") if isinstance(source.get("configs"), list) else None
    configs: list[dict[str, Any]] = []

    if raw_configs is None:
        configs.append(
            _normalize_service_setting_config(
                source,
                default_provider=default_provider,
                default_api_mode=default_api_mode,
                fallback_label="Default",
            )
        )
    else:
        for index, item in enumerate(raw_configs, start=1):
            config_payload = item if isinstance(item, dict) else {}
            configs.append(
                _normalize_service_setting_config(
                    config_payload,
                    default_provider=default_provider,
                    default_api_mode=default_api_mode,
                    fallback_label=f"Config {index}",
                )
            )

    if not configs:
        configs.append(
            _normalize_service_setting_config(
                {},
                default_provider=default_provider,
                default_api_mode=default_api_mode,
                fallback_label="Default",
            )
        )

    config_ids = [config["id"] for config in configs]
    active_config_id = str(source.get("active_config_id") or "").strip()
    if active_config_id not in config_ids:
        active_config_id = config_ids[0]

    fallback_order: list[str] = []
    seen_ids = {active_config_id}
    for item in source.get("fallback_order") or []:
        config_id = str(item or "").strip()
        if config_id and config_id in config_ids and config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)
    for config_id in config_ids:
        if config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)

    return {
        "version": 2,
        "active_config_id": active_config_id,
        "fallback_order": fallback_order,
        "configs": configs,
    }


def _normalize_service_setting_config(
    payload: dict[str, Any] | None,
    *,
    default_provider: str,
    default_api_mode: str,
    fallback_label: str,
) -> dict[str, Any]:
    source = dict(payload or {})
    normalized_base_url = str(source.get("base_url") or "").strip()
    normalized_provider = normalize_provider_kind(
        source.get("provider_kind") or ("openai-compatible" if normalized_base_url else default_provider)
    )
    available_models: list[str] = []
    seen_models: set[str] = set()
    for item in source.get("available_models") or []:
        model_name = str(item or "").strip()
        if model_name and model_name not in seen_models:
            available_models.append(model_name)
            seen_models.add(model_name)
    return {
        "id": str(source.get("id") or uuid4().hex).strip() or uuid4().hex,
        "label": str(source.get("label") or fallback_label).strip() or fallback_label,
        "provider_kind": normalized_provider,
        "base_url": normalized_base_url,
        "api_key": str(source.get("api_key") or "").strip(),
        "model": str(source.get("model") or "").strip(),
        "api_mode": normalize_api_mode(source.get("api_mode") or default_api_mode),
        "available_models": available_models,
    }


def _ordered_service_setting_configs(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    configs = [dict(item) for item in bundle.get("configs") or [] if isinstance(item, dict)]
    if not configs:
        return []
    by_id = {config["id"]: config for config in configs}
    ordered_ids = [bundle.get("active_config_id"), *(bundle.get("fallback_order") or [])]
    ordered_configs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for config_id in ordered_ids:
        normalized_id = str(config_id or "").strip()
        if normalized_id and normalized_id in by_id and normalized_id not in seen_ids:
            ordered_configs.append(by_id[normalized_id])
            seen_ids.add(normalized_id)
    for config in configs:
        if config["id"] not in seen_ids:
            ordered_configs.append(config)
            seen_ids.add(config["id"])
    return ordered_configs


def _build_service_config(payload: dict[str, Any]) -> ServiceConfig | None:
    api_key = str(payload.get("api_key") or "").strip()
    if not api_key:
        return None
    base_url = str(payload.get("base_url") or "").strip() or None
    provider_kind = normalize_provider_kind(
        payload.get("provider_kind") or ("openai-compatible" if base_url else "openai")
    )
    if provider_kind == "openai-compatible" and not base_url:
        return None
    return ServiceConfig(
        base_url=base_url,
        api_key=api_key,
        model=str(payload.get("model") or "").strip() or None,
        provider_kind=provider_kind,
        api_mode=normalize_api_mode(payload.get("api_mode")),
    )
