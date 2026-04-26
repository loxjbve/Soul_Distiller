from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, func, or_, select
from sqlalchemy.orm import Session, defer

from app.db.models import (
    DocumentRecord,
    TelegramChat,
    TelegramMessage,
    TelegramParticipant,
    TelegramPreprocessActiveUser,
    TelegramPreprocessRun,
    TelegramPreprocessTopUser,
    TelegramPreprocessTopic,
    TelegramPreprocessTopicParticipant,
    TelegramPreprocessTopicQuote,
    TelegramPreprocessWeeklyTopicCandidate,
    TelegramRelationshipEdge,
    TelegramRelationshipSnapshot,
    TelegramTopicReport,
    TextChunk,
)
from app.db.repositories.projects import get_target_project_id


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


def list_project_documents_by_ids(session: Session, project_id: str, document_ids: list[str]) -> list[DocumentRecord]:
    target_project_id = get_target_project_id(session, project_id)
    if not document_ids:
        return []
    stmt = (
        select(DocumentRecord)
        .where(DocumentRecord.project_id == target_project_id, DocumentRecord.id.in_(document_ids))
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
            or_(DocumentRecord.filename.ilike(needle), DocumentRecord.title.ilike(needle)),
        )
        .order_by(desc(DocumentRecord.created_at))
        .limit(limit)
        .options(defer(DocumentRecord.raw_text), defer(DocumentRecord.clean_text))
    )
    return list(session.scalars(stmt))


def get_document(session: Session, document_id: str) -> DocumentRecord | None:
    stmt = select(DocumentRecord).where(DocumentRecord.id == document_id)
    return session.scalar(stmt)


def delete_telegram_export_by_document(session: Session, document_id: str) -> None:
    chat_ids = list(session.scalars(select(TelegramChat.id).where(TelegramChat.document_id == document_id)))
    if not chat_ids:
        return
    run_ids = list(session.scalars(select(TelegramPreprocessRun.id).where(TelegramPreprocessRun.chat_id.in_(chat_ids))))
    if run_ids:
        snapshot_ids = list(
            session.scalars(select(TelegramRelationshipSnapshot.id).where(TelegramRelationshipSnapshot.run_id.in_(run_ids)))
        )
        if snapshot_ids:
            session.execute(delete(TelegramRelationshipEdge).where(TelegramRelationshipEdge.snapshot_id.in_(snapshot_ids)))
            session.execute(delete(TelegramRelationshipSnapshot).where(TelegramRelationshipSnapshot.id.in_(snapshot_ids)))
        session.execute(delete(TelegramPreprocessTopicQuote).where(TelegramPreprocessTopicQuote.run_id.in_(run_ids)))
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


def delete_text_chunks_by_ids(session: Session, chunk_ids: list[str]) -> int:
    if not chunk_ids:
        return 0
    return session.execute(delete(TextChunk).where(TextChunk.id.in_(chunk_ids))).rowcount or 0


def delete_documents_by_ids(session: Session, document_ids: list[str]) -> int:
    if not document_ids:
        return 0
    return session.execute(delete(DocumentRecord).where(DocumentRecord.id.in_(document_ids))).rowcount or 0


__all__ = [name for name in globals() if not name.startswith("_")]
