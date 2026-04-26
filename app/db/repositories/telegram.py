from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, desc, or_, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import (
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
)
from app.db.repositories.documents import (
    delete_telegram_export_by_document,
    replace_document_telegram_export,
)
from app.db.repositories.projects import get_target_project_id
from app.db.repositories.stone import (
    create_stone_preprocess_run,
    get_active_stone_preprocess_run,
    get_latest_resumable_stone_preprocess_run,
    get_latest_stone_preprocess_run,
    get_latest_successful_stone_preprocess_run,
    get_stone_preprocess_run,
    list_stone_preprocess_runs,
)


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
            selectinload(TelegramPreprocessRun.topics)
            .selectinload(TelegramPreprocessTopic.participants)
            .selectinload(TelegramPreprocessTopicParticipant.participant),
            selectinload(TelegramPreprocessRun.topics)
            .selectinload(TelegramPreprocessTopic.quotes)
            .selectinload(TelegramPreprocessTopicQuote.participant),
            selectinload(TelegramPreprocessRun.topic_quotes),
            selectinload(TelegramPreprocessRun.active_users),
        )
    )
    return session.scalar(stmt)


def list_telegram_preprocess_runs(
    session: Session,
    project_id: str,
    *,
    limit: int | None = None,
) -> list[TelegramPreprocessRun]:
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
    session.execute(
        delete(TelegramPreprocessWeeklyTopicCandidate).where(TelegramPreprocessWeeklyTopicCandidate.run_id == run_id)
    )
    created: list[TelegramPreprocessWeeklyTopicCandidate] = []
    for payload in weekly_candidates:
        item = TelegramPreprocessWeeklyTopicCandidate(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            week_key=str(payload.get("week_key") or "").strip(),
            window_index=max(1, int(payload.get("window_index") or len(created) + 1)),
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
            TelegramPreprocessWeeklyTopicCandidate.window_index.asc(),
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
        session.execute(
            delete(TelegramPreprocessTopicQuote).where(TelegramPreprocessTopicQuote.topic_id.in_(existing_topic_ids))
        )
        session.execute(
            delete(TelegramPreprocessTopicParticipant).where(
                TelegramPreprocessTopicParticipant.topic_id.in_(existing_topic_ids)
            )
        )
    session.execute(delete(TelegramPreprocessTopic).where(TelegramPreprocessTopic.run_id == run_id))

    created: list[TelegramPreprocessTopic] = []
    for payload in topics:
        metadata = dict(payload.get("metadata_json") or {})
        topic = TelegramPreprocessTopic(
            run_id=run_id,
            project_id=project_id,
            chat_id=chat_id,
            topic_index=int(payload.get("topic_index") or len(created) + 1),
            week_key=str(payload.get("week_key") or metadata.get("week_key") or "").strip() or None,
            week_topic_index=int(payload.get("week_topic_index") or payload.get("topic_index") or len(created) + 1),
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
            metadata_json=metadata,
        )
        session.add(topic)
        created.append(topic)
    session.flush()

    for topic, payload in zip(created, topics):
        topic_id = topic.id
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
                    stance_summary=str(participant_payload.get("stance_summary") or "").strip() or None,
                    message_count=int(participant_payload.get("message_count") or 0),
                    mention_count=int(participant_payload.get("mention_count") or 0),
                )
            )
        quote_payloads = list(payload.get("participant_quotes") or [])
        for participant_payload in payload.get("participants") or []:
            participant_id = str(participant_payload.get("participant_id") or "").strip()
            if not participant_id:
                continue
            for quote_payload in participant_payload.get("quotes") or []:
                quote_payloads.append({"participant_id": participant_id, **dict(quote_payload or {})})
        for rank, quote_payload in enumerate(quote_payloads, start=1):
            participant_id = str(quote_payload.get("participant_id") or "").strip()
            quote_text = str(quote_payload.get("quote") or "").strip()
            if not participant_id or not quote_text:
                continue
            sent_at = quote_payload.get("sent_at")
            if isinstance(sent_at, str) and sent_at.strip():
                try:
                    sent_at = datetime.fromisoformat(sent_at)
                except ValueError:
                    sent_at = None
            session.add(
                TelegramPreprocessTopicQuote(
                    run_id=run_id,
                    project_id=project_id,
                    topic_id=topic_id,
                    participant_id=participant_id,
                    rank=int(quote_payload.get("rank") or rank),
                    telegram_message_id=(
                        int(quote_payload.get("message_id")) if quote_payload.get("message_id") is not None else None
                    ),
                    sent_at=sent_at,
                    quote=quote_text,
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
            selectinload(TelegramPreprocessTopic.participants).selectinload(TelegramPreprocessTopicParticipant.participant),
            selectinload(TelegramPreprocessTopic.quotes).selectinload(TelegramPreprocessTopicQuote.participant),
        )
        .order_by(
            TelegramPreprocessTopic.week_key.asc(),
            TelegramPreprocessTopic.week_topic_index.asc(),
            TelegramPreprocessTopic.topic_index.asc(),
            TelegramPreprocessTopic.start_at.asc(),
        )
    )
    return list(session.scalars(stmt))


def list_telegram_preprocess_topics_for_participant(
    session: Session,
    project_id: str,
    *,
    run_id: str,
    participant_id: str,
) -> list[TelegramPreprocessTopic]:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramPreprocessTopic)
        .join(TelegramPreprocessTopicParticipant, TelegramPreprocessTopicParticipant.topic_id == TelegramPreprocessTopic.id)
        .where(
            TelegramPreprocessTopic.project_id == target_project_id,
            TelegramPreprocessTopic.run_id == run_id,
            TelegramPreprocessTopicParticipant.participant_id == participant_id,
        )
        .options(
            selectinload(TelegramPreprocessTopic.participants).selectinload(TelegramPreprocessTopicParticipant.participant),
            selectinload(TelegramPreprocessTopic.quotes).selectinload(TelegramPreprocessTopicQuote.participant),
        )
        .order_by(
            TelegramPreprocessTopic.week_key.asc(),
            TelegramPreprocessTopic.week_topic_index.asc(),
            TelegramPreprocessTopic.topic_index.asc(),
            TelegramPreprocessTopic.start_at.asc(),
        )
    )
    return list(session.scalars(stmt).unique())


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


def create_or_replace_telegram_relationship_snapshot(
    session: Session,
    *,
    run_id: str,
    project_id: str,
    chat_id: str | None,
    status: str = "running",
    analyzed_user_count: int = 0,
    candidate_pair_count: int = 0,
    llm_pair_count: int = 0,
    label_scheme: str = "friendly|neutral|tense|unclear",
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error_message: str | None = None,
    summary_json: dict[str, Any] | None = None,
) -> TelegramRelationshipSnapshot:
    existing = get_telegram_relationship_snapshot_for_run(session, run_id)
    if existing:
        session.execute(delete(TelegramRelationshipEdge).where(TelegramRelationshipEdge.snapshot_id == existing.id))
        session.execute(delete(TelegramRelationshipSnapshot).where(TelegramRelationshipSnapshot.id == existing.id))
        session.flush()

    snapshot = TelegramRelationshipSnapshot(
        run_id=run_id,
        project_id=project_id,
        chat_id=chat_id,
        status=status,
        analyzed_user_count=analyzed_user_count,
        candidate_pair_count=candidate_pair_count,
        llm_pair_count=llm_pair_count,
        label_scheme=label_scheme,
        started_at=started_at,
        finished_at=finished_at,
        error_message=error_message,
        summary_json=dict(summary_json or {}),
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def replace_telegram_relationship_edges(
    session: Session,
    *,
    snapshot_id: str,
    project_id: str,
    edges: list[dict[str, Any]],
) -> list[TelegramRelationshipEdge]:
    session.execute(delete(TelegramRelationshipEdge).where(TelegramRelationshipEdge.snapshot_id == snapshot_id))
    created: list[TelegramRelationshipEdge] = []
    for payload in edges:
        participant_a_id = str(payload.get("participant_a_id") or "").strip()
        participant_b_id = str(payload.get("participant_b_id") or "").strip()
        if not participant_a_id or not participant_b_id or participant_a_id == participant_b_id:
            continue
        if participant_b_id < participant_a_id:
            participant_a_id, participant_b_id = participant_b_id, participant_a_id
        item = TelegramRelationshipEdge(
            snapshot_id=snapshot_id,
            project_id=project_id,
            participant_a_id=participant_a_id,
            participant_b_id=participant_b_id,
            interaction_strength=float(payload.get("interaction_strength") or 0.0),
            confidence=float(payload.get("confidence") or 0.0),
            relation_label=str(payload.get("relation_label") or "unclear").strip() or "unclear",
            summary=str(payload.get("summary") or "").strip() or None,
            evidence_json=list(payload.get("evidence_json") or []),
            counterevidence_json=list(payload.get("counterevidence_json") or []),
            metrics_json=dict(payload.get("metrics_json") or {}),
        )
        session.add(item)
        created.append(item)
    session.flush()
    return created


def get_telegram_relationship_snapshot(session: Session, snapshot_id: str) -> TelegramRelationshipSnapshot | None:
    stmt = (
        select(TelegramRelationshipSnapshot)
        .where(TelegramRelationshipSnapshot.id == snapshot_id)
        .options(selectinload(TelegramRelationshipSnapshot.edges))
    )
    return session.scalar(stmt)


def get_telegram_relationship_snapshot_for_run(
    session: Session,
    run_id: str,
) -> TelegramRelationshipSnapshot | None:
    stmt = (
        select(TelegramRelationshipSnapshot)
        .where(TelegramRelationshipSnapshot.run_id == run_id)
        .options(selectinload(TelegramRelationshipSnapshot.edges))
    )
    return session.scalar(stmt)


def get_latest_telegram_relationship_snapshot(
    session: Session,
    project_id: str,
    *,
    status: str | None = None,
) -> TelegramRelationshipSnapshot | None:
    target_project_id = get_target_project_id(session, project_id)
    stmt = (
        select(TelegramRelationshipSnapshot)
        .where(TelegramRelationshipSnapshot.project_id == target_project_id)
        .order_by(desc(TelegramRelationshipSnapshot.created_at))
        .options(selectinload(TelegramRelationshipSnapshot.edges))
    )
    if status:
        stmt = stmt.where(TelegramRelationshipSnapshot.status == status)
    return session.scalars(stmt).first()


def list_telegram_relationship_edges(
    session: Session,
    snapshot_id: str,
) -> list[TelegramRelationshipEdge]:
    stmt = (
        select(TelegramRelationshipEdge)
        .where(TelegramRelationshipEdge.snapshot_id == snapshot_id)
        .order_by(TelegramRelationshipEdge.interaction_strength.desc(), TelegramRelationshipEdge.created_at.asc())
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


__all__ = [name for name in globals() if not name.startswith("_")]
