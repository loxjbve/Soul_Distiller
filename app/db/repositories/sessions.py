from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import ChatSession, ChatTurn, GeneratedArtifact, utcnow

CHAT_SESSION_KIND_PLAYGROUND = "playground"
CHAT_SESSION_KIND_WRITING = "writing"
SUPPORTED_CHAT_SESSION_KINDS = frozenset({CHAT_SESSION_KIND_PLAYGROUND, CHAT_SESSION_KIND_WRITING})


def _require_supported_session_kind(session_kind: str) -> str:
    normalized = str(session_kind or "").strip().lower()
    if normalized not in SUPPORTED_CHAT_SESSION_KINDS:
        raise ValueError(f"Unsupported chat session kind: {session_kind!r}")
    return normalized


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


def delete_chat_sessions_by_ids(session: Session, session_ids: list[str]) -> int:
    if not session_ids:
        return 0
    return session.execute(delete(ChatSession).where(ChatSession.id.in_(session_ids))).rowcount or 0


def delete_chat_turns_by_ids(session: Session, turn_ids: list[str]) -> int:
    if not turn_ids:
        return 0
    return session.execute(delete(ChatTurn).where(ChatTurn.id.in_(turn_ids))).rowcount or 0


def create_chat_session(
    session: Session,
    *,
    project_id: str,
    session_kind: str,
    title: str | None = None,
) -> ChatSession:
    normalized_kind = _require_supported_session_kind(session_kind)
    chat_session = ChatSession(
        project_id=project_id,
        session_kind=normalized_kind,
        title=(title or "").strip() or None,
        last_active_at=utcnow(),
    )
    session.add(chat_session)
    session.flush()
    return chat_session


def list_chat_sessions(session: Session, project_id: str, *, session_kind: str) -> list[ChatSession]:
    normalized_kind = _require_supported_session_kind(session_kind)
    stmt = (
        select(ChatSession)
        .where(ChatSession.project_id == project_id, ChatSession.session_kind == normalized_kind)
        .order_by(desc(ChatSession.last_active_at), desc(ChatSession.created_at))
    )
    return list(session.scalars(stmt))


def get_or_create_chat_session(session: Session, project_id: str, *, session_kind: str = CHAT_SESSION_KIND_PLAYGROUND) -> ChatSession:
    normalized_kind = _require_supported_session_kind(session_kind)
    stmt = (
        select(ChatSession)
        .where(ChatSession.project_id == project_id, ChatSession.session_kind == normalized_kind)
        .options(selectinload(ChatSession.turns))
        .order_by(desc(ChatSession.last_active_at), desc(ChatSession.created_at))
    )
    chat_session = session.scalars(stmt).first()
    if chat_session:
        return chat_session
    return create_chat_session(session, project_id=project_id, session_kind=normalized_kind)


def get_chat_session(session: Session, session_id: str, *, session_kind: str | None = None) -> ChatSession | None:
    stmt = (
        select(ChatSession)
        .where(ChatSession.id == session_id)
        .options(selectinload(ChatSession.turns))
    )
    if session_kind:
        stmt = stmt.where(ChatSession.session_kind == _require_supported_session_kind(session_kind))
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


__all__ = [name for name in globals() if not name.startswith("_")]
