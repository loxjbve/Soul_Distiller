from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from app.db.models import (
    AnalysisRun,
    ChatSession,
    DocumentRecord,
    Project,
    SkillDraft,
    SkillVersion,
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
    utcnow,
)

PROJECT_LIFECYCLE_ACTIVE = "active"
PROJECT_LIFECYCLE_DELETING = "deleting"
PROJECT_LIFECYCLE_DELETE_FAILED = "delete_failed"
STONE_WRITING_METADATA_KEY = "stone_writing"
STONE_WRITING_DEFAULT_MAX_CONCURRENCY = 4
STONE_WRITING_MIN_MAX_CONCURRENCY = 1
STONE_WRITING_MAX_MAX_CONCURRENCY = 8


def normalize_stone_writing_max_concurrency(value: Any, *, default: int = STONE_WRITING_DEFAULT_MAX_CONCURRENCY) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(STONE_WRITING_MIN_MAX_CONCURRENCY, min(STONE_WRITING_MAX_MAX_CONCURRENCY, normalized))


def normalize_stone_writing_settings(payload: Any) -> dict[str, Any]:
    raw = payload if isinstance(payload, dict) else {}
    return {
        "max_concurrency": normalize_stone_writing_max_concurrency(raw.get("max_concurrency")),
    }


def get_project_stone_writing_settings(project: Project | None) -> dict[str, Any]:
    metadata = dict(getattr(project, "metadata_json", None) or {})
    return normalize_stone_writing_settings(metadata.get(STONE_WRITING_METADATA_KEY))


def update_project_stone_writing_settings(
    session: Session,
    project: Project,
    *,
    max_concurrency: int | None = None,
) -> dict[str, Any]:
    metadata = dict(project.metadata_json or {})
    current = normalize_stone_writing_settings(metadata.get(STONE_WRITING_METADATA_KEY))
    if max_concurrency is not None:
        current["max_concurrency"] = normalize_stone_writing_max_concurrency(max_concurrency)
    metadata[STONE_WRITING_METADATA_KEY] = current
    project.metadata_json = metadata
    session.flush()
    return dict(current)


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
    stmt = select(Project).where(Project.lifecycle_state == lifecycle_state).order_by(desc(Project.updated_at))
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

    relationship_snapshot_ids = session.scalars(
        select(TelegramRelationshipSnapshot.id).where(TelegramRelationshipSnapshot.project_id == project_id)
    ).all()
    if relationship_snapshot_ids:
        session.execute(delete(TelegramRelationshipEdge).where(TelegramRelationshipEdge.snapshot_id.in_(relationship_snapshot_ids)))
        session.execute(delete(TelegramRelationshipSnapshot).where(TelegramRelationshipSnapshot.id.in_(relationship_snapshot_ids)))

    session.execute(delete(TelegramPreprocessTopicQuote).where(TelegramPreprocessTopicQuote.run_id.in_(select(TelegramPreprocessRun.id).where(TelegramPreprocessRun.project_id == project_id))))
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


def list_project_model_ids(session: Session, model: Any, project_ids: list[str], *, limit: int) -> list[str]:
    if not project_ids or limit <= 0:
        return []
    stmt = select(model.id).where(model.project_id.in_(project_ids)).limit(limit)
    return [str(item) for item in session.scalars(stmt).all()]


def delete_projects_by_ids(session: Session, project_ids: list[str]) -> int:
    if not project_ids:
        return 0
    return session.execute(delete(Project).where(Project.id.in_(project_ids))).rowcount or 0


__all__ = [name for name in globals() if not name.startswith("_")]
