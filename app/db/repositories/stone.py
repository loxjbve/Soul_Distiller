from __future__ import annotations

from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db.models import StonePreprocessRun


def create_stone_preprocess_run(
    session: Session,
    *,
    project_id: str,
    status: str = "queued",
    llm_model: str | None = None,
    summary_json: dict[str, Any] | None = None,
) -> StonePreprocessRun:
    run = StonePreprocessRun(
        project_id=project_id,
        status=status,
        llm_model=llm_model,
        summary_json=summary_json,
    )
    session.add(run)
    session.flush()
    return run


def get_stone_preprocess_run(session: Session, run_id: str) -> StonePreprocessRun | None:
    return session.get(StonePreprocessRun, run_id)


def list_stone_preprocess_runs(session: Session, project_id: str, *, limit: int | None = None) -> list[StonePreprocessRun]:
    stmt = (
        select(StonePreprocessRun)
        .where(StonePreprocessRun.project_id == project_id)
        .order_by(desc(StonePreprocessRun.created_at))
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.scalars(stmt))


def get_latest_stone_preprocess_run(session: Session, project_id: str, *, status: str | None = None) -> StonePreprocessRun | None:
    stmt = (
        select(StonePreprocessRun)
        .where(StonePreprocessRun.project_id == project_id)
        .order_by(desc(StonePreprocessRun.created_at))
    )
    if status:
        stmt = stmt.where(StonePreprocessRun.status == status)
    return session.scalars(stmt).first()


def get_latest_successful_stone_preprocess_run(session: Session, project_id: str) -> StonePreprocessRun | None:
    stmt = (
        select(StonePreprocessRun)
        .where(
            StonePreprocessRun.project_id == project_id,
            StonePreprocessRun.status.in_(("completed", "partial_failed")),
        )
        .order_by(desc(StonePreprocessRun.created_at))
    )
    return session.scalars(stmt).first()


def get_active_stone_preprocess_run(session: Session, project_id: str) -> StonePreprocessRun | None:
    stmt = (
        select(StonePreprocessRun)
        .where(
            StonePreprocessRun.project_id == project_id,
            StonePreprocessRun.status.in_(("queued", "running")),
        )
        .order_by(desc(StonePreprocessRun.created_at))
    )
    return session.scalars(stmt).first()


def get_latest_resumable_stone_preprocess_run(session: Session, project_id: str) -> StonePreprocessRun | None:
    stmt = (
        select(StonePreprocessRun)
        .where(
            StonePreprocessRun.project_id == project_id,
            StonePreprocessRun.status.in_(("failed", "cancelled", "queued", "running")),
        )
        .order_by(desc(StonePreprocessRun.created_at))
    )
    return session.scalars(stmt).first()


__all__ = [name for name in globals() if not name.startswith("_")]
