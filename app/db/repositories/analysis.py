from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import AnalysisEvent, AnalysisFacet, AnalysisRun


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


def delete_analysis_facets_by_ids(session: Session, facet_ids: list[str]) -> int:
    if not facet_ids:
        return 0
    return session.execute(delete(AnalysisFacet).where(AnalysisFacet.id.in_(facet_ids))).rowcount or 0


def delete_analysis_events_by_ids(session: Session, event_ids: list[str]) -> int:
    if not event_ids:
        return 0
    return session.execute(delete(AnalysisEvent).where(AnalysisEvent.id.in_(event_ids))).rowcount or 0


def delete_analysis_runs_by_ids(session: Session, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    return session.execute(delete(AnalysisRun).where(AnalysisRun.id.in_(run_ids))).rowcount or 0


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
    stmt = select(AnalysisRun).where(AnalysisRun.project_id == project_id).order_by(desc(AnalysisRun.created_at))
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalars(stmt).first()


def get_active_analysis_run(session: Session, project_id: str, *, load_facets: bool = True, load_events: bool = True) -> AnalysisRun | None:
    stmt = (
        select(AnalysisRun)
        .where(AnalysisRun.project_id == project_id, AnalysisRun.status.in_(("queued", "running")))
        .order_by(desc(AnalysisRun.created_at))
    )
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalars(stmt).first()


def list_active_analysis_runs(session: Session, *, load_facets: bool = True, load_events: bool = True) -> list[AnalysisRun]:
    stmt = select(AnalysisRun).where(AnalysisRun.status.in_(("queued", "running"))).order_by(desc(AnalysisRun.created_at))
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return list(session.scalars(stmt))


def get_analysis_run(session: Session, run_id: str, *, load_facets: bool = True, load_events: bool = True) -> AnalysisRun | None:
    stmt = select(AnalysisRun).where(AnalysisRun.id == run_id)
    if load_facets:
        stmt = stmt.options(selectinload(AnalysisRun.facets))
    if load_events:
        stmt = stmt.options(selectinload(AnalysisRun.events))
    return session.scalar(stmt)


def list_analysis_runs(session: Session, project_id: str, *, load_facets: bool = True, load_events: bool = True) -> list[AnalysisRun]:
    stmt = select(AnalysisRun).where(AnalysisRun.project_id == project_id).order_by(desc(AnalysisRun.created_at))
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


__all__ = [name for name in globals() if not name.startswith("_")]
