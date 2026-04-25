from __future__ import annotations

from fastapi import APIRouter

from app.web import routes as legacy

router = APIRouter(tags=["telegram"])


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/topics")
def list_telegram_preprocess_topics_api(project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.list_telegram_preprocess_topics_api(project_id, run_id, session)


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/weekly-candidates")
def list_telegram_preprocess_weekly_candidates_api(project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.list_telegram_preprocess_weekly_candidates_api(project_id, run_id, session)


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/top-users")
def list_telegram_preprocess_top_users_api(project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.list_telegram_preprocess_top_users_api(project_id, run_id, session)


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/active-users")
def list_telegram_preprocess_active_users_api(project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.list_telegram_preprocess_active_users_api(project_id, run_id, session)


@router.get("/api/projects/{project_id}/relationships/latest")
def get_latest_telegram_relationship_snapshot_api(project_id: str, session: legacy.SessionDep):
    return legacy.get_latest_telegram_relationship_snapshot_api(project_id, session)


@router.get("/api/projects/{project_id}/relationships/{snapshot_id}")
def get_telegram_relationship_snapshot_api(project_id: str, snapshot_id: str, session: legacy.SessionDep):
    return legacy.get_telegram_relationship_snapshot_api(project_id, snapshot_id, session)
