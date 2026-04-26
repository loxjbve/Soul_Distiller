from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.api.schemas.preprocess import TelegramPreprocessRunCreatePayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["preprocess"])


@router.post("/api/projects/{project_id}/preprocess/runs")
def create_preprocess_run_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    payload: TelegramPreprocessRunCreatePayload | None = None,
):
    return runtime.create_preprocess_run_api(request, project_id, session, payload)


@router.get("/api/projects/{project_id}/preprocess/runs")
def list_preprocess_runs_api(project_id: str, session: SessionDep):
    return runtime.list_preprocess_runs_api(project_id, session)


@router.get("/api/projects/{project_id}/preprocess/runs/latest")
def get_latest_preprocess_run_api(project_id: str, session: SessionDep, successful: bool = Query(default=True)):
    return runtime.get_latest_preprocess_run_api(project_id, session, successful)


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}")
def get_preprocess_run_api(request: Request, project_id: str, run_id: str, session: SessionDep):
    return runtime.get_preprocess_run_api(request, project_id, run_id, session)
