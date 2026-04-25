from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.web import routes as legacy

router = APIRouter(tags=["preprocess"])


@router.post("/api/projects/{project_id}/preprocess/runs")
def create_preprocess_run_api(
    request: Request,
    project_id: str,
    session: legacy.SessionDep,
    payload: legacy.TelegramPreprocessRunCreatePayload | None = None,
):
    return legacy.create_preprocess_run_api(request, project_id, session, payload)


@router.get("/api/projects/{project_id}/preprocess/runs")
def list_preprocess_runs_api(project_id: str, session: legacy.SessionDep):
    return legacy.list_preprocess_runs_api(project_id, session)


@router.get("/api/projects/{project_id}/preprocess/runs/latest")
def get_latest_preprocess_run_api(project_id: str, session: legacy.SessionDep, successful: bool = Query(default=True)):
    return legacy.get_latest_preprocess_run_api(project_id, session, successful)


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}")
def get_preprocess_run_api(request: Request, project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.get_preprocess_run_api(request, project_id, run_id, session)


@router.get("/api/projects/{project_id}/preprocess/sessions")
def list_preprocess_sessions_api(project_id: str, session: legacy.SessionDep):
    return legacy.list_preprocess_sessions_api(project_id, session)


@router.post("/api/projects/{project_id}/preprocess/sessions")
def create_preprocess_session_api(
    project_id: str,
    payload: legacy.PreprocessSessionCreatePayload,
    session: legacy.SessionDep,
):
    return legacy.create_preprocess_session_api(project_id, payload, session)


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def get_preprocess_session_api(project_id: str, session_id: str, session: legacy.SessionDep):
    return legacy.get_preprocess_session_api(project_id, session_id, session)


@router.patch("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def update_preprocess_session_api(
    project_id: str,
    session_id: str,
    payload: legacy.PreprocessSessionUpdatePayload,
    session: legacy.SessionDep,
):
    return legacy.update_preprocess_session_api(project_id, session_id, payload, session)


@router.delete("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def delete_preprocess_session_api(project_id: str, session_id: str, session: legacy.SessionDep):
    return legacy.delete_preprocess_session_api(project_id, session_id, session)


@router.post("/api/projects/{project_id}/preprocess/sessions/{session_id}/messages")
def create_preprocess_message_api(
    request: Request,
    project_id: str,
    session_id: str,
    payload: legacy.PreprocessMessagePayload,
    session: legacy.SessionDep,
):
    return legacy.create_preprocess_message_api(request, project_id, session_id, payload, session)


@router.get("/api/projects/{project_id}/preprocess/artifacts/{artifact_id}/download")
def download_preprocess_artifact_api(project_id: str, artifact_id: str, session: legacy.SessionDep):
    return legacy.download_preprocess_artifact_api(project_id, artifact_id, session)
