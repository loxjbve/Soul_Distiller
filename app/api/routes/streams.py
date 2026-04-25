from __future__ import annotations

from fastapi import APIRouter, Query, Request, WebSocket

from app.web import routes as legacy

router = APIRouter(tags=["streams"])


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/stream")
def stream_preprocess_run_api(request: Request, project_id: str, run_id: str, session: legacy.SessionDep):
    return legacy.stream_preprocess_run_api(request, project_id, run_id, session)


@router.get("/api/projects/{project_id}/analysis/stream")
def stream_analysis_api(
    request: Request,
    project_id: str,
    session: legacy.SessionDep,
    run_id: str | None = Query(default=None),
):
    return legacy.stream_analysis_api(request, project_id, session, run_id)


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}")
def stream_preprocess_events_api(
    request: Request,
    project_id: str,
    session_id: str,
    stream_id: str,
    session: legacy.SessionDep,
):
    return legacy.stream_preprocess_events_api(request, project_id, session_id, stream_id, session)


@router.websocket("/api/projects/{project_id}/documents/ws")
async def websocket_document_status(websocket: WebSocket, project_id: str):
    await legacy.websocket_document_status(websocket, project_id)
