from __future__ import annotations

from fastapi import APIRouter, Query, Request, WebSocket

from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["streams"])


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/stream")
def stream_preprocess_run_api(request: Request, project_id: str, run_id: str, session: SessionDep):
    return runtime.stream_preprocess_run_api(request, project_id, run_id, session)


@router.get("/api/projects/{project_id}/analysis/stream")
def stream_analysis_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    run_id: str | None = Query(default=None),
):
    return runtime.stream_analysis_api(request, project_id, session, run_id)


@router.websocket("/api/projects/{project_id}/documents/ws")
async def websocket_document_status(websocket: WebSocket, project_id: str):
    await runtime.websocket_document_status(websocket, project_id)
