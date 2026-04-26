from __future__ import annotations

from fastapi import APIRouter, Request

from app.api.schemas.projects import ChatPayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["playground"])


@router.post("/api/projects/{project_id}/playground/chat")
def playground_chat_api(request: Request, project_id: str, payload: ChatPayload, session: SessionDep):
    return runtime.playground_chat_api(request, project_id, payload, session)
