from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.api.schemas.preprocess import PreprocessSessionCreatePayload, PreprocessSessionUpdatePayload
from app.api.schemas.writing import StoneWritingSettingsPayload, WritingMessagePayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["writing"])


@router.get("/api/projects/{project_id}/writing/sessions")
def list_writing_sessions_api(project_id: str, session: SessionDep):
    runtime._ensure_stone_project(session, project_id)
    rows = [
        runtime._serialize_chat_session(chat_session)
        for chat_session in runtime.repository.list_chat_sessions(session, project_id, session_kind="writing")
    ]
    return runtime._ok_response("Returned writing sessions.", sessions=rows)


@router.patch("/api/projects/{project_id}/writing/settings")
def update_writing_settings_api(
    project_id: str,
    payload: StoneWritingSettingsPayload,
    session: SessionDep,
):
    project = runtime._ensure_stone_project(session, project_id)
    settings = runtime.repository.update_project_stone_writing_settings(
        session,
        project,
        max_concurrency=payload.max_concurrency,
    )
    return runtime._ok_response("Writing settings updated.", stone_writing=settings)


@router.post("/api/projects/{project_id}/writing/sessions")
def create_writing_session_api(
    project_id: str,
    payload: PreprocessSessionCreatePayload,
    session: SessionDep,
):
    runtime._ensure_stone_project(session, project_id)
    chat_session = runtime.repository.create_chat_session(
        session,
        project_id=project_id,
        session_kind="writing",
        title=payload.title,
    )
    return runtime._ok_response("Writing session created.", **runtime._serialize_chat_session(chat_session))


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}")
def get_writing_session_api(project_id: str, session_id: str, session: SessionDep):
    runtime._ensure_stone_project(session, project_id)
    chat_session = runtime.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Writing session not found.")
    return runtime._ok_response("Returned writing session detail.", **runtime._serialize_writing_session_detail(chat_session))


@router.patch("/api/projects/{project_id}/writing/sessions/{session_id}")
def update_writing_session_api(
    project_id: str,
    session_id: str,
    payload: PreprocessSessionUpdatePayload,
    session: SessionDep,
):
    runtime._ensure_stone_project(session, project_id)
    chat_session = runtime.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Writing session not found.")
    runtime.repository.rename_chat_session(session, chat_session, title=payload.title)
    return runtime._ok_response("Writing session updated.", **runtime._serialize_chat_session(chat_session))


@router.delete("/api/projects/{project_id}/writing/sessions/{session_id}")
def delete_writing_session_api(project_id: str, session_id: str, session: SessionDep):
    runtime._ensure_stone_project(session, project_id)
    chat_session = runtime.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Writing session not found.")
    runtime.repository.delete_chat_session(session, chat_session)
    return runtime._ok_response("Writing session deleted.", ok=True, session_id=session_id)


@router.post("/api/projects/{project_id}/writing/sessions/{session_id}/messages")
def create_writing_message_api(
    request: Request,
    project_id: str,
    session_id: str,
    payload: WritingMessagePayload,
    session: SessionDep,
):
    runtime._ensure_stone_project(session, project_id)
    try:
        request_payload = runtime._resolve_writing_request_payload(payload)
        result = request.app.state.writing_service.start_stream(
            project_id=project_id,
            session_id=session_id,
            topic=request_payload["topic"],
            target_word_count=request_payload["target_word_count"],
            extra_requirements=request_payload["extra_requirements"],
            raw_message=request_payload["message"],
            max_concurrency=request_payload["max_concurrency"],
            target_word_count_source=request_payload["target_word_count_source"],
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return runtime._ok_response("Writing task submitted.", **result)


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}")
def stream_writing_events_api(
    request: Request,
    project_id: str,
    session_id: str,
    stream_id: str,
    session: SessionDep,
):
    runtime._ensure_stone_project(session, project_id)
    chat_session = runtime.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Writing session not found.")
    try:
        generator = request.app.state.writing_service.stream_events(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Writing stream not found.") from exc
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
