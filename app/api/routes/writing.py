from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.web import routes as legacy

router = APIRouter(tags=["writing"])


@router.get("/api/projects/{project_id}/writing/sessions")
def list_writing_sessions_api(project_id: str, session: legacy.SessionDep):
    legacy._ensure_stone_project(session, project_id)
    rows = [
        legacy._serialize_chat_session(chat_session)
        for chat_session in legacy.repository.list_chat_sessions(session, project_id, session_kind="writing")
    ]
    return legacy._ok_response("已返回写作会话列表。", sessions=rows)


@router.post("/api/projects/{project_id}/writing/sessions")
def create_writing_session_api(
    project_id: str,
    payload: legacy.PreprocessSessionCreatePayload,
    session: legacy.SessionDep,
):
    legacy._ensure_stone_project(session, project_id)
    chat_session = legacy.repository.create_chat_session(
        session,
        project_id=project_id,
        session_kind="writing",
        title=payload.title or "新建写作会话",
    )
    return legacy._ok_response("写作会话已创建。", **legacy._serialize_chat_session(chat_session))


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}")
def get_writing_session_api(project_id: str, session_id: str, session: legacy.SessionDep):
    legacy._ensure_stone_project(session, project_id)
    chat_session = legacy.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    return legacy._ok_response("已返回写作会话详情。", **legacy._serialize_writing_session_detail(chat_session))


@router.patch("/api/projects/{project_id}/writing/sessions/{session_id}")
def update_writing_session_api(
    project_id: str,
    session_id: str,
    payload: legacy.PreprocessSessionUpdatePayload,
    session: legacy.SessionDep,
):
    legacy._ensure_stone_project(session, project_id)
    chat_session = legacy.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    legacy.repository.rename_chat_session(session, chat_session, title=payload.title)
    return legacy._ok_response("写作会话已更新。", **legacy._serialize_chat_session(chat_session))


@router.delete("/api/projects/{project_id}/writing/sessions/{session_id}")
def delete_writing_session_api(project_id: str, session_id: str, session: legacy.SessionDep):
    legacy._ensure_stone_project(session, project_id)
    chat_session = legacy.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    legacy.repository.delete_chat_session(session, chat_session)
    return legacy._ok_response("写作会话已删除。", ok=True, session_id=session_id)


@router.post("/api/projects/{project_id}/writing/sessions/{session_id}/messages")
def create_writing_message_api(
    request: Request,
    project_id: str,
    session_id: str,
    payload: legacy.WritingMessagePayload,
    session: legacy.SessionDep,
):
    legacy._ensure_stone_project(session, project_id)
    pipeline, _ = legacy._pipeline_for_project(request, session, project_id)
    try:
        request_payload = legacy._resolve_writing_request_payload(payload)
        result = pipeline.start_writing_stream(
            project_id=project_id,
            session_id=session_id,
            request=legacy.WritingRequest(
                topic=request_payload["topic"],
                target_word_count=request_payload["target_word_count"],
                extra_requirements=request_payload["extra_requirements"],
                message=request_payload["message"],
            ),
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return legacy._ok_response("写作任务已提交。", **result)


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}")
def stream_writing_events_api(
    request: Request,
    project_id: str,
    session_id: str,
    stream_id: str,
    session: legacy.SessionDep,
):
    legacy._ensure_stone_project(session, project_id)
    pipeline, _ = legacy._pipeline_for_project(request, session, project_id)
    chat_session = legacy.repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    try:
        generator = pipeline.stream_writing_events(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="未找到写作流。") from exc
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
