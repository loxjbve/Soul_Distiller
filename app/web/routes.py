from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analysis.facets import FACETS
from app.llm.client import OpenAICompatibleClient, normalize_api_mode, normalize_provider_kind
from app.models import AnalysisFacet, AnalysisRun, DocumentRecord, GeneratedArtifact, utcnow
from app.schemas import ASSET_KINDS, ServiceConfig
from app.storage import repository


router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
PROVIDER_OPTIONS = (
    {"value": "openai", "label": "OpenAI 官方"},
    {"value": "xai", "label": "xAI 官方"},
    {"value": "gemini", "label": "Gemini 官方"},
    {"value": "openai-compatible", "label": "OpenAI Compatible 自定义入口"},
)
API_MODE_OPTIONS = (
    {"value": "responses", "label": "Responses API"},
    {"value": "chat_completions", "label": "Chat Completions API"},
)
ASSET_KIND_OPTIONS = (
    {"value": "skill", "label": "Skill"},
    {"value": "profile_report", "label": "用户剖析报告"},
)


class ProjectCreatePayload(BaseModel):
    name: str
    description: str | None = None


class ChatPayload(BaseModel):
    message: str
    session_id: str | None = None


class AnalysisRequestPayload(BaseModel):
    target_role: str | None = None
    analysis_context: str | None = None


class DocumentUpdatePayload(BaseModel):
    title: str | None = None
    source_type: str | None = None
    user_note: str | None = None


class PreprocessSessionCreatePayload(BaseModel):
    title: str | None = None


class PreprocessSessionUpdatePayload(BaseModel):
    title: str | None = None


class PreprocessMessagePayload(BaseModel):
    message: str


class AssetGeneratePayload(BaseModel):
    asset_kind: str = "skill"


class AssetSavePayload(BaseModel):
    asset_kind: str = "skill"
    markdown_text: str
    json_payload: dict[str, Any]
    prompt_text: str
    notes: str | None = None


def get_session(request: Request):
    with request.app.state.db.session() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


@router.get("/", response_class=HTMLResponse)
def index(request: Request, session: SessionDep):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "projects": repository.list_projects(session),
            "chat_configured": repository.get_service_config(session, "chat_service") is not None,
            "embedding_configured": repository.get_service_config(session, "embedding_service") is not None,
        },
    )


@router.post("/projects")
def create_project_form(
    session: SessionDep,
    name: Annotated[str, Form(...)],
    description: Annotated[str | None, Form()] = None,
):
    project = repository.create_project(session, name=name, description=description)
    return RedirectResponse(url=f"/projects/{project.id}", status_code=303)


@router.get("/projects/{project_id}", response_class=HTMLResponse)
def project_detail(request: Request, project_id: str, session: SessionDep):
    context = _project_context(session, project_id)
    return templates.TemplateResponse(request=request, name="project_detail.html", context=context)


@router.post("/projects/{project_id}/delete")
def delete_project_form(request: Request, project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    _delete_project_resources(request, session, project.id)
    return RedirectResponse(url="/", status_code=303)


@router.post("/projects/{project_id}/documents")
async def upload_documents_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    files: list[UploadFile] = File(...),
):
    _ensure_project(session, project_id)
    ingest = request.app.state.ingest_service
    for upload in files:
        content = await upload.read()
        ingest.ingest_bytes(
            session,
            project_id=project_id,
            filename=upload.filename or "upload.bin",
            content=content,
            mime_type=upload.content_type,
        )
    return RedirectResponse(url=f"/projects/{project_id}", status_code=303)


@router.post("/projects/{project_id}/documents/{document_id}/update")
def update_document_form(
    project_id: str,
    document_id: str,
    session: SessionDep,
    title: Annotated[str | None, Form()] = None,
    source_type: Annotated[str | None, Form()] = None,
    user_note: Annotated[str | None, Form()] = None,
):
    document = _get_project_document(session, project_id, document_id)
    repository.update_document(session, document, title=title, source_type=source_type, user_note=user_note)
    return RedirectResponse(url=f"/projects/{project_id}#document-{document_id}", status_code=303)


@router.post("/projects/{project_id}/documents/{document_id}/delete")
def delete_document_form(project_id: str, document_id: str, session: SessionDep):
    document = _get_project_document(session, project_id, document_id)
    _delete_document_with_file(document)
    repository.delete_document(session, document)
    return RedirectResponse(url=f"/projects/{project_id}", status_code=303)


@router.post("/projects/{project_id}/analyze")
def analyze_project_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    target_role: Annotated[str | None, Form()] = None,
    analysis_context: Annotated[str | None, Form()] = None,
):
    run = _enqueue_analysis(
        request,
        session,
        project_id,
        target_role=target_role,
        analysis_context=analysis_context,
    )
    return RedirectResponse(url=f"/projects/{project_id}/analysis?run_id={run.id}", status_code=303)


@router.get("/projects/{project_id}/analysis", response_class=HTMLResponse)
def analysis_page(
    request: Request,
    project_id: str,
    session: SessionDep,
    run_id: str | None = Query(default=None),
):
    project = _ensure_project(session, project_id)
    run = _resolve_run(session, project_id, run_id)
    serialized_run = _serialize_analysis_run(run) if run else None
    return templates.TemplateResponse(
        request=request,
        name="analysis.html",
        context={
            "project": project,
            "run": run,
            "serialized_run": json.dumps(serialized_run, ensure_ascii=False) if serialized_run else "null",
            "run_id": run.id if run else "",
            "facet_catalog": FACETS,
        },
    )


@router.post("/projects/{project_id}/analysis/{facet_key}/accept")
def accept_facet(project_id: str, facet_key: str, session: SessionDep):
    run = repository.get_latest_analysis_run(session, project_id)
    if not run:
        raise HTTPException(status_code=404, detail="No analysis run found.")
    facet = repository.get_facet(session, run.id, facet_key)
    if not facet:
        raise HTTPException(status_code=404, detail="Facet not found.")
    facet.accepted = 1
    return RedirectResponse(url=f"/projects/{project_id}/analysis?run_id={run.id}", status_code=303)


@router.post("/projects/{project_id}/analysis/{facet_key}/rerun")
def rerun_facet(request: Request, project_id: str, facet_key: str, session: SessionDep):
    run = repository.get_active_analysis_run(session, project_id)
    if run:
        if request.app.state.analysis_runner.is_tracking(run.id):
            raise HTTPException(status_code=409, detail="An analysis is already running for this project.")
        _mark_run_as_stale(
            session,
            run,
            reason="Detected an unfinished run record without a live worker before facet rerun.",
        )
    latest_run = repository.get_latest_analysis_run(session, project_id)
    if not latest_run:
        raise HTTPException(status_code=404, detail="No analysis run found.")
    request.app.state.analysis_runner.submit_facet_rerun(project_id, facet_key)
    return RedirectResponse(url=f"/projects/{project_id}/analysis?run_id={latest_run.id}", status_code=303)


@router.get("/projects/{project_id}/assets", response_class=HTMLResponse)
def assets_page(
    request: Request,
    project_id: str,
    session: SessionDep,
    kind: str = Query(default="skill"),
):
    project = _ensure_project(session, project_id)
    asset_kind = _normalize_asset_kind(kind)
    draft = repository.get_latest_asset_draft(session, project_id, asset_kind=asset_kind)
    versions = repository.list_asset_versions(session, project_id, asset_kind=asset_kind)
    latest_run = repository.get_latest_analysis_run(session, project_id)
    return templates.TemplateResponse(
        request=request,
        name="assets.html",
        context={
            "project": project,
            "asset_kind": asset_kind,
            "asset_label": _asset_label(asset_kind),
            "asset_options": ASSET_KIND_OPTIONS,
            "draft": draft,
            "versions": versions,
            "latest_run": latest_run,
            "draft_json_pretty": json.dumps(draft.json_payload, ensure_ascii=False, indent=2) if draft else "{}",
        },
    )


@router.get("/projects/{project_id}/skill", response_class=HTMLResponse)
def skill_page(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=skill", status_code=303)


@router.post("/projects/{project_id}/assets/generate")
def generate_asset_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    asset_kind: Annotated[str, Form()] = "skill",
):
    normalized_kind = _normalize_asset_kind(asset_kind)
    draft = _generate_asset_draft(request, session, project_id, asset_kind=normalized_kind)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind={normalized_kind}&draft={draft.id}", status_code=303)


@router.post("/projects/{project_id}/assets/{draft_id}/save")
def save_asset_draft_form(
    request: Request,
    project_id: str,
    draft_id: str,
    session: SessionDep,
    asset_kind: Annotated[str, Form()] = "skill",
    markdown_text: Annotated[str, Form(...)] = "",
    json_payload: Annotated[str, Form(...)] = "{}",
    prompt_text: Annotated[str | None, Form()] = None,
    system_prompt: Annotated[str | None, Form()] = None,
    notes: Annotated[str | None, Form()] = None,
):
    draft = repository.get_asset_draft(session, draft_id, asset_kind=_normalize_asset_kind(asset_kind))
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    draft.markdown_text = markdown_text
    draft.json_payload = json.loads(json_payload)
    draft.system_prompt = (prompt_text or system_prompt or "").strip()
    draft.notes = notes
    _persist_asset_files(
        request,
        project_id,
        draft.asset_kind,
        f"draft_{draft.id}",
        draft.markdown_text,
        draft.json_payload,
        draft.system_prompt,
    )
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind={draft.asset_kind}", status_code=303)


@router.post("/projects/{project_id}/assets/{draft_id}/publish")
def publish_asset_form(
    request: Request,
    project_id: str,
    draft_id: str,
    session: SessionDep,
    asset_kind: Annotated[str, Form()] = "skill",
):
    draft = repository.get_asset_draft(session, draft_id, asset_kind=_normalize_asset_kind(asset_kind))
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    version = repository.publish_asset_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        version.asset_kind,
        f"published_v{version.version_number}",
        version.markdown_text,
        version.json_payload,
        version.system_prompt,
    )
    if version.asset_kind == "skill":
        return RedirectResponse(url=f"/projects/{project_id}/playground", status_code=303)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind={version.asset_kind}", status_code=303)


@router.post("/projects/{project_id}/skills/generate")
def generate_skill_form(request: Request, project_id: str, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind="skill")
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=skill&draft={draft.id}", status_code=303)


@router.post("/projects/{project_id}/skills/{draft_id}/save")
def save_skill_draft_form(
    request: Request,
    project_id: str,
    draft_id: str,
    session: SessionDep,
    markdown_text: Annotated[str, Form(...)],
    json_payload: Annotated[str, Form(...)],
    system_prompt: Annotated[str, Form(...)],
    notes: Annotated[str | None, Form()] = None,
):
    draft = repository.get_skill_draft(session, draft_id)
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    draft.markdown_text = markdown_text
    draft.json_payload = json.loads(json_payload)
    draft.system_prompt = system_prompt
    draft.notes = notes
    _persist_asset_files(request, project_id, "skill", f"draft_{draft.id}", draft.markdown_text, draft.json_payload, draft.system_prompt)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=skill", status_code=303)


@router.post("/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_form(request: Request, project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_skill_draft(session, draft_id)
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    version = repository.publish_skill_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        "skill",
        f"published_v{version.version_number}",
        version.markdown_text,
        version.json_payload,
        version.system_prompt,
    )
    return RedirectResponse(url=f"/projects/{project_id}/playground", status_code=303)


@router.get("/projects/{project_id}/playground", response_class=HTMLResponse)
def playground_page(request: Request, project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    version = repository.get_latest_skill_version(session, project_id)
    chat_session = repository.get_or_create_chat_session(session, project_id, session_kind="playground") if version else None
    turns = sorted(chat_session.turns, key=lambda item: item.created_at) if chat_session else []
    return templates.TemplateResponse(
        request=request,
        name="playground.html",
        context={
            "project": project,
            "version": version,
            "chat_session": chat_session,
            "turns": turns,
        },
    )


@router.post("/projects/{project_id}/playground/chat")
def playground_chat_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    message: Annotated[str, Form(...)],
):
    payload = _chat_with_persona(request, session, project_id, message)
    return RedirectResponse(url=f"/projects/{project_id}/playground#turn-{payload['assistant_turn_id']}", status_code=303)


@router.get("/projects/{project_id}/preprocess", response_class=HTMLResponse)
def preprocess_page(
    request: Request,
    project_id: str,
    session: SessionDep,
    session_id: str | None = Query(default=None),
    mention: str | None = Query(default=None),
):
    context = _project_context(session, project_id)
    sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    if not sessions:
        sessions = [
            repository.create_chat_session(
                session,
                project_id=project_id,
                session_kind="preprocess",
                title="New Preprocess Session",
            )
        ]
    selected_session = sessions[0]
    if session_id:
        explicit = repository.get_chat_session(session, session_id, session_kind="preprocess")
        if explicit and explicit.project_id == project_id:
            selected_session = explicit
    bootstrap = {
        "project": {"id": context["project"].id, "name": context["project"].name},
        "sessions": [_serialize_chat_session(item) for item in sessions],
        "selected_session_id": selected_session.id,
        "selected_session": _serialize_preprocess_session_detail(selected_session),
        "documents": [_serialize_document(item) for item in context["documents"]],
        "initial_mention": mention or "",
    }
    return templates.TemplateResponse(
        request=request,
        name="preprocess.html",
        context={
            "project": context["project"],
            "bootstrap": json.dumps(bootstrap, ensure_ascii=False),
        },
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, session: SessionDep):
    chat_setting = repository.get_setting(session, "chat_service")
    embedding_setting = repository.get_setting(session, "embedding_service")
    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context={
            "chat_setting": _settings_payload(chat_setting.value_json if chat_setting else {}, default_provider="openai"),
            "embedding_setting": _settings_payload(
                embedding_setting.value_json if embedding_setting else {},
                default_provider="openai",
            ),
            "provider_options": PROVIDER_OPTIONS,
            "api_mode_options": API_MODE_OPTIONS,
        },
    )


@router.post("/settings/{service_name}")
def save_service_settings(
    service_name: str,
    session: SessionDep,
    api_key: Annotated[str, Form(...)],
    base_url: Annotated[str | None, Form()] = None,
    model: Annotated[str | None, Form()] = None,
    provider_kind: Annotated[str, Form()] = "openai",
    api_mode: Annotated[str | None, Form()] = None,
):
    if service_name not in {"chat", "embedding"}:
        raise HTTPException(status_code=404, detail="Unknown service.")
    normalized_provider = normalize_provider_kind(provider_kind)
    normalized_base_url = (base_url or "").strip()
    if normalized_provider == "openai-compatible" and not normalized_base_url:
        raise HTTPException(status_code=400, detail="Base URL is required for custom OpenAI-compatible providers.")
    repository.upsert_setting(
        session,
        f"{service_name}_service",
        {
            "base_url": normalized_base_url,
            "api_key": api_key.strip(),
            "model": (model or "").strip(),
            "provider_kind": normalized_provider,
            "api_mode": normalize_api_mode(api_mode if service_name == "chat" else "responses"),
        },
    )
    return RedirectResponse(url="/settings", status_code=303)


@router.post("/api/projects")
def create_project_api(payload: ProjectCreatePayload, session: SessionDep):
    project = repository.create_project(session, payload.name, payload.description)
    return {"id": project.id, "name": project.name, "description": project.description}


@router.delete("/api/projects/{project_id}")
def delete_project_api(request: Request, project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    _delete_project_resources(request, session, project.id)
    return {"ok": True, "project_id": project.id}


@router.post("/api/projects/{project_id}/documents")
async def upload_documents_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    files: list[UploadFile] = File(...),
):
    _ensure_project(session, project_id)
    ingest = request.app.state.ingest_service
    created = []
    for upload in files:
        content = await upload.read()
        document = ingest.ingest_bytes(
            session,
            project_id=project_id,
            filename=upload.filename or "upload.bin",
            content=content,
            mime_type=upload.content_type,
        )
        created.append(_serialize_document(document))
    return {"documents": created}


@router.post("/api/projects/{project_id}/documents/{document_id}")
def update_document_api(
    project_id: str,
    document_id: str,
    payload: DocumentUpdatePayload,
    session: SessionDep,
):
    document = _get_project_document(session, project_id, document_id)
    repository.update_document(session, document, title=payload.title, source_type=payload.source_type, user_note=payload.user_note)
    return _serialize_document(document)


@router.post("/api/projects/{project_id}/documents/{document_id}/delete")
def delete_document_api(project_id: str, document_id: str, session: SessionDep):
    document = _get_project_document(session, project_id, document_id)
    _delete_document_with_file(document)
    repository.delete_document(session, document)
    return {"ok": True, "document_id": document_id}


@router.get("/api/projects/{project_id}/documents/mentions")
def list_document_mentions_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    q: str = Query(default=""),
):
    _ensure_project(session, project_id)
    return {
        "items": request.app.state.preprocess_service.list_mentions(session, project_id, q, limit=8),
    }


@router.post("/api/projects/{project_id}/analyze")
def analyze_project_api(
    request: Request,
    project_id: str,
    payload: AnalysisRequestPayload,
    session: SessionDep,
):
    run = _enqueue_analysis(
        request,
        session,
        project_id,
        target_role=payload.target_role,
        analysis_context=payload.analysis_context,
    )
    return _serialize_analysis_run(run)


@router.get("/api/projects/{project_id}/analysis")
def get_analysis_api(
    project_id: str,
    session: SessionDep,
    run_id: str | None = Query(default=None),
):
    run = _resolve_run(session, project_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="No analysis run found.")
    return _serialize_analysis_run(run)


@router.get("/api/projects/{project_id}/analysis/stream")
def stream_analysis_api(request: Request, project_id: str, session: SessionDep, run_id: str | None = Query(default=None)):
    run = _resolve_run(session, project_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="No analysis run found.")

    def generate():
        last_snapshot = ""
        while True:
            with request.app.state.db.session() as live_session:
                live_run = _resolve_run(live_session, project_id, run_id or run.id)
                if not live_run:
                    break
                payload = _serialize_analysis_run(live_run)
            encoded = json.dumps(payload, ensure_ascii=False)
            if encoded != last_snapshot:
                last_snapshot = encoded
                yield _format_sse("snapshot", payload)
            if payload["status"] not in {"queued", "running"}:
                yield _format_sse("done", {"run_id": payload["id"], "status": payload["status"]})
                break
            time.sleep(0.35)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.post("/api/projects/{project_id}/analysis/{facet_key}/rerun")
def rerun_facet_api(request: Request, project_id: str, facet_key: str, session: SessionDep):
    run = repository.get_active_analysis_run(session, project_id)
    if run:
        if request.app.state.analysis_runner.is_tracking(run.id):
            raise HTTPException(status_code=409, detail="An analysis is already running for this project.")
        _mark_run_as_stale(
            session,
            run,
            reason="Detected an unfinished run record without a live worker before facet rerun API call.",
        )
    latest_run = repository.get_latest_analysis_run(session, project_id)
    if not latest_run:
        raise HTTPException(status_code=404, detail="No analysis run found.")
    request.app.state.analysis_runner.submit_facet_rerun(project_id, facet_key)
    session.expire_all()
    refreshed = repository.get_analysis_run(session, latest_run.id) or latest_run
    return _serialize_analysis_run(refreshed)


@router.post("/api/projects/{project_id}/rechunk")
def start_rechunk_api(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    embedding_config = repository.get_service_config(session, "embedding_service")
    manager = request.app.state.rechunk_manager
    try:
        return manager.submit(project_id=project_id, embedding_config=embedding_config)
    except ValueError as exc:
        task_id = str(exc)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A rechunk task is already running for this project.",
                "task_id": task_id,
                "task": manager.get(task_id),
            },
        ) from exc


@router.get("/api/projects/{project_id}/rechunk/{task_id}")
def get_rechunk_task_api(request: Request, project_id: str, task_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    task = request.app.state.rechunk_manager.get(task_id)
    if not task or task.get("project_id") != project_id:
        raise HTTPException(status_code=404, detail="Rechunk task not found.")
    return task


@router.post("/api/projects/{project_id}/assets/generate")
def generate_asset_api(request: Request, project_id: str, payload: AssetGeneratePayload, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind=_normalize_asset_kind(payload.asset_kind))
    return _serialize_draft(draft)


@router.post("/api/projects/{project_id}/assets/{draft_id}/save")
def save_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: AssetSavePayload,
    session: SessionDep,
):
    draft = repository.get_asset_draft(session, draft_id, asset_kind=_normalize_asset_kind(payload.asset_kind))
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    draft.markdown_text = payload.markdown_text
    draft.json_payload = payload.json_payload
    draft.system_prompt = payload.prompt_text
    draft.notes = payload.notes
    _persist_asset_files(
        request,
        project_id,
        draft.asset_kind,
        f"draft_{draft.id}",
        draft.markdown_text,
        draft.json_payload,
        draft.system_prompt,
    )
    return _serialize_draft(draft)


@router.post("/api/projects/{project_id}/assets/{draft_id}/publish")
def publish_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: AssetGeneratePayload,
    session: SessionDep,
):
    draft = repository.get_asset_draft(session, draft_id, asset_kind=_normalize_asset_kind(payload.asset_kind))
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    version = repository.publish_asset_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        version.asset_kind,
        f"published_v{version.version_number}",
        version.markdown_text,
        version.json_payload,
        version.system_prompt,
    )
    return {
        "id": version.id,
        "asset_kind": version.asset_kind,
        "version_number": version.version_number,
        "published_at": version.published_at.isoformat(),
    }


@router.post("/api/projects/{project_id}/skills/generate")
def generate_skill_api(request: Request, project_id: str, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind="skill")
    return _serialize_draft(draft)


@router.post("/api/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_api(request: Request, project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_skill_draft(session, draft_id)
    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found.")
    version = repository.publish_skill_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        "skill",
        f"published_v{version.version_number}",
        version.markdown_text,
        version.json_payload,
        version.system_prompt,
    )
    return {
        "id": version.id,
        "asset_kind": version.asset_kind,
        "version_number": version.version_number,
        "published_at": version.published_at.isoformat(),
    }


@router.post("/api/projects/{project_id}/playground/chat")
def playground_chat_api(request: Request, project_id: str, payload: ChatPayload, session: SessionDep):
    return _chat_with_persona(request, session, project_id, payload.message, payload.session_id)


@router.get("/api/projects/{project_id}/preprocess/sessions")
def list_preprocess_sessions_api(project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    return {"sessions": [_serialize_chat_session(item) for item in sessions]}


@router.post("/api/projects/{project_id}/preprocess/sessions")
def create_preprocess_session_api(project_id: str, payload: PreprocessSessionCreatePayload, session: SessionDep):
    _ensure_project(session, project_id)
    chat_session = repository.create_chat_session(
        session,
        project_id=project_id,
        session_kind="preprocess",
        title=payload.title or "New Preprocess Session",
    )
    return _serialize_chat_session(chat_session)


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def get_preprocess_session_api(project_id: str, session_id: str, session: SessionDep):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Preprocess session not found.")
    return _serialize_preprocess_session_detail(chat_session)


@router.patch("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def update_preprocess_session_api(
    project_id: str,
    session_id: str,
    payload: PreprocessSessionUpdatePayload,
    session: SessionDep,
):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Preprocess session not found.")
    repository.rename_chat_session(session, chat_session, title=payload.title)
    return _serialize_chat_session(chat_session)


@router.delete("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def delete_preprocess_session_api(project_id: str, session_id: str, session: SessionDep):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="Preprocess session not found.")
    repository.delete_chat_session(session, chat_session)
    return {"ok": True, "session_id": session_id}


@router.post("/api/projects/{project_id}/preprocess/sessions/{session_id}/messages")
def create_preprocess_message_api(
    request: Request,
    project_id: str,
    session_id: str,
    payload: PreprocessMessagePayload,
):
    try:
        result = request.app.state.preprocess_service.start_stream(
            project_id=project_id,
            session_id=session_id,
            message=payload.message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return result


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}")
def stream_preprocess_events_api(request: Request, project_id: str, session_id: str, stream_id: str):
    del project_id, session_id
    try:
        generator = request.app.state.preprocess_service.stream_events(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Stream not found.") from exc
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.get("/api/projects/{project_id}/preprocess/artifacts/{artifact_id}/download")
def download_preprocess_artifact_api(project_id: str, artifact_id: str, session: SessionDep):
    artifact = repository.get_generated_artifact(session, artifact_id)
    if not artifact or artifact.project_id != project_id:
        raise HTTPException(status_code=404, detail="Artifact not found.")
    path = Path(artifact.storage_path)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact file not found.")
    return FileResponse(path, media_type=artifact.mime_type or "application/octet-stream", filename=artifact.filename)


@router.get("/api/settings/models")
def list_models_api(
    request: Request,
    service: Annotated[str, Query(pattern="^(chat|embedding)$")],
    session: SessionDep,
):
    config = repository.get_service_config(session, f"{service}_service")
    if not config:
        raise HTTPException(status_code=400, detail=f"{service} service is not configured.")
    client = OpenAICompatibleClient(config, log_path=str(request.app.state.config.llm_log_path))
    try:
        return {"service": service, "models": client.list_models()}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _project_context(session: Session, project_id: str) -> dict[str, Any]:
    project = _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id)
    latest_run = repository.get_latest_analysis_run(session, project_id)
    latest_draft = repository.get_latest_skill_draft(session, project_id)
    latest_version = repository.get_latest_skill_version(session, project_id)
    latest_summary = latest_run.summary_json or {} if latest_run else {}
    ready_count = sum(1 for document in documents if document.ingest_status == "ready")
    failed_count = sum(1 for document in documents if document.ingest_status == "failed")
    preprocess_sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    return {
        "project": project,
        "documents": documents,
        "latest_run": latest_run,
        "latest_draft": latest_draft,
        "latest_version": latest_version,
        "preprocess_sessions": preprocess_sessions,
        "stats": {
            "document_count": len(documents),
            "ready_count": ready_count,
            "failed_count": failed_count,
        },
        "analysis_defaults": {
            "target_role": latest_summary.get("target_role") or project.name,
            "analysis_context": latest_summary.get("analysis_context") or project.description or "",
        },
    }


def _enqueue_analysis(
    request: Request,
    session: Session,
    project_id: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> AnalysisRun:
    _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id)
    ready_documents = [document for document in documents if document.ingest_status == "ready"]
    if not ready_documents:
        raise HTTPException(status_code=400, detail="Upload at least one successfully ingested document first.")
    existing_run = repository.get_active_analysis_run(session, project_id)
    if existing_run:
        if request.app.state.analysis_runner.is_tracking(existing_run.id):
            return existing_run
        _mark_run_as_stale(
            session,
            existing_run,
            reason="Detected an unfinished run record without a live worker. Marked as failed before starting a new run.",
        )
        session.flush()
    run = request.app.state.analysis_engine.create_run(
        session,
        project_id,
        target_role=(target_role or "").strip() or None,
        analysis_context=(analysis_context or "").strip() or None,
    )
    session.commit()
    request.app.state.analysis_runner.submit(run.id)
    session.expire_all()
    return repository.get_analysis_run(session, run.id) or run


def _mark_run_as_stale(session: Session, run: AnalysisRun, *, reason: str) -> None:
    summary = dict(run.summary_json or {})
    summary["current_stage"] = "检测到旧任务卡住，已重置为失败"
    summary["current_facet"] = None
    summary["finished_at"] = utcnow().isoformat()
    run.summary_json = summary
    run.status = "failed"
    run.finished_at = utcnow()
    repository.add_analysis_event(
        session,
        run.id,
        event_type="lifecycle",
        level="warning",
        message="检测到旧的分析任务没有活跃 worker，已自动标记为失败。",
        payload_json={"stale_recovered": True, "reason": reason},
    )


def _resolve_run(session: Session, project_id: str, run_id: str | None) -> AnalysisRun | None:
    if run_id:
        run = repository.get_analysis_run(session, run_id)
        if not run or run.project_id != project_id:
            raise HTTPException(status_code=404, detail="Analysis run not found.")
        return run
    return repository.get_latest_analysis_run(session, project_id)


def _generate_asset_draft(request: Request, session: Session, project_id: str, *, asset_kind: str):
    project = _ensure_project(session, project_id)
    run = repository.get_latest_analysis_run(session, project_id)
    if not run:
        raise HTTPException(status_code=400, detail="Run analysis before generating an asset.")
    if run.status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="Wait for the current analysis run to finish first.")
    facets = run.facets or []
    if not facets:
        raise HTTPException(status_code=400, detail="Analysis has no facets to synthesize.")
    chat_config = repository.get_service_config(session, "chat_service")
    summary = run.summary_json or {}
    bundle = request.app.state.asset_synthesizer.build(
        asset_kind,
        project,
        facets,
        chat_config,
        target_role=summary.get("target_role"),
        analysis_context=summary.get("analysis_context"),
    )
    draft = repository.create_asset_draft(
        session,
        project_id=project_id,
        run_id=run.id,
        asset_kind=bundle.asset_kind,
        markdown_text=bundle.markdown_text,
        json_payload=bundle.json_payload,
        prompt_text=bundle.prompt_text,
        notes="Auto-generated draft. Review before publishing.",
    )
    _persist_asset_files(
        request,
        project_id,
        draft.asset_kind,
        f"draft_{draft.id}",
        draft.markdown_text,
        draft.json_payload,
        draft.system_prompt,
    )
    return draft


def _chat_with_persona(
    request: Request,
    session: Session,
    project_id: str,
    message: str,
    session_id: str | None = None,
):
    version = repository.get_latest_skill_version(session, project_id)
    if not version:
        raise HTTPException(status_code=400, detail="Publish a skill version before using the playground.")
    chat_config = repository.get_service_config(session, "chat_service")
    embedding_config = repository.get_service_config(session, "embedding_service")
    if session_id:
        chat_session = repository.get_chat_session(session, session_id, session_kind="playground")
        if not chat_session:
            raise HTTPException(status_code=404, detail="Chat session not found.")
    else:
        chat_session = repository.get_or_create_chat_session(session, project_id, session_kind="playground")
    history = sorted(chat_session.turns, key=lambda item: item.created_at)
    repository.add_chat_turn(session, session_id=chat_session.id, role="user", content=message)
    hits, retrieval_mode, retrieval_trace = request.app.state.retrieval.search(
        session,
        project_id=project_id,
        query=message,
        embedding_config=embedding_config,
        log_path=str(request.app.state.config.llm_log_path),
        limit=4,
    )
    evidence_block = "\n\n".join(
        f"[{hit.chunk_id}] {hit.document_title} / {hit.filename}\n{hit.content[:900]}"
        for hit in hits
    )
    prompt_excerpt = f"SKILL:\n{version.system_prompt[:600]}\n\nEVIDENCE:\n{evidence_block[:1200]}"
    assistant_reply, llm_meta = _generate_chat_reply(
        chat_config,
        version.system_prompt,
        history,
        message,
        evidence_block,
        log_path=str(request.app.state.config.llm_log_path),
    )
    trace = {
        "skill_version_id": version.id,
        "skill_version_number": version.version_number,
        "retrieval_mode": retrieval_mode,
        "retrieval_trace": retrieval_trace,
        "evidence": [
            {
                "chunk_id": hit.chunk_id,
                "anchor_chunk_id": hit.anchor_chunk_id or hit.chunk_id,
                "anchor_chunk_index": hit.anchor_chunk_index,
                "document_title": hit.document_title,
                "filename": hit.filename,
                "page_number": hit.page_number,
                "score": hit.score,
                "quote": hit.content[:900],
                "context_span": dict(hit.context_span or {}),
            }
            for hit in hits
        ],
        "prompt_excerpt": prompt_excerpt,
        "llm": llm_meta,
    }
    assistant_turn = repository.add_chat_turn(
        session,
        session_id=chat_session.id,
        role="assistant",
        content=assistant_reply,
        trace_json=trace,
    )
    return {
        "session_id": chat_session.id,
        "assistant_turn_id": assistant_turn.id,
        "response": assistant_reply,
        "trace": trace,
    }


def _generate_chat_reply(
    config: ServiceConfig | None,
    system_prompt: str,
    history: list[Any],
    message: str,
    evidence_block: str,
    *,
    log_path: str | None = None,
) -> tuple[str, dict[str, Any]]:
    if not config:
        prefix = "当前为无外部 LLM 降级模式。"
        evidence_lines = [line for line in evidence_block.splitlines() if line.strip()]
        evidence_hint = evidence_lines[1] if len(evidence_lines) > 1 else "暂无命中证据。"
        return (
            (
                f"{prefix}\n\n"
                f"根据已发布 skill，我会尽量保持设定中的语气与立场。\n"
                f"本轮检索提示：{evidence_hint}\n\n"
                f"你刚刚说的是：{message}"
            ),
            {"provider_kind": "local", "api_mode": "responses", "model": "fallback"},
        )
    client = OpenAICompatibleClient(config, log_path=log_path)
    messages = [{"role": "system", "content": system_prompt}]
    if evidence_block:
        messages.append({"role": "system", "content": f"Retrieved evidence from source documents:\n{evidence_block}"})
    for turn in history[-8:]:
        messages.append({"role": turn.role, "content": turn.content})
    messages.append({"role": "user", "content": message})
    try:
        result = client.chat_completion_result(messages, model=config.model, temperature=0.7, max_tokens=900)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Chat completion failed: {exc}") from exc
    return (
        result.content,
        {
            "provider_kind": config.provider_kind,
            "api_mode": config.api_mode,
            "model": result.model,
            "usage": result.usage,
            "request_url": result.request_url,
        },
    )


def _persist_asset_files(
    request: Request,
    project_id: str,
    asset_kind: str,
    base_name: str,
    markdown_text: str,
    json_payload: dict[str, Any],
    prompt_text: str,
) -> None:
    asset_dir = request.app.state.config.assets_dir / project_id / asset_kind
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / f"{base_name}.md").write_text(markdown_text, encoding="utf-8")
    (asset_dir / f"{base_name}.json").write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (asset_dir / f"{base_name}.prompt.txt").write_text(prompt_text, encoding="utf-8")


def _ordered_facets(facets: list[AnalysisFacet]) -> list[AnalysisFacet]:
    order = {facet.key: index for index, facet in enumerate(FACETS)}
    return sorted(facets, key=lambda item: order.get(item.facet_key, 999))


def _ensure_project(session: Session, project_id: str):
    project = repository.get_project(session, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")
    return project


def _get_project_document(session: Session, project_id: str, document_id: str) -> DocumentRecord:
    document = repository.get_document(session, document_id)
    if not document or document.project_id != project_id:
        raise HTTPException(status_code=404, detail="Document not found.")
    return document


def _delete_document_with_file(document: DocumentRecord) -> None:
    path = Path(document.storage_path)
    if path.exists() and path.is_file():
        path.unlink()


def _delete_project_resources(request: Request, session: Session, project_id: str) -> None:
    document_paths = [Path(document.storage_path) for document in repository.list_project_documents(session, project_id)]
    artifact_paths = list(
        Path(artifact.storage_path)
        for artifact in session.scalars(select(GeneratedArtifact).where(GeneratedArtifact.project_id == project_id))
    )
    repository.delete_project_cascade(session, project_id)
    config = request.app.state.config
    for path in document_paths + artifact_paths:
        if path.exists() and path.is_file():
            path.unlink(missing_ok=True)
    for directory in (
        config.upload_dir / project_id,
        config.assets_dir / project_id,
        config.output_dir / project_id,
        config.skill_dir / project_id,
    ):
        if directory.exists():
            shutil.rmtree(directory, ignore_errors=True)


def _serialize_document(document: DocumentRecord) -> dict[str, Any]:
    metadata = document.metadata_json or {}
    return {
        "id": document.id,
        "filename": document.filename,
        "title": document.title or document.filename,
        "source_type": document.source_type,
        "status": document.ingest_status,
        "error_message": document.error_message,
        "user_note": metadata.get("user_note", ""),
    }


def _serialize_analysis_run(run: AnalysisRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "summary": run.summary_json or {},
        "events": [
            {
                "id": event.id,
                "event_type": event.event_type,
                "level": event.level,
                "message": event.message,
                "payload": event.payload_json or {},
                "created_at": event.created_at.isoformat(),
            }
            for event in sorted(run.events, key=lambda item: item.created_at, reverse=True)
        ],
        "facets": [
            {
                "facet_key": facet.facet_key,
                "status": facet.status,
                "accepted": bool(facet.accepted),
                "confidence": facet.confidence,
                "findings": facet.findings_json or {},
                "evidence": facet.evidence_json or [],
                "conflicts": facet.conflicts_json or [],
                "error_message": facet.error_message,
            }
            for facet in _ordered_facets(run.facets)
        ],
    }


def _serialize_draft(draft) -> dict[str, Any]:
    return {
        "id": draft.id,
        "asset_kind": getattr(draft, "asset_kind", "skill"),
        "status": draft.status,
        "markdown_text": draft.markdown_text,
        "json_payload": draft.json_payload,
        "prompt_text": draft.system_prompt,
        "system_prompt": draft.system_prompt,
        "notes": draft.notes,
    }


def _serialize_chat_session(chat_session) -> dict[str, Any]:
    turns = sorted(chat_session.turns, key=lambda item: item.created_at) if getattr(chat_session, "turns", None) else []
    return {
        "id": chat_session.id,
        "session_kind": chat_session.session_kind,
        "title": chat_session.title or "Untitled Session",
        "created_at": chat_session.created_at.isoformat() if chat_session.created_at else None,
        "last_active_at": chat_session.last_active_at.isoformat() if chat_session.last_active_at else None,
        "turn_count": len(turns),
    }


def _serialize_artifact(artifact: GeneratedArtifact) -> dict[str, Any]:
    return {
        "id": artifact.id,
        "filename": artifact.filename,
        "summary": artifact.summary,
        "mime_type": artifact.mime_type,
        "created_at": artifact.created_at.isoformat(),
        "download_url": f"/api/projects/{artifact.project_id}/preprocess/artifacts/{artifact.id}/download",
    }


def _serialize_chat_turn(turn) -> dict[str, Any]:
    return {
        "id": turn.id,
        "role": turn.role,
        "content": turn.content,
        "trace": turn.trace_json or {},
        "created_at": turn.created_at.isoformat(),
    }


def _serialize_preprocess_session_detail(chat_session) -> dict[str, Any]:
    turns = sorted(chat_session.turns, key=lambda item: item.created_at)
    artifacts = sorted(chat_session.artifacts, key=lambda item: item.created_at, reverse=True)
    return {
        **_serialize_chat_session(chat_session),
        "turns": [_serialize_chat_turn(turn) for turn in turns],
        "artifacts": [_serialize_artifact(artifact) for artifact in artifacts],
    }


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _settings_payload(payload: dict[str, Any], *, default_provider: str) -> dict[str, Any]:
    normalized = dict(payload)
    provider_kind = normalize_provider_kind(
        normalized.get("provider_kind") or ("openai-compatible" if normalized.get("base_url") else default_provider)
    )
    normalized["provider_kind"] = provider_kind
    normalized["base_url"] = normalized.get("base_url", "")
    normalized["api_key"] = normalized.get("api_key", "")
    normalized["model"] = normalized.get("model", "")
    normalized["api_mode"] = normalize_api_mode(normalized.get("api_mode"))
    return normalized


def _normalize_asset_kind(value: str | None) -> str:
    candidate = (value or "skill").strip().lower()
    return candidate if candidate in ASSET_KINDS else "skill"


def _asset_label(asset_kind: str) -> str:
    return "用户剖析报告" if asset_kind == "profile_report" else "Skill"
