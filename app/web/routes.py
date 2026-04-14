from __future__ import annotations

import io
import json
import re
import shutil
import time
import zipfile
from queue import Empty
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
import asyncio
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analysis.facets import FACETS
from app.llm.client import OpenAICompatibleClient, normalize_api_mode, normalize_provider_kind
from app.models import AnalysisFacet, AnalysisRun, DocumentRecord, GeneratedArtifact, utcnow
from app.schemas import (
    ASSET_KINDS,
    DEFAULT_ANALYSIS_CONCURRENCY,
    MAX_ANALYSIS_CONCURRENCY,
    MIN_ANALYSIS_CONCURRENCY,
    ServiceConfig,
)
from app.storage import repository
from app.web.ui_strings import DEFAULT_LOCALE, page_strings


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
    {"value": "cc_skill", "label": "Claude Code Skill"},
    {"value": "profile_report", "label": "用户剖析报告"},
)


PROVIDER_OPTIONS = (
    {"value": "openai", "label": "OpenAI 官方"},
    {"value": "xai", "label": "xAI 官方"},
    {"value": "gemini", "label": "Gemini 官方"},
    {"value": "openai-compatible", "label": "OpenAI Compatible 自定义入口"},
)
ASSET_KIND_OPTIONS = (
    {"value": "skill", "label": "Skill"},
    {"value": "cc_skill", "label": "Claude Code Skill"},
    {"value": "profile_report", "label": "用户画像报告"},
)

ANALYSIS_EVENT_LIMIT = 48
ANALYSIS_SUMMARY_PREVIEW_LIMIT = 420
ANALYSIS_LIVE_TEXT_PREVIEW_LIMIT = 3200
ANALYSIS_RESPONSE_TEXT_PREVIEW_LIMIT = 2400
ANALYSIS_REQUEST_PAYLOAD_PREVIEW_LIMIT = 1600
SKILL_DOCUMENT_ORDER = ("skill", "personality", "memories", "merge")
SKILL_DOCUMENT_FILENAMES = {
    "skill": "Skill.md",
    "personality": "personality.md",
    "memories": "memories.md",
    "merge": "Skill_merge.md",
}
CC_SKILL_DOCUMENT_ORDER = ("skill", "personality", "memories")
CC_SKILL_DOCUMENT_FILENAMES = {
    "skill": "SKILL.md",
    "personality": "personality.md",
    "memories": "memories.md",
}


class ProjectCreatePayload(BaseModel):
    name: str
    description: str | None = None
    mode: str = "group"


class ChatPayload(BaseModel):
    message: str
    session_id: str | None = None


class AnalysisRequestPayload(BaseModel):
    target_role: str | None = None
    analysis_context: str | None = None
    concurrency: int | None = Field(default=None, ge=MIN_ANALYSIS_CONCURRENCY, le=MAX_ANALYSIS_CONCURRENCY)


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


def _page_context(page_name: str, **kwargs: Any) -> dict[str, Any]:
    return {
        "locale": DEFAULT_LOCALE,
        "ui": page_strings(page_name),
        **kwargs,
    }


def _ok_response(message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "ok", "message": message, **payload}


def _task_response(message: str, task: dict[str, Any], **payload: Any) -> dict[str, Any]:
    return {
        **task,
        "request_status": "ok",
        "message": message,
        "task": task,
        "task_id": task.get("task_id"),
        "progress_percent": task.get("progress_percent", 0),
        **payload,
    }


@router.get("/", response_class=HTMLResponse)
def index(request: Request, session: SessionDep):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=_page_context(
            "index",
            projects=repository.list_projects(session),
            chat_configured=repository.get_service_config(session, "chat_service") is not None,
            embedding_configured=repository.get_service_config(session, "embedding_service") is not None,
        ),
    )


@router.post("/projects")
def create_project_form(
    session: SessionDep,
    name: Annotated[str, Form(...)],
    description: Annotated[str | None, Form()] = None,
    mode: Annotated[str, Form()] = "group",
):
    project = repository.create_project(session, name=name, description=description, mode=mode)
    return RedirectResponse(url=f"/projects/{project.id}", status_code=303)


@router.post("/projects/{project_id}/profiles")
def create_profile_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    name: Annotated[str, Form(...)],
    description: Annotated[str | None, Form()] = None,
):
    _ensure_project(session, project_id)
    child = repository.create_project(session, name=name, description=description, mode="single", parent_id=project_id)
    return RedirectResponse(url=f"/projects/{project_id}", status_code=303)


@router.get("/projects/{project_id}", response_class=HTMLResponse)
def project_detail(request: Request, project_id: str, session: SessionDep):
    context = _project_context(session, project_id)
    return templates.TemplateResponse(
        request=request,
        name="project_detail.html",
        context=_page_context("project", **context),
    )


@router.post("/projects/{project_id}/update")
def update_project_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    name: Annotated[str, Form(...)],
    description: Annotated[str | None, Form()] = None,
    mode: Annotated[str, Form()] = "group",
):
    project = _ensure_project(session, project_id)
    project.name = name.strip()
    project.description = (description or "").strip() or None
    project.mode = mode
    session.commit()
    return RedirectResponse(url=f"/projects/{project.id}", status_code=303)


@router.post("/projects/{project_id}/delete")
def delete_project_form(request: Request, project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    parent_id = project.parent_id
    _delete_project_resources(request, session, project.id)
    session.commit()
    if parent_id:
        return RedirectResponse(url=f"/projects/{parent_id}", status_code=303)
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
    await ingest.create_documents_from_uploads(session, project_id=project_id, uploads=files)
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
    concurrency: Annotated[int | None, Form(ge=MIN_ANALYSIS_CONCURRENCY, le=MAX_ANALYSIS_CONCURRENCY)] = None,
):
    run = _enqueue_analysis(
        request,
        session,
        project_id,
        target_role=target_role,
        analysis_context=analysis_context,
        concurrency=concurrency,
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
        context=_page_context(
            "analysis",
            project=project,
            run=run,
            serialized_run=json.dumps(serialized_run, ensure_ascii=False) if serialized_run else "null",
            run_id=run.id if run else "",
            facet_catalog=FACETS,
        ),
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


@router.get("/projects/{project_id}/analysis/export")
def export_analysis_zip(request: Request, project_id: str, session: SessionDep, run_id: str | None = Query(default=None)):
    project = _ensure_project(session, project_id)
    run = _resolve_run(session, project_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="No analysis run found.")
        
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for facet in run.facets:
            facet_data = {
                "facet_key": facet.facet_key,
                "status": facet.status,
                "confidence": facet.confidence,
                "findings": facet.findings_json,
                "evidence": facet.evidence_json,
                "conflicts": facet.conflicts_json,
            }
            json_str = json.dumps(facet_data, ensure_ascii=False, indent=2)
            zip_file.writestr(f"{facet.facet_key}.json", json_str)
            
    zip_buffer.seek(0)
    filename = f"analysis_export_{project.name}_{run.id[:8]}.zip"
    return StreamingResponse(
        iter([zip_buffer.getvalue()]), 
        media_type="application/zip",
        headers=_download_headers(filename, fallback_name=f"analysis_export_{run.id[:8]}.zip")
    )


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
        context=_page_context(
            "assets",
            project=project,
            asset_kind=asset_kind,
            asset_label=_asset_label(asset_kind),
            asset_options=(
                {"value": "skill", "label": "Skill"},
                {"value": "cc_skill", "label": "Claude Code Skill"},
                {"value": "profile_report", "label": "用户画像报告"},
            ),
            draft=draft,
            versions=versions,
            latest_run=latest_run,
            draft_json_pretty=json.dumps(draft.json_payload, ensure_ascii=False, indent=2) if draft else "{}",
        ),
    )


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/{document_key}")
def download_asset_draft_document_api(project_id: str, draft_id: str, document_key: str, session: SessionDep):
    draft = repository.get_asset_draft(session, draft_id)
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    if document_key == "bundle":
        filename, payload = _build_skill_export_zip(
            draft.asset_kind,
            draft.json_payload,
            draft.markdown_text,
            base_name=f"skill_bundle_draft_{draft.id[:8]}",
        )
        return StreamingResponse(iter([payload]), media_type="application/zip", headers=_download_headers(filename))
    filename, content = _resolve_skill_export_document(draft.asset_kind, draft.json_payload, draft.markdown_text, document_key)
    return _markdown_download_response(filename, content)


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/bundle")
def download_asset_draft_bundle_api(project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_asset_draft(session, draft_id)
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    filename, payload = _build_skill_export_zip(
        draft.asset_kind,
        draft.json_payload,
        draft.markdown_text,
        base_name=f"skill_bundle_draft_{draft.id[:8]}",
    )
    return StreamingResponse(iter([payload]), media_type="application/zip", headers=_download_headers(filename))


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/{document_key}")
def download_asset_version_document_api(project_id: str, version_id: str, document_key: str, session: SessionDep):
    version = repository.get_asset_version(session, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(status_code=404, detail="Asset version not found.")
    if document_key == "bundle":
        filename, payload = _build_skill_export_zip(
            version.asset_kind,
            version.json_payload,
            version.markdown_text,
            base_name=f"skill_bundle_v{version.version_number}",
        )
        return StreamingResponse(iter([payload]), media_type="application/zip", headers=_download_headers(filename))
    filename, content = _resolve_skill_export_document(version.asset_kind, version.json_payload, version.markdown_text, document_key)
    return _markdown_download_response(filename, content)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/bundle")
def download_asset_version_bundle_api(project_id: str, version_id: str, session: SessionDep):
    version = repository.get_asset_version(session, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(status_code=404, detail="Asset version not found.")
    filename, payload = _build_skill_export_zip(
        version.asset_kind,
        version.json_payload,
        version.markdown_text,
        base_name=f"skill_bundle_v{version.version_number}",
    )
    return StreamingResponse(iter([payload]), media_type="application/zip", headers=_download_headers(filename))


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
    payload_data = json.loads(json_payload)
    normalized_payload, normalized_prompt = _normalize_saved_asset_content(
        draft.asset_kind,
        payload_data,
        markdown_text,
        (prompt_text or system_prompt or "").strip(),
    )
    draft.markdown_text = markdown_text
    draft.json_payload = normalized_payload
    draft.system_prompt = normalized_prompt
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
    normalized_payload, normalized_prompt = _normalize_saved_asset_content(
        "skill",
        json.loads(json_payload),
        markdown_text,
        system_prompt,
    )
    draft.markdown_text = markdown_text
    draft.json_payload = normalized_payload
    draft.system_prompt = normalized_prompt
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
        context=_page_context(
            "playground",
            project=project,
            version=version,
            chat_session=chat_session,
            turns=turns,
        ),
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
                title="新建预分析会话",
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
        "locale": DEFAULT_LOCALE,
        "ui_strings": page_strings("preprocess"),
    }
    return templates.TemplateResponse(
        request=request,
        name="preprocess.html",
        context=_page_context(
            "preprocess",
            project=context["project"],
            bootstrap=json.dumps(bootstrap, ensure_ascii=False),
        ),
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, session: SessionDep):
    chat_setting = repository.get_setting(session, "chat_service")
    embedding_setting = repository.get_setting(session, "embedding_service")
    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context=_page_context(
            "settings",
            chat_setting=_settings_payload(chat_setting.value_json if chat_setting else {}, default_provider="openai"),
            embedding_setting=_settings_payload(
                embedding_setting.value_json if embedding_setting else {},
                default_provider="openai",
            ),
            provider_options=(
                {"value": "openai", "label": "OpenAI 官方"},
                {"value": "xai", "label": "xAI 官方"},
                {"value": "gemini", "label": "Gemini 官方"},
                {"value": "openai-compatible", "label": "OpenAI Compatible 自定义"},
            ),
            api_mode_options=API_MODE_OPTIONS,
        ),
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
        raise HTTPException(status_code=404, detail="未知服务类型。")
    normalized_provider = normalize_provider_kind(provider_kind)
    normalized_base_url = (base_url or "").strip()
    if normalized_provider == "openai-compatible" and not normalized_base_url:
        raise HTTPException(status_code=400, detail="自定义 OpenAI Compatible 服务必须填写 Base URL。")
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
    project = repository.create_project(session, payload.name, payload.description, mode=payload.mode)
    return _ok_response(
        "项目已创建。",
        id=project.id,
        name=project.name,
        description=project.description,
        mode=project.mode,
    )


@router.delete("/api/projects/{project_id}")
def delete_project_api(request: Request, project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    _delete_project_resources(request, session, project.id)
    session.commit()
    return _ok_response("项目已删除。", ok=True, project_id=project.id)


@router.post("/api/projects/{project_id}/documents")
async def upload_documents_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    files: list[UploadFile] = File(...),
):
    _ensure_project(session, project_id)
    ingest = request.app.state.ingest_service
    created = await ingest.create_documents_from_uploads(session, project_id=project_id, uploads=files)
    return _ok_response("文档上传完成。", documents=[_serialize_document(document) for document in created])





@router.get("/api/projects/{project_id}/documents")
def list_documents_api(project_id: str, session: SessionDep, offset: int = 0, limit: int = 20):
    _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id, limit=limit, offset=offset)
    doc_counts = repository.count_project_documents(session, project_id)
    return {
        "status": "ok",
        "message": "已返回文档列表。",
        "documents": [_serialize_document(doc) for doc in documents],
        "total": doc_counts["total"],
        "ready": doc_counts["ready"],
        "failed": doc_counts["failed"],
        "queued": doc_counts.get("queued", 0),
        "processing": doc_counts.get("processing", 0),
        "pending": doc_counts.get("pending", 0),
        "has_more": offset + len(documents) < doc_counts["total"],
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/projects/{project_id}/documents/{document_id}/task")
def get_document_task_status(request: Request, project_id: str, document_id: str):
    task = request.app.state.ingest_task_manager.get_by_document(document_id)
    if not task:
        return {"task_id": None, "status": "missing", "progress_percent": 0, "message": "当前文档没有活动任务。"}
    return _task_response("已返回文档任务状态。", task)


@router.get("/api/projects/{project_id}/tasks")
def get_project_tasks(request: Request, project_id: str):
    tasks = request.app.state.ingest_task_manager.get_by_project(project_id)
    return _ok_response("已返回项目任务列表。", tasks=tasks)


@router.post("/api/projects/{project_id}/documents/{document_id}/process")
def process_document_api(request: Request, project_id: str, document_id: str, session: SessionDep):
    document = _get_project_document(session, project_id, document_id)
    embedding_config = repository.get_service_config(session, "embedding_service")
    task_manager = request.app.state.ingest_task_manager
    task_manager.set_embedding_config(embedding_config)

    task = task_manager.submit(
        project_id=project_id,
        document_id=document_id,
        filename=document.filename,
        storage_path=document.storage_path,
        mime_type=None,
    )
    return _task_response("文档已加入处理队列。", task)


@router.post("/api/projects/{project_id}/process-all")
def process_all_documents_api(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    embedding_config = repository.get_service_config(session, "embedding_service")
    task_manager = request.app.state.ingest_task_manager
    task_manager.set_embedding_config(embedding_config)
    documents = repository.list_project_documents(session, project_id)
    submitted = []
    for doc in documents:
        if doc.ingest_status not in ("ready", "processing", "queued"):
            task = task_manager.submit(
                project_id=project_id,
                document_id=doc.id,
                filename=doc.filename,
                storage_path=doc.storage_path,
                mime_type=None,
            )
            # update db status to queued to immediately reflect in UI
            doc.ingest_status = "queued"
            submitted.append({"document_id": doc.id, "filename": doc.filename, "task": task})
    session.commit()
    return _ok_response("批量处理任务已提交。", submitted=submitted)


@router.post("/api/projects/{project_id}/retry-all")
def retry_all_documents_api(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    embedding_config = repository.get_service_config(session, "embedding_service")
    task_manager = request.app.state.ingest_task_manager
    task_manager.set_embedding_config(embedding_config)
    
    # First, forcefully stop any existing processing for this project
    task_manager.stop_project_tasks(project_id)
    
    documents = repository.list_project_documents(session, project_id)
    submitted = []
    for doc in documents:
        # Retry all documents that are not 'ready'
        if doc.ingest_status != "ready":
            doc.error_message = None
            task = task_manager.submit(
                project_id=project_id,
                document_id=doc.id,
                filename=doc.filename,
                storage_path=doc.storage_path,
                mime_type=None,
            )
            # update db status to queued to immediately reflect in UI
            doc.ingest_status = "queued"
            submitted.append({"document_id": doc.id, "filename": doc.filename, "task": task})
    session.commit()
    return _ok_response("重试任务已提交。", submitted=submitted)


@router.post("/api/projects/{project_id}/stop-processing")
def stop_processing_api(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    task_manager = request.app.state.ingest_task_manager
    task_manager.stop_project_tasks(project_id)
    return _ok_response("当前项目的处理任务已停止。", stopped=True)


@router.post("/api/projects/{project_id}/documents/{document_id}")
def update_document_api(
    project_id: str,
    document_id: str,
    payload: DocumentUpdatePayload,
    session: SessionDep,
):
    document = _get_project_document(session, project_id, document_id)
    repository.update_document(session, document, title=payload.title, source_type=payload.source_type, user_note=payload.user_note)
    return _ok_response("文档信息已更新。", **_serialize_document(document))


@router.post("/api/projects/{project_id}/documents/{document_id}/delete")
def delete_document_api(project_id: str, document_id: str, session: SessionDep):
    document = _get_project_document(session, project_id, document_id)
    _delete_document_with_file(document)
    repository.delete_document(session, document)
    return _ok_response("文档已删除。", ok=True, document_id=document_id)


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
        concurrency=payload.concurrency,
    )
    serialized = _serialize_analysis_run(run)
    return _ok_response("分析任务已创建。", **serialized)


@router.get("/api/projects/{project_id}/analysis")
def get_analysis_api(
    project_id: str,
    session: SessionDep,
    run_id: str | None = Query(default=None),
):
    run = _resolve_run(session, project_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="未找到分析记录。")
    payload = _serialize_analysis_run(run)
    return {"status": "ok", "message": "已返回分析状态。", **payload}


@router.get("/api/projects/{project_id}/analysis/stream")
def stream_analysis_api(request: Request, project_id: str, session: SessionDep, run_id: str | None = Query(default=None)):
    run = _resolve_run(session, project_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="未找到分析记录。")

    hub = request.app.state.analysis_stream_hub
    subscription = hub.subscribe(run.id)

    async def generate():
        last_snapshot = ""
        from starlette.concurrency import run_in_threadpool

        def fetch_payload():
            with request.app.state.db.session() as live_session:
                live_run = _resolve_run(live_session, project_id, run_id or run.id)
                if not live_run:
                    return None
                return _serialize_analysis_run(live_run)

        try:
            while True:
                try:
                    await run_in_threadpool(subscription.get, True, 15.0)
                except Empty:
                    pass

                payload = await run_in_threadpool(fetch_payload)
                if not payload:
                    break

                encoded = json.dumps(payload, ensure_ascii=False)
                if encoded != last_snapshot:
                    last_snapshot = encoded
                    yield _format_sse("snapshot", payload)
                if payload["status"] not in {"queued", "running"}:
                    yield _format_sse("done", {"run_id": payload["id"], "status": payload["status"]})
                    break
        finally:
            hub.unsubscribe(run.id, subscription)

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
            raise HTTPException(status_code=409, detail="当前项目已有分析任务正在运行。")
        _mark_run_as_stale(
            session,
            run,
            reason="Detected an unfinished run record without a live worker before facet rerun API call.",
        )
    latest_run = repository.get_latest_analysis_run(session, project_id)
    if not latest_run:
        raise HTTPException(status_code=404, detail="未找到分析记录。")
    request.app.state.analysis_runner.submit_facet_rerun(project_id, facet_key)
    session.expire_all()
    refreshed = repository.get_analysis_run(session, latest_run.id) or latest_run
    return _ok_response("维度重跑任务已提交。", **_serialize_analysis_run(refreshed))


@router.post("/api/projects/{project_id}/rechunk")
def start_rechunk_api(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    embedding_config = repository.get_service_config(session, "embedding_service")
    manager = request.app.state.rechunk_manager
    try:
        task = manager.submit(project_id=project_id, embedding_config=embedding_config)
        return _task_response("重分块任务已提交。", task)
    except ValueError as exc:
        task_id = str(exc)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前项目已有重分块任务在运行。",
                "task_id": task_id,
                "task": manager.get(task_id),
            },
        ) from exc


@router.get("/api/projects/{project_id}/rechunk/{task_id}")
def get_rechunk_task_api(request: Request, project_id: str, task_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    task = request.app.state.rechunk_manager.get(task_id)
    if not task or task.get("project_id") != project_id:
        raise HTTPException(status_code=404, detail="未找到重分块任务。")
    return _task_response("已返回重分块任务状态。", task)


@router.post("/api/projects/{project_id}/assets/generate/stream")
def generate_asset_stream_api(request: Request, project_id: str, payload: AssetGeneratePayload):
    from queue import Queue
    from threading import Thread
    
    events: Queue[dict[str, Any] | None] = Queue()
    asset_kind = _normalize_asset_kind(payload.asset_kind)

    def emit_status(
        phase: str,
        progress_percent: int,
        message: str,
        *,
        status: str = "running",
    ) -> None:
        events.put(
            {
                "type": "status",
                "status": status,
                "phase": phase,
                "progress_percent": progress_percent,
                "message": message,
                "asset_kind": asset_kind,
            }
        )
    
    def worker():
        try:
            emit_status("prepare", 6, f"开始生成{_asset_label(asset_kind)}草稿")
            with request.app.state.db.session() as session:
                project = _ensure_project(session, project_id)
                run = repository.get_latest_analysis_run(session, project_id)
                if not run or run.status in {"queued", "running"}:
                    events.put({"type": "error", "message": "分析结果尚未就绪。"})
                    return
                facets = run.facets or []
                if not facets:
                    events.put({"type": "error", "message": "当前分析没有可合成的维度结果。"})
                    return
                chat_config = repository.get_service_config(session, "chat_service")
                summary = run.summary_json or {}

                emit_status("load", 14, "正在读取最新分析结果")

                def stream_callback(chunk: str):
                    events.put({"type": "delta", "chunk": chunk})

                def progress_callback(progress: dict[str, Any]):
                    events.put(
                        {
                            "type": "status",
                            "status": "running",
                            "phase": progress.get("phase", "running"),
                            "progress_percent": int(progress.get("progress_percent", 0) or 0),
                            "message": str(progress.get("message", "") or ""),
                            "asset_kind": asset_kind,
                        }
                    )
                
                bundle = request.app.state.asset_synthesizer.build(
                    asset_kind,
                    project,
                    facets,
                    chat_config,
                    target_role=summary.get("target_role"),
                    analysis_context=summary.get("analysis_context"),
                    stream_callback=stream_callback,
                    progress_callback=progress_callback,
                    session=session,
                    retrieval_service=request.app.state.retrieval,
                )

                emit_status("persist", 94, "正在保存草稿和导出文件")
                draft = repository.create_asset_draft(
                    session,
                    project_id=project_id,
                    run_id=run.id,
                    asset_kind=bundle.asset_kind,
                    markdown_text=bundle.markdown_text,
                    json_payload=bundle.json_payload,
                    prompt_text=bundle.prompt_text,
                    notes="系统自动生成草稿，发布前请先复核。",
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
                events.put(
                    {
                        "type": "done",
                        "status": "completed",
                        "phase": "done",
                        "progress_percent": 100,
                        "message": "草稿生成完成，正在跳转。",
                        "draft_id": draft.id,
                        "asset_kind": asset_kind,
                    }
                )
        except Exception as e:
            events.put({"type": "error", "message": str(e)})
        finally:
            events.put(None)
            
    Thread(target=worker, daemon=True).start()
    
    def generator():
        while True:
            item = events.get()
            if item is None:
                break
            yield f"event: {item['type']}\ndata: {json.dumps(item, ensure_ascii=False)}\n\n"
            
    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.post("/api/projects/{project_id}/assets/generate")
def generate_asset_api(request: Request, project_id: str, payload: AssetGeneratePayload, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind=_normalize_asset_kind(payload.asset_kind))
    return {**_serialize_draft(draft), "request_status": "ok", "message": "资产草稿已生成。"}


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
        raise HTTPException(status_code=404, detail="未找到资产草稿。")
    normalized_payload, normalized_prompt = _normalize_saved_asset_content(
        draft.asset_kind,
        payload.json_payload,
        payload.markdown_text,
        payload.prompt_text,
    )
    draft.markdown_text = payload.markdown_text
    draft.json_payload = normalized_payload
    draft.system_prompt = normalized_prompt
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
    return {**_serialize_draft(draft), "request_status": "ok", "message": "资产草稿已保存。"}


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
        raise HTTPException(status_code=404, detail="未找到资产草稿。")
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
        "request_status": "ok",
        "message": "资产版本已发布。",
    }


@router.post("/api/projects/{project_id}/skills/generate")
def generate_skill_api(request: Request, project_id: str, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind="skill")
    return {**_serialize_draft(draft), "request_status": "ok", "message": "Skill 草稿已生成。"}


@router.post("/api/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_api(request: Request, project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_skill_draft(session, draft_id)
    if not draft:
        raise HTTPException(status_code=404, detail="未找到 Skill 草稿。")
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
        "request_status": "ok",
        "message": "Skill 版本已发布。",
    }


@router.post("/api/projects/{project_id}/playground/chat")
def playground_chat_api(request: Request, project_id: str, payload: ChatPayload, session: SessionDep):
    return _ok_response("试聊回复已生成。", **_chat_with_persona(request, session, project_id, payload.message, payload.session_id))


@router.get("/api/projects/{project_id}/preprocess/sessions")
def list_preprocess_sessions_api(project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    return _ok_response("已返回预分析会话列表。", sessions=[_serialize_chat_session(item) for item in sessions])


@router.post("/api/projects/{project_id}/preprocess/sessions")
def create_preprocess_session_api(project_id: str, payload: PreprocessSessionCreatePayload, session: SessionDep):
    _ensure_project(session, project_id)
    chat_session = repository.create_chat_session(
        session,
        project_id=project_id,
        session_kind="preprocess",
        title=payload.title or "新建预分析会话",
    )
    return _ok_response("预分析会话已创建。", **_serialize_chat_session(chat_session))


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def get_preprocess_session_api(project_id: str, session_id: str, session: SessionDep):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到预分析会话。")
    return _ok_response("已返回预分析会话详情。", **_serialize_preprocess_session_detail(chat_session))


@router.patch("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def update_preprocess_session_api(
    project_id: str,
    session_id: str,
    payload: PreprocessSessionUpdatePayload,
    session: SessionDep,
):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到预分析会话。")
    repository.rename_chat_session(session, chat_session, title=payload.title)
    return _ok_response("预分析会话已更新。", **_serialize_chat_session(chat_session))


@router.delete("/api/projects/{project_id}/preprocess/sessions/{session_id}")
def delete_preprocess_session_api(project_id: str, session_id: str, session: SessionDep):
    chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到预分析会话。")
    repository.delete_chat_session(session, chat_session)
    return _ok_response("预分析会话已删除。", ok=True, session_id=session_id)


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
    return _ok_response("预分析消息已提交。", **result)


@router.get("/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}")
def stream_preprocess_events_api(request: Request, project_id: str, session_id: str, stream_id: str):
    del project_id, session_id
    try:
        generator = request.app.state.preprocess_service.stream_events(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="未找到预分析流。") from exc
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
        raise HTTPException(status_code=400, detail=f"{service} 服务尚未配置。")
    client = OpenAICompatibleClient(config, log_path=str(request.app.state.config.llm_log_path))
    try:
        return _ok_response("已返回模型列表。", service=service, models=client.list_models())
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _project_context(session: Session, project_id: str, *, document_limit: int = 20, document_offset: int = 0) -> dict[str, Any]:
    project = _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id, limit=document_limit, offset=document_offset)
    doc_counts = repository.count_project_documents(session, project_id)
    latest_run = repository.get_latest_analysis_run(session, project_id, load_facets=False, load_events=False)
    latest_draft = repository.get_latest_skill_draft(session, project_id)
    latest_version = repository.get_latest_skill_version(session, project_id)
    latest_summary = latest_run.summary_json or {} if latest_run else {}
    preprocess_sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    
    profiles = []
    if project.mode == "group":
        for p in repository.list_child_projects(session, project_id):
            p.latest_run = repository.get_latest_analysis_run(session, p.id)
            p.latest_skill = repository.get_latest_skill_version(session, p.id)
            profiles.append(p)
            
    return {
        "project": project,
        "profiles": profiles,
        "documents": documents,
        "project_bootstrap": json.dumps(
            {
                "project": {"id": project.id, "name": project.name, "mode": project.mode},
                "documents": [_serialize_document(item) for item in documents],
                "pagination": {
                    "limit": document_limit,
                    "offset": document_offset,
                    "has_more": document_offset + len(documents) < doc_counts["total"],
                },
                "stats": {
                    "document_count": doc_counts["total"],
                    "ready_count": doc_counts["ready"],
                    "failed_count": doc_counts["failed"],
                    "queued_count": doc_counts.get("queued", 0),
                    "processing_count": doc_counts.get("processing", 0),
                    "pending_count": doc_counts.get("pending", 0),
                },
                "locale": DEFAULT_LOCALE,
                "ui_strings": page_strings("project"),
            },
            ensure_ascii=False,
        ),
        "latest_run": latest_run,
        "latest_draft": latest_draft,
        "latest_version": latest_version,
        "preprocess_sessions": preprocess_sessions,
        "stats": {
            "document_count": doc_counts["total"],
            "ready_count": doc_counts["ready"],
            "failed_count": doc_counts["failed"],
            "queued_count": doc_counts.get("queued", 0),
            "processing_count": doc_counts.get("processing", 0),
            "pending_count": doc_counts.get("pending", 0),
        },
        "document_pagination": {
            "limit": document_limit,
            "offset": document_offset,
            "has_more": document_offset + len(documents) < doc_counts["total"],
        },
        "analysis_defaults": {
            "target_role": latest_summary.get("target_role") or project.name,
            "analysis_context": latest_summary.get("analysis_context") or project.description or "",
            "concurrency": latest_summary.get("concurrency") or DEFAULT_ANALYSIS_CONCURRENCY,
        },
        "analysis_concurrency_default": DEFAULT_ANALYSIS_CONCURRENCY,
    }


def _enqueue_analysis(
    request: Request,
    session: Session,
    project_id: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
    concurrency: int | None = None,
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
        concurrency=concurrency,
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
        session=session,
        retrieval_service=request.app.state.retrieval,
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

    evidence_block = ""
    prompt_excerpt = f"SKILL:\n{version.system_prompt[:1800]}"
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
        return (
            (
                f"{prefix}\n\n"
                f"根据已发布 skill，我会尽量保持设定中的语气与立场。\n\n"
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
    if asset_kind in {"skill", "cc_skill"}:
        for key, document in _skill_documents_for_export(asset_kind, json_payload, markdown_text).items():
            filename = document["filename"]
            content = document["markdown"]
            (asset_dir / f"{base_name}.{filename}").write_text(content, encoding="utf-8")


def _download_headers(filename: str, *, fallback_name: str | None = None) -> dict[str, str]:
    ascii_name = _safe_ascii_filename(fallback_name or filename)
    return {
        "Content-Disposition": f"attachment; filename={ascii_name}; filename*=UTF-8''{quote(filename)}"
    }


def _safe_ascii_filename(value: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    candidate = candidate.strip("._")
    return candidate or "download"


def _markdown_download_response(filename: str, content: str) -> StreamingResponse:
    return StreamingResponse(
        iter([(content or "").encode("utf-8")]),
        media_type="text/markdown; charset=utf-8",
        headers=_download_headers(filename),
    )


def _skill_documents_for_export(
    asset_kind: str,
    json_payload: dict[str, Any] | None,
    markdown_text: str,
) -> dict[str, dict[str, str]]:
    if asset_kind not in {"skill", "cc_skill"}:
        raise HTTPException(status_code=400, detail="Only skill assets support split document export.")

    document_order = CC_SKILL_DOCUMENT_ORDER if asset_kind == "cc_skill" else SKILL_DOCUMENT_ORDER
    filename_map = CC_SKILL_DOCUMENT_FILENAMES if asset_kind == "cc_skill" else SKILL_DOCUMENT_FILENAMES

    payload = json_payload or {}
    documents = payload.get("documents") if isinstance(payload, dict) else None
    export_docs: dict[str, dict[str, str]] = {}

    if isinstance(documents, dict):
        for key in document_order:
            document = documents.get(key) or {}
            if isinstance(document, dict):
                export_docs[key] = {
                    "filename": str(document.get("filename") or filename_map[key]),
                    "markdown": str(document.get("markdown") or "").strip(),
                }

    if export_docs:
        for key in document_order:
            export_docs.setdefault(
                key,
                {"filename": filename_map[key], "markdown": ""},
            )
        if asset_kind == "skill":
            merge_markdown = str(markdown_text or export_docs.get("merge", {}).get("markdown") or "").strip()
            if merge_markdown:
                export_docs["merge"] = {
                    "filename": export_docs.get("merge", {}).get("filename", filename_map["merge"]),
                    "markdown": merge_markdown,
                }
        return export_docs

    core_identity = str(payload.get("core_identity") or "").strip()
    mental_state = str(payload.get("mental_state") or "").strip()
    memories = [str(item).strip() for item in (payload.get("memories") or []) if str(item).strip()]
    personality_lines = [
        "# 核心身份与精神底色",
        "",
        "## 核心身份",
        core_identity or "旧版 Skill 未提供可拆分的人格文档，导出时仅保留现有身份摘要。",
        "",
        "## 精神底色",
        mental_state or "旧版 Skill 未提供独立精神底色描述。",
    ]
    memories_lines = [
        "# 核心记忆与经历",
        "",
        "## 关键记忆",
        *(f"- {item}" for item in memories),
        "",
        "## 长期经历脉络",
        "旧版 Skill 未保存独立记忆文档，导出时仅从已有记忆列表回填。",
    ]
    if not memories:
        memories_lines.insert(3, "- 旧版 Skill 未保存可拆分的记忆条目。")
    merge_markdown = str(markdown_text or "").strip()
    legacy_docs = {
        "skill": {
            "filename": filename_map["skill"],
            "markdown": merge_markdown,
        },
        "personality": {
            "filename": filename_map["personality"],
            "markdown": "\n".join(personality_lines).strip(),
        },
        "memories": {
            "filename": filename_map["memories"],
            "markdown": "\n".join(memories_lines).strip(),
        },
    }
    if asset_kind == "skill":
        legacy_docs["merge"] = {
            "filename": filename_map["merge"],
            "markdown": merge_markdown,
        }
    return legacy_docs


def _resolve_skill_export_document(
    asset_kind: str,
    json_payload: dict[str, Any] | None,
    markdown_text: str,
    document_key: str,
) -> tuple[str, str]:
    documents = _skill_documents_for_export(asset_kind, json_payload, markdown_text)
    if document_key not in documents:
        raise HTTPException(status_code=404, detail="Document export not found.")
    document = documents[document_key]
    return document["filename"], document["markdown"]


def _build_skill_export_zip(
    asset_kind: str,
    json_payload: dict[str, Any] | None,
    markdown_text: str,
    *,
    base_name: str,
) -> tuple[str, bytes]:
    documents = _skill_documents_for_export(asset_kind, json_payload, markdown_text)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        document_order = CC_SKILL_DOCUMENT_ORDER if asset_kind == "cc_skill" else SKILL_DOCUMENT_ORDER
        for key in document_order:
            document = documents[key]
            zip_file.writestr(document["filename"], document["markdown"])
    buffer.seek(0)
    return f"{base_name}.zip", buffer.getvalue()


def _normalize_saved_asset_content(
    asset_kind: str,
    json_payload: dict[str, Any],
    markdown_text: str,
    prompt_text: str,
) -> tuple[dict[str, Any], str]:
    if asset_kind not in {"skill", "cc_skill"}:
        return json_payload, prompt_text

    payload = dict(json_payload or {})
    documents = dict(payload.get("documents") or {})
    if asset_kind == "skill":
        merge_document = dict(documents.get("merge") or {})
        merge_document["filename"] = str(merge_document.get("filename") or SKILL_DOCUMENT_FILENAMES["merge"])
        merge_document["markdown"] = markdown_text
        documents["merge"] = merge_document
    else:
        skill_document = dict(documents.get("skill") or {})
        skill_document["filename"] = str(skill_document.get("filename") or CC_SKILL_DOCUMENT_FILENAMES["skill"])
        skill_document["markdown"] = markdown_text
        documents["skill"] = skill_document
    payload["documents"] = documents
    return payload, markdown_text


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
    from app.models import Project
    from sqlalchemy import select
    child_ids = session.scalars(select(Project.id).where(Project.parent_id == project_id)).all()
    for cid in child_ids:
        _delete_project_resources(request, session, cid)

    repository.delete_project_cascade(session, project_id)
    config = request.app.state.config
    request.app.state.vector_store_manager.delete_store(project_id)
    for directory in (
        config.upload_dir / project_id,
        config.assets_dir / project_id,
        config.output_dir / project_id,
        config.skill_dir / project_id,
        config.data_dir / "vectors" / project_id,
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
        "language": getattr(document, 'language', None),
        "ingest_status": document.ingest_status,
        "error_message": document.error_message,
        "metadata_json": metadata,
        "created_at": document.created_at.isoformat() if getattr(document, "created_at", None) else None,
        "updated_at": document.updated_at.isoformat() if getattr(document, "updated_at", None) else None,
    }


def _truncate_preview(
    value: Any,
    limit: int,
    *,
    mode: str = "head",
) -> tuple[str, bool]:
    text = "" if value is None else str(value)
    if limit <= 0 or len(text) <= limit:
        return text, False

    marker = "\n...\n" if "\n" in text else " ... "
    if len(marker) >= limit:
        return text[:limit], True

    if mode == "tail":
        keep = max(1, limit - len(marker))
        return f"{marker}{text[-keep:]}", True
    if mode == "middle":
        head_keep = max(1, (limit - len(marker)) // 2)
        tail_keep = max(1, limit - len(marker) - head_keep)
        return f"{text[:head_keep]}{marker}{text[-tail_keep:]}", True

    keep = max(1, limit - len(marker))
    return f"{text[:keep]}{marker}", True


def _normalize_analysis_status(value: Any) -> str:
    normalized = str(value or "queued").strip().lower().replace(" ", "_")
    if normalized in {"", "pending"}:
        return "queued"
    if normalized not in {"queued", "preparing", "running", "completed", "failed"}:
        return "queued"
    return normalized


def _normalize_analysis_phase(status: str, value: Any) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if normalized:
        return normalized
    return {
        "queued": "queued",
        "preparing": "retrieving",
        "running": "analyzing",
        "completed": "completed",
        "failed": "failed",
    }.get(status, "queued")


def _normalize_analysis_concurrency(value: Any) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        candidate = DEFAULT_ANALYSIS_CONCURRENCY
    return max(MIN_ANALYSIS_CONCURRENCY, min(MAX_ANALYSIS_CONCURRENCY, candidate))


def _analysis_stage_label(facet_label: str | None, phase: str, *, queued: int = 0) -> str:
    label = facet_label or "Analysis"
    if phase == "retrieving":
        return f"{label}: retrieving evidence"
    if phase == "llm":
        return f"{label}: generating with LLM"
    if phase == "analyzing":
        return f"{label}: analyzing"
    if phase == "completed":
        return "Analysis completed"
    if phase == "failed":
        return "Analysis finished with failures"
    if phase == "persisting":
        return "Finalizing analysis"
    if queued:
        return f"{queued} facet(s) waiting for a slot"
    return "Waiting to start"


def _serialize_analysis_event(event) -> dict[str, Any]:
    payload = dict(event.payload_json or {})

    if payload.get("response_text"):
        preview, truncated = _truncate_preview(
            payload.get("response_text"),
            ANALYSIS_RESPONSE_TEXT_PREVIEW_LIMIT,
            mode="middle",
        )
        payload["response_text"] = preview
        payload["response_text_truncated"] = truncated

    if payload.get("request_payload") is not None:
        preview, truncated = _truncate_preview(
            json.dumps(payload.get("request_payload"), ensure_ascii=False, indent=2),
            ANALYSIS_REQUEST_PAYLOAD_PREVIEW_LIMIT,
            mode="middle",
        )
        payload.pop("request_payload", None)
        payload["request_payload_preview"] = preview
        payload["request_payload_truncated"] = truncated

    return {
        "id": event.id,
        "event_type": event.event_type,
        "level": event.level,
        "message": event.message,
        "payload": payload,
        "created_at": event.created_at.isoformat(),
    }


def _serialize_analysis_facet(facet: AnalysisFacet) -> dict[str, Any]:
    status = _normalize_analysis_status(facet.status)
    findings = dict(facet.findings_json or {})
    findings["label"] = findings.get("label") or facet.facet_key
    findings["phase"] = _normalize_analysis_phase(status, findings.get("phase"))
    findings["queue_position"] = findings.get("queue_position")
    findings["started_at"] = findings.get("started_at")
    findings["finished_at"] = findings.get("finished_at")
    if status != "queued":
        findings["queue_position"] = None
    summary_preview, summary_truncated = _truncate_preview(
        findings.get("summary"),
        ANALYSIS_SUMMARY_PREVIEW_LIMIT,
        mode="head",
    )
    live_text_preview, live_text_truncated = _truncate_preview(
        findings.get("llm_live_text"),
        ANALYSIS_LIVE_TEXT_PREVIEW_LIMIT,
        mode="tail",
    )
    response_preview, response_truncated = _truncate_preview(
        findings.get("llm_response_text"),
        ANALYSIS_RESPONSE_TEXT_PREVIEW_LIMIT,
        mode="middle",
    )
    findings["summary"] = summary_preview
    findings["summary_truncated"] = summary_truncated
    findings["llm_live_text"] = live_text_preview
    findings["llm_live_text_truncated"] = live_text_truncated
    findings["llm_response_text"] = response_preview
    findings["llm_response_text_truncated"] = response_truncated
    if findings.get("llm_request_payload") is not None:
        preview, truncated = _truncate_preview(
            json.dumps(findings.get("llm_request_payload"), ensure_ascii=False, indent=2),
            ANALYSIS_REQUEST_PAYLOAD_PREVIEW_LIMIT,
            mode="middle",
        )
        findings.pop("llm_request_payload", None)
        findings["llm_request_payload_preview"] = preview
        findings["llm_request_payload_truncated"] = truncated

    return {
        "facet_key": facet.facet_key,
        "status": status,
        "accepted": bool(facet.accepted),
        "confidence": facet.confidence,
        "findings": findings,
        "evidence": facet.evidence_json or [],
        "conflicts": facet.conflicts_json or [],
        "error_message": facet.error_message,
    }


def _serialize_analysis_run(run: AnalysisRun) -> dict[str, Any]:
    ordered_events = sorted(run.events, key=lambda item: item.created_at, reverse=True)[:ANALYSIS_EVENT_LIMIT]
    serialized_facets = [_serialize_analysis_facet(facet) for facet in _ordered_facets(run.facets)]
    summary = dict(run.summary_json or {})
    summary["total_facets"] = int(summary.get("total_facets") or len(FACETS))
    summary["concurrency"] = _normalize_analysis_concurrency(summary.get("concurrency"))

    completed = sum(1 for facet in serialized_facets if facet["status"] == "completed")
    failed = sum(1 for facet in serialized_facets if facet["status"] == "failed")
    active = [facet for facet in serialized_facets if facet["status"] in {"preparing", "running"}]
    queued = [facet for facet in serialized_facets if facet["status"] == "queued"]

    queue_position = 1
    for facet in serialized_facets:
        if facet["status"] == "queued":
            facet["findings"]["queue_position"] = queue_position
            queue_position += 1
        else:
            facet["findings"]["queue_position"] = None

    summary["completed_facets"] = completed
    summary["failed_facets"] = failed
    summary["active_facets"] = len(active)
    summary["queued_facets"] = len(queued)
    progress_total = max(1, int(summary.get("total_facets") or len(FACETS) or 1))
    summary["progress_percent"] = int(((completed + failed) / progress_total) * 100)

    if active:
        current = active[0]
        summary["current_facet"] = current["facet_key"]
        summary["current_phase"] = current["findings"].get("phase") or _normalize_analysis_phase(
            current["status"],
            None,
        )
        summary["current_stage"] = _analysis_stage_label(
            current["findings"].get("label") or current["facet_key"],
            summary["current_phase"],
        )
    elif run.status == "completed":
        summary["current_facet"] = None
        summary["current_phase"] = "completed"
        summary["current_stage"] = _analysis_stage_label(None, "completed")
    elif run.status in {"failed", "partial_failed"}:
        summary["current_facet"] = None
        summary["current_phase"] = "failed"
        summary["current_stage"] = _analysis_stage_label(None, "failed")
    elif queued:
        summary["current_facet"] = None
        summary["current_phase"] = "queued"
        summary["current_stage"] = _analysis_stage_label(None, "queued", queued=len(queued))
    elif run.status == "running":
        summary["current_facet"] = None
        summary["current_phase"] = "persisting"
        summary["current_stage"] = _analysis_stage_label(None, "persisting")
    else:
        summary["current_facet"] = None
        summary["current_phase"] = "queued"
        summary["current_stage"] = _analysis_stage_label(None, "queued", queued=len(queued))

    return {
        "id": run.id,
        "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "summary": summary,
        "events": [_serialize_analysis_event(event) for event in ordered_events],
        "facets": serialized_facets,
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
        "title": chat_session.title or "未命名会话",
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
    if asset_kind == "profile_report":
        return "用户剖析报告"
    if asset_kind == "cc_skill":
        return "Claude Code Skill"
    return "Skill"


def _enqueue_analysis(
    request: Request,
    session: Session,
    project_id: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
    concurrency: int | None = None,
) -> AnalysisRun:
    _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id)
    ready_documents = [document for document in documents if document.ingest_status == "ready"]
    if not ready_documents:
        raise HTTPException(status_code=400, detail="请先完成至少一份文档的解析处理。")
    existing_run = repository.get_active_analysis_run(session, project_id)
    if existing_run:
        if request.app.state.analysis_runner.is_tracking(existing_run.id):
            return existing_run
        _mark_run_as_stale(
            session,
            existing_run,
            reason="检测到旧的分析记录没有活动 worker，启动新任务前已自动标记为失败。",
        )
        session.flush()
    run = request.app.state.analysis_engine.create_run(
        session,
        project_id,
        target_role=(target_role or "").strip() or None,
        analysis_context=(analysis_context or "").strip() or None,
        concurrency=concurrency,
    )
    session.commit()
    request.app.state.analysis_runner.submit(run.id)
    session.expire_all()
    return repository.get_analysis_run(session, run.id) or run


def _mark_run_as_stale(session: Session, run: AnalysisRun, *, reason: str) -> None:
    summary = dict(run.summary_json or {})
    summary["current_stage"] = "检测到旧任务卡住，已自动恢复为失败状态"
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
        message="检测到旧分析任务没有活动 worker，已自动标记为失败。",
        payload_json={"stale_recovered": True, "reason": reason},
    )


def _resolve_run(session: Session, project_id: str, run_id: str | None) -> AnalysisRun | None:
    if run_id:
        run = repository.get_analysis_run(session, run_id)
        if not run or run.project_id != project_id:
            raise HTTPException(status_code=404, detail="未找到分析记录。")
        return run
    return repository.get_latest_analysis_run(session, project_id)


def _generate_asset_draft(request: Request, session: Session, project_id: str, *, asset_kind: str):
    project = _ensure_project(session, project_id)
    run = repository.get_latest_analysis_run(session, project_id)
    if not run:
        raise HTTPException(status_code=400, detail="请先完成一次分析，再生成资产。")
    if run.status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="当前分析仍在进行中，请等待完成后再生成资产。")
    facets = run.facets or []
    if not facets:
        raise HTTPException(status_code=400, detail="当前分析没有可用于合成资产的维度结果。")
    chat_config = repository.get_service_config(session, "chat_service")
    summary = run.summary_json or {}
    bundle = request.app.state.asset_synthesizer.build(
        asset_kind,
        project,
        facets,
        chat_config,
        target_role=summary.get("target_role"),
        analysis_context=summary.get("analysis_context"),
        session=session,
        retrieval_service=request.app.state.retrieval,
    )
    draft = repository.create_asset_draft(
        session,
        project_id=project_id,
        run_id=run.id,
        asset_kind=bundle.asset_kind,
        markdown_text=bundle.markdown_text,
        json_payload=bundle.json_payload,
        prompt_text=bundle.prompt_text,
        notes="系统自动生成草稿，发布前请先复核。",
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
        raise HTTPException(status_code=400, detail="请先发布一个 Skill 版本，再进入试聊。")
    chat_config = repository.get_service_config(session, "chat_service")
    if session_id:
        chat_session = repository.get_chat_session(session, session_id, session_kind="playground")
        if not chat_session:
            raise HTTPException(status_code=404, detail="未找到试聊会话。")
    else:
        chat_session = repository.get_or_create_chat_session(session, project_id, session_kind="playground")
    history = sorted(chat_session.turns, key=lambda item: item.created_at)
    repository.add_chat_turn(session, session_id=chat_session.id, role="user", content=message)

    assistant_reply, llm_meta = _generate_chat_reply(
        chat_config,
        version.system_prompt,
        history,
        message,
        "",
        log_path=str(request.app.state.config.llm_log_path),
    )
    trace = {
        "skill_version_id": version.id,
        "skill_version_number": version.version_number,
        "prompt_excerpt": f"SKILL:\n{version.system_prompt[:1800]}",
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
        prefix = "当前未配置外部 LLM，系统正在使用本地降级模式。"
        return (
            (
                f"{prefix}\n\n"
                "我会尽量按照已发布 Skill 中的语气与立场来回应。\n\n"
                f"你刚才说的是：{message}"
            ),
            {"provider_kind": "local", "api_mode": "responses", "model": "fallback"},
        )

    client = OpenAICompatibleClient(config, log_path=log_path)
    messages = [{"role": "system", "content": system_prompt}]
    if evidence_block:
        messages.append({"role": "system", "content": f"来源文档证据：\n{evidence_block}"})
    for turn in history[-8:]:
        messages.append({"role": turn.role, "content": turn.content})
    messages.append({"role": "user", "content": message})
    try:
        result = client.chat_completion_result(messages, model=config.model, temperature=0.7, max_tokens=900)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"对话生成失败：{exc}") from exc
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


def _ensure_project(session: Session, project_id: str):
    project = repository.get_project(session, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="未找到项目。")
    return project


def _get_project_document(session: Session, project_id: str, document_id: str) -> DocumentRecord:
    document = repository.get_document(session, document_id)
    if not document or document.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到文档。")
    return document


def _analysis_stage_label(facet_label: str | None, phase: str, *, queued: int = 0) -> str:
    label = facet_label or "分析任务"
    if phase == "retrieving":
        return f"{label}：检索证据中"
    if phase == "llm":
        return f"{label}：调用 LLM 生成中"
    if phase == "analyzing":
        return f"{label}：分析中"
    if phase == "completed":
        return "分析已完成"
    if phase == "failed":
        return "分析已结束，但存在失败维度"
    if phase == "persisting":
        return "正在整理最终结果"
    if queued:
        return f"还有 {queued} 个维度等待空闲槽位"
    return "等待开始"


def _asset_label(asset_kind: str) -> str:
    if asset_kind == "profile_report":
        return "用户画像报告"
    if asset_kind == "cc_skill":
        return "Claude Code Skill"
    return "Skill"


@router.websocket("/api/projects/{project_id}/documents/ws")
async def websocket_document_status(websocket: WebSocket, project_id: str):
    await websocket.accept()
    task_manager = websocket.app.state.ingest_task_manager
    try:
        from sqlalchemy import select
        from app.models import DocumentRecord
        while True:
            with websocket.app.state.db.session() as session:
                stmt = select(DocumentRecord.id, DocumentRecord.ingest_status).where(DocumentRecord.project_id == project_id)
                rows = session.execute(stmt).all()
                
                payload = []
                for doc_id, ingest_status in rows:
                    task = task_manager.get_by_document(doc_id)
                    payload.append({
                        "id": doc_id,
                        "ingest_status": ingest_status,
                        "task": task
                    })
            await websocket.send_json({"documents": payload})
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
