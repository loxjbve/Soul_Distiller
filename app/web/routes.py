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

from app.analysis.facets import FACETS, get_facets_for_mode
from app.llm.client import OpenAICompatibleClient, normalize_api_mode, normalize_provider_kind
from app.models import (
    AnalysisFacet,
    AnalysisRun,
    DocumentRecord,
    GeneratedArtifact,
    TelegramRelationshipSnapshot,
    TelegramPreprocessActiveUser,
    TelegramPreprocessTopUser,
    TelegramPreprocessRun,
    TelegramPreprocessWeeklyTopicCandidate,
    TelegramPreprocessTopic,
    utcnow,
)
from app.pipeline.project_deletion import ACTIVE_TASK_STATUSES
from app.schemas import (
    ASSET_KINDS,
    DEFAULT_ANALYSIS_CONCURRENCY,
    MIN_ANALYSIS_CONCURRENCY,
    ServiceConfig,
)
from app.storage import repository
from app.web.ui_strings import DEFAULT_LOCALE, page_strings


router = APIRouter()

from fastapi.responses import RedirectResponse

def get_locale(request: Request) -> str:
    return request.cookies.get("locale", DEFAULT_LOCALE)

@router.get("/set-locale")
def set_locale(locale: str, next: str = "/"):
    response = RedirectResponse(url=next)
    response.set_cookie(key="locale", value=locale, max_age=31536000)
    return response

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
    {"value": "profile_report", "label": "用户画像报告"},
    {"value": "writing_guide", "label": "Writing Guide"},
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
    {"value": "writing_guide", "label": "Writing Guide"},
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
CC_SKILL_DOCUMENT_ORDER = ("skill", "personality", "memories", "analysis")
CC_SKILL_DOCUMENT_FILENAMES = {
    "skill": "SKILL.md",
    "personality": "references/personality.md",
    "memories": "references/memories.md",
    "analysis": "references/analysis.md",
}


class ProjectCreatePayload(BaseModel):
    name: str | None = None
    description: str | None = None
    mode: str = "group"


class ChatPayload(BaseModel):
    message: str
    session_id: str | None = None


class AnalysisRequestPayload(BaseModel):
    target_role: str | None = None
    target_user_query: str | None = None
    participant_id: str | None = None
    analysis_context: str | None = None
    concurrency: int | None = Field(default=None, ge=MIN_ANALYSIS_CONCURRENCY)


class DocumentUpdatePayload(BaseModel):
    title: str | None = None
    source_type: str | None = None
    user_note: str | None = None


class TextDocumentCreatePayload(BaseModel):
    title: str | None = None
    content: str
    source_type: str | None = None
    user_note: str | None = None


class PreprocessSessionCreatePayload(BaseModel):
    title: str | None = None


class PreprocessSessionUpdatePayload(BaseModel):
    title: str | None = None


class PreprocessMessagePayload(BaseModel):
    message: str


class WritingMessagePayload(BaseModel):
    topic: str
    target_word_count: int = Field(..., ge=100)
    extra_requirements: str | None = None


class TelegramPreprocessRunCreatePayload(BaseModel):
    weekly_summary_concurrency: int | None = Field(default=None, ge=1)


class AssetGeneratePayload(BaseModel):
    asset_kind: str = "cc_skill"


class AssetSavePayload(BaseModel):
    asset_kind: str = "cc_skill"
    markdown_text: str
    json_payload: dict[str, Any]
    prompt_text: str
    notes: str | None = None


class ServiceSettingConfigPayload(BaseModel):
    id: str | None = None
    label: str | None = None
    provider_kind: str = "openai"
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    api_mode: str | None = None
    available_models: list[str] = Field(default_factory=list)


class ServiceSettingsBundlePayload(BaseModel):
    active_config_id: str | None = None
    discover_config_id: str | None = None
    fallback_order: list[str] = Field(default_factory=list)
    configs: list[ServiceSettingConfigPayload] = Field(default_factory=list)


def get_session(request: Request):
    with request.app.state.db.session() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


def _page_context(request: Request, page_name: str, **kwargs: Any) -> dict[str, Any]:
    locale = get_locale(request)
    return {
        "locale": locale,
        "ui": page_strings(page_name, locale),
        **kwargs,
    }


def _stone_mode_label(locale: str) -> str:
    return "Stone Mode" if locale == "en-US" else "搬石模式"


def _stone_mode_hint(locale: str) -> str:
    if locale == "en-US":
        return "For single-author corpus, article profiling, writing guide synthesis, and controlled article drafting"
    return "适合单作者文本、逐篇预分析、写作指南生成与定向写作"


def _writing_workspace_ui(locale: str) -> dict[str, Any]:
    base = page_strings("preprocess", locale)
    labels = {
        "title": "Writing Workspace" if locale == "en-US" else "写作台",
        "eyebrow": "Writing Workspace" if locale == "en-US" else "搬石写作台",
        "hero_note": (
            "Draft against the latest writing guide, run reviewer passes, and keep style fidelity visible."
            if locale == "en-US"
            else "围绕最新 writing_guide 出稿，查看 reviewer 维度意见，并显式展示文风一致性。"
        ),
        "new_session": "New Session" if locale == "en-US" else "新建会话",
        "rename_session": "Rename" if locale == "en-US" else "重命名",
        "delete_session": "Delete Session" if locale == "en-US" else "删除会话",
        "sessions": "Sessions" if locale == "en-US" else "会话",
        "composer_placeholder": (
            "Enter the topic, target word count, and optional extra requirements."
            if locale == "en-US"
            else "输入主题、目标字数和可选附加要求。"
        ),
        "topic_label": "Topic" if locale == "en-US" else "主题",
        "target_word_count_label": "Target Word Count" if locale == "en-US" else "目标字数",
        "extra_requirements_label": "Extra Requirements" if locale == "en-US" else "附加要求",
        "send": "Start Writing" if locale == "en-US" else "开始写作",
        "sending": "Writing..." if locale == "en-US" else "写作中...",
        "guide_label": "Guide" if locale == "en-US" else "当前指南",
        "guide_missing": "No writing guide yet." if locale == "en-US" else "还没有 writing_guide。",
        "guide_unpublished": "Using latest draft guide." if locale == "en-US" else "当前使用未发布指南。",
        "guide_published": "Using latest published guide." if locale == "en-US" else "当前使用已发布指南。",
        "empty_turns": "No writing tasks yet." if locale == "en-US" else "还没有写作任务。",
        "working": "Working..." if locale == "en-US" else "执行中...",
        "untitled_session": "Untitled Session" if locale == "en-US" else "未命名会话",
        "rename_prompt": "Enter a new session title" if locale == "en-US" else "输入新的会话标题",
        "execution_failed": "Writing failed" if locale == "en-US" else "写作失败",
        "connection_interrupted": "Connection interrupted" if locale == "en-US" else "连接中断",
        "stage_feed": "Pipeline" if locale == "en-US" else "流水线",
    }
    base.update(labels)
    return base


def _primary_asset_kind_for_mode(mode: str | None) -> str:
    return "writing_guide" if str(mode or "").strip().lower() == "stone" else "cc_skill"


def _asset_options_for_project(project) -> tuple[dict[str, str], ...]:
    if str(project.mode or "").strip().lower() == "stone":
        return (
            {"value": "writing_guide", "label": "Writing Guide"},
        )
    return (
        {"value": "cc_skill", "label": "Claude Code Skill"},
        {"value": "profile_report", "label": "用户画像报告"},
    )


def _resolve_asset_kind_for_project(project, requested_kind: str | None) -> str:
    default_kind = _primary_asset_kind_for_mode(project.mode)
    asset_kind = _normalize_asset_kind(requested_kind or default_kind)
    if str(project.mode or "").strip().lower() == "stone" and asset_kind != "writing_guide":
        return "writing_guide"
    return asset_kind


def _ensure_stone_project(session: Session, project_id: str):
    project = _ensure_project(session, project_id)
    if project.mode != "stone":
        raise HTTPException(status_code=400, detail="Only stone projects support this workspace.")
    return project


def _resolve_writing_guide_status(session: Session, project_id: str) -> dict[str, Any]:
    version = repository.get_latest_asset_version(session, project_id, asset_kind="writing_guide")
    if version:
        return {
            "status": "published",
            "asset_id": version.id,
            "label": f"v{version.version_number}",
        }
    draft = repository.get_latest_asset_draft(session, project_id, asset_kind="writing_guide")
    if draft:
        return {
            "status": "draft",
            "asset_id": draft.id,
            "label": "draft",
        }
    return {"status": "missing", "asset_id": None, "label": None}


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
    locale = get_locale(request)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=_page_context(
            request, "index",
            projects=repository.list_projects(session),
            chat_configured=repository.get_service_config(session, "chat_service") is not None,
            embedding_configured=repository.get_service_config(session, "embedding_service") is not None,
            stone_mode_label=_stone_mode_label(locale),
            stone_mode_hint=_stone_mode_hint(locale),
        ),
    )


@router.post("/projects")
def create_project_form(
    session: SessionDep,
    name: Annotated[str | None, Form()] = None,
    description: Annotated[str | None, Form()] = None,
    mode: Annotated[str, Form()] = "group",
):
    actual_name = (name or "").strip()
    if not actual_name:
        if mode == "telegram":
            actual_name = "未命名 Telegram 项目"
        else:
            raise HTTPException(status_code=400, detail="Project name is required.")
    project = repository.create_project(session, name=actual_name, description=description, mode=mode)
    return RedirectResponse(url=f"/projects/{project.id}", status_code=303)


@router.post("/projects/{project_id}/profiles")
def create_profile_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    name: Annotated[str | None, Form()] = None,
    description: Annotated[str | None, Form()] = None,
    participant_id: Annotated[str | None, Form()] = None,
    target_user_query: Annotated[str | None, Form()] = None,
    analysis_context: Annotated[str | None, Form()] = None,
    concurrency: Annotated[int | None, Form(ge=MIN_ANALYSIS_CONCURRENCY)] = None,
    auto_analyze: Annotated[str | None, Form()] = None,
):
    parent = _ensure_project(session, project_id)
    if parent.mode not in {"group", "telegram"}:
        raise HTTPException(status_code=400, detail="Only group or Telegram projects can create child profiles.")
    child_mode = "telegram" if parent.mode == "telegram" else "single"
    participant_id = (participant_id or "").strip() or None
    target_user_query = (target_user_query or "").strip() or None
    analysis_context = (analysis_context or "").strip() or None
    base_name = (name or "").strip()
    if parent.mode == "telegram" and not base_name:
        inferred_name = target_user_query or ""
        if participant_id and not inferred_name:
            top_run = repository.get_latest_successful_telegram_preprocess_run(session, project_id)
            if top_run:
                for item in repository.list_telegram_preprocess_top_users(session, project_id, run_id=top_run.id):
                    if item.participant_id == participant_id:
                        inferred_name = item.display_name or item.username or item.uid or ""
                        break
        base_name = inferred_name or "Telegram Persona"
    if not base_name:
        raise HTTPException(status_code=400, detail="Profile name is required.")
    child_description = analysis_context or description
    child = repository.create_project(
        session,
        name=base_name,
        description=child_description,
        mode=child_mode,
        parent_id=project_id,
    )
    if parent.mode == "telegram" and str(auto_analyze or "").strip() in {"1", "true", "yes", "on"}:
        run = _enqueue_analysis(
            request,
            session,
            child.id,
            target_role=None,
            target_user_query=target_user_query or base_name,
            participant_id=participant_id,
            analysis_context=analysis_context or child.description,
            concurrency=concurrency,
        )
        return RedirectResponse(url=f"/projects/{child.id}/analysis?run_id={run.id}", status_code=303)
    return RedirectResponse(url=f"/projects/{project_id}", status_code=303)


@router.get("/projects/{project_id}", response_class=HTMLResponse)
def project_detail(request: Request, project_id: str, session: SessionDep):
    context = _project_context(request, session, project_id)
    return templates.TemplateResponse(
        request=request,
        name="project_detail.html",
        context=_page_context(request, "project", **context),
    )


@router.get("/projects/{project_id}/relationships", response_class=HTMLResponse)
def project_relationships(request: Request, project_id: str, session: SessionDep):
    context = _project_context(request, session, project_id)
    return templates.TemplateResponse(
        request=request,
        name="project_relationships.html",
        context=_page_context(request, "relationships", **context),
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
    project, _task = _schedule_project_deletion(request, session, project_id)
    parent_id = project.parent_id
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
    project = _ensure_project(session, project_id)
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
    target_user_query: Annotated[str | None, Form()] = None,
    participant_id: Annotated[str | None, Form()] = None,
    analysis_context: Annotated[str | None, Form()] = None,
    concurrency: Annotated[int | None, Form(ge=MIN_ANALYSIS_CONCURRENCY)] = None,
):
    run = _enqueue_analysis(
        request,
        session,
        project_id,
        target_role=target_role,
        target_user_query=target_user_query,
        participant_id=participant_id,
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
            request, "analysis",
            project=project,
            run=run,
            serialized_run=json.dumps(serialized_run, ensure_ascii=False) if serialized_run else "null",
            run_id=run.id if run else "",
            facet_catalog=get_facets_for_mode(project.mode),
            primary_asset_kind=_primary_asset_kind_for_mode(project.mode),
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
    kind: str | None = Query(default=None),
):
    project = _ensure_project(session, project_id)
    asset_kind = _resolve_asset_kind_for_project(project, kind)
    draft = repository.get_latest_asset_draft(session, project_id, asset_kind=asset_kind)
    versions = repository.list_asset_versions(session, project_id, asset_kind=asset_kind)
    latest_run = repository.get_latest_analysis_run(session, project_id)
    draft_documents = (
        _skill_documents_for_export(asset_kind, draft.json_payload, draft.markdown_text)
        if draft and asset_kind in {"skill", "cc_skill"}
        else {}
    )
    return templates.TemplateResponse(
        request=request,
        name="assets.html",
        context=_page_context(
            request, "assets",
            project=project,
            asset_kind=asset_kind,
            asset_label=_asset_label(asset_kind),
            asset_options=_asset_options_for_project(project),
            draft=draft,
            draft_documents=draft_documents,
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


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/download")
def download_asset_version_api(project_id: str, version_id: str, session: SessionDep):
    version = repository.get_asset_version(session, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(status_code=404, detail="Asset version not found.")
    if version.asset_kind == "cc_skill":
        filename, payload = _build_skill_export_zip(
            version.asset_kind,
            version.json_payload,
            version.markdown_text,
            base_name=f"cc_skill_v{version.version_number}",
        )
        return StreamingResponse(iter([payload]), media_type="application/zip", headers=_download_headers(filename))
    filename = f"{version.asset_kind}_v{version.version_number}.md"
    return _markdown_download_response(filename, version.markdown_text)


@router.post("/projects/{project_id}/asset-versions/{version_id}/delete")
def delete_asset_version_form(request: Request, project_id: str, version_id: str, session: SessionDep):
    version = repository.get_asset_version(session, version_id)
    if not version or version.project_id != project_id:
        raise HTTPException(status_code=404, detail="Asset version not found.")
    asset_kind = version.asset_kind
    version_number = version.version_number
    repository.delete_asset_version(session, version)
    _delete_asset_files(request, project_id, asset_kind, f"published_v{version_number}")
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind={asset_kind}", status_code=303)


@router.get("/projects/{project_id}/skill", response_class=HTMLResponse)
def skill_page(request: Request, project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=cc_skill", status_code=303)


@router.post("/projects/{project_id}/assets/generate")
def generate_asset_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    asset_kind: Annotated[str, Form()] = "cc_skill",
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
    asset_kind: Annotated[str, Form()] = "cc_skill",
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
    asset_kind: Annotated[str, Form()] = "cc_skill",
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
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind={version.asset_kind}", status_code=303)


@router.post("/projects/{project_id}/skills/generate")
def generate_skill_form(request: Request, project_id: str, session: SessionDep):
    draft = _generate_asset_draft(request, session, project_id, asset_kind="cc_skill")
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=cc_skill&draft={draft.id}", status_code=303)


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
    draft = repository.get_asset_draft(session, draft_id, asset_kind="cc_skill")
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    normalized_payload, normalized_prompt = _normalize_saved_asset_content(
        "cc_skill",
        json.loads(json_payload),
        markdown_text,
        system_prompt,
    )
    draft.markdown_text = markdown_text
    draft.json_payload = normalized_payload
    draft.system_prompt = normalized_prompt
    draft.notes = notes
    _persist_asset_files(request, project_id, "cc_skill", f"draft_{draft.id}", draft.markdown_text, draft.json_payload, draft.system_prompt)
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=cc_skill", status_code=303)


@router.post("/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_form(request: Request, project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_asset_draft(session, draft_id, asset_kind="cc_skill")
    if not draft or draft.project_id != project_id:
        raise HTTPException(status_code=404, detail="Draft not found.")
    version = repository.publish_skill_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        "cc_skill",
        f"published_v{version.version_number}",
        version.markdown_text,
        version.json_payload,
        version.system_prompt,
    )
    return RedirectResponse(url=f"/projects/{project_id}/assets?kind=cc_skill", status_code=303)


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
            request, "playground",
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
    run_id: str | None = Query(default=None),
    mention: str | None = Query(default=None),
):
    context = _project_context(request, session, project_id)
    if context["project"].mode == "telegram":
        telegram_context = _telegram_preprocess_context(session, project_id, run_id=run_id)
        return templates.TemplateResponse(
            request=request,
            name="telegram_preprocess.html",
            context=_page_context(request, "preprocess", **telegram_context),
        )
    elif context["project"].mode == "stone":
        stone_context = _stone_preprocess_context(session, project_id, run_id=run_id)
        return templates.TemplateResponse(
            request=request,
            name="stone_preprocess.html",
            context=_page_context(request, "preprocess", **stone_context),
        )
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
        "locale": get_locale(request),
        "ui_strings": page_strings("preprocess", get_locale(request)),
    }
    return templates.TemplateResponse(
        request=request,
        name="preprocess.html",
        context=_page_context(
            request, "preprocess",
            project=context["project"],
            bootstrap=json.dumps(bootstrap, ensure_ascii=False),
        ),
    )


@router.get("/projects/{project_id}/writing", response_class=HTMLResponse)
def writing_page(
    request: Request,
    project_id: str,
    session: SessionDep,
    session_id: str | None = Query(default=None),
):
    project = _ensure_stone_project(session, project_id)
    sessions = repository.list_chat_sessions(session, project_id, session_kind="writing")
    if not sessions:
        sessions = [
            repository.create_chat_session(
                session,
                project_id=project_id,
                session_kind="writing",
                title="新建写作会话",
            )
        ]
    selected_session = sessions[0]
    if session_id:
        explicit = repository.get_chat_session(session, session_id, session_kind="writing")
        if explicit and explicit.project_id == project_id:
            selected_session = explicit
    locale = get_locale(request)
    writing_ui = _writing_workspace_ui(locale)
    bootstrap = {
        "project": {"id": project.id, "name": project.name, "mode": project.mode},
        "sessions": [_serialize_chat_session(item) for item in sessions],
        "selected_session_id": selected_session.id,
        "selected_session": _serialize_writing_session_detail(selected_session),
        "documents": [_serialize_document(item) for item in repository.list_project_documents(session, project_id)],
        "guide": _resolve_writing_guide_status(session, project_id),
        "locale": locale,
        "ui_strings": writing_ui,
    }
    return templates.TemplateResponse(
        request=request,
        name="writing.html",
        context=_page_context(
            request,
            "preprocess",
            project=project,
            bootstrap=json.dumps(bootstrap, ensure_ascii=False),
            writing_ui=writing_ui,
            primary_asset_kind="writing_guide",
        ),
    )


@router.post("/projects/{project_id}/preprocess/run")
def start_preprocess_form(
    request: Request,
    project_id: str,
    session: SessionDep,
    weekly_summary_concurrency: Annotated[int | None, Form(ge=1)] = None,
    concurrency: Annotated[int | None, Form(ge=1)] = None,
):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = _create_telegram_preprocess_run(
            request,
            session,
            project_id,
            weekly_summary_concurrency=weekly_summary_concurrency,
        )
    elif project.mode == "stone":
        run = _create_stone_preprocess_run(
            request,
            session,
            project_id,
            concurrency=concurrency or 1,
        )
    else:
        raise HTTPException(status_code=400, detail="Only Telegram and Stone projects use this preprocess flow.")
    return RedirectResponse(url=f"/projects/{project_id}/preprocess?run_id={run.id}", status_code=303)


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, session: SessionDep):
    settings_bootstrap = {
        "provider_options": PROVIDER_OPTIONS,
        "api_mode_options": API_MODE_OPTIONS,
        "services": {
            "chat": repository.get_service_setting_bundle(
                session,
                "chat_service",
                default_provider="openai",
                default_api_mode="responses",
            ),
            "embedding": repository.get_service_setting_bundle(
                session,
                "embedding_service",
                default_provider="openai",
                default_api_mode="responses",
            ),
        },
    }
    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context=_page_context(
            request, "settings",
            settings_bootstrap=settings_bootstrap,
            provider_options=PROVIDER_OPTIONS,
            legacy_provider_options=(
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
    config_payload = _normalize_service_setting_config_payload(
        {
            "label": "Default",
            "provider_kind": provider_kind,
            "base_url": base_url,
            "api_key": api_key,
            "model": model,
            "api_mode": api_mode if service_name == "chat" else "responses",
            "available_models": [],
        },
        service_name=service_name,
        fallback_label="Default",
    )
    repository.upsert_service_setting_bundle(
        session,
        f"{service_name}_service",
        {
            "active_config_id": config_payload["id"],
            "fallback_order": [],
            "configs": [config_payload],
        },
        default_provider="openai",
        default_api_mode="responses",
    )
    return RedirectResponse(url="/settings", status_code=303)


@router.post("/api/settings/{service_name}")
def save_service_settings_api(
    request: Request,
    service_name: str,
    payload: ServiceSettingsBundlePayload,
    session: SessionDep,
):
    if service_name not in {"chat", "embedding"}:
        raise HTTPException(status_code=404, detail="未知服务类型。")
    normalized_bundle = _normalize_service_setting_bundle_payload(payload, service_name=service_name)
    repository.upsert_service_setting_bundle(
        session,
        f"{service_name}_service",
        normalized_bundle,
        default_provider="openai",
        default_api_mode="responses",
    )

    discovered_models: list[str] = []
    discover_error: str | None = None
    discover_config_id = str(payload.discover_config_id or "").strip()
    target_config = next((item for item in normalized_bundle["configs"] if item["id"] == discover_config_id), None)
    if target_config and _is_service_setting_config_usable(target_config):
        try:
            discover_client = OpenAICompatibleClient(
                ServiceConfig(
                    base_url=str(target_config.get("base_url") or "").strip() or None,
                    api_key=str(target_config.get("api_key") or "").strip(),
                    model=str(target_config.get("model") or "").strip() or None,
                    provider_kind=str(target_config.get("provider_kind") or "openai"),
                    api_mode=str(target_config.get("api_mode") or "responses"),
                ),
                log_path=str(request.app.state.config.llm_log_path),
            )
            discovered_models = discover_client.list_models()
        except Exception as exc:
            discover_error = str(exc)

    if target_config is not None:
        if discovered_models or not discover_error:
            target_config["available_models"] = discovered_models
        if discovered_models and not str(target_config.get("model") or "").strip():
            target_config["model"] = discovered_models[0]
        repository.upsert_service_setting_bundle(
            session,
            f"{service_name}_service",
            normalized_bundle,
            default_provider="openai",
            default_api_mode="responses",
        )

    return _ok_response(
        "服务配置已保存。",
        service=service_name,
        bundle=normalized_bundle,
        discovered_config_id=discover_config_id or None,
        discovered_models=discovered_models,
        discover_error=discover_error,
    )


@router.post("/api/projects")
def create_project_api(payload: ProjectCreatePayload, session: SessionDep):
    actual_name = (payload.name or "").strip()
    if not actual_name:
        if payload.mode == "telegram":
            actual_name = "未命名 Telegram 项目"
        else:
            raise HTTPException(status_code=400, detail="Project name is required.")
    project = repository.create_project(session, actual_name, payload.description, mode=payload.mode)
    return _ok_response(
        "项目已创建。",
        id=project.id,
        name=project.name,
        description=project.description,
        mode=project.mode,
    )


@router.post("/api/projects/{project_id}/deletion")
def delete_project_api_v2(request: Request, project_id: str, session: SessionDep):
    project, task = _schedule_project_deletion(request, session, project_id)
    return _task_response("已受理项目删除任务。", task, ok=True, project_id=project.id)


@router.get("/api/projects/{project_id}/deletion")
def get_project_deletion_api(request: Request, project_id: str, session: SessionDep):
    task = request.app.state.project_deletion_manager.get_by_project(project_id)
    if task:
        return _task_response("已返回项目删除任务状态。", task, project_id=project_id)
    project = repository.get_project(session, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")
    if project.lifecycle_state == repository.PROJECT_LIFECYCLE_ACTIVE:
        raise HTTPException(status_code=404, detail="No project deletion task found.")
    return _ok_response(
        "已返回项目删除状态。",
        project_id=project.id,
        lifecycle_state=project.lifecycle_state,
        deletion_error=project.deletion_error,
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
    project = _ensure_project(session, project_id)
    ingest = request.app.state.ingest_service
    try:
        created = await ingest.create_documents_from_uploads(session, project_id=project_id, uploads=files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if project.mode == "stone" and created:
        task_manager = request.app.state.ingest_task_manager
        for document in created:
            document.ingest_status = "queued"
        task_manager.set_embedding_config(None)
        session.commit()
        tasks = [
            task_manager.submit(
                project_id=project.id,
                document_id=document.id,
                filename=document.filename,
                storage_path=document.storage_path,
                mime_type=document.mime_type,
            )
            for document in created
        ]
        return _ok_response(
            "Stone documents uploaded and queued.",
            documents=[_serialize_document(document) for document in created],
            tasks=tasks,
        )
    return _ok_response("文档上传完成。", documents=[_serialize_document(document) for document in created])





@router.post("/api/projects/{project_id}/documents/text")
def create_text_document_api(
    request: Request,
    project_id: str,
    payload: TextDocumentCreatePayload,
    session: SessionDep,
):
    project = _ensure_stone_project(session, project_id)
    content = str(payload.content or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Content is required.")
    ingest = request.app.state.ingest_service
    document = ingest.create_text_document(
        session,
        project_id=project.id,
        title=payload.title,
        content=content,
        source_type=payload.source_type,
        user_note=payload.user_note,
    )
    document.ingest_status = "queued"
    task_manager = request.app.state.ingest_task_manager
    task_manager.set_embedding_config(None)
    session.commit()
    task = task_manager.submit(
        project_id=project.id,
        document_id=document.id,
        filename=document.filename,
        storage_path=document.storage_path,
        mime_type=document.mime_type,
    )
    return _task_response("文本文章已创建并加入处理队列。", task, **_serialize_document(document))


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
        target_user_query=payload.target_user_query,
        participant_id=payload.participant_id,
        analysis_context=payload.analysis_context,
        concurrency=payload.concurrency,
    )
    serialized = _serialize_analysis_run(run)
    return _ok_response("分析任务已创建。", **serialized)


@router.post("/api/projects/{project_id}/preprocess/runs")
def create_preprocess_run_api(
    request: Request,
    project_id: str,
    session: SessionDep,
    payload: TelegramPreprocessRunCreatePayload | None = None,
):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = _create_telegram_preprocess_run(
            request,
            session,
            project_id,
            weekly_summary_concurrency=(payload.weekly_summary_concurrency if payload else None),
        )
        return _ok_response("Telegram 预处理任务已创建。", **_serialize_telegram_preprocess_run(run))
    elif project.mode == "stone":
        run = _create_stone_preprocess_run(
            request,
            session,
            project_id,
        )
        return _ok_response("Stone 预分析任务已创建。", **_serialize_stone_preprocess_run(run))
    else:
        raise HTTPException(status_code=400, detail="Project mode does not support preprocess.")


@router.get("/api/projects/{project_id}/preprocess/runs")
def list_preprocess_runs_api(project_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        runs = repository.list_telegram_preprocess_runs(session, project_id, limit=40)
        return _ok_response("已返回 Telegram 预处理历史。", runs=[_serialize_telegram_preprocess_run(item) for item in runs])
    elif project.mode == "stone":
        runs = repository.list_stone_preprocess_runs(session, project_id, limit=40)
        return _ok_response("已返回 Stone 预分析历史。", runs=[_serialize_stone_preprocess_run(item) for item in runs])
    else:
        raise HTTPException(status_code=400, detail="Only Telegram and Stone projects use preprocess runs.")


@router.get("/api/projects/{project_id}/preprocess/runs/latest")
def get_latest_preprocess_run_api(project_id: str, session: SessionDep, successful: bool = Query(default=True)):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = (
            repository.get_latest_successful_telegram_preprocess_run(session, project_id)
            if successful
            else repository.get_latest_telegram_preprocess_run(session, project_id)
        )
        if not run:
            raise HTTPException(status_code=404, detail="No Telegram preprocess run found.")
        return _ok_response("已返回最新 Telegram 预处理结果。", **_serialize_telegram_preprocess_run(run))
    elif project.mode == "stone":
        run = (
            repository.get_latest_successful_stone_preprocess_run(session, project_id)
            if successful
            else repository.get_latest_stone_preprocess_run(session, project_id)
        )
        if not run:
            raise HTTPException(status_code=404, detail="No Stone preprocess run found.")
        return _ok_response("已返回最新 Stone 预分析结果。", **_serialize_stone_preprocess_run(run))
    else:
        raise HTTPException(status_code=400, detail="Only Telegram and Stone projects use preprocess runs.")


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}")
def get_preprocess_run_api(project_id: str, run_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = _resolve_telegram_preprocess_run(session, project_id, run_id)
        return _ok_response("已返回 Telegram 预处理详情。", **_serialize_telegram_preprocess_detail(session, project_id, run))
    elif project.mode == "stone":
        run = repository.get_stone_preprocess_run(session, run_id)
        if not run or run.project_id != project_id:
            raise HTTPException(status_code=404, detail="Run not found.")
        return _ok_response("已返回 Stone 预分析详情。", **_serialize_stone_preprocess_run(run))
    else:
        raise HTTPException(status_code=400, detail="Only Telegram and Stone projects use preprocess runs.")


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/stream")
def stream_preprocess_run_api(request: Request, project_id: str, run_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = _resolve_telegram_preprocess_run(session, project_id, run_id)
        hub = request.app.state.telegram_preprocess_stream_hub
        subscription = hub.subscribe(run.id)

        async def generate():
            from starlette.concurrency import run_in_threadpool

            last_snapshot = ""

            def fetch_payload():
                with request.app.state.db.session() as live_session:
                    live_run = _resolve_telegram_preprocess_run(live_session, project_id, run_id)
                    return _serialize_telegram_preprocess_detail(live_session, project_id, live_run)

            try:
                initial_payload = await run_in_threadpool(fetch_payload)
                last_snapshot = json.dumps(initial_payload, ensure_ascii=False)
                yield _format_sse("snapshot", initial_payload)
                if initial_payload["status"] not in {"queued", "running"}:
                    yield _format_sse("done", {"run_id": initial_payload["id"], "status": initial_payload["status"]})
                    return

                while True:
                    try:
                        event = await run_in_threadpool(subscription.get, True, 15.0)
                    except Empty:
                        event = {"event": "heartbeat", "payload": {}}

                    if event.get("event") == "trace":
                        yield _format_sse("trace", event.get("payload") or {})

                    if event.get("event") in {"snapshot", "heartbeat"}:
                        payload = await run_in_threadpool(fetch_payload)
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
    elif project.mode == "stone":
        run = repository.get_stone_preprocess_run(session, run_id)
        if not run or run.project_id != project_id:
            raise HTTPException(status_code=404, detail="Run not found.")
        hub = request.app.state.stone_preprocess_stream_hub
        
        async def generate_stone():
            async for chunk in hub.stream_events(run.id):
                yield chunk
                
                # Fetch payload to send snapshot periodically
                with request.app.state.db.session() as live_session:
                    live_run = repository.get_stone_preprocess_run(live_session, run_id)
                    if live_run:
                        payload = _serialize_stone_preprocess_run(live_run)
                        yield _format_sse("snapshot", payload)
                        if live_run.status not in {"queued", "running"}:
                            yield _format_sse("done", {"run_id": live_run.id, "status": live_run.status})
                            break

        return StreamingResponse(
            generate_stone(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    else:
        raise HTTPException(status_code=400, detail="Only Telegram and Stone projects use preprocess runs.")


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/topics")
def list_telegram_preprocess_topics_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    topics = repository.list_telegram_preprocess_topics(session, project_id, run_id=run_id)
    return _ok_response("已返回 Telegram 话题表。", topics=[_serialize_telegram_preprocess_topic(item) for item in topics])


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/weekly-candidates")
def list_telegram_preprocess_weekly_candidates_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    candidates = repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=run_id)
    return _ok_response(
        "已返回 Telegram 周话题候选表。",
        weekly_candidates=[_serialize_telegram_preprocess_weekly_candidate(item) for item in candidates],
    )


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/top-users")
def list_telegram_preprocess_top_users_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=run_id)
    return _ok_response(
        "已返回 Telegram SQL Top Users 表。",
        top_users=[_serialize_telegram_preprocess_top_user(item) for item in users],
    )


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/active-users")
def list_telegram_preprocess_active_users_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=run_id)
    return _ok_response(
        "已返回 Telegram 活跃用户快照。",
        active_users=[_serialize_telegram_preprocess_active_user(item) for item in active_users],
    )


@router.get("/api/projects/{project_id}/relationships/latest")
def get_latest_telegram_relationship_snapshot_api(project_id: str, session: SessionDep):
    _ensure_project(session, project_id)
    source_project_id = repository.get_target_project_id(session, project_id)
    latest_run = repository.get_latest_successful_telegram_preprocess_run(session, source_project_id)
    if not latest_run:
        return _ok_response(
            "Telegram relationship snapshot is not ready yet.",
            snapshot=None,
            users=[],
            edges=[],
        )
    snapshot = repository.get_telegram_relationship_snapshot_for_run(session, latest_run.id)
    if not snapshot:
        return _ok_response(
            "Telegram relationship snapshot is not ready yet.",
            snapshot=None,
            users=[],
            edges=[],
        )
    bundle = _serialize_telegram_relationship_bundle(session, source_project_id, snapshot)
    return _ok_response("Returned the latest Telegram relationship snapshot.", **bundle)


@router.get("/api/projects/{project_id}/relationships/{snapshot_id}")
def get_telegram_relationship_snapshot_api(project_id: str, snapshot_id: str, session: SessionDep):
    snapshot = _resolve_telegram_relationship_snapshot(session, project_id, snapshot_id)
    bundle = _serialize_telegram_relationship_bundle(session, snapshot.project_id, snapshot)
    return _ok_response("Returned Telegram relationship snapshot details.", **bundle)


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
            initial_payload = await run_in_threadpool(fetch_payload)
            if initial_payload:
                last_snapshot = json.dumps(initial_payload, ensure_ascii=False)
                yield _format_sse("snapshot", initial_payload)
                if initial_payload["status"] not in {"queued", "running"}:
                    yield _format_sse("done", {"run_id": initial_payload["id"], "status": initial_payload["status"]})
                    return
            while True:
                try:
                    event = await run_in_threadpool(subscription.get, True, 15.0)
                except Empty:
                    event = {"event": "heartbeat", "payload": {}}

                if event.get("event") == "trace":
                    yield _format_sse("trace", event.get("payload") or {})

                if event.get("event") in {"snapshot", "heartbeat"}:
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
    default_document_key = "skill" if asset_kind == "cc_skill" else "asset"

    def emit_status(
        phase: str,
        progress_percent: int,
        message: str,
        *,
        status: str = "running",
        document_key: str | None = None,
    ) -> None:
        events.put(
            {
                "type": "status",
                "status": status,
                "phase": phase,
                "progress_percent": progress_percent,
                "message": message,
                "asset_kind": asset_kind,
                "document_key": document_key,
            }
        )

    def worker():
        try:
            emit_status("prepare", 6, f"开始生成{_asset_label(asset_kind)}草稿", document_key=default_document_key)
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

                emit_status("load", 14, "正在读取最新分析结果", document_key=default_document_key)

                def stream_callback(payload: Any):
                    if isinstance(payload, dict):
                        chunk = str(payload.get("chunk", "") or "")
                        document_key = str(payload.get("document_key") or "").strip() or default_document_key
                    else:
                        chunk = str(payload or "")
                        document_key = default_document_key
                    if not chunk:
                        return
                    events.put({"type": "delta", "document_key": document_key, "chunk": chunk, "asset_kind": asset_kind})

                def progress_callback(progress: dict[str, Any]):
                    events.put(
                        {
                            "type": "status",
                            "status": "running",
                            "phase": progress.get("phase", "running"),
                            "progress_percent": int(progress.get("progress_percent", 0) or 0),
                            "message": str(progress.get("message", "") or ""),
                            "asset_kind": asset_kind,
                            "document_key": str(progress.get("document_key") or "").strip() or None,
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

                emit_status("persist", 94, "正在保存草稿和导出文件", document_key=default_document_key)
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
                        "message": "草稿生成完成，已同步到编辑区。",
                        "draft_id": draft.id,
                        "draft": _serialize_draft(draft),
                        "asset_kind": asset_kind,
                        "document_key": default_document_key,
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
    draft = _generate_asset_draft(request, session, project_id, asset_kind="cc_skill")
    return {**_serialize_draft(draft), "request_status": "ok", "message": "Claude Code Skill 草稿已生成。"}


@router.post("/api/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_api(request: Request, project_id: str, draft_id: str, session: SessionDep):
    draft = repository.get_asset_draft(session, draft_id, asset_kind="cc_skill")
    if not draft:
        raise HTTPException(status_code=404, detail="未找到 Claude Code Skill 草稿。")
    version = repository.publish_skill_draft(session, project_id, draft)
    _persist_asset_files(
        request,
        project_id,
        "cc_skill",
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
        "message": "Claude Code Skill 版本已发布。",
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


@router.get("/api/projects/{project_id}/writing/sessions")
def list_writing_sessions_api(project_id: str, session: SessionDep):
    _ensure_stone_project(session, project_id)
    sessions = repository.list_chat_sessions(session, project_id, session_kind="writing")
    return _ok_response("已返回写作会话列表。", sessions=[_serialize_chat_session(item) for item in sessions])


@router.post("/api/projects/{project_id}/writing/sessions")
def create_writing_session_api(project_id: str, payload: PreprocessSessionCreatePayload, session: SessionDep):
    _ensure_stone_project(session, project_id)
    chat_session = repository.create_chat_session(
        session,
        project_id=project_id,
        session_kind="writing",
        title=payload.title or "新建写作会话",
    )
    return _ok_response("写作会话已创建。", **_serialize_chat_session(chat_session))


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}")
def get_writing_session_api(project_id: str, session_id: str, session: SessionDep):
    _ensure_stone_project(session, project_id)
    chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    return _ok_response("已返回写作会话详情。", **_serialize_writing_session_detail(chat_session))


@router.patch("/api/projects/{project_id}/writing/sessions/{session_id}")
def update_writing_session_api(
    project_id: str,
    session_id: str,
    payload: PreprocessSessionUpdatePayload,
    session: SessionDep,
):
    _ensure_stone_project(session, project_id)
    chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    repository.rename_chat_session(session, chat_session, title=payload.title)
    return _ok_response("写作会话已更新。", **_serialize_chat_session(chat_session))


@router.delete("/api/projects/{project_id}/writing/sessions/{session_id}")
def delete_writing_session_api(project_id: str, session_id: str, session: SessionDep):
    _ensure_stone_project(session, project_id)
    chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    repository.delete_chat_session(session, chat_session)
    return _ok_response("写作会话已删除。", ok=True, session_id=session_id)


@router.post("/api/projects/{project_id}/writing/sessions/{session_id}/messages")
def create_writing_message_api(
    request: Request,
    project_id: str,
    session_id: str,
    payload: WritingMessagePayload,
    session: SessionDep,
):
    _ensure_stone_project(session, project_id)
    try:
        result = request.app.state.writing_service.start_stream(
            project_id=project_id,
            session_id=session_id,
            topic=payload.topic,
            target_word_count=payload.target_word_count,
            extra_requirements=payload.extra_requirements,
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return _ok_response("写作任务已提交。", **result)


@router.get("/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}")
def stream_writing_events_api(request: Request, project_id: str, session_id: str, stream_id: str, session: SessionDep):
    _ensure_stone_project(session, project_id)
    chat_session = repository.get_chat_session(session, session_id, session_kind="writing")
    if not chat_session or chat_session.project_id != project_id:
        raise HTTPException(status_code=404, detail="未找到写作会话。")
    try:
        generator = request.app.state.writing_service.stream_events(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="未找到写作流。") from exc
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
    config_id: str | None = Query(default=None),
):
    if config_id:
        payload = repository.get_service_setting_config(
            session,
            f"{service}_service",
            config_id,
            default_provider="openai",
            default_api_mode="responses",
        )
        if not payload or not _is_service_setting_config_usable(payload):
            raise HTTPException(status_code=400, detail=f"{service} 服务尚未配置。")
        config = ServiceConfig(
            base_url=str(payload.get("base_url") or "").strip() or None,
            api_key=str(payload.get("api_key") or "").strip(),
            model=str(payload.get("model") or "").strip() or None,
            provider_kind=str(payload.get("provider_kind") or "openai"),
            api_mode=str(payload.get("api_mode") or "responses"),
        )
    else:
        config = repository.get_service_config(session, f"{service}_service")
        if not config:
            raise HTTPException(status_code=400, detail=f"{service} 服务尚未配置。")
    client = OpenAICompatibleClient(config, log_path=str(request.app.state.config.llm_log_path))
    try:
        return _ok_response("已返回模型列表。", service=service, models=client.list_models())
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _extract_telegram_binding(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    source = dict(summary or {})
    target_user = source.get("target_user") if isinstance(source.get("target_user"), dict) else {}
    participant_id = str(
        target_user.get("participant_id")
        or source.get("participant_id")
        or ""
    ).strip()
    target_user_query = str(source.get("target_user_query") or "").strip()
    label = str(
        target_user.get("label")
        or target_user.get("primary_alias")
        or target_user.get("display_name")
        or target_user_query
        or participant_id
        or ""
    ).strip()
    if not any((participant_id, target_user_query, label)):
        return None
    return {
        "participant_id": participant_id or None,
        "target_user_query": target_user_query or label or None,
        "label": label or None,
        "display_name": str(target_user.get("display_name") or "").strip() or None,
        "username": str(target_user.get("username") or "").strip() or None,
        "uid": str(target_user.get("uid") or "").strip() or None,
        "primary_alias": str(target_user.get("primary_alias") or "").strip() or None,
        "analysis_context": str(source.get("analysis_context") or "").strip() or None,
        "preprocess_run_id": str(
            source.get("preprocess_run_id")
            or target_user.get("preprocess_run_id")
            or ""
        ).strip() or None,
        "source": str(target_user.get("source") or "").strip() or None,
        "resolved": bool(target_user or participant_id),
    }


def _enrich_telegram_binding(
    binding: dict[str, Any] | None,
    *,
    top_user_lookup: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not binding:
        return None
    merged = dict(binding)
    participant_id = str(merged.get("participant_id") or "").strip()
    top_user = (top_user_lookup or {}).get(participant_id) if participant_id else None
    label = (
        merged.get("label")
        or (top_user or {}).get("label")
        or (top_user or {}).get("display_name")
        or (top_user or {}).get("username")
        or (top_user or {}).get("uid")
        or participant_id
    )
    merged["label"] = label
    merged["target_user_query"] = merged.get("target_user_query") or label
    merged["display_name"] = merged.get("display_name") or (top_user or {}).get("display_name")
    merged["username"] = merged.get("username") or (top_user or {}).get("username")
    merged["uid"] = merged.get("uid") or (top_user or {}).get("uid")
    merged["primary_alias"] = (
        merged.get("primary_alias")
        or (top_user or {}).get("primary_alias")
        or (top_user or {}).get("display_name")
        or (top_user or {}).get("username")
        or (top_user or {}).get("uid")
    )
    merged["resolved"] = bool(merged.get("resolved") or top_user)
    return merged


def _project_context(request: Request, session: Session, project_id: str, *, document_limit: int = 20, document_offset: int = 0) -> dict[str, Any]:
    project = _ensure_project(session, project_id)
    primary_asset_kind = _primary_asset_kind_for_mode(project.mode)
    source_project_id = repository.get_target_project_id(session, project_id)
    telegram_data_project_id = source_project_id if project.mode == "telegram" else project.id
    documents = repository.list_project_documents(session, project_id, limit=document_limit, offset=document_offset)
    doc_counts = repository.count_project_documents(session, project_id)
    source_doc_counts = doc_counts if source_project_id == project_id else repository.count_project_documents(session, source_project_id)
    latest_run = repository.get_latest_analysis_run(session, project_id, load_facets=False, load_events=False)
    latest_draft = repository.get_latest_asset_draft(session, project_id, asset_kind=primary_asset_kind)
    latest_version = repository.get_latest_asset_version(session, project_id, asset_kind=primary_asset_kind)
    latest_summary = latest_run.summary_json or {} if latest_run else {}
    preprocess_sessions = repository.list_chat_sessions(session, project_id, session_kind="preprocess")
    writing_guide_status = _resolve_writing_guide_status(session, project_id) if project.mode == "stone" else None
    latest_preprocess_run = None
    latest_successful_preprocess_run = None
    
    if project.mode == "telegram":
        latest_preprocess_run = repository.get_latest_telegram_preprocess_run(session, telegram_data_project_id)
        latest_successful_preprocess_run = repository.get_latest_successful_telegram_preprocess_run(session, telegram_data_project_id)
    elif project.mode == "stone":
        latest_preprocess_run = repository.get_latest_stone_preprocess_run(session, project.id)
        latest_successful_preprocess_run = repository.get_latest_successful_stone_preprocess_run(session, project.id)
    telegram_top_users = (
        repository.list_telegram_preprocess_top_users(session, telegram_data_project_id, run_id=latest_successful_preprocess_run.id)
        if latest_successful_preprocess_run
        else []
    )
    telegram_active_users = (
        repository.list_telegram_preprocess_active_users(session, telegram_data_project_id, run_id=latest_successful_preprocess_run.id)
        if latest_successful_preprocess_run
        else []
    )
    latest_relationship_snapshot = (
        repository.get_telegram_relationship_snapshot_for_run(session, latest_successful_preprocess_run.id)
        if latest_successful_preprocess_run
        else None
    )
    latest_relationship_bundle = (
        _serialize_telegram_relationship_bundle(session, telegram_data_project_id, latest_relationship_snapshot)
        if latest_relationship_snapshot
        else None
    )
    serialized_top_users = [_serialize_telegram_preprocess_top_user(item) for item in telegram_top_users]
    serialized_active_users = [_serialize_telegram_preprocess_active_user(item) for item in telegram_active_users]
    top_user_lookup = {
        item["participant_id"]: item
        for item in serialized_top_users
        if item.get("participant_id")
    }
    ready_count_for_analysis = source_doc_counts["ready"] if project.mode == "telegram" else doc_counts["ready"]
    can_analyze = ready_count_for_analysis > 0 and (
        project.mode != "telegram" or latest_successful_preprocess_run is not None
    )
    current_binding = _enrich_telegram_binding(_extract_telegram_binding(latest_summary), top_user_lookup=top_user_lookup)
    parent_project = repository.get_project(session, project.parent_id) if project.parent_id else None
    telegram_is_parent_workspace = project.mode == "telegram" and project.parent_id is None
    telegram_is_child_persona = project.mode == "telegram" and project.parent_id is not None

    profiles = []
    supports_profiles = project.parent_id is None and project.mode in {"group", "telegram"}
    if supports_profiles:
        for p in repository.list_child_projects(session, project_id):
            child_latest_run = repository.get_latest_analysis_run(session, p.id)
            child_latest_summary = child_latest_run.summary_json or {} if child_latest_run else {}
            child_binding = _enrich_telegram_binding(
                _extract_telegram_binding(child_latest_summary),
                top_user_lookup=top_user_lookup,
            )
            profiles.append(
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description,
                    "mode": p.mode,
                    "latest_run": child_latest_run,
                    "latest_skill": repository.get_latest_skill_version(session, p.id),
                    "binding": child_binding,
                    "is_bound": child_binding is not None,
                    "analysis_context": (
                        (child_binding or {}).get("analysis_context")
                        or p.description
                        or ""
                    ),
                }
            )

    return {
        "project": project,
        "parent_project": parent_project,
        "profiles": profiles,
        "supports_profiles": supports_profiles,
        "documents": documents,
        "project_bootstrap": json.dumps(
            {
                "project": {"id": project.id, "name": project.name, "mode": project.mode},
                "primary_asset_kind": primary_asset_kind,
                "telegram": {
                    "is_parent_workspace": telegram_is_parent_workspace,
                    "is_child_persona": telegram_is_child_persona,
                    "can_create_persona": bool(latest_successful_preprocess_run),
                    "top_users": serialized_top_users,
                    "active_users": serialized_active_users,
                    "current_binding": current_binding,
                    "relationships": latest_relationship_bundle,
                    "profiles": [
                        {
                            "id": item["id"],
                            "name": item["name"],
                            "is_bound": item["is_bound"],
                            "binding": item["binding"],
                        }
                        for item in profiles
                    ],
                },
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
                "locale": get_locale(request),
                "ui_strings": page_strings("project", get_locale(request)),
                "writing_guide_status": writing_guide_status,
            },
            ensure_ascii=False,
        ),
        "latest_run": latest_run,
        "latest_preprocess_run": latest_preprocess_run,
        "latest_successful_preprocess_run": latest_successful_preprocess_run,
        "latest_relationship_snapshot": latest_relationship_snapshot,
        "telegram_relationship_bundle": latest_relationship_bundle,
        "telegram_top_users": serialized_top_users,
        "telegram_active_users": serialized_active_users,
        "telegram_binding": current_binding,
        "telegram_is_parent_workspace": telegram_is_parent_workspace,
        "telegram_is_child_persona": telegram_is_child_persona,
        "preprocess_project_id": source_project_id if project.mode == "telegram" else project.id,
        "primary_asset_kind": primary_asset_kind,
        "can_analyze": can_analyze,
        "latest_draft": latest_draft,
        "latest_version": latest_version,
        "writing_guide_status": writing_guide_status,
        "preprocess_sessions": preprocess_sessions,
        "stone_mode_label": _stone_mode_label(get_locale(request)),
        "stone_mode_hint": _stone_mode_hint(get_locale(request)),
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
            "target_user_query": (
                (current_binding or {}).get("target_user_query")
                or latest_summary.get("target_user_query")
                or (project.name if project.mode == "telegram" else "")
                or ""
            ),
            "participant_id": (current_binding or {}).get("participant_id") or latest_summary.get("participant_id") or "",
            "analysis_context": (current_binding or {}).get("analysis_context") or latest_summary.get("analysis_context") or project.description or "",
            "concurrency": latest_summary.get("concurrency") or DEFAULT_ANALYSIS_CONCURRENCY,
        },
        "analysis_concurrency_default": DEFAULT_ANALYSIS_CONCURRENCY,
    }


def _stone_preprocess_context(
    session: Session,
    project_id: str,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    project = _ensure_project(session, project_id)
    runs = repository.list_stone_preprocess_runs(session, project_id, limit=24)
    selected_run = None
    if run_id:
        selected_run = repository.get_stone_preprocess_run(session, run_id)
        if selected_run and selected_run.project_id != project_id:
            selected_run = None
    if not selected_run:
        selected_run = repository.get_latest_successful_stone_preprocess_run(session, project_id)
    if not selected_run and runs:
        selected_run = runs[0]

    documents = repository.list_project_documents(session, project_id)
    doc_counts = repository.count_project_documents(session, project_id)
    serialized_run = _serialize_stone_preprocess_run(selected_run) if selected_run else None

    # Check if a run is already active
    active_run = repository.get_active_stone_preprocess_run(session, project_id)
    can_start = doc_counts["ready"] > 0 and not active_run

    ui_strings = page_strings("preprocess", "zh-CN")

    return {
        "project": project,
        "runs": [_serialize_stone_preprocess_run(item) for item in runs],
        "selected_run_data": serialized_run,
        "documents": [_serialize_document(d) for d in documents],
        "can_start": can_start,
        "stone_preprocess_bootstrap": json.dumps(
            {
                "project_id": project.id,
                "run_id": selected_run.id if selected_run else None,
                "initial_run": serialized_run,
                "ui_strings": ui_strings,
            },
            ensure_ascii=False,
        ),
    }


def _telegram_preprocess_context(
    session: Session,
    project_id: str,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    project = _ensure_project(session, project_id)
    runs = repository.list_telegram_preprocess_runs(session, project_id, limit=24)
    selected_run = None
    if run_id:
        selected_run = repository.get_telegram_preprocess_run(session, run_id)
        if selected_run and selected_run.project_id != project_id:
            selected_run = None
    if not selected_run:
        selected_run = repository.get_latest_successful_telegram_preprocess_run(session, project_id)
    if not selected_run and runs:
        selected_run = runs[0]
    topics = (
        repository.list_telegram_preprocess_topics(session, project_id, run_id=selected_run.id)
        if selected_run
        else []
    )
    selected_run_payload = (
        _serialize_telegram_preprocess_detail(session, project_id, selected_run)
        if selected_run
        else None
    )
    return {
        "project": project,
        "selected_run": selected_run,
        "selected_run_data": selected_run_payload,
        "run_history": [_serialize_telegram_preprocess_run(item) for item in runs],
        "top_users": selected_run_payload.get("top_users", []) if selected_run_payload else [],
        "weekly_candidates": selected_run_payload.get("weekly_candidates", []) if selected_run_payload else [],
        "topics": [_serialize_telegram_preprocess_topic(item) for item in topics],
        "can_start": repository.get_latest_telegram_chat(session, project_id) is not None,
        "telegram_preprocess_bootstrap": json.dumps(
            {
                "project_id": project_id,
                "run_id": selected_run.id if selected_run else None,
                "bundle": selected_run_payload,
            },
            ensure_ascii=False,
        ),
    }


def _create_stone_preprocess_run(
    request: Request,
    session: Session,
    project_id: str,
    concurrency: int = 1,
) -> "StonePreprocessRun":
    project = _ensure_project(session, project_id)
    chat_config = repository.get_service_config(session, "chat_service")

    run = repository.create_stone_preprocess_run(
        session,
        project_id=project_id,
        llm_model=chat_config.model if chat_config else None,
        summary_json={"concurrency": concurrency},
    )
    request.app.state.stone_preprocess_worker.process(run.id, project_id)
    return run


def _create_telegram_preprocess_run(
    request: Request,
    session: Session,
    project_id: str,
    *,
    weekly_summary_concurrency: int | None = None,
) -> TelegramPreprocessRun:
    project = _ensure_project(session, project_id)
    if project.mode != "telegram":
        raise HTTPException(status_code=400, detail="Only Telegram projects use preprocess runs.")
    source_project_id = repository.get_target_project_id(session, project_id)
    if not repository.get_latest_telegram_chat(session, source_project_id):
        raise HTTPException(status_code=400, detail="Please upload and ingest a Telegram JSON export first.")
    created = request.app.state.telegram_preprocess_manager.submit(
        source_project_id,
        weekly_summary_concurrency=weekly_summary_concurrency,
    )
    session.expire_all()
    refreshed = repository.get_telegram_preprocess_run(session, created.id)
    return refreshed or created


def _resolve_telegram_preprocess_run(session: Session, project_id: str, run_id: str) -> TelegramPreprocessRun:
    project = _ensure_project(session, project_id)
    if project.mode != "telegram":
        raise HTTPException(status_code=400, detail="Only Telegram projects use preprocess runs.")
    source_project_id = repository.get_target_project_id(session, project_id)
    run = repository.get_telegram_preprocess_run(session, run_id)
    if not run or run.project_id != source_project_id:
        raise HTTPException(status_code=404, detail="Telegram preprocess run not found.")
    return run


def _resolve_telegram_relationship_snapshot(
    session: Session,
    project_id: str,
    snapshot_id: str,
) -> TelegramRelationshipSnapshot:
    project = _ensure_project(session, project_id)
    if project.mode != "telegram":
        raise HTTPException(status_code=400, detail="Only Telegram projects use relationship snapshots.")
    source_project_id = repository.get_target_project_id(session, project_id)
    snapshot = repository.get_telegram_relationship_snapshot(session, snapshot_id)
    if not snapshot or snapshot.project_id != source_project_id:
        raise HTTPException(status_code=404, detail="Telegram relationship snapshot not found.")
    return snapshot


def _enqueue_analysis(
    request: Request,
    session: Session,
    project_id: str,
    *,
    target_role: str | None,
    target_user_query: str | None = None,
    participant_id: str | None = None,
    analysis_context: str | None,
    concurrency: int | None = None,
) -> AnalysisRun:
    project = _ensure_project(session, project_id)
    source_project_id = repository.get_target_project_id(session, project_id)
    document_project_id = source_project_id if project.mode == "telegram" else project_id
    documents = repository.list_project_documents(session, document_project_id)
    ready_documents = [document for document in documents if document.ingest_status == "ready"]
    if not ready_documents:
        raise HTTPException(status_code=400, detail="Upload at least one successfully ingested document first.")
    if project.mode == "telegram" and not repository.get_latest_successful_telegram_preprocess_run(session, source_project_id):
        raise HTTPException(status_code=400, detail="Run Telegram preprocess successfully before analysis.")
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
        target_role=(target_role or "").strip() or None if project.mode != "telegram" else None,
        target_user_query=(target_user_query or "").strip() or None,
        participant_id=(participant_id or "").strip() or None,
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
            doc_path = asset_dir / f"{base_name}.{filename}"
            doc_path.parent.mkdir(parents=True, exist_ok=True)
            doc_path.write_text(content, encoding="utf-8")


def _delete_asset_files(
    request: Request,
    project_id: str,
    asset_kind: str,
    base_name: str,
) -> None:
    asset_dir = request.app.state.config.assets_dir / project_id / asset_kind
    targets = [
        asset_dir / f"{base_name}.md",
        asset_dir / f"{base_name}.json",
        asset_dir / f"{base_name}.prompt.txt",
    ]
    for path in targets:
        if path.exists() and path.is_file():
            path.unlink()
    if asset_kind in {"skill", "cc_skill"}:
        for filename in _skill_documents_for_export(asset_kind, {}, "").values():
            doc_path = asset_dir / f"{base_name}.{filename['filename']}"
            if doc_path.exists() and doc_path.is_file():
                doc_path.unlink()


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
    analysis_markdown = _build_analysis_reference_markdown(payload)
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
    if asset_kind == "cc_skill":
        legacy_docs["analysis"] = {
            "filename": filename_map["analysis"],
            "markdown": analysis_markdown,
        }
    if asset_kind == "skill":
        legacy_docs["merge"] = {
            "filename": filename_map["merge"],
            "markdown": merge_markdown,
        }
    return legacy_docs


def _build_analysis_reference_markdown(payload: dict[str, Any]) -> str:
    stored_markdown = str(payload.get("analysis_markdown") or payload.get("analysis_reference_markdown") or "").strip()
    if stored_markdown:
        return stored_markdown
    summary = payload.get("analysis_summary") if isinstance(payload.get("analysis_summary"), dict) else {}
    if not summary:
        return "# 十维分析摘要\n\n当前版本没有可用的十维分析文本。"
    lines = ["# 十维分析摘要", ""]
    for key, value in summary.items():
        text = str(value or "").strip()
        if not text:
            continue
        lines.extend([f"## {key}", text, ""])
    return "\n".join(lines).strip() or "# 十维分析摘要\n\n当前版本没有可用的十维分析文本。"


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
    export_docs = _skill_documents_for_export(asset_kind, payload, markdown_text)
    documents = {
        key: {
            "filename": str(document.get("filename") or ""),
            "markdown": str(document.get("markdown") or ""),
        }
        for key, document in export_docs.items()
    }
    if asset_kind == "skill":
        documents["merge"] = {
            "filename": str(documents.get("merge", {}).get("filename") or SKILL_DOCUMENT_FILENAMES["merge"]),
            "markdown": markdown_text,
        }
    else:
        documents["skill"] = {
            "filename": str(documents.get("skill", {}).get("filename") or CC_SKILL_DOCUMENT_FILENAMES["skill"]),
            "markdown": markdown_text,
        }
    payload["documents"] = documents
    return payload, markdown_text


def _ordered_facets(facets: list[AnalysisFacet], summary: dict[str, Any] | None = None) -> list[AnalysisFacet]:
    facet_keys = [str(item).strip() for item in ((summary or {}).get("facet_keys") or []) if str(item).strip()]
    if not facet_keys:
        facet_keys = [facet.key for facet in FACETS]
    order = {facet_key: index for index, facet_key in enumerate(facet_keys)}
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
    return max(MIN_ANALYSIS_CONCURRENCY, candidate)


def _project_write_block_detail(project) -> dict[str, Any]:
    message = "项目正在删除中，当前操作已禁用。" if project.lifecycle_state == repository.PROJECT_LIFECYCLE_DELETING else "项目删除失败，当前仅允许重试删除。"
    return {
        "message": message,
        "project_id": project.id,
        "lifecycle_state": project.lifecycle_state,
        "deletion_error": project.deletion_error,
    }


def _ensure_project(session: Session, project_id: str, *, require_active: bool = False):
    project = repository.get_project(session, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")
    if require_active and project.lifecycle_state != repository.PROJECT_LIFECYCLE_ACTIVE:
        raise HTTPException(status_code=409, detail=_project_write_block_detail(project))
    return project


def _ensure_project_writable(session: Session, project_id: str):
    return _ensure_project(session, project_id, require_active=True)


def _get_project_document(
    session: Session,
    project_id: str,
    document_id: str,
    *,
    require_active: bool = False,
) -> DocumentRecord:
    _ensure_project(session, project_id, require_active=require_active)
    document = repository.get_document(session, document_id)
    if not document or document.project_id != project_id:
        raise HTTPException(status_code=404, detail="Document not found.")
    return document


def _schedule_project_deletion(request: Request, session: Session, project_id: str):
    manager = request.app.state.project_deletion_manager
    existing_task = manager.get_by_project(project_id)
    if existing_task and existing_task.get("status") in ACTIVE_TASK_STATUSES:
        project = _ensure_project(session, project_id)
        return project, existing_task

    project = _ensure_project(session, project_id)
    project_ids = repository.get_project_tree_ids(session, project.id)
    repository.mark_projects_for_deletion(session, project_ids)
    session.commit()
    try:
        task = manager.submit(project.id, project_ids=project_ids)
    except Exception as exc:
        error_text = str(exc).strip() or exc.__class__.__name__
        with request.app.state.db.session() as repair_session:
            repository.mark_projects_delete_failed(repair_session, project_ids, error=error_text)
        raise HTTPException(status_code=500, detail=f"Failed to enqueue project deletion: {error_text}") from exc
    return project, task


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
    summary = dict(run.summary_json or {})
    facet_keys = [str(item).strip() for item in (summary.get("facet_keys") or []) if str(item).strip()]
    facet_total = len(facet_keys) or len(FACETS)
    serialized_facets = [_serialize_analysis_facet(facet) for facet in _ordered_facets(run.facets, summary)]
    requested_concurrency = _normalize_analysis_concurrency(
        summary.get("requested_concurrency") or summary.get("concurrency")
    )
    summary["total_facets"] = int(summary.get("total_facets") or facet_total)
    summary["concurrency"] = requested_concurrency
    summary["requested_concurrency"] = requested_concurrency

    completed = sum(1 for facet in serialized_facets if facet["status"] == "completed")
    failed = sum(1 for facet in serialized_facets if facet["status"] == "failed")
    active = [facet for facet in serialized_facets if facet["status"] in {"preparing", "running"}]
    queued = [facet for facet in serialized_facets if facet["status"] == "queued"]
    effective_concurrency = min(requested_concurrency, facet_total)
    agent_tracks = []

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
    summary["effective_concurrency"] = effective_concurrency
    summary["active_agents"] = len(active)
    summary["effective_active_agents"] = len(active)
    progress_total = max(1, int(summary.get("total_facets") or facet_total or 1))
    summary["progress_percent"] = int(((completed + failed) / progress_total) * 100)

    for facet in active:
        findings = dict(facet.get("findings") or {})
        retrieval_trace = findings.get("retrieval_trace") if isinstance(findings.get("retrieval_trace"), dict) else {}
        tool_calls = retrieval_trace.get("tool_calls") if isinstance(retrieval_trace, dict) else []
        request_keys: list[str] = []
        if isinstance(tool_calls, list):
            for call in tool_calls:
                if not isinstance(call, dict):
                    continue
                request_key = str(call.get("request_key") or "").strip()
                if request_key and request_key not in request_keys:
                    request_keys.append(request_key)
        agent_tracks.append(
            {
                "facet_key": facet["facet_key"],
                "label": findings.get("label") or facet["facet_key"],
                "status": facet["status"],
                "phase": findings.get("phase"),
                "tool_call_count": len(tool_calls) if isinstance(tool_calls, list) else 0,
                "request_keys": request_keys,
                "updated_at": findings.get("finished_at") or findings.get("started_at"),
                "started_at": findings.get("started_at"),
            }
        )
    summary["agent_tracks"] = agent_tracks

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
    elif summary.get("current_phase") == "document_profiling":
        summary["current_facet"] = None
        summary["current_stage"] = str(summary.get("current_stage") or _analysis_stage_label(None, "document_profiling"))
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
        "asset_kind": getattr(draft, "asset_kind", "cc_skill"),
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


def _serialize_writing_session_detail(chat_session) -> dict[str, Any]:
    turns = sorted(chat_session.turns, key=lambda item: item.created_at)
    return {
        **_serialize_chat_session(chat_session),
        "turns": [_serialize_chat_turn(turn) for turn in turns],
    }


def _serialize_stone_preprocess_run(run: "StonePreprocessRun") -> dict[str, Any]:
    summary = dict(run.summary_json or {})
    return {
        "id": run.id,
        "project_id": run.project_id,
        "status": run.status,
        "started_at": run.started_at.isoformat() + "Z" if run.started_at else None,
        "finished_at": run.finished_at.isoformat() + "Z" if run.finished_at else None,
        "llm_model": run.llm_model,
        "progress_percent": run.progress_percent,
        "current_stage": run.current_stage,
        "prompt_tokens": run.prompt_tokens,
        "completion_tokens": run.completion_tokens,
        "total_tokens": run.total_tokens,
        "error_message": run.error_message,
        "stone_profile_completed": summary.get("stone_profile_completed", 0),
        "stone_profile_total": summary.get("stone_profile_total", 0),
        "concurrency": summary.get("concurrency", 1),
        "created_at": run.created_at.isoformat() + "Z",
    }


def _serialize_telegram_preprocess_run(run: TelegramPreprocessRun) -> dict[str, Any]:
    summary = dict(run.summary_json or {})
    weekly_concurrency = int(summary.get("weekly_summary_concurrency") or 1)
    completed_week_count = int(summary.get("completed_week_count") or summary.get("topic_count") or run.topic_count or 0)
    total_week_count = int(summary.get("weekly_candidate_count") or summary.get("window_count") or run.window_count or 0)
    remaining_week_count = max(int(summary.get("remaining_week_count") or (total_week_count - completed_week_count)), 0)
    current_topic_total = int(summary.get("current_topic_total") or total_week_count or 0)
    current_topic_index = int(
        summary.get("current_topic_index")
        or (current_topic_total if str(run.status or "").lower() == "completed" and current_topic_total else 0)
    )
    return {
        "id": run.id,
        "status": run.status,
        "chat_id": run.chat_id,
        "llm_model": run.llm_model,
        "progress_percent": int(run.progress_percent or summary.get("progress_percent") or 0),
        "current_stage": run.current_stage or summary.get("current_stage"),
        "prompt_tokens": int(run.prompt_tokens or 0),
        "completion_tokens": int(run.completion_tokens or 0),
        "total_tokens": int(run.total_tokens or 0),
        "cache_creation_tokens": int(run.cache_creation_tokens or 0),
        "cache_read_tokens": int(run.cache_read_tokens or 0),
        "window_count": int(run.window_count or summary.get("window_count") or summary.get("weekly_candidate_count") or 0),
        "top_user_count": int(summary.get("top_user_count") or 0),
        "weekly_candidate_count": int(summary.get("weekly_candidate_count") or summary.get("window_count") or 0),
        "topic_count": int(run.topic_count or summary.get("topic_count") or 0),
        "weekly_summary_concurrency": weekly_concurrency,
        "requested_weekly_concurrency": weekly_concurrency,
        "active_agents": int(summary.get("active_agents") or 0),
        "completed_week_count": completed_week_count,
        "remaining_week_count": remaining_week_count,
        "active_user_count": int(summary.get("active_user_count") or run.active_user_count or 0),
        "relationship_snapshot_id": summary.get("relationship_snapshot_id"),
        "relationship_status": summary.get("relationship_status"),
        "relationship_edge_count": int(summary.get("relationship_edge_count") or 0),
        "relationship_summary": dict(summary.get("relationship_summary") or {}),
        "current_topic_index": max(current_topic_index, 0),
        "current_topic_total": max(current_topic_total, 0),
        "current_topic_label": str(summary.get("current_topic_label") or "").strip(),
        "resume_available": bool(summary.get("resume_available")),
        "resume_count": int(summary.get("resume_count") or 0),
        "error_message": run.error_message,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": summary.get("updated_at") or (run.finished_at.isoformat() if run.finished_at else None),
        "snapshot_version": int(summary.get("snapshot_version") or 0),
        "trace_event_count": int(summary.get("trace_event_count") or 0),
        "trace_events": [dict(item) for item in (summary.get("trace_events") or []) if isinstance(item, dict)],
        "summary": summary,
    }


def _serialize_telegram_preprocess_top_user(user: TelegramPreprocessTopUser) -> dict[str, Any]:
    label = user.display_name or user.username or user.uid or user.participant_id
    return {
        "id": user.id,
        "run_id": user.run_id,
        "participant_id": user.participant_id,
        "rank": user.rank,
        "uid": user.uid,
        "username": user.username,
        "display_name": user.display_name,
        "primary_alias": label,
        "label": label,
        "message_count": user.message_count,
        "first_seen_at": user.first_seen_at.isoformat() if user.first_seen_at else None,
        "last_seen_at": user.last_seen_at.isoformat() if user.last_seen_at else None,
        "metadata": user.metadata_json or {},
    }


def _serialize_telegram_preprocess_weekly_candidate(candidate: TelegramPreprocessWeeklyTopicCandidate) -> dict[str, Any]:
    return {
        "id": candidate.id,
        "run_id": candidate.run_id,
        "week_key": candidate.week_key,
        "window_index": int(candidate.window_index or 1),
        "start_at": candidate.start_at.isoformat() if candidate.start_at else None,
        "end_at": candidate.end_at.isoformat() if candidate.end_at else None,
        "start_message_id": candidate.start_message_id,
        "end_message_id": candidate.end_message_id,
        "message_count": candidate.message_count,
        "participant_count": candidate.participant_count,
        "top_participants": list(candidate.top_participants_json or []),
        "sample_messages": list(candidate.sample_messages_json or [])[:12],
        "metadata": candidate.metadata_json or {},
    }


def _serialize_telegram_preprocess_topic(topic: TelegramPreprocessTopic) -> dict[str, Any]:
    metadata = dict(topic.metadata_json or {})
    quotes = sorted(
        list(topic.quotes or []),
        key=lambda item: (
            item.participant_id or "",
            int(item.rank or 0),
            int(item.telegram_message_id or 0),
        ),
    )
    quotes_by_participant: dict[str, list[dict[str, Any]]] = {}
    flat_quotes: list[dict[str, Any]] = []
    for quote in quotes:
        payload = {
            "participant_id": quote.participant_id,
            "display_name": quote.participant.display_name if quote.participant else None,
            "username": quote.participant.username if quote.participant else None,
            "rank": int(quote.rank or 0),
            "message_id": quote.telegram_message_id,
            "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
            "quote": quote.quote,
        }
        flat_quotes.append(payload)
        quotes_by_participant.setdefault(quote.participant_id, []).append(payload)
    return {
        "id": topic.id,
        "topic_index": topic.topic_index,
        "week_key": topic.week_key or metadata.get("week_key"),
        "week_topic_index": int(topic.week_topic_index or 0),
        "title": topic.title,
        "summary": topic.summary,
        "start_at": topic.start_at.isoformat() if topic.start_at else None,
        "end_at": topic.end_at.isoformat() if topic.end_at else None,
        "start_message_id": topic.start_message_id,
        "end_message_id": topic.end_message_id,
        "message_count": topic.message_count,
        "participant_count": topic.participant_count,
        "keywords": topic.keywords_json or [],
        "evidence": topic.evidence_json or [],
        "subtopics": [str(item).strip() for item in (metadata.get("subtopics") or []) if str(item).strip()],
        "interaction_patterns": [
            str(item).strip()
            for item in (metadata.get("interaction_patterns") or [])
            if str(item).strip()
        ],
        "participant_viewpoints": [
            dict(item)
            for item in (metadata.get("participant_viewpoints") or [])
            if isinstance(item, dict)
        ],
        "participant_quotes": flat_quotes,
        "metadata": metadata,
        "participants": [
            {
                "participant_id": link.participant_id,
                "display_name": link.participant.display_name if link.participant else None,
                "username": link.participant.username if link.participant else None,
                "role_hint": link.role_hint,
                "stance_summary": link.stance_summary,
                "message_count": link.message_count,
                "mention_count": link.mention_count,
                "quotes": quotes_by_participant.get(link.participant_id, []),
            }
            for link in topic.participants
        ],
    }


def _serialize_telegram_preprocess_active_user(user: TelegramPreprocessActiveUser) -> dict[str, Any]:
    return {
        "id": user.id,
        "run_id": user.run_id,
        "participant_id": user.participant_id,
        "rank": user.rank,
        "uid": user.uid,
        "username": user.username,
        "display_name": user.display_name,
        "primary_alias": user.primary_alias,
        "aliases": user.aliases_json or [],
        "message_count": user.message_count,
        "first_seen_at": user.first_seen_at.isoformat() if user.first_seen_at else None,
        "last_seen_at": user.last_seen_at.isoformat() if user.last_seen_at else None,
        "evidence": user.evidence_json or [],
    }


def _serialize_telegram_relationship_snapshot(snapshot: TelegramRelationshipSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "run_id": snapshot.run_id,
        "project_id": snapshot.project_id,
        "chat_id": snapshot.chat_id,
        "status": snapshot.status,
        "analyzed_user_count": int(snapshot.analyzed_user_count or 0),
        "candidate_pair_count": int(snapshot.candidate_pair_count or 0),
        "llm_pair_count": int(snapshot.llm_pair_count or 0),
        "label_scheme": snapshot.label_scheme,
        "error_message": snapshot.error_message,
        "started_at": snapshot.started_at.isoformat() if snapshot.started_at else None,
        "finished_at": snapshot.finished_at.isoformat() if snapshot.finished_at else None,
        "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
        "updated_at": snapshot.updated_at.isoformat() if snapshot.updated_at else None,
        "summary": dict(snapshot.summary_json or {}),
    }


def _serialize_telegram_relationship_bundle(
    session: Session,
    project_id: str,
    snapshot: TelegramRelationshipSnapshot,
) -> dict[str, Any]:
    active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=snapshot.run_id)
    if active_users:
        participant_lookup = {
            item.participant_id: {
                "participant_id": item.participant_id,
                "label": item.primary_alias or item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in active_users
        }
        participant_rows = [
            {
                "participant_id": item.participant_id,
                "label": item.primary_alias or item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in active_users
        ]
    else:
        top_users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=snapshot.run_id)
        participant_lookup = {
            item.participant_id: {
                "participant_id": item.participant_id,
                "label": item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in top_users
        }
        participant_rows = list(participant_lookup.values())

    edges = []
    edges_by_participant: dict[str, list[dict[str, Any]]] = {}
    for edge in repository.list_telegram_relationship_edges(session, snapshot.id):
        participant_a = participant_lookup.get(edge.participant_a_id, {})
        participant_b = participant_lookup.get(edge.participant_b_id, {})
        payload = {
            "id": edge.id,
            "participant_a_id": edge.participant_a_id,
            "participant_b_id": edge.participant_b_id,
            "participant_a_label": participant_a.get("label") or edge.participant_a_id,
            "participant_b_label": participant_b.get("label") or edge.participant_b_id,
            "relation_label": edge.relation_label,
            "interaction_strength": round(float(edge.interaction_strength or 0.0), 4),
            "confidence": round(float(edge.confidence or 0.0), 4),
            "summary": edge.summary,
            "evidence": list(edge.evidence_json or []),
            "counterevidence": list(edge.counterevidence_json or []),
            "metrics": dict(edge.metrics_json or {}),
        }
        edges.append(payload)
        edges_by_participant.setdefault(edge.participant_a_id, []).append(payload)
        edges_by_participant.setdefault(edge.participant_b_id, []).append(payload)

    users = []
    for participant in participant_rows:
        participant_id = str(participant.get("participant_id") or "").strip()
        relation_edges = sorted(
            list(edges_by_participant.get(participant_id, [])),
            key=lambda item: (float(item.get("interaction_strength") or 0.0), float(item.get("confidence") or 0.0)),
            reverse=True,
        )
        strongest_edges = []
        for edge in relation_edges[:3]:
            counterpart_id = edge["participant_b_id"] if edge["participant_a_id"] == participant_id else edge["participant_a_id"]
            counterpart_label = edge["participant_b_label"] if edge["participant_a_id"] == participant_id else edge["participant_a_label"]
            strongest_edges.append(
                {
                    "counterpart_id": counterpart_id,
                    "counterpart_label": counterpart_label,
                    "relation_label": edge["relation_label"],
                    "interaction_strength": edge["interaction_strength"],
                    "confidence": edge["confidence"],
                }
            )
        users.append(
            {
                "participant_id": participant_id,
                "label": participant.get("label") or participant_id,
                "message_count": int(participant.get("message_count") or 0),
                "ally_count": sum(1 for edge in relation_edges if edge.get("relation_label") == "friendly"),
                "tense_count": sum(1 for edge in relation_edges if edge.get("relation_label") == "tense"),
                "strongest_edges": strongest_edges,
                "relations": relation_edges,
            }
        )

    users.sort(
        key=lambda item: (
            int(item.get("ally_count") or 0) + int(item.get("tense_count") or 0),
            int(item.get("message_count") or 0),
        ),
        reverse=True,
    )
    return {
        "snapshot": _serialize_telegram_relationship_snapshot(snapshot),
        "users": users,
        "edges": edges,
    }


def _serialize_telegram_preprocess_detail(
    session: Session,
    project_id: str,
    run: TelegramPreprocessRun,
) -> dict[str, Any]:
    payload = _serialize_telegram_preprocess_run(run)
    active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=run.id)
    payload["top_users"] = [
        _serialize_telegram_preprocess_top_user(item)
        for item in repository.list_telegram_preprocess_top_users(session, project_id, run_id=run.id)
    ]
    payload["weekly_candidates"] = [
        _serialize_telegram_preprocess_weekly_candidate(item)
        for item in repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=run.id)
    ]
    payload["topics"] = [
        _serialize_telegram_preprocess_topic(item)
        for item in repository.list_telegram_preprocess_topics(session, project_id, run_id=run.id)
    ]
    payload["active_users"] = [_serialize_telegram_preprocess_active_user(item) for item in active_users]
    payload["active_user_count"] = len(active_users)
    relationship_snapshot = repository.get_telegram_relationship_snapshot_for_run(session, run.id)
    payload["relationship_snapshot"] = (
        _serialize_telegram_relationship_snapshot(relationship_snapshot)
        if relationship_snapshot
        else None
    )
    return payload


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _normalize_service_setting_bundle_payload(
    payload: ServiceSettingsBundlePayload,
    *,
    service_name: str,
) -> dict[str, Any]:
    normalized_configs = [
        _normalize_service_setting_config_payload(
            item.model_dump(),
            service_name=service_name,
            fallback_label=f"{'Chat' if service_name == 'chat' else 'Embedding'} {index}",
        )
        for index, item in enumerate(payload.configs, start=1)
    ]
    if not normalized_configs:
        normalized_configs.append(
            _normalize_service_setting_config_payload(
                {"label": f"{'Chat' if service_name == 'chat' else 'Embedding'} 1"},
                service_name=service_name,
                fallback_label=f"{'Chat' if service_name == 'chat' else 'Embedding'} 1",
            )
        )

    config_ids = [item["id"] for item in normalized_configs]
    active_config_id = str(payload.active_config_id or "").strip()
    if active_config_id not in config_ids:
        active_config_id = config_ids[0]

    fallback_order: list[str] = []
    seen_ids = {active_config_id}
    for item in payload.fallback_order:
        config_id = str(item or "").strip()
        if config_id and config_id in config_ids and config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)
    for config_id in config_ids:
        if config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)

    return {
        "active_config_id": active_config_id,
        "fallback_order": fallback_order,
        "configs": normalized_configs,
    }


def _normalize_service_setting_config_payload(
    payload: dict[str, Any],
    *,
    service_name: str,
    fallback_label: str,
) -> dict[str, Any]:
    normalized_base_url = str(payload.get("base_url") or "").strip()
    normalized_provider = normalize_provider_kind(
        payload.get("provider_kind") or ("openai-compatible" if normalized_base_url else "openai")
    )
    if normalized_provider == "openai-compatible" and not normalized_base_url:
        raise HTTPException(status_code=400, detail="自定义 OpenAI Compatible 服务必须填写 Base URL。")

    available_models: list[str] = []
    seen_models: set[str] = set()
    for item in payload.get("available_models") or []:
        model_name = str(item or "").strip()
        if model_name and model_name not in seen_models:
            available_models.append(model_name)
            seen_models.add(model_name)

    return {
        "id": str(payload.get("id") or "").strip() or re.sub(r"[^a-z0-9]+", "-", f"{service_name}-{time.time_ns()}").strip("-"),
        "label": str(payload.get("label") or "").strip() or fallback_label,
        "provider_kind": normalized_provider,
        "base_url": normalized_base_url,
        "api_key": str(payload.get("api_key") or "").strip(),
        "model": str(payload.get("model") or "").strip(),
        "api_mode": normalize_api_mode(payload.get("api_mode") if service_name == "chat" else "responses"),
        "available_models": available_models,
    }


def _is_service_setting_config_usable(payload: dict[str, Any]) -> bool:
    api_key = str(payload.get("api_key") or "").strip()
    provider_kind = normalize_provider_kind(payload.get("provider_kind"))
    base_url = str(payload.get("base_url") or "").strip()
    if not api_key:
        return False
    if provider_kind == "openai-compatible" and not base_url:
        return False
    return True


def _normalize_asset_kind(value: str | None) -> str:
    candidate = (value or "cc_skill").strip().lower()
    if candidate == "skill":
        return "cc_skill"
    return candidate if candidate in ASSET_KINDS else "cc_skill"


def _asset_label(asset_kind: str) -> str:
    if asset_kind == "writing_guide":
        return "Writing Guide"
    if asset_kind == "profile_report":
        return "用户画像报告"
    return "Claude Code Skill"


def _enqueue_analysis(
    request: Request,
    session: Session,
    project_id: str,
    *,
    target_role: str | None,
    target_user_query: str | None = None,
    participant_id: str | None = None,
    analysis_context: str | None,
    concurrency: int | None = None,
) -> AnalysisRun:
    project = _ensure_project(session, project_id)
    documents = repository.list_project_documents(session, project_id)
    ready_documents = [document for document in documents if document.ingest_status == "ready"]
    if not ready_documents:
        raise HTTPException(status_code=400, detail="请先完成至少一份文档的解析处理。")
    if project.mode == "telegram":
        latest_successful_preprocess_run = repository.get_latest_successful_telegram_preprocess_run(session, project_id)
        if latest_successful_preprocess_run is None:
            raise HTTPException(status_code=400, detail="Telegram 项目必须先完成预处理后才能开始分析。")
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
        target_role=(target_role or "").strip() or None if project.mode != "telegram" else None,
        target_user_query=(target_user_query or "").strip() or None,
        participant_id=(participant_id or "").strip() or None,
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
    if phase == "document_profiling":
        return "逐篇文章预分析中"
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
    if asset_kind == "writing_guide":
        return "Writing Guide"
    if asset_kind == "profile_report":
        return "用户画像报告"
    return "Claude Code Skill"


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
