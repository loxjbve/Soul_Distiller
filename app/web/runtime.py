from __future__ import annotations

import io
import json
import logging
import re
import shutil
import time
import zipfile
from queue import Empty
from pathlib import Path
from typing import Annotated, Any, Callable
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
import asyncio
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.errors import LegacyStoneDataError
from app.analysis.stone_v3 import (
    STONE_V3_ASSET_KINDS,
    STONE_V3_PROFILE_KEY,
    is_valid_stone_v3_asset_payload,
    normalize_stone_profile_v3,
    render_stone_author_model_v3_markdown,
    render_stone_prototype_index_v3_markdown,
    validate_stone_v3_asset_payload,
)
from app.analysis.facets import FACETS, get_facets_for_mode
from app.llm.client import OpenAICompatibleClient, normalize_api_mode, normalize_provider_kind
from app.models import (
    AnalysisFacet,
    AnalysisRun,
    DocumentRecord,
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
from app.stone_runtime import (
    get_latest_usable_stone_preprocess_run,
    has_valid_asset_payload as shared_has_valid_asset_payload,
)
from app.stone_v3_checkpoint import (
    clear_stone_v3_checkpoint,
    load_stone_v3_checkpoint,
    save_stone_v3_checkpoint,
)
from app.storage import repository
from app.web.ui_strings import DEFAULT_LOCALE, page_strings


router = APIRouter()
logger = logging.getLogger(__name__)
ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS = 120.0
ASSET_STREAM_QUEUE_POLL_SECONDS = 5.0
STONE_V2_ASSET_KINDS = frozenset({"stone_author_model_v2", "stone_prototype_index_v2"})
LEGACY_STONE_V2_REBUILD_MESSAGE = "Stone v2 已停用，请重新运行 Stone 预处理并重建 Stone v3 基线。"

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
    {"value": "stone_author_model_v3", "label": "Stone Author Model V3"},
    {"value": "stone_prototype_index_v3", "label": "Stone Prototype Index V3"},
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
    {"value": "stone_author_model_v3", "label": "Stone Author Model V3"},
    {"value": "stone_prototype_index_v3", "label": "Stone Prototype Index V3"},
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


class WritingMessagePayload(BaseModel):
    message: str | None = None
    topic: str | None = None
    target_word_count: int | None = Field(default=None, ge=100)
    extra_requirements: str | None = None
    max_concurrency: int | None = Field(default=None, ge=1, le=8)


class StoneWritingSettingsPayload(BaseModel):
    max_concurrency: int = Field(default=4, ge=1, le=8)


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
    return "Stone Mode" if locale == "en-US" else "鎼煶妯″紡"






def _ensure_stone_project(session: Session, project_id: str):
    project = _ensure_project(session, project_id)
    if project.mode != "stone":
        raise HTTPException(status_code=400, detail="Only stone projects support this workspace.")
    return project




def _stone_mode_hint(locale: str) -> str:
    if locale == "en-US":
        return "For single-author corpus, article profiling, multi-facet analysis, and analysis-driven drafting"
    return "适合单作者语料、逐篇画像、多维分析与分析驱动写作"


def _raise_legacy_stone_v2_http_error() -> None:
    raise HTTPException(status_code=400, detail=str(LegacyStoneDataError(LEGACY_STONE_V2_REBUILD_MESSAGE)))


def _resolve_stone_channel_title(session: Session, project) -> str:
    run = repository.get_latest_analysis_run(session, project.id, load_facets=False, load_events=False)
    summary = dict(getattr(run, "summary_json", None) or {})
    owner_name = str(summary.get("target_role") or "").strip() or str(project.name or "").strip() or "Stone"
    return f"{owner_name}的 Stone 写作工作区"


_WRITING_MESSAGE_COUNT_PATTERN = re.compile(r"(?P<count>\d+)\s*(?P<unit>字|words)\b", re.IGNORECASE)
_WRITING_TOPIC_PREFIX_PATTERN = re.compile(
    r"^(?:请|请帮我|麻烦)?\s*(?:写(?:一篇|个)?|来(?:一篇|个)?|draft|write(?:\s+me)?(?:\s+about)?)\s*",
    re.IGNORECASE,
)


def _normalize_writing_topic(topic_text: str) -> str:
    cleaned = _WRITING_TOPIC_PREFIX_PATTERN.sub("", str(topic_text or "").strip())
    cleaned = re.sub(r"^(?:关于|围绕|以)\s*", "", cleaned)
    return cleaned.strip(" ，。;:!?\"'“”‘’[]()（）")


def _infer_writing_target_word_count(text: str, *, explicit_target: int | None = None) -> int:
    if explicit_target is not None and int(explicit_target) >= 100:
        return int(explicit_target)
    raw = str(text or "").strip()
    match = _WRITING_MESSAGE_COUNT_PATTERN.search(raw)
    if match:
        count = int(match.group("count"))
        if count >= 100:
            return count
    lowered = raw.lower()
    short_signals = ("短一点", "简短", "几句", "片段", "微博", "便签", "一句", "简洁")
    long_signals = ("长一点", "展开", "详细", "完整", "长文", "深写", "多写一点")
    medium_signals = ("中等", "适中", "中篇")
    if any(token in raw for token in short_signals) or any(token in lowered for token in ("brief", "short", "concise")):
        return 260
    if any(token in raw for token in long_signals) or any(token in lowered for token in ("long", "detailed", "full")):
        return 1200
    if any(token in raw for token in medium_signals):
        return 700
    return 800


def _resolve_writing_request_payload(payload: WritingMessagePayload) -> dict[str, Any]:
    raw_message = str(payload.message or "").strip() or None
    if raw_message:
        explicit_in_message = bool(_WRITING_MESSAGE_COUNT_PATTERN.search(raw_message))
        return {
            "message": raw_message,
            "topic": raw_message,
            "target_word_count": _infer_writing_target_word_count(
                raw_message,
                explicit_target=payload.target_word_count,
            ),
            "extra_requirements": str(payload.extra_requirements or "").strip() or None,
            "max_concurrency": payload.max_concurrency,
            "target_word_count_source": "explicit" if explicit_in_message or payload.target_word_count is not None else "inferred",
        }

    topic = str(payload.topic or "").strip()
    if not topic:
        raise ValueError("请输入创作主题。")
    return {
        "message": None,
        "topic": topic,
        "target_word_count": _infer_writing_target_word_count(topic, explicit_target=payload.target_word_count),
        "extra_requirements": str(payload.extra_requirements or "").strip() or None,
        "max_concurrency": payload.max_concurrency,
        "target_word_count_source": "explicit" if payload.target_word_count is not None else "inferred",
    }


def _resolve_project_stone_writing_settings(project) -> dict[str, Any]:
    return repository.get_project_stone_writing_settings(project)


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
            actual_name = "鏈懡鍚?Telegram 椤圭洰"
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
    if draft.asset_kind in STONE_V3_ASSET_KINDS:
        try:
            validate_stone_v3_asset_payload(draft.asset_kind, draft.json_payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    elif draft.asset_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
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
    run_id: str | None = Query(default=None),
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
        stone_context = _stone_preprocess_context(request, session, project_id, run_id=run_id)
        return templates.TemplateResponse(
            request=request,
            name="stone_preprocess.html",
            context=_page_context(request, "preprocess", **stone_context),
        )
    raise HTTPException(status_code=404, detail="Preprocess workspace has been removed for this project mode.")


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
                title=None,
            )
        ]
    selected_session = sessions[0]
    if session_id:
        explicit = repository.get_chat_session(session, session_id, session_kind="writing")
        if explicit and explicit.project_id == project_id:
            selected_session = explicit
    locale = get_locale(request)
    writing_ui = _writing_workspace_ui(locale)
    channel_title = _resolve_stone_channel_title(session, project)
    bootstrap = {
        "project": {"id": project.id, "name": project.name, "mode": project.mode},
        "sessions": [_serialize_chat_session(item) for item in sessions],
        "selected_session_id": selected_session.id,
        "selected_session": _serialize_writing_session_detail(selected_session),
        "baseline": _resolve_stone_writing_status(session, project_id),
        "writing_settings": _resolve_project_stone_writing_settings(project),
        "channel_title": channel_title,
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
            channel_title=channel_title,
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
            concurrency=concurrency or DEFAULT_ANALYSIS_CONCURRENCY,
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
                {"value": "openai-compatible", "label": "OpenAI Compatible 自定义入口"},
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
def get_preprocess_run_api(request: Request, project_id: str, run_id: str, session: SessionDep):
    project = _ensure_project(session, project_id)
    if project.mode == "telegram":
        run = _resolve_telegram_preprocess_run(session, project_id, run_id)
        return _ok_response("已返回 Telegram 预处理详情。", **_serialize_telegram_preprocess_detail(session, project_id, run))
    elif project.mode == "stone":
        run = repository.get_stone_preprocess_run(session, run_id)
        if not run or run.project_id != project_id:
            raise HTTPException(status_code=404, detail="Run not found.")
        run = _recover_stone_preprocess_run_if_stale(request, session, project_id, run)
        return _ok_response("已返回 Stone 预分析详情。", **_serialize_stone_preprocess_detail(session, project_id, run))
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
            with request.app.state.db.session() as live_session:
                live_run = repository.get_stone_preprocess_run(live_session, run_id)
                if live_run:
                    initial_payload = _serialize_stone_preprocess_detail(live_session, project_id, live_run)
                    yield _format_sse("snapshot", initial_payload)
                    if live_run.status not in {"queued", "running"}:
                        yield _format_sse("done", {"run_id": live_run.id, "status": live_run.status})
                        return
            async for chunk in hub.stream_events(run.id):
                yield chunk
                
                # Fetch payload to send snapshot periodically
                with request.app.state.db.session() as live_session:
                    live_run = repository.get_stone_preprocess_run(live_session, run_id)
                    if live_run:
                        payload = _serialize_stone_preprocess_detail(live_session, project_id, live_run)
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
    return _ok_response("已返回 Telegram 话题列表。", topics=[_serialize_telegram_preprocess_topic(item) for item in topics])


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/weekly-candidates")
def list_telegram_preprocess_weekly_candidates_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    candidates = repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=run_id)
    return _ok_response(
        "已返回 Telegram 周话题候选列表。",
        weekly_candidates=[_serialize_telegram_preprocess_weekly_candidate(item) for item in candidates],
    )


@router.get("/api/projects/{project_id}/preprocess/runs/{run_id}/top-users")
def list_telegram_preprocess_top_users_api(project_id: str, run_id: str, session: SessionDep):
    _resolve_telegram_preprocess_run(session, project_id, run_id)
    users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=run_id)
    return _ok_response(
        "已返回 Telegram SQL Top Users 列表。",
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
    from threading import Event, Lock, Thread

    events: Queue[dict[str, Any] | None] = Queue()
    cancel_event = Event()
    worker_finished = Event()
    activity_lock = Lock()
    last_activity_at = time.monotonic()
    asset_kind = _normalize_asset_kind(payload.asset_kind)
    if asset_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
    default_document_key = "skill" if asset_kind == "cc_skill" else "asset"

    def touch_activity() -> None:
        nonlocal last_activity_at
        with activity_lock:
            last_activity_at = time.monotonic()

    def seconds_since_activity() -> float:
        with activity_lock:
            return max(0.0, time.monotonic() - last_activity_at)

    def emit_event(item: dict[str, Any]) -> None:
        if cancel_event.is_set() and item.get("type") not in {"error", "done"}:
            return
        touch_activity()
        event_type = str(item.get("type") or "status")
        if event_type == "status":
            logger.info(
                "Asset stream status for project %s asset %s: phase=%s stage=%s progress=%s message=%s",
                project_id,
                asset_kind,
                item.get("phase"),
                item.get("stage"),
                item.get("progress_percent"),
                item.get("message"),
            )
        elif event_type == "error":
            logger.warning(
                "Asset stream error for project %s asset %s: %s",
                project_id,
                asset_kind,
                item.get("message"),
            )
        events.put(item)

    def emit_status(
        phase: str,
        progress_percent: int,
        message: str,
        *,
        status: str = "running",
        document_key: str | None = None,
        stage: str | None = None,
        attempt: int | None = None,
        batch_index: int | None = None,
        batch_total: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        payload = {
            "type": "status",
            "status": status,
            "phase": phase,
            "stage": stage or phase,
            "progress_percent": progress_percent,
            "message": message,
            "asset_kind": asset_kind,
            "document_key": document_key,
        }
        if attempt is not None:
            payload["attempt"] = attempt
        if batch_index is not None:
            payload["batch_index"] = batch_index
        if batch_total is not None:
            payload["batch_total"] = batch_total
        if failure_reason:
            payload["failure_reason"] = failure_reason
        emit_event(payload)

    def emit_error(message: str, *, detail: Any | None = None, status_code: int | None = None) -> None:
        emit_event(
            {
                "type": "error",
                "message": message,
                "detail": detail,
                "status_code": status_code,
            }
        )

    def worker():
        try:
            emit_status("prepare", 6, f"Preparing {_asset_label(asset_kind)} draft.", document_key=default_document_key)
            with request.app.state.db.session() as session:
                project = _ensure_project(session, project_id)
                if project.mode == "stone" and asset_kind in {*STONE_V2_ASSET_KINDS, *STONE_V3_ASSET_KINDS}:
                    baseline_label = "Stone v3" if asset_kind in STONE_V3_ASSET_KINDS else "Stone v2"
                    emit_status("load", 28, f"Loading the latest {baseline_label} preprocess output.", document_key=default_document_key)

                    def stone_progress_callback(progress: dict[str, Any]) -> None:
                        emit_status(
                            str(progress.get("phase") or "running"),
                            int(progress.get("progress_percent", 0) or 0),
                            str(progress.get("message") or ""),
                            status=str(progress.get("status") or "running"),
                            document_key=default_document_key,
                            stage=str(progress.get("stage") or progress.get("phase") or "running"),
                            attempt=(int(progress["attempt"]) if progress.get("attempt") is not None else None),
                            batch_index=(int(progress["batch_index"]) if progress.get("batch_index") is not None else None),
                            batch_total=(int(progress["batch_total"]) if progress.get("batch_total") is not None else None),
                            failure_reason=str(progress.get("failure_reason") or "") or None,
                        )

                    draft = _generate_asset_draft(
                        request,
                        session,
                        project_id,
                        asset_kind=asset_kind,
                        progress_callback=stone_progress_callback if asset_kind in STONE_V3_ASSET_KINDS else None,
                        cancel_requested=cancel_event.is_set if asset_kind in STONE_V3_ASSET_KINDS else None,
                    )
                    emit_status("persist", 98, f"Persisting {baseline_label} baseline assets.", document_key=default_document_key)
                    emit_event(
                        {
                            "type": "done",
                            "status": "completed",
                            "phase": "done",
                            "stage": "done",
                            "progress_percent": 100,
                            "message": "Draft generation completed and the editor has been hydrated.",
                            "draft_id": draft.id,
                            "draft": _serialize_draft(draft),
                            "asset_kind": asset_kind,
                            "document_key": default_document_key,
                        }
                    )
                    return
                run = repository.get_latest_analysis_run(session, project_id)
                if not run or run.status in {"queued", "running"}:
                    emit_error("Analysis is not ready yet.")
                    return
                facets = run.facets or []
                if not facets:
                    emit_error("The latest analysis has no facets to synthesize.")
                    return
                chat_config = repository.get_service_config(session, "chat_service")
                summary = run.summary_json or {}

                emit_status("load", 14, "Loading the latest analysis bundle.", document_key=default_document_key)

                def stream_callback(payload: Any):
                    if isinstance(payload, dict):
                        chunk = str(payload.get("chunk", "") or "")
                        document_key = str(payload.get("document_key") or "").strip() or default_document_key
                    else:
                        chunk = str(payload or "")
                        document_key = default_document_key
                    if not chunk:
                        return
                    emit_event({"type": "delta", "document_key": document_key, "chunk": chunk, "asset_kind": asset_kind})

                def progress_callback(progress: dict[str, Any]):
                    emit_status(
                        str(progress.get("phase") or "running"),
                        int(progress.get("progress_percent", 0) or 0),
                        str(progress.get("message", "") or ""),
                        status="running",
                        document_key=str(progress.get("document_key") or "").strip() or None,
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

                emit_status("persist", 94, "Persisting the generated draft files.", document_key=default_document_key)
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
                emit_event(
                    {
                        "type": "done",
                        "status": "completed",
                        "phase": "done",
                        "stage": "done",
                        "progress_percent": 100,
                        "message": "Draft generation completed and the editor has been hydrated.",
                        "draft_id": draft.id,
                        "draft": _serialize_draft(draft),
                        "asset_kind": asset_kind,
                        "document_key": default_document_key,
                    }
                )
        except HTTPException as exc:
            logger.warning(
                "Asset stream generation failed for project %s asset %s: %s",
                project_id,
                asset_kind,
                exc.detail,
            )
            detail = exc.detail.get("message") if isinstance(exc.detail, dict) else exc.detail
            emit_error(str(detail or exc), detail=exc.detail, status_code=exc.status_code)
        except Exception as exc:
            logger.exception(
                "Asset stream generation crashed for project %s asset %s",
                project_id,
                asset_kind,
            )
            emit_error(f"{type(exc).__name__}: {exc}")
        finally:
            worker_finished.set()
            events.put(None)

    Thread(target=worker, daemon=True).start()

    def generator():
        while True:
            try:
                item = events.get(timeout=ASSET_STREAM_QUEUE_POLL_SECONDS)
            except Empty:
                idle_for = seconds_since_activity()
                if idle_for >= ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS:
                    cancel_event.set()
                    timeout_message = (
                        f"Streaming asset generation timed out after "
                        f"{int(ASSET_STREAM_INACTIVITY_TIMEOUT_SECONDS)} seconds without any progress event."
                    )
                    logger.warning(
                        "Asset stream timed out for project %s asset %s after %.1f seconds of inactivity.",
                        project_id,
                        asset_kind,
                        idle_for,
                    )
                    yield f"event: error\ndata: {json.dumps({'message': timeout_message, 'status': 'failed', 'phase': 'timeout', 'stage': 'timeout', 'progress_percent': 0}, ensure_ascii=False)}\n\n"
                    break
                if worker_finished.is_set():
                    break
                continue
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
    normalized_kind = _normalize_asset_kind(payload.asset_kind)
    if normalized_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
    draft = _generate_asset_draft(request, session, project_id, asset_kind=normalized_kind)
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
    if draft.asset_kind in STONE_V3_ASSET_KINDS:
        try:
            validate_stone_v3_asset_payload(draft.asset_kind, draft.json_payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    elif draft.asset_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
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
        title=payload.title,
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
        request_payload = _resolve_writing_request_payload(payload)
        result = request.app.state.writing_service.start_stream(
            project_id=project_id,
            session_id=session_id,
            topic=request_payload["topic"],
            target_word_count=request_payload["target_word_count"],
            extra_requirements=request_payload["extra_requirements"],
            raw_message=request_payload["message"],
            target_word_count_source=request_payload["target_word_count_source"],
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
    if candidate in {"stone_author_model_v2", "stone_prototype_index_v2"}:
        return candidate
    return candidate if candidate in ASSET_KINDS else "cc_skill"




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
                "我会尽量按照已发布 Skill 里的语气与立场来回应。\n\n"
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


def _stone_profile_progress_stats(session: Session, project_id: str) -> dict[str, int]:
    documents = [
        document
        for document in repository.list_project_documents(session, project_id)
        if document.ingest_status == "ready"
    ]
    profile_count = 0
    for document in documents:
        metadata = dict(document.metadata_json or {})
        if isinstance(metadata.get(STONE_V3_PROFILE_KEY), dict):
            profile_count += 1
    return {
        "ready_document_count": len(documents),
        "profile_count": profile_count,
        "failed_count": max(len(documents) - profile_count, 0),
    }


def _stone_profiles_meet_analysis_threshold(ready_document_count: int, profile_count: int) -> bool:
    total = max(0, int(ready_document_count or 0))
    completed = max(0, int(profile_count or 0))
    if total <= 0:
        return False
    return completed * 2 > total


def _recover_stone_preprocess_run_if_stale(
    request: Request,
    session: Session,
    project_id: str,
    run: "StonePreprocessRun | None",
) -> "StonePreprocessRun | None":
    if not run:
        return None
    if str(run.status or "").lower() not in {"queued", "running"}:
        return run
    stone_worker = getattr(request.app.state, "stone_preprocess_worker", None)
    if stone_worker and hasattr(stone_worker, "is_tracking") and stone_worker.is_tracking(run.id):
        return run

    stats = _stone_profile_progress_stats(session, project_id)
    ready_count = int(stats["ready_document_count"] or 0)
    profile_count = int(stats["profile_count"] or 0)
    summary = dict(run.summary_json or {})
    summary["stone_profile_total"] = max(int(summary.get("stone_profile_total") or 0), ready_count)
    summary["stone_profile_completed"] = max(int(summary.get("stone_profile_completed") or 0), profile_count)
    summary["stone_profile_failed"] = max(
        int(summary.get("stone_profile_failed") or 0),
        max(ready_count - profile_count, 0),
    )
    summary["progress_percent"] = int((profile_count / ready_count) * 100) if ready_count > 0 else 0
    summary["current_stage"] = "partial_failed" if _stone_profiles_meet_analysis_threshold(ready_count, profile_count) else "failed"
    summary["analysis_ready"] = _stone_profiles_meet_analysis_threshold(ready_count, profile_count)
    run.summary_json = summary
    run.progress_percent = int(summary.get("progress_percent") or 0)
    run.finished_at = utcnow()
    if _stone_profiles_meet_analysis_threshold(ready_count, profile_count):
        run.status = "partial_failed"
        run.current_stage = "Partial failed"
        run.error_message = (
            f"Detected a stale Stone preprocess run without a live worker. "
            f"Profile coverage is still usable for analysis: {profile_count}/{ready_count}."
        )
    else:
        run.status = "failed"
        run.current_stage = "Failed"
        run.error_message = (
            f"Detected a stale Stone preprocess run without a live worker. "
            f"Profile coverage is below the analysis threshold: {profile_count}/{ready_count}."
        )
    session.commit()
    return run


def _stone_document_status(index: int, run: "StonePreprocessRun | None", has_profile: bool) -> str:
    if has_profile:
        return "completed"
    if not run:
        return "queued"
    status = str(run.status or "").lower()
    if status not in {"queued", "running", "failed", "partial_failed"}:
        return "queued"
    summary = dict(run.summary_json or {})
    completed = int(summary.get("stone_profile_completed") or 0)
    failed = int(summary.get("stone_profile_failed") or 0)
    concurrency = max(1, int(summary.get("concurrency") or DEFAULT_ANALYSIS_CONCURRENCY))
    if status in {"queued", "running"} and completed < index <= completed + concurrency:
        return "running"
    if status in {"failed", "partial_failed"} and completed < index <= completed + max(failed, 1):
        return "failed"
    return "queued"


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
    elif project.mode == "stone":
        stats = _stone_profile_progress_stats(session, project_id)
        if not _stone_profiles_meet_analysis_threshold(stats["ready_document_count"], stats["profile_count"]):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Stone 作者分析要求超过一半的已就绪文档拥有预处理画像。"
                    f"当前覆盖率：{stats['profile_count']}/{stats['ready_document_count']}。"
                ),
            )
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


def _primary_asset_kind_for_mode(mode: str | None) -> str:
    return "stone_author_model_v3" if str(mode or "").strip().lower() == "stone" else "cc_skill"


def _asset_options_for_project(project) -> tuple[dict[str, str], ...]:
    if str(project.mode or "").strip().lower() == "stone":
        return (
            {"value": "stone_author_model_v3", "label": "Stone Author Model V3"},
            {"value": "stone_prototype_index_v3", "label": "Stone Prototype Index V3"},
        )
    return (
        {"value": "cc_skill", "label": "Claude Code Skill"},
        {"value": "profile_report", "label": "用户画像报告"},
    )


def _resolve_asset_kind_for_project(project, requested_kind: str | None) -> str:
    default_kind = _primary_asset_kind_for_mode(project.mode)
    asset_kind = _normalize_asset_kind(requested_kind or default_kind)
    if str(project.mode or "").strip().lower() != "stone":
        return asset_kind
    if asset_kind in {
        "stone_author_model_v3",
        "stone_prototype_index_v3",
    }:
        return asset_kind
    return "stone_author_model_v3"

from app.web.assets import *
from app.web.presenters.analysis import *
from app.web.presenters.chat import *
from app.web.presenters.projects import *
from app.web.presenters.stone import *
from app.web.presenters.telegram import *

