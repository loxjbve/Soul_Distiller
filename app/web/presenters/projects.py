from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.storage import repository
from app.web import assets as web_assets
from app.web import runtime as web_runtime
from app.web.presenters import analysis as analysis_presenter
from app.web.presenters import chat as chat_presenter
from app.web.presenters import telegram as telegram_presenter
from app.web.ui_strings import DEFAULT_LOCALE, page_strings

_ensure_project = web_runtime._ensure_project
get_locale = web_runtime.get_locale
_primary_asset_kind_for_mode = web_runtime._primary_asset_kind_for_mode
_recover_stone_preprocess_run_if_stale = web_runtime._recover_stone_preprocess_run_if_stale
_page_context = web_runtime._page_context
_stone_mode_label = web_runtime._stone_mode_label
_stone_mode_hint = web_runtime._stone_mode_hint
_resolve_stone_channel_title = web_runtime._resolve_stone_channel_title
_normalize_writing_topic = web_runtime._normalize_writing_topic
_infer_writing_target_word_count = web_runtime._infer_writing_target_word_count
_resolve_writing_request_payload = web_runtime._resolve_writing_request_payload
_ok_response = web_runtime._ok_response
_task_response = web_runtime._task_response
_chat_with_persona = web_runtime._chat_with_persona
_resolve_run = web_runtime._resolve_run
_mark_run_as_stale = web_runtime._mark_run_as_stale
_normalize_asset_kind = web_runtime._normalize_asset_kind
_asset_options_for_project = web_runtime._asset_options_for_project
_resolve_asset_kind_for_project = web_runtime._resolve_asset_kind_for_project
_delete_document_with_file = web_runtime._delete_document_with_file
_delete_project_resources = web_runtime._delete_project_resources
_schedule_project_deletion = web_runtime._schedule_project_deletion
_format_sse = web_runtime._format_sse
_normalize_service_setting_bundle_payload = web_runtime._normalize_service_setting_bundle_payload
_normalize_service_setting_config_payload = web_runtime._normalize_service_setting_config_payload
_is_service_setting_config_usable = web_runtime._is_service_setting_config_usable
_enqueue_analysis = web_runtime._enqueue_analysis
_get_project_document = web_runtime._get_project_document
_analysis_stage_label = web_runtime._analysis_stage_label

_serialize_document = chat_presenter.serialize_document
_serialize_chat_session = chat_presenter.serialize_chat_session
_serialize_writing_session_detail = chat_presenter.serialize_writing_session_detail
_serialize_analysis_run = analysis_presenter.serialize_analysis_run
_serialize_telegram_preprocess_run = telegram_presenter.serialize_telegram_preprocess_run
_serialize_telegram_preprocess_top_user = telegram_presenter.serialize_telegram_preprocess_top_user
_serialize_telegram_preprocess_weekly_candidate = telegram_presenter.serialize_telegram_preprocess_weekly_candidate
_serialize_telegram_preprocess_topic = telegram_presenter.serialize_telegram_preprocess_topic
_serialize_telegram_preprocess_active_user = telegram_presenter.serialize_telegram_preprocess_active_user
_serialize_telegram_relationship_bundle = telegram_presenter.serialize_telegram_relationship_bundle
_serialize_telegram_relationship_snapshot = telegram_presenter.serialize_telegram_relationship_snapshot
_persist_asset_files = web_assets._persist_asset_files
_delete_asset_files = web_assets._delete_asset_files
_download_headers = web_assets._download_headers
_markdown_download_response = web_assets._markdown_download_response
_skill_documents_for_export = web_assets._skill_documents_for_export
_build_skill_export_zip = web_assets._build_skill_export_zip
_normalize_saved_asset_content = web_assets._normalize_saved_asset_content
_asset_label = web_assets.asset_label
_generate_asset_draft = web_assets.generate_asset_draft


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
    from app.web.presenters.chat import serialize_document as _serialize_document
    from app.web.presenters.stone import resolve_stone_writing_status as _resolve_stone_writing_status
    from app.web.presenters.telegram import (
        serialize_telegram_preprocess_active_user as _serialize_telegram_preprocess_active_user,
        serialize_telegram_preprocess_top_user as _serialize_telegram_preprocess_top_user,
        serialize_telegram_relationship_bundle as _serialize_telegram_relationship_bundle,
    )

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
    stone_writing_status = _resolve_stone_writing_status(session, project_id) if project.mode == "stone" else None
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
                "stone_writing_status": stone_writing_status,
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
        "primary_asset_kind": primary_asset_kind,
        "can_analyze": can_analyze,
        "latest_draft": latest_draft,
        "latest_version": latest_version,
        "stone_writing_status": stone_writing_status,
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
    request: Request,
    session: Session,
    project_id: str,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    from app.web.presenters.stone import (
        serialize_stone_preprocess_detail as _serialize_stone_preprocess_detail,
        serialize_stone_preprocess_documents as _serialize_stone_preprocess_documents,
        serialize_stone_preprocess_run as _serialize_stone_preprocess_run,
    )

    project = _ensure_project(session, project_id)
    runs = repository.list_stone_preprocess_runs(session, project_id, limit=24)
    active_run = repository.get_active_stone_preprocess_run(session, project_id)
    if active_run:
        active_run = _recover_stone_preprocess_run_if_stale(request, session, project_id, active_run)
    selected_run = None
    if run_id:
        selected_run = repository.get_stone_preprocess_run(session, run_id)
        if selected_run and selected_run.project_id != project_id:
            selected_run = None
    if not selected_run and active_run:
        selected_run = active_run
    if not selected_run:
        selected_run = repository.get_latest_successful_stone_preprocess_run(session, project_id)
    if not selected_run and runs:
        selected_run = runs[0]

    documents = repository.list_project_documents(session, project_id)
    doc_counts = repository.count_project_documents(session, project_id)
    serialized_documents = _serialize_stone_preprocess_documents(documents, selected_run)
    serialized_run = _serialize_stone_preprocess_detail(session, project_id, selected_run) if selected_run else None

    # Check if a run is already active
    can_start = doc_counts["ready"] > 0 and not active_run

    ui_strings = page_strings("preprocess", "zh-CN")

    return {
        "project": project,
        "runs": [_serialize_stone_preprocess_run(item) for item in runs],
        "selected_run_data": serialized_run,
        "documents": serialized_documents,
        "can_start": can_start,
        "stone_preprocess_bootstrap": json.dumps(
            {
                "project_id": project.id,
                "run_id": selected_run.id if selected_run else None,
                "initial_run": serialized_run,
                "initial_documents": serialized_documents,
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
    from app.web.presenters.telegram import (
        serialize_telegram_preprocess_detail as _serialize_telegram_preprocess_detail,
        serialize_telegram_preprocess_run as _serialize_telegram_preprocess_run,
        serialize_telegram_preprocess_topic as _serialize_telegram_preprocess_topic,
    )

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
    concurrency: int = DEFAULT_ANALYSIS_CONCURRENCY,
) -> "StonePreprocessRun":
    project = _ensure_project(session, project_id)
    if project.mode != "stone":
        raise HTTPException(status_code=400, detail="Only Stone projects use preprocess runs.")
    created = request.app.state.stone_preprocess_worker.submit(
        project_id,
        concurrency=concurrency,
    )
    session.expire_all()
    refreshed = repository.get_stone_preprocess_run(session, created.id)
    return refreshed or created


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

page_context = _page_context
project_context = _project_context
telegram_preprocess_context = _telegram_preprocess_context
stone_preprocess_context = _stone_preprocess_context

__all__ = [
    "_extract_telegram_binding",
    "_enrich_telegram_binding",
    "_project_context",
    "_stone_preprocess_context",
    "_telegram_preprocess_context",
    "_create_stone_preprocess_run",
    "_create_telegram_preprocess_run",
    "_resolve_telegram_preprocess_run",
    "_resolve_telegram_relationship_snapshot",
    "page_context",
    "project_context",
    "telegram_preprocess_context",
    "stone_preprocess_context",
]
