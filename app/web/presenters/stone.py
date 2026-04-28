from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.analysis.facets import get_facets_for_mode
from app.analysis.stone_v3 import (
    STONE_V3_PROFILE_KEY,
    is_valid_stone_v3_asset_payload,
    normalize_stone_profile_v3,
)
from app.models import DocumentRecord
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.stone_runtime import (
    get_latest_usable_stone_preprocess_run,
    has_valid_asset_payload as shared_has_valid_asset_payload,
)
from app.storage import repository
from app.web.presenters.projects import stone_preprocess_context
from app.web.ui_strings import page_strings

def _writing_workspace_ui(locale: str) -> dict[str, Any]:
    base = page_strings("preprocess", locale)
    labels = {
        "title": "Writing Workspace" if locale == "en-US" else "写作台",
        "eyebrow": "Writing Workspace" if locale == "en-US" else "Stone 写作台",
        "hero_note": (
            "Write against the live Stone v3 baseline with tighter evidence routing, quieter controls, and reusable session memory."
            if locale == "en-US"
            else "直接贴着 Stone v3 基线写作：原型证据、整体 critic 和会话主题都会在这里收束。"
        ),
        "new_session": "New Session" if locale == "en-US" else "新建会话",
        "rename_session": "Rename" if locale == "en-US" else "重命名",
        "delete_session": "Delete Session" if locale == "en-US" else "删除会话",
        "sessions": "Sessions" if locale == "en-US" else "任务会话",
        "toggle_sessions": "Sessions" if locale == "en-US" else "切换会话",
        "channel_live": "Execution Panel" if locale == "en-US" else "执行面板",
        "pinned_label": "Pinned Baseline" if locale == "en-US" else "基线状态",
        "composer_placeholder": (
            "Write something like: Write about a rainy station, restrained tone, keep it quiet"
            if locale == "en-US"
            else "例如：写雨夜车站，克制一点，别太抒情"
        ),
        "message_hint": (
            "Type the first instruction freely. The system will infer scale and auto-title the session from that opening prompt."
            if locale == "en-US"
            else "直接输入第一句创作要求就行，系统会估算篇幅，并自动给当前话题起个简短标题。"
        ),
        "message_parse_error": (
            "Please enter a writing topic."
            if locale == "en-US"
            else "请输入创作主题。"
        ),
        "send": "Run" if locale == "en-US" else "运行",
        "sending": "Running..." if locale == "en-US" else "执行中...",
        "baseline_label": "Baseline" if locale == "en-US" else "当前基线",
        "baseline_ready": "Using latest Stone v3 baseline." if locale == "en-US" else "当前使用最新 Stone v3 基线。",
        "baseline_requires_rebuild": (
            "Only legacy Stone v2 data exists. Re-run Stone preprocess to rebuild the v3 baseline."
            if locale == "en-US"
            else "当前只有旧版 Stone v2 数据，请重新运行 Stone 预处理以重建 v3 基线。"
        ),
        "baseline_missing_preprocess": "Run Stone preprocess first." if locale == "en-US" else "请先完成 Stone 预处理。",
        "baseline_running_preprocess": "Stone preprocess is still running." if locale == "en-US" else "Stone 预处理仍在运行中。",
        "baseline_missing_profiles": "No Stone v3 article profiles yet." if locale == "en-US" else "当前还没有 Stone v3 逐篇画像。",
        "baseline_missing_analysis": "Run Stone analysis before writing." if locale == "en-US" else "请先完成 Stone 分析，再进入写作。",
        "baseline_analysis_incomplete": "Stone analysis is incomplete. Writing can run only in degraded mode." if locale == "en-US" else "Stone 分析还不完整，当前只具备降级写作条件。",
        "baseline_incomplete_baseline": "Stone v3 baseline assets are incomplete." if locale == "en-US" else "Stone v3 基线资产还不完整。",
        "empty_turns": "No writing tasks yet." if locale == "en-US" else "还没有写作记录，先发第一句要求开始。",
        "working": "Working..." if locale == "en-US" else "处理中...",
        "untitled_session": "Waiting for Topic" if locale == "en-US" else "等待主题",
        "rename_prompt": "Enter a new session title" if locale == "en-US" else "输入新的会话标题",
        "execution_failed": "Writing failed" if locale == "en-US" else "写作失败",
        "connection_interrupted": "Connection interrupted" if locale == "en-US" else "连接中断",
        "you_label": "You" if locale == "en-US" else "你",
        "agent_label": "Writing Agent" if locale == "en-US" else "写作 Agent",
    }
    base.update(labels)
    return base


def _resolve_stone_writing_status(session: Session, project_id: str) -> dict[str, Any]:
    author_model_v3_available = shared_has_valid_asset_payload(
        session,
        project_id,
        asset_kind="stone_author_model_v3",
        validator=is_valid_stone_v3_asset_payload,
    )
    prototype_index_v3_available = shared_has_valid_asset_payload(
        session,
        project_id,
        asset_kind="stone_prototype_index_v3",
        validator=is_valid_stone_v3_asset_payload,
    )
    documents = repository.list_project_documents(session, project_id)
    profile_count_v3 = sum(
        1 for document in documents if isinstance(dict(document.metadata_json or {}).get(STONE_V3_PROFILE_KEY), dict)
    )
    latest_analysis_run = repository.get_latest_analysis_run(session, project_id, load_facets=True, load_events=False)
    required_analysis_facet_keys = [facet.key for facet in get_facets_for_mode("stone")]
    latest_analysis_facet_keys = [
        str(facet.facet_key or "").strip()
        for facet in list(latest_analysis_run.facets or [])
        if str(facet.facet_key or "").strip()
    ] if latest_analysis_run else []
    analysis_ready = bool(
        latest_analysis_run
        and latest_analysis_run.status == "completed"
        and all(key in latest_analysis_facet_keys for key in required_analysis_facet_keys)
    )
    writing_packet_ready = bool(
        profile_count_v3 > 0 and analysis_ready and author_model_v3_available and prototype_index_v3_available
    )
    legacy_v2_detected = any(
        isinstance(dict(document.metadata_json or {}).get("stone_profile_v2"), dict)
        for document in documents
    ) or bool(
        repository.get_latest_asset_version(session, project_id, asset_kind="stone_author_model_v2")
        or repository.get_latest_asset_draft(session, project_id, asset_kind="stone_author_model_v2")
        or repository.get_latest_asset_version(session, project_id, asset_kind="stone_prototype_index_v2")
        or repository.get_latest_asset_draft(session, project_id, asset_kind="stone_prototype_index_v2")
    )
    active_preprocess = repository.get_active_stone_preprocess_run(session, project_id)
    preprocess_run = get_latest_usable_stone_preprocess_run(
        session,
        project_id,
        profile_key=STONE_V3_PROFILE_KEY,
    )
    if legacy_v2_detected:
        status = "requires_rebuild"
    elif profile_count_v3 <= 0:
        status = "running_preprocess" if active_preprocess else ("missing_profiles" if preprocess_run else "missing_preprocess")
    elif not latest_analysis_run:
        status = "missing_analysis"
    elif not analysis_ready:
        status = "analysis_incomplete"
    elif not writing_packet_ready:
        status = "incomplete_baseline"
    elif preprocess_run:
        status = "ready"
    else:
        status = "ready"
    return {
        "status": status,
        "run_id": (preprocess_run.id if preprocess_run else active_preprocess.id if active_preprocess else None),
        "label": (
            f"preprocess {preprocess_run.id[:8]}"
            if preprocess_run and status not in {"requires_rebuild", "missing_analysis", "analysis_incomplete"}
            else "requires_rebuild" if status == "requires_rebuild" else None
        ),
        "profile_count": profile_count_v3,
        "corpus_ready": profile_count_v3 > 0,
        "analysis_run_id": latest_analysis_run.id if latest_analysis_run else None,
        "analysis_run_status": latest_analysis_run.status if latest_analysis_run else None,
        "analysis_facet_keys": latest_analysis_facet_keys,
        "analysis_ready": analysis_ready,
        "writing_packet_ready": writing_packet_ready,
        "author_model_ready": author_model_v3_available,
        "prototype_index_ready": prototype_index_v3_available,
        "author_model_v3_ready": author_model_v3_available,
        "prototype_index_v3_ready": prototype_index_v3_available,
        "profile_version": "v3" if profile_count_v3 > 0 else None,
        "baseline_version": "v3" if author_model_v3_available and prototype_index_v3_available else None,
        "rebuild_required": status == "requires_rebuild",
    }


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
        "stone_profile_failed": summary.get("stone_profile_failed", 0),
        "concurrency": summary.get("concurrency", DEFAULT_ANALYSIS_CONCURRENCY),
        "profile_version": summary.get("profile_version"),
        "baseline_version": summary.get("baseline_version"),
        "analysis_ready": bool(summary.get("analysis_ready")),
        "baseline_review_v3": summary.get("baseline_review_v3"),
        "stage_trace": list(summary.get("stage_trace") or []),
        "created_at": run.created_at.isoformat() + "Z",
    }


def _serialize_stone_profile_v3(profile: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(profile, dict):
        return None
    normalized = normalize_stone_profile_v3(profile)
    document_core = dict(normalized.get("document_core") or {})
    voice_contract = dict(normalized.get("voice_contract") or {})
    structure_moves = dict(normalized.get("structure_moves") or {})
    motifs = dict(normalized.get("motif_and_scene_bank") or {})
    value_and_judgment = dict(normalized.get("value_and_judgment") or {})
    anchors = dict(normalized.get("anchor_windows") or {})
    prototype_affordances = dict(normalized.get("prototype_affordances") or {})
    if not document_core.get("summary") and not anchors.get("opening") and not anchors.get("closing"):
        return None
    return {
        "length_band": document_core.get("length_band"),
        "content_kernel": document_core.get("summary"),
        "surface_form": document_core.get("surface_form"),
        "voice_mask": {
            "person": voice_contract.get("person"),
            "address_target": voice_contract.get("address_target"),
            "distance": voice_contract.get("distance"),
            "self_position": voice_contract.get("self_position"),
        },
        "lexicon_markers": list(motifs.get("lexicon_markers") or [])[:8],
        "syntax_signature": {
            "cadence": voice_contract.get("cadence"),
            "sentence_shape": voice_contract.get("sentence_shape"),
            "punctuation_habits": [],
        },
        "segment_map": list(filter(None, [structure_moves.get("opening_move"), structure_moves.get("development_move"), structure_moves.get("closure_move")]))[:4],
        "opening_move": structure_moves.get("opening_move"),
        "turning_move": structure_moves.get("turning_move"),
        "closure_move": structure_moves.get("closure_move"),
        "motif_tags": list(motifs.get("motif_tags") or [])[:4],
        "stance_vector": {
            "target": value_and_judgment.get("judgment_target"),
            "judgment": value_and_judgment.get("judgment_mode"),
            "value_lens": value_and_judgment.get("value_lens"),
        },
        "emotion_curve": [],
        "rhetorical_devices": list(motifs.get("scene_terms") or [])[:6],
        "prototype_family": prototype_affordances.get("prototype_family"),
        "anchor_spans": {
            "opening": anchors.get("opening"),
            "pivot": anchors.get("pivot"),
            "closing": anchors.get("closing"),
            "signature": list(anchors.get("signature_lines") or [])[:3],
        },
        "anti_patterns": list(normalized.get("anti_patterns") or [])[:6],
    }


def _serialize_stone_preprocess_documents(
    documents: list[DocumentRecord],
    run: "StonePreprocessRun | None" = None,
) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for index, document in enumerate(documents, start=1):
        metadata = dict(document.metadata_json or {})
        stone_profile_v3 = _serialize_stone_profile_v3(metadata.get(STONE_V3_PROFILE_KEY))
        serialized.append(
            {
                "id": document.id,
                "title": document.title or document.filename,
                "filename": document.filename,
                "document_index": index,
                "ingest_status": document.ingest_status,
                "lamp_status": _stone_document_status(index, run, stone_profile_v3 is not None),
                "has_profile": stone_profile_v3 is not None,
                "stone_profile_v3": stone_profile_v3,
                "profile_version": "v3" if stone_profile_v3 is not None else None,
                "updated_at": document.updated_at.isoformat() if getattr(document, "updated_at", None) else None,
                "profile_preview": (
                    (stone_profile_v3 or {}).get("content_kernel")
                    or (((stone_profile_v3 or {}).get("anchor_spans") or {}).get("opening"))
                    or ""
                ),
            }
        )
    return serialized


def _serialize_stone_preprocess_detail(
    session: Session,
    project_id: str,
    run: "StonePreprocessRun",
) -> dict[str, Any]:
    documents = repository.list_project_documents(session, project_id)
    return {
        **_serialize_stone_preprocess_run(run),
        "documents": _serialize_stone_preprocess_documents(documents, run),
    }

resolve_stone_writing_status = _resolve_stone_writing_status
stone_writing_workspace_ui = _writing_workspace_ui
serialize_stone_preprocess_run = _serialize_stone_preprocess_run
serialize_stone_preprocess_documents = _serialize_stone_preprocess_documents
serialize_stone_preprocess_detail = _serialize_stone_preprocess_detail

__all__ = [
    "_writing_workspace_ui",
    "_resolve_stone_writing_status",
    "_serialize_stone_preprocess_run",
    "_serialize_stone_profile_v3",
    "_serialize_stone_preprocess_documents",
    "_serialize_stone_preprocess_detail",
    "resolve_stone_writing_status",
    "stone_preprocess_context",
    "stone_writing_workspace_ui",
    "serialize_stone_preprocess_run",
    "serialize_stone_preprocess_documents",
    "serialize_stone_preprocess_detail",
]
