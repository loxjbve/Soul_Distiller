from __future__ import annotations

import io
import json
import re
import zipfile
from typing import Any, Callable
from urllib.parse import quote

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.analysis.stone_v3 import (
    STONE_V3_ASSET_KINDS,
    STONE_V3_PROFILE_KEY,
    normalize_stone_profile_v3,
    render_stone_author_model_v3_markdown,
    render_stone_prototype_index_v3_markdown,
    validate_stone_v3_asset_payload,
)
from app.stone_runtime import get_latest_usable_stone_preprocess_run
from app.stone_v3_checkpoint import clear_stone_v3_checkpoint, load_stone_v3_checkpoint, save_stone_v3_checkpoint
from app.storage import repository

STONE_V2_ASSET_KINDS = frozenset({"stone_author_model_v2", "stone_prototype_index_v2"})
LEGACY_STONE_V2_REBUILD_MESSAGE = "Stone v2 已停用，请重新运行 Stone 预处理并重建 Stone v3 基线。"

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

def _raise_legacy_stone_v2_http_error() -> None:
    raise HTTPException(status_code=400, detail=LEGACY_STONE_V2_REBUILD_MESSAGE)


def _ensure_project(session: Session, project_id: str):
    project = repository.get_project(session, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="未找到项目。")
    return project

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
    if asset_kind in STONE_V3_ASSET_KINDS:
        try:
            validate_stone_v3_asset_payload(asset_kind, json_payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return dict(json_payload or {}), prompt_text
    if asset_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
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


def _asset_label(asset_kind: str) -> str:
    if asset_kind == "writing_guide":
        return "Writing Guide"
    if asset_kind == "stone_author_model_v3":
        return "Stone Author Model V3"
    if asset_kind == "stone_prototype_index_v3":
        return "Stone Prototype Index V3"
    if asset_kind == "stone_author_model_v2":
        return "Stone Author Model V2"
    if asset_kind == "stone_prototype_index_v2":
        return "Stone Prototype Index V2"
    if asset_kind == "profile_report":
        return "用户画像报告"
    return "Claude Code Skill"


def _load_stone_v3_profiles_and_documents(session: Session, project_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    profiles: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        if document.ingest_status != "ready":
            continue
        article_text = str(document.clean_text or document.raw_text or "")
        documents.append(
            {
                "document_id": document.id,
                "title": document.title or document.filename,
                "filename": document.filename,
                "source_type": document.source_type,
                "created_at_guess": document.created_at_guess,
                "text": article_text,
                "clean_text": document.clean_text,
                "raw_text": document.raw_text,
            }
        )
        profile = dict(document.metadata_json or {}).get(STONE_V3_PROFILE_KEY)
        if not isinstance(profile, dict):
            continue
        normalized = normalize_stone_profile_v3(
            profile,
            article_text=article_text,
            fallback_title=document.title or document.filename,
            document_id=document.id,
            source_meta={
                "created_at_guess": document.created_at_guess,
                "source_type": document.source_type,
            },
        )
        normalized["document_id"] = document.id
        normalized["title"] = document.title or document.filename
        profiles.append(normalized)
    return profiles, documents


def _generate_asset_draft(
    request: Request,
    session: Session,
    project_id: str,
    *,
    asset_kind: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
):
    project = _ensure_project(session, project_id)
    if project.mode == "stone" and asset_kind in STONE_V3_ASSET_KINDS:
        preprocess_run = get_latest_usable_stone_preprocess_run(
            session,
            project_id,
            profile_key=STONE_V3_PROFILE_KEY,
        )
        if not preprocess_run:
            raise HTTPException(status_code=400, detail="请先完成 Stone 预处理，再生成 Stone v3 基线资产。")
        profiles, documents = _load_stone_v3_profiles_and_documents(session, project_id)
        if not profiles:
            raise HTTPException(status_code=400, detail="当前没有可用的 stone_profile_v3。")
        chat_config = repository.get_service_config(session, "chat_service")
        if not chat_config:
            raise HTTPException(status_code=400, detail="Stone v3 baseline synthesis requires a configured chat model.")
        resume_checkpoint = load_stone_v3_checkpoint(request.app.state.config.assets_dir, project_id)

        def persist_checkpoint(payload: dict[str, Any]) -> None:
            save_stone_v3_checkpoint(request.app.state.config.assets_dir, project_id, payload)

        synthesis = request.app.state.stone_v3_synthesizer.build(
            project_name=project.name,
            profiles=profiles,
            documents=documents,
            config=chat_config,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
            checkpoint_callback=persist_checkpoint,
            resume_from=resume_checkpoint,
        )
        if cancel_requested and cancel_requested():
            raise TimeoutError("Stone v3 asset generation was cancelled after stream inactivity timeout.")
        if asset_kind == "stone_author_model_v3":
            payload = dict(synthesis.get("author_model") or {})
            markdown = render_stone_author_model_v3_markdown(payload)
        else:
            payload = dict(synthesis.get("prototype_index") or {})
            markdown = render_stone_prototype_index_v3_markdown(payload)
        try:
            validate_stone_v3_asset_payload(asset_kind, payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if cancel_requested and cancel_requested():
            raise TimeoutError("Stone v3 asset generation was cancelled after stream inactivity timeout.")
        draft = repository.create_asset_draft(
            session,
            project_id=project_id,
            run_id=preprocess_run.id,
            asset_kind=asset_kind,
            markdown_text=markdown,
            json_payload=payload,
            prompt_text=json.dumps(
                {
                    "asset_kind": asset_kind,
                    "baseline_version": "v3",
                    "critic_review": synthesis.get("critic_review") or {},
                    "stage_trace": synthesis.get("stage_trace") or [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            notes="Stone v3 baseline draft regenerated from preprocess output.",
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
        clear_stone_v3_checkpoint(request.app.state.config.assets_dir, project_id)
        return draft
    project = _ensure_project(session, project_id)
    if project.mode == "stone" and asset_kind in STONE_V2_ASSET_KINDS:
        _raise_legacy_stone_v2_http_error()
    run = repository.get_latest_analysis_run(session, project_id)
    if not run:
        raise HTTPException(status_code=400, detail="请先完成一次分析，再生成资产。")
    if run.status in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="当前分析仍在进行中，请等完成后再生成资产。")
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

generate_asset_draft = _generate_asset_draft
normalize_saved_asset_content = _normalize_saved_asset_content
asset_label = _asset_label

__all__ = [
    "_persist_asset_files",
    "_delete_asset_files",
    "_download_headers",
    "_safe_ascii_filename",
    "_markdown_download_response",
    "_skill_documents_for_export",
    "_build_analysis_reference_markdown",
    "_resolve_skill_export_document",
    "_build_skill_export_zip",
    "_normalize_saved_asset_content",
    "_asset_label",
    "_load_stone_v3_profiles_and_documents",
    "_generate_asset_draft",
    "generate_asset_draft",
    "normalize_saved_asset_content",
    "asset_label",
]
