from __future__ import annotations

import json
from typing import Any

from app.analysis.facets import FACETS
from app.models import AnalysisFacet, AnalysisRun
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY, MIN_ANALYSIS_CONCURRENCY

ANALYSIS_EVENT_LIMIT = 48
ANALYSIS_SUMMARY_PREVIEW_LIMIT = 420
ANALYSIS_LIVE_TEXT_PREVIEW_LIMIT = 3200
ANALYSIS_RESPONSE_TEXT_PREVIEW_LIMIT = 2400
ANALYSIS_REQUEST_PAYLOAD_PREVIEW_LIMIT = 1600
RAW_TEXT_PREVIEW_LIMIT = 20000
GLOBAL_PERSONA_CARD_LABELS = (
    "角色规则",
    "心智模型",
    "决策启发式",
    "表达DNA",
    "表达 DNA",
    "时间线",
    "价值观",
    "反模式",
    "诚实边界",
    "识别谱系",
)

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

def _ordered_facets(facets: list[AnalysisFacet], summary: dict[str, Any] | None = None) -> list[AnalysisFacet]:
    facet_keys = [str(item).strip() for item in ((summary or {}).get("facet_keys") or []) if str(item).strip()]
    if not facet_keys:
        facet_keys = [facet.key for facet in FACETS]
    order = {facet_key: index for index, facet_key in enumerate(facet_keys)}
    return sorted(facets, key=lambda item: order.get(item.facet_key, 999))

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

serialize_analysis_event = _serialize_analysis_event
serialize_analysis_facet = _serialize_analysis_facet
serialize_analysis_run = _serialize_analysis_run

__all__ = [
    "_ordered_facets",
    "_serialize_analysis_event",
    "_serialize_analysis_facet",
    "_serialize_analysis_run",
    "serialize_analysis_event",
    "serialize_analysis_facet",
    "serialize_analysis_run",
]
