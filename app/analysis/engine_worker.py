from __future__ import annotations

import traceback
from dataclasses import asdict
from typing import Any

from app.agents.analysis.facet_llm import analyze_facet_with_llm
from app.analysis.facets import FacetDefinition
from app.analysis.engine_helpers import (
    RAW_TEXT_PREVIEW_LIMIT,
    _analyze_heuristically,
    _normalize_facet_payload,
    _parse_confidence,
)
from app.llm.client import normalize_api_mode
from app.schemas import FacetResult


def analyze_facet_worker(
    facet: FacetDefinition,
    project_name: str,
    chunks: list[dict[str, Any]],
    llm_config: dict[str, Any] | None,
    llm_log_path: str | None,
    target_role: str | None,
    analysis_context: str | None,
    stream_callback: Any | None = None,
) -> dict[str, Any]:
    try:
        if llm_config:
            try:
                payload = analyze_facet_with_llm(
                    facet,
                    project_name,
                    chunks,
                    llm_config,
                    llm_log_path=llm_log_path,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    normalize_payload=lambda payload: _normalize_facet_payload(payload, chunks, facet),
                    raw_text_limit=RAW_TEXT_PREVIEW_LIMIT,
                    stream_callback=stream_callback,
                )
            except Exception as exc:
                payload = _analyze_heuristically(
                    facet,
                    chunks,
                    target_role=target_role,
                    analysis_context=analysis_context,
                )
                payload["_meta"] = {
                    **dict(payload.get("_meta") or {}),
                    "llm_called": True,
                    "llm_success": False,
                    "llm_attempts": 1,
                    "provider_kind": llm_config.get("provider_kind"),
                    "api_mode": normalize_api_mode(llm_config.get("api_mode")),
                    "llm_error": str(exc),
                    "raw_text": getattr(exc, "raw_text", None),
                    "request_url": getattr(exc, "request_url", None),
                    "request_payload": getattr(exc, "request_payload", None),
                    "log_path": llm_log_path,
                }
                payload["notes"] = (
                    f"{payload.get('notes') or ''}\n"
                    f"LLM 返回不可用结果，系统已切换到启发式回退逻辑恢复当前维度：{exc}"
                ).strip()
        else:
            payload = _analyze_heuristically(
                facet,
                chunks,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        return asdict(
            FacetResult(
                facet_key=facet.key,
                status="completed",
                confidence=_parse_confidence(payload.get("confidence"), 0.55),
                summary=payload.get("summary", ""),
                bullets=list(payload.get("bullets", [])),
                evidence=list(payload.get("evidence", [])),
                conflicts=list(payload.get("conflicts", [])),
                notes=payload.get("notes"),
                raw_payload=payload,
            )
        )
    except Exception as exc:
        return asdict(
            FacetResult(
                facet_key=facet.key,
                status="failed",
                confidence=0.0,
                summary="",
                bullets=[],
                evidence=[],
                conflicts=[],
                notes=str(exc),
                raw_payload={
                    "_meta": {
                        "llm_called": bool(llm_config),
                        "llm_success": False,
                        "llm_attempts": 1 if llm_config else 0,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        )


__all__ = ["analyze_facet_worker"]
