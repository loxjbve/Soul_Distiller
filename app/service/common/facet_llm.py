from __future__ import annotations

import json
from time import perf_counter
from typing import Any, Callable

from app.service.common.prompt_builders import build_facet_analysis_messages
from app.service.common.facets import FacetDefinition
from app.service.common.llm.client import LLMError, OpenAICompatibleClient, normalize_api_mode, parse_json_response
from app.schemas import ServiceConfig


def analyze_facet_with_llm(
    facet: FacetDefinition,
    project_name: str,
    chunks: list[dict[str, Any]],
    llm_config: dict[str, Any],
    *,
    llm_log_path: str | None,
    target_role: str | None,
    analysis_context: str | None,
    normalize_payload: Callable[[dict[str, Any]], dict[str, Any]],
    raw_text_limit: int,
    stream_callback: Any | None = None,
) -> dict[str, Any]:
    config = ServiceConfig(**llm_config)
    client = OpenAICompatibleClient(config, log_path=llm_log_path)
    excerpt_text = "\n\n".join(
        f"[{chunk['chunk_id']}] {chunk['document_title']} / {chunk['filename']}\n{chunk['content'][:900]}"
        for chunk in chunks
    )
    messages = build_facet_analysis_messages(
        project_name,
        facet,
        excerpt_text,
        target_role=target_role,
        analysis_context=analysis_context,
    )
    endpoint_path = "/responses" if normalize_api_mode(config.api_mode) == "responses" else "/chat/completions"
    request_payload: dict[str, Any] = {
        "messages": messages,
        "model": config.model,
        "api_mode": config.api_mode,
        "endpoint_url": client.endpoint_url(endpoint_path),
    }
    started = perf_counter()
    last_error: Exception | None = None
    attempts = 0
    for _ in range(2):
        attempts += 1
        try:
            completion = client.chat_completion_result(
                messages,
                model=config.model,
                temperature=0.2,
                max_tokens=None,
                stream_handler=stream_callback,
            )
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            try:
                parsed = parse_json_response(completion.content, fallback=False)
                llm_success = True
            except LLMError as exc:
                parsed = parse_json_response(completion.content, fallback=True)
                llm_success = False
                llm_error_text = str(exc)
            normalized = normalize_payload(parsed)
            if not llm_success:
                normalized["notes"] = (
                    f"{normalized.get('notes') or ''}\n"
                    "LLM 返回的不是标准 JSON，系统已用回退解析尽量恢复当前维度结果。"
                ).strip()
            normalized["_meta"] = {
                "llm_called": True,
                "llm_success": llm_success,
                "llm_attempts": attempts,
                "provider_kind": config.provider_kind,
                "api_mode": normalize_api_mode(config.api_mode),
                "llm_model": completion.model,
                "prompt_tokens": completion.usage.get("prompt_tokens", 0),
                "completion_tokens": completion.usage.get("completion_tokens", 0),
                "total_tokens": completion.usage.get("total_tokens", 0),
                "duration_ms": int((perf_counter() - started) * 1000),
                "request_url": completion.request_url,
                "request_payload": completion.request_payload or request_payload,
                "raw_text": completion.content[:raw_text_limit],
                "llm_error": None if llm_success else llm_error_text,
                "log_path": llm_log_path,
            }
            return normalized
        except (LLMError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            if getattr(exc, "request_payload", None) is None:
                setattr(exc, "request_payload", request_payload)
            if getattr(exc, "request_url", None) is None:
                setattr(exc, "request_url", client.endpoint_url(endpoint_path))
            last_error = exc
    raise LLMError(
        str(last_error) if last_error else "维度分析失败。",
        raw_text=(getattr(last_error, "raw_text", None) or "")[:raw_text_limit] or None,
        request_url=getattr(last_error, "request_url", None),
        request_payload=getattr(last_error, "request_payload", None),
    )
