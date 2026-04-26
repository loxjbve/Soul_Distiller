from __future__ import annotations

import json
import queue
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

OFFICIAL_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
}
MAX_CONCURRENT_LLM_REQUESTS = 20

# Global httpx client for connection pooling
_HTTP_CLIENT = httpx.Client(limits=httpx.Limits(max_keepalive_connections=20, max_connections=50))
_LOG_QUEUE = queue.Queue()


def _log_worker():
    while True:
        log_item = _LOG_QUEUE.get()
        if log_item is None:
            break
        path, record = log_item
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            pass
        finally:
            _LOG_QUEUE.task_done()


_log_thread = threading.Thread(target=_log_worker, daemon=True)
_log_thread.start()


class LLMError(Exception):
    """Raised when the remote LLM service returns an invalid response."""

    def __init__(
        self,
        message: str,
        *,
        raw_text: str | None = None,
        request_url: str | None = None,
        status_code: int | None = None,
        request_payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.request_url = request_url
        self.status_code = status_code
        self.request_payload = request_payload


def normalize_provider_kind(provider_kind: str | None) -> str:
    provider = (provider_kind or "openai-compatible").strip().lower()
    aliases = {
        "openai_compatible": "openai-compatible",
        "custom": "openai-compatible",
    }
    return aliases.get(provider, provider)


def normalize_api_mode(api_mode: str | None) -> str:
    mode = (api_mode or "responses").strip().lower()
    aliases = {
        "response": "responses",
        "response_api": "responses",
        "chat": "chat_completions",
        "chat_completion": "chat_completions",
        "chat-completions": "chat_completions",
    }
    normalized = aliases.get(mode, mode)
    if normalized not in {"responses", "chat_completions"}:
        return "responses"
    return normalized


def parse_json_response(text: str, fallback: bool = False) -> dict[str, Any]:
    body = text.strip()
    if body.startswith("```json"):
        body = body[7:]
    elif body.startswith("```"):
        body = body[3:]
    if body.endswith("```"):
        body = body[:-3]
    body = body.strip()

    try:
        return json.loads(body)
    except json.JSONDecodeError:
        start = body.find("{")
        end = body.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(body[start : end + 1])
            except json.JSONDecodeError:
                pass

        if fallback:
            return {"summary": text, "bullets": [], "confidence": 0.5, "notes": "Recovered from unparseable JSON."}

        raise LLMError("Model did not return valid JSON.", raw_text=body)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "LLMError",
    "MAX_CONCURRENT_LLM_REQUESTS",
    "OFFICIAL_PROVIDER_BASE_URLS",
    "_HTTP_CLIENT",
    "_LOG_QUEUE",
    "_utcnow_iso",
    "normalize_api_mode",
    "normalize_provider_kind",
    "parse_json_response",
]
