from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

WRITER_ACTOR_NAME = "写作 Agent"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"






































def _build_writer_message_payload(
    *,
    message_kind: str,
    label: str,
    body: str,
    detail: dict[str, Any] | None = None,
    stage: str = "writer",
    stream_key: str | None = None,
    stream_state: str = "complete",
    render_mode: str = "markdown",
) -> dict[str, Any]:
    return {
        "stage": stage,
        "label": label,
        "actor_id": f"writer-{message_kind}",
        "actor_name": WRITER_ACTOR_NAME,
        "actor_role": "writer",
        "message_kind": message_kind,
        "body": body,
        "detail": detail or {},
        "created_at": _iso_now(),
        "stream_key": stream_key,
        "stream_state": stream_state,
        "render_mode": render_mode,
    }

format_sse = _format_sse
build_writer_message_payload = _build_writer_message_payload

__all__ = [
    "_format_sse",
    "_build_writer_message_payload",
    "format_sse",
    "build_writer_message_payload",
]
