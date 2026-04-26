from __future__ import annotations

from typing import Any


def ok_response(message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "ok", "message": message, **payload}


def task_response(message: str, task: dict[str, Any], **payload: Any) -> dict[str, Any]:
    return {
        **task,
        "request_status": "ok",
        "message": message,
        "task": task,
        "task_id": task.get("task_id"),
        "progress_percent": task.get("progress_percent", 0),
        **payload,
    }


__all__ = ["ok_response", "task_response"]
