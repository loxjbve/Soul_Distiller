from __future__ import annotations

from queue import Queue
from threading import Lock
from typing import Any


class AnalysisStreamHub:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Queue[dict[str, Any]]]] = {}
        self._lock = Lock()

    def subscribe(self, run_id: str) -> Queue[dict[str, Any]]:
        queue: Queue[dict[str, Any]] = Queue()
        with self._lock:
            self._listeners.setdefault(run_id, []).append(queue)
        return queue

    def unsubscribe(self, run_id: str, queue: Queue[dict[str, Any]]) -> None:
        with self._lock:
            listeners = self._listeners.get(run_id)
            if not listeners:
                return
            self._listeners[run_id] = [item for item in listeners if item is not queue]
            if not self._listeners[run_id]:
                self._listeners.pop(run_id, None)

    def publish(self, run_id: str, *, event: str = "snapshot", payload: dict[str, Any] | None = None) -> None:
        with self._lock:
            listeners = list(self._listeners.get(run_id, []))
        for queue in listeners:
            queue.put({"event": event, "payload": payload or {}})

