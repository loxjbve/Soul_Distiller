from __future__ import annotations

from contextlib import contextmanager
from threading import BoundedSemaphore
from typing import Iterator

MAX_CONCURRENT_BACKGROUND_TASKS = 4
_BACKGROUND_TASK_SEMAPHORE = BoundedSemaphore(MAX_CONCURRENT_BACKGROUND_TASKS)


@contextmanager
def background_task_slot() -> Iterator[None]:
    with _BACKGROUND_TASK_SEMAPHORE:
        yield
