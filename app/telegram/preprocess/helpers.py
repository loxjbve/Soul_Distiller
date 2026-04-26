from __future__ import annotations

import json
import traceback
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.analysis.streaming import AnalysisStreamHub
from app.db import Database
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import (
    Project,
    TelegramMessage,
    TelegramParticipant,
    TelegramPreprocessRun,
    TelegramPreprocessTopUser,
    TelegramPreprocessWeeklyTopicCandidate,
    utcnow,
)
from app.runtime_limits import background_task_slot
from app.schemas import ServiceConfig
from app.storage import repository
from app.utils.text import top_terms

TELEGRAM_ACTIVE_USER_LIMIT = 20
TELEGRAM_RELATIONSHIP_USER_LIMIT = 30
TELEGRAM_RELATIONSHIP_LLM_EDGE_LIMIT = 40
TELEGRAM_RELATIONSHIP_MIN_STRENGTH = 0.35
TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE = 4
TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS = 6
TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE = 3
TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT = 250
TELEGRAM_WEEKLY_MAX_WINDOWS = 2
TELEGRAM_WEEKLY_TOPIC_CAP = 4
TELEGRAM_WEEKLY_PARTICIPANT_QUOTE_LIMIT = 4
TELEGRAM_WEEKLY_TOOL_MAX_MESSAGES = 80
TELEGRAM_ALIAS_TOOL_MAX_MESSAGES = 16
TELEGRAM_WEEKLY_AGENT_MAX_ITERATIONS = 4
TELEGRAM_ALIAS_AGENT_MAX_ITERATIONS = 4
TELEGRAM_WEEKLY_AGENT_RETRIES = 2
TELEGRAM_ALIAS_AGENT_RETRIES = 2
TELEGRAM_PREPROCESS_TRACE_LIMIT = 160
TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT = 3200
TELEGRAM_PREPROCESS_PAYLOAD_PREVIEW_LIMIT = 1200
TELEGRAM_PREPROCESS_MIN_CONCURRENCY = 1


def _safe_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _compact_text(value: Any, *, limit: int = 240) -> str:
    text = " ".join(str(value or "").split()).strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _compact_message_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "message_id": payload.get("message_id"),
        "participant_id": payload.get("participant_id"),
        "sender_name": payload.get("sender_name"),
        "sent_at": payload.get("sent_at"),
        "text": _compact_text(payload.get("text")),
    }


def _compact_message_line(payload: dict[str, Any]) -> str:
    return (
        f"[{payload.get('message_id')}] "
        f"{payload.get('sent_at') or 'unknown-time'} "
        f"{payload.get('sender_name') or 'unknown'}: "
        f"{_compact_text(payload.get('text'))}"
    )


def _preview_text(value: Any, *, limit: int = TELEGRAM_PREPROCESS_PAYLOAD_PREVIEW_LIMIT) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, indent=2)
        except TypeError:
            text = str(value)
    text = text.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _iso_week_key(value: datetime) -> str:
    iso_year, iso_week, _ = value.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _coerce_message_ids(values: Any) -> list[int]:
    normalized: list[int] = []
    for item in values or []:
        try:
            normalized.append(int(item))
        except (TypeError, ValueError):
            continue
    return normalized


def _dedupe_strings(values: list[Any], *, limit: int = 8) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
        if len(normalized) >= limit:
            break
    return normalized


@dataclass(slots=True)
class WeeklyCandidateScope:
    candidate: TelegramPreprocessWeeklyTopicCandidate
    messages: list[dict[str, Any]]



safe_iso = _safe_iso
compact_text = _compact_text
compact_message_payload = _compact_message_payload
compact_message_line = _compact_message_line
preview_text = _preview_text
iso_week_key = _iso_week_key
coerce_message_ids = _coerce_message_ids
dedupe_strings = _dedupe_strings

__all__ = [
    "TELEGRAM_ACTIVE_USER_LIMIT",
    "TELEGRAM_RELATIONSHIP_USER_LIMIT",
    "TELEGRAM_RELATIONSHIP_LLM_EDGE_LIMIT",
    "TELEGRAM_RELATIONSHIP_MIN_STRENGTH",
    "TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE",
    "TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS",
    "TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE",
    "TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT",
    "TELEGRAM_WEEKLY_MAX_WINDOWS",
    "TELEGRAM_WEEKLY_TOPIC_CAP",
    "TELEGRAM_WEEKLY_PARTICIPANT_QUOTE_LIMIT",
    "TELEGRAM_WEEKLY_TOOL_MAX_MESSAGES",
    "TELEGRAM_ALIAS_TOOL_MAX_MESSAGES",
    "TELEGRAM_WEEKLY_AGENT_MAX_ITERATIONS",
    "TELEGRAM_ALIAS_AGENT_MAX_ITERATIONS",
    "TELEGRAM_WEEKLY_AGENT_RETRIES",
    "TELEGRAM_ALIAS_AGENT_RETRIES",
    "TELEGRAM_PREPROCESS_TRACE_LIMIT",
    "TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT",
    "TELEGRAM_PREPROCESS_PAYLOAD_PREVIEW_LIMIT",
    "TELEGRAM_PREPROCESS_MIN_CONCURRENCY",
    "WeeklyCandidateScope",
    "safe_iso",
    "compact_text",
    "compact_message_payload",
    "compact_message_line",
    "preview_text",
    "iso_week_key",
    "coerce_message_ids",
    "dedupe_strings",
]
