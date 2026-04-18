from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any


TELEGRAM_REPORT_MAX_WINDOWS = 24
TELEGRAM_REPORT_TARGET_WINDOW_MESSAGES = 5000
TELEGRAM_PREVIEW_LINE_LIMIT = 120
TELEGRAM_PREVIEW_CHAR_LIMIT = 12000


@dataclass(slots=True)
class TelegramImportBundle:
    chat: dict[str, Any]
    participants: list[dict[str, Any]]
    messages: list[dict[str, Any]]
    preview_text: str
    metadata: dict[str, Any]


def is_telegram_export_payload(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("messages"), list)
        and "type" in payload
    )


def parse_telegram_export(payload: dict[str, Any]) -> TelegramImportBundle:
    if not is_telegram_export_payload(payload):
        raise ValueError("Payload is not a Telegram export.")

    raw_messages = list(payload.get("messages") or [])
    participant_stats: dict[str, dict[str, Any]] = {}
    normalized_messages: list[dict[str, Any]] = []
    preview_lines: list[str] = []
    service_count = 0
    media_counter: Counter[str] = Counter()
    message_type_counter: Counter[str] = Counter()

    for raw_message in raw_messages:
        if not isinstance(raw_message, dict):
            continue

        sent_at = _parse_telegram_datetime(raw_message.get("date"))
        sender_name = _first_nonempty(
            raw_message.get("from"),
            raw_message.get("actor"),
            raw_message.get("author"),
        )
        sender_ref = _first_nonempty(
            raw_message.get("from_id"),
            raw_message.get("actor_id"),
            raw_message.get("author_id"),
        )
        participant_key = _build_participant_key(sender_ref, sender_name)
        message_type = str(raw_message.get("type") or "message")
        message_type_counter[message_type] += 1
        media_type = _first_nonempty(raw_message.get("media_type"))
        if media_type:
            media_counter[media_type] += 1
        if message_type != "message":
            service_count += 1

        text_normalized = _normalize_message_text(raw_message)
        if not text_normalized:
            text_normalized = _compose_service_text(raw_message)
        if not text_normalized:
            text_normalized = _build_message_stub(raw_message)

        metadata_json = _build_message_metadata(raw_message)
        record = {
            "telegram_message_id": _safe_int(raw_message.get("id")),
            "message_type": message_type,
            "participant_key": participant_key,
            "sent_at": sent_at,
            "sent_at_text": _first_nonempty(raw_message.get("date")),
            "unix_ts": _safe_int(raw_message.get("date_unixtime")),
            "sender_name": sender_name or None,
            "sender_ref": sender_ref or None,
            "reply_to_message_id": _safe_int(raw_message.get("reply_to_message_id")),
            "reply_to_peer_id": _first_nonempty(raw_message.get("reply_to_peer_id")),
            "media_type": media_type or None,
            "action_type": _first_nonempty(raw_message.get("action")),
            "file_path": _first_nonempty(raw_message.get("file")),
            "file_name": _first_nonempty(raw_message.get("file_name")),
            "mime_type": _first_nonempty(raw_message.get("mime_type")),
            "width": _safe_int(raw_message.get("width")),
            "height": _safe_int(raw_message.get("height")),
            "duration_seconds": _safe_int(raw_message.get("duration_seconds"), raw_message.get("duration")),
            "forwarded_from": _first_nonempty(raw_message.get("forwarded_from")),
            "forwarded_from_id": _first_nonempty(raw_message.get("forwarded_from_id")),
            "text_normalized": text_normalized,
            "text_raw_json": _jsonable_value(raw_message.get("text")),
            "reactions_json": _jsonable_value(raw_message.get("reactions")),
            "metadata_json": metadata_json,
        }
        normalized_messages.append(record)

        stats = participant_stats.setdefault(
            participant_key,
            {
                "participant_key": participant_key,
                "telegram_user_id": sender_ref or None,
                "display_name": sender_name or participant_key,
                "username": _extract_username(sender_name),
                "first_seen_at": sent_at,
                "last_seen_at": sent_at,
                "message_count": 0,
                "service_event_count": 0,
                "metadata_json": {},
            },
        )
        if sent_at and (stats["first_seen_at"] is None or sent_at < stats["first_seen_at"]):
            stats["first_seen_at"] = sent_at
        if sent_at and (stats["last_seen_at"] is None or sent_at > stats["last_seen_at"]):
            stats["last_seen_at"] = sent_at
        if message_type == "message":
            stats["message_count"] += 1
        else:
            stats["service_event_count"] += 1

        if len(preview_lines) < TELEGRAM_PREVIEW_LINE_LIMIT:
            preview_line = _format_preview_line(record)
            if preview_line:
                preview_lines.append(preview_line)

    participants = sorted(
        participant_stats.values(),
        key=lambda item: (-int(item["message_count"]), str(item["display_name"] or "").lower()),
    )

    preview_text = "\n".join(preview_lines)
    if len(preview_text) > TELEGRAM_PREVIEW_CHAR_LIMIT:
        preview_text = preview_text[:TELEGRAM_PREVIEW_CHAR_LIMIT].rstrip() + "\n..."

    metadata = {
        "format": "telegram_export",
        "chat_name": _first_nonempty(payload.get("name")),
        "chat_type": _first_nonempty(payload.get("type")),
        "telegram_chat_id": str(payload.get("id") or "") or None,
        "message_total": len(normalized_messages),
        "participant_total": len(participants),
        "service_message_total": service_count,
        "media_types": dict(media_counter),
        "message_types": dict(message_type_counter),
    }

    return TelegramImportBundle(
        chat={
            "telegram_chat_id": str(payload.get("id") or "") or None,
            "chat_type": _first_nonempty(payload.get("type")),
            "title": _first_nonempty(payload.get("name")),
            "message_count": len(normalized_messages),
            "participant_count": len(participants),
            "metadata_json": {
                "raw_type": payload.get("type"),
                "export_keys": sorted(payload.keys()),
                **metadata,
            },
        },
        participants=participants,
        messages=normalized_messages,
        preview_text=preview_text,
        metadata=metadata,
    )


def build_report_windows(messages: list[Any]) -> list[list[Any]]:
    if not messages:
        return []
    window_count = max(1, min(TELEGRAM_REPORT_MAX_WINDOWS, math.ceil(len(messages) / TELEGRAM_REPORT_TARGET_WINDOW_MESSAGES)))
    window_size = max(1, math.ceil(len(messages) / window_count))
    windows: list[list[Any]] = []
    for start in range(0, len(messages), window_size):
        window = messages[start : start + window_size]
        if window:
            windows.append(window)
    return windows


def build_report_seed(window_messages: list[Any]) -> dict[str, Any]:
    if not window_messages:
        return {
            "message_count": 0,
            "participant_count": 0,
            "start_message_id": None,
            "end_message_id": None,
            "start_at": None,
            "end_at": None,
            "top_participants": [],
            "sample_messages": [],
        }

    participant_counter: Counter[str] = Counter()
    sample_messages = _sample_messages(window_messages, sample_limit=12)
    for message in window_messages:
        sender_name = _message_attr(message, "sender_name")
        if sender_name:
            participant_counter[str(sender_name)] += 1

    first_message = window_messages[0]
    last_message = window_messages[-1]
    return {
        "message_count": len(window_messages),
        "participant_count": len(participant_counter),
        "start_message_id": _message_attr(first_message, "telegram_message_id"),
        "end_message_id": _message_attr(last_message, "telegram_message_id"),
        "start_at": _message_attr(first_message, "sent_at"),
        "end_at": _message_attr(last_message, "sent_at"),
        "top_participants": [
            {"name": name, "message_count": count}
            for name, count in participant_counter.most_common(8)
        ],
        "sample_messages": sample_messages,
    }


def build_report_prompt_text(chat_title: str, seed: dict[str, Any]) -> str:
    sample_lines: list[str] = []
    for item in seed.get("sample_messages") or []:
        message_id = item.get("telegram_message_id")
        sender = item.get("sender_name") or "unknown"
        sent_at = item.get("sent_at") or "unknown-time"
        text = item.get("text_normalized") or ""
        sample_lines.append(f"[{message_id}] {sent_at} {sender}: {text}")

    participant_lines = [
        f"- {item.get('name')}: {item.get('message_count')} messages"
        for item in seed.get("top_participants") or []
    ]
    return (
        f"Chat: {chat_title or 'Telegram Chat'}\n"
        f"Window messages: {seed.get('message_count', 0)}\n"
        f"Participant count: {seed.get('participant_count', 0)}\n"
        f"Message range: {seed.get('start_message_id')} - {seed.get('end_message_id')}\n"
        f"Time range: {_format_time(seed.get('start_at'))} -> {_format_time(seed.get('end_at'))}\n"
        "Top participants:\n"
        + ("\n".join(participant_lines) if participant_lines else "- none")
        + "\n\nRepresentative messages:\n"
        + ("\n".join(sample_lines) if sample_lines else "- no representative messages")
    )


def build_report_heuristic_payload(chat_title: str, stage_index: int, seed: dict[str, Any]) -> dict[str, Any]:
    samples = list(seed.get("sample_messages") or [])
    terms = Counter()
    for item in samples:
        for token in _simple_terms(str(item.get("text_normalized") or "")):
            terms[token] += 1
    top_terms = [token for token, _count in terms.most_common(6)]
    summary = (
        f"{chat_title or 'Telegram chat'} 的第 {stage_index} 阶段主要围绕 "
        f"{'、'.join(top_terms[:3]) if top_terms else '连续群聊互动'} 展开，"
        f"时间范围为 {_format_time(seed.get('start_at'))} 到 {_format_time(seed.get('end_at'))}。"
    )
    return {
        "title": f"阶段 {stage_index}",
        "summary": summary,
        "time_summary": (
            f"{_format_time(seed.get('start_at'))} -> {_format_time(seed.get('end_at'))}, "
            f"{seed.get('message_count', 0)} messages"
        ),
        "topics_json": top_terms[:6],
        "participants_json": list(seed.get("top_participants") or [])[:6],
        "evidence_json": [
            {
                "telegram_message_id": item.get("telegram_message_id"),
                "sender_name": item.get("sender_name"),
                "sent_at": item.get("sent_at"),
                "quote": item.get("text_normalized"),
            }
            for item in samples[:5]
        ],
        "metadata_json": seed,
    }


def build_report_digest(reports: list[Any], *, limit: int = 24) -> str:
    lines: list[str] = []
    for report in list(reports or [])[:limit]:
        topics = _report_attr(report, "topics_json") or []
        if isinstance(topics, list):
            topic_text = ", ".join(
                str(item.get("topic") if isinstance(item, dict) else item)
                for item in topics[:4]
                if str(item.get("topic") if isinstance(item, dict) else item).strip()
            )
        else:
            topic_text = str(topics)
        lines.append(
            (
                f"[stage {_report_attr(report, 'stage_index')}] "
                f"{_report_attr(report, 'title') or 'untitled'} | "
                f"messages {_report_attr(report, 'start_message_id')} - {_report_attr(report, 'end_message_id')} | "
                f"time {_format_time(_report_attr(report, 'start_at'))} -> {_format_time(_report_attr(report, 'end_at'))} | "
                f"topics: {topic_text or 'n/a'} | "
                f"summary: {str(_report_attr(report, 'summary') or '')[:220]}"
            )
        )
    return "\n".join(lines)


def normalize_query_messages(messages: list[Any], *, limit: int = 20, text_limit: int = 240) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for message in list(messages or [])[:limit]:
        text = str(_message_attr(message, "text_normalized") or "").strip()
        rows.append(
            {
                "telegram_message_id": _message_attr(message, "telegram_message_id"),
                "sent_at": _format_time(_message_attr(message, "sent_at"), fallback=_message_attr(message, "sent_at_text")),
                "sender_name": _message_attr(message, "sender_name"),
                "message_type": _message_attr(message, "message_type"),
                "reply_to_message_id": _message_attr(message, "reply_to_message_id"),
                "media_type": _message_attr(message, "media_type"),
                "text": text[:text_limit] + ("..." if len(text) > text_limit else ""),
            }
        )
    return rows


def normalize_evidence_messages(messages: list[Any], *, quote_limit: int = 180) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for message in messages:
        text = str(_message_attr(message, "text_normalized") or "").strip()
        evidence.append(
            {
                "message_id": _message_attr(message, "telegram_message_id"),
                "sender_name": _message_attr(message, "sender_name"),
                "sent_at": _format_time(_message_attr(message, "sent_at"), fallback=_message_attr(message, "sent_at_text")),
                "quote": text[:quote_limit] + ("..." if len(text) > quote_limit else ""),
            }
        )
    return evidence


def _format_preview_line(message: dict[str, Any]) -> str:
    text = str(message.get("text_normalized") or "").strip()
    if not text:
        return ""
    sent_at = message.get("sent_at_text") or _format_time(message.get("sent_at"))
    sender_name = message.get("sender_name") or "unknown"
    preview = text[:160] + ("..." if len(text) > 160 else "")
    return f"[{sent_at}] {sender_name}: {preview}"


def _normalize_message_text(raw_message: dict[str, Any]) -> str:
    value = raw_message.get("text")
    if isinstance(value, str):
        return " ".join(value.split()).strip()
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("href") or "").strip())
        return " ".join(part for part in parts if part).strip()
    if value is None:
        return ""
    return str(value).strip()


def _compose_service_text(raw_message: dict[str, Any]) -> str:
    bits = [
        _first_nonempty(raw_message.get("action")),
        _first_nonempty(raw_message.get("actor")),
    ]
    members = raw_message.get("members")
    if isinstance(members, list):
        member_names = [str(item) for item in members if str(item).strip()]
        if member_names:
            bits.append("members: " + ", ".join(member_names[:10]))
    media_type = _first_nonempty(raw_message.get("media_type"))
    if media_type:
        bits.append(f"media: {media_type}")
    return " | ".join(bit for bit in bits if bit)


def _build_message_stub(raw_message: dict[str, Any]) -> str:
    media_type = _first_nonempty(raw_message.get("media_type"))
    if media_type:
        return f"[{media_type}]"
    message_type = _first_nonempty(raw_message.get("type"))
    return f"[{message_type or 'message'}]"


def _build_message_metadata(raw_message: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "text_entities",
        "photo",
        "thumbnail",
        "edited",
        "edited_unixtime",
        "via_bot",
        "performer",
        "title",
        "members",
    )
    metadata: dict[str, Any] = {}
    for key in keys:
        if key in raw_message:
            metadata[key] = _jsonable_value(raw_message.get(key))
    return metadata


def _jsonable_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable_value(item) for key, item in value.items()}
    return str(value)


def _safe_int(*values: Any) -> int | None:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _parse_telegram_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    for candidate in (normalized, normalized.replace(" ", "T")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.replace(tzinfo=None)
        except ValueError:
            continue
    return None


def _extract_username(name: str | None) -> str | None:
    text = str(name or "").strip()
    if text.startswith("@") and len(text) > 1:
        return text[1:]
    return None


def _build_participant_key(sender_ref: str, sender_name: str) -> str:
    if sender_ref:
        return sender_ref
    if sender_name:
        return f"name::{sender_name.strip().lower()}"
    return "unknown"


def _sample_messages(messages: list[Any], *, sample_limit: int) -> list[dict[str, Any]]:
    if not messages:
        return []
    indexes: set[int] = set()
    if len(messages) <= sample_limit:
        indexes = set(range(len(messages)))
    else:
        step = max(1, len(messages) // sample_limit)
        indexes = {min(len(messages) - 1, index) for index in range(0, len(messages), step)}
        indexes.update({0, len(messages) - 1})
    sampled: list[dict[str, Any]] = []
    for index in sorted(indexes)[:sample_limit]:
        message = messages[index]
        text = str(_message_attr(message, "text_normalized") or "").strip()
        if not text:
            continue
        sampled.append(
            {
                "telegram_message_id": _message_attr(message, "telegram_message_id"),
                "sent_at": _format_time(_message_attr(message, "sent_at"), fallback=_message_attr(message, "sent_at_text")),
                "sender_name": _message_attr(message, "sender_name"),
                "text_normalized": text[:260] + ("..." if len(text) > 260 else ""),
            }
        )
    return sampled


def _message_attr(message: Any, key: str) -> Any:
    if isinstance(message, dict):
        return message.get(key)
    return getattr(message, key, None)


def _report_attr(report: Any, key: str) -> Any:
    if isinstance(report, dict):
        return report.get(key)
    return getattr(report, key, None)


def _format_time(value: Any, *, fallback: Any = None) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="minutes")
    text = str(value or fallback or "").strip()
    return text or "unknown"


def _simple_terms(text: str) -> list[str]:
    tokens = [token.strip(" ,.!?[](){}<>\"'") for token in text.split()]
    return [
        token.lower()
        for token in tokens
        if len(token) >= 3 and not token.isdigit()
    ]


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""
