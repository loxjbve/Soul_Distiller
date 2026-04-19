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
TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT = 300
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
TELEGRAM_PREPROCESS_MAX_CONCURRENCY = 8


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


class TelegramPreprocessWorker:
    def __init__(
        self,
        session: Session,
        project: Project,
        *,
        llm_config: ServiceConfig | None,
        log_path: str | None = None,
        cancel_checker: Callable[[], bool] | None = None,
        trace_callback: Callable[[dict[str, Any], bool], None] | None = None,
    ) -> None:
        self.session = session
        self.project = project
        self.llm_config = llm_config
        self.log_path = str(log_path) if log_path else None
        self.cancel_checker = cancel_checker
        self.trace_callback = trace_callback
        self.client = OpenAICompatibleClient(llm_config, log_path=self.log_path) if llm_config else None
        self.usage_totals: dict[str, int] = defaultdict(int)
        self.weekly_summary_concurrency = TELEGRAM_PREPROCESS_MIN_CONCURRENCY

    def process(
        self,
        run: TelegramPreprocessRun,
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> dict[str, Any]:
        chat = (
            repository.get_telegram_chat(self.session, run.chat_id)
            if run.chat_id
            else repository.get_latest_telegram_chat(self.session, self.project.id)
        )
        if not chat:
            raise ValueError("Telegram chat not found.")

        self._restore_usage_totals(run)
        self.weekly_summary_concurrency = self._resolve_weekly_summary_concurrency(run)
        bootstrap = self._build_sql_bootstrap(chat.id)
        self._trace(
            "stage_progress",
            stage="sql_bootstrap",
            message="Collected Telegram SQL bootstrap statistics.",
            bootstrap=bootstrap,
        )
        self._progress(
            progress_callback,
            "sql_bootstrap",
            12,
            {
                "bootstrap": bootstrap,
                "window_count": 0,
                "top_user_count": 0,
                "weekly_candidate_count": 0,
            },
        )

        top_users = repository.list_telegram_preprocess_top_users(
            self.session,
            self.project.id,
            run_id=run.id,
        )
        weekly_candidates = repository.list_telegram_preprocess_weekly_topic_candidates(
            self.session,
            self.project.id,
            run_id=run.id,
        )
        if top_users and weekly_candidates:
            self._trace(
                "stage_progress",
                stage="sql_materialize",
                message="Reusing materialized top users and weekly topic candidates from a saved checkpoint.",
                top_user_count=len(top_users),
                weekly_candidate_count=len(weekly_candidates),
            )
        else:
            top_users = self._materialize_top_users(run.id, chat.id)
            weekly_candidates = self._materialize_weekly_topic_candidates(run.id, chat.id)
            self.session.commit()
            self._trace(
                "stage_progress",
                stage="sql_materialize",
                message="Materialized top users and weekly topic candidates into SQLite.",
                top_user_count=len(top_users),
                weekly_candidate_count=len(weekly_candidates),
            )
        self._progress(
            progress_callback,
            "sql_materialize",
            36,
            {
                "window_count": len(weekly_candidates),
                "top_user_count": len(top_users),
                "weekly_candidate_count": len(weekly_candidates),
            },
        )

        topics = self._run_weekly_topic_summary(
            run.id,
            chat.id,
            progress_callback=progress_callback,
        )
        self._progress(
            progress_callback,
            "weekly_topic_summary",
            92,
            {
                "topic_count": len(topics),
                "window_count": len(weekly_candidates),
                "top_user_count": len(top_users),
                "weekly_candidate_count": len(weekly_candidates),
                "weekly_summary_concurrency": self.weekly_summary_concurrency,
                "usage": dict(self.usage_totals),
            },
        )

        return {
            "bootstrap": bootstrap,
            "window_count": len(weekly_candidates),
            "top_user_count": len(top_users),
            "weekly_candidate_count": len(weekly_candidates),
            "topic_count": len(topics),
            "topics": topics,
            "usage": dict(self.usage_totals),
        }

    def _restore_usage_totals(self, run: TelegramPreprocessRun) -> None:
        usage = dict((run.summary_json or {}).get("usage") or {})
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "cache_creation_tokens",
            "cache_read_tokens",
        ):
            self.usage_totals[key] = int(usage.get(key) or getattr(run, key, 0) or 0)

    @staticmethod
    def _normalize_weekly_summary_concurrency(value: Any) -> int:
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            candidate = TELEGRAM_PREPROCESS_MIN_CONCURRENCY
        return max(
            TELEGRAM_PREPROCESS_MIN_CONCURRENCY,
            min(TELEGRAM_PREPROCESS_MAX_CONCURRENCY, candidate),
        )

    def _resolve_weekly_summary_concurrency(self, run: TelegramPreprocessRun) -> int:
        summary = dict(run.summary_json or {})
        return self._normalize_weekly_summary_concurrency(
            summary.get("weekly_summary_concurrency") or TELEGRAM_PREPROCESS_MIN_CONCURRENCY
        )

    def _topic_payload_from_model(self, topic: Any) -> dict[str, Any]:
        return {
            "topic_id": getattr(topic, "id", None),
            "topic_index": int(getattr(topic, "topic_index", 0) or 0),
            "title": getattr(topic, "title", ""),
            "summary": getattr(topic, "summary", ""),
            "start_at": getattr(topic, "start_at", None),
            "end_at": getattr(topic, "end_at", None),
            "start_message_id": getattr(topic, "start_message_id", None),
            "end_message_id": getattr(topic, "end_message_id", None),
            "message_count": int(getattr(topic, "message_count", 0) or 0),
            "participant_count": int(getattr(topic, "participant_count", 0) or 0),
            "keywords_json": list(getattr(topic, "keywords_json", None) or []),
            "evidence_json": list(getattr(topic, "evidence_json", None) or []),
            "participants": [
                {
                    "participant_id": link.participant_id,
                    "role_hint": link.role_hint,
                    "message_count": int(link.message_count or 0),
                    "mention_count": int(link.mention_count or 0),
                }
                for link in list(getattr(topic, "participants", None) or [])
            ],
            "metadata_json": dict(getattr(topic, "metadata_json", None) or {}),
        }

    @staticmethod
    def _topic_checkpoint_key(payload: dict[str, Any]) -> str:
        metadata = dict(payload.get("metadata_json") or {})
        candidate_id = str(metadata.get("candidate_id") or "").strip()
        if candidate_id:
            return candidate_id
        week_key = str(metadata.get("week_key") or "").strip()
        if week_key:
            return week_key
        return str(payload.get("topic_index") or "")

    def _upsert_topic_payload(
        self,
        topics: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        checkpoint_key = self._topic_checkpoint_key(payload)
        updated: list[dict[str, Any]] = []
        replaced = False
        for item in topics:
            if self._topic_checkpoint_key(item) == checkpoint_key:
                updated.append(payload)
                replaced = True
            else:
                updated.append(item)
        if not replaced:
            updated.append(payload)
        updated.sort(
            key=lambda item: (
                int(item.get("topic_index") or 0),
                _safe_iso(item.get("start_at")) or "",
            )
        )
        return updated

    @staticmethod
    def _build_active_user_payload(top_user: TelegramPreprocessTopUser) -> dict[str, Any]:
        primary_alias = (
            top_user.display_name
            or top_user.username
            or top_user.uid
            or top_user.participant_id
        )
        return {
            "rank": top_user.rank,
            "participant_id": top_user.participant_id,
            "uid": top_user.uid,
            "username": top_user.username,
            "display_name": top_user.display_name,
            "primary_alias": primary_alias,
            "aliases_json": _dedupe_strings(
                [primary_alias, top_user.display_name, top_user.username, top_user.uid],
                limit=6,
            ),
            "message_count": top_user.message_count,
            "first_seen_at": top_user.first_seen_at,
            "last_seen_at": top_user.last_seen_at,
            "evidence_json": [
                {
                    "source": "telegram_preprocess_top_users",
                    "rank": top_user.rank,
                    "display_name": top_user.display_name,
                    "username": top_user.username,
                    "uid": top_user.uid,
                }
            ],
        }

    def _trace(self, kind: str, *, persist: bool = True, **payload: Any) -> None:
        if not self.trace_callback:
            return
        event = {
            "timestamp": utcnow().isoformat(),
            "kind": kind,
            **payload,
        }
        try:
            self.trace_callback(event, persist)
        except Exception:
            return

    def _build_stream_callback(
        self,
        *,
        stage: str,
        agent: str,
        request_key: str,
        label: str,
        extra: dict[str, Any] | None = None,
    ):
        state = {"text": "", "pending": ""}
        metadata = dict(extra or {})

        def emit(force: bool = False) -> None:
            if not state["pending"]:
                return
            if not force and len(state["pending"]) < 80 and not state["pending"].endswith(("\n", ".", "}", "]")):
                return
            self._trace(
                "llm_delta",
                persist=False,
                stage=stage,
                agent=agent,
                request_key=request_key,
                label=label,
                text_preview=state["text"],
                **metadata,
            )
            state["pending"] = ""

        def callback(delta: str) -> None:
            if not delta:
                return
            state["text"] = f"{state['text']}{delta}"
            if len(state["text"]) > TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT:
                state["text"] = state["text"][-TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT:]
            state["pending"] += delta
            if len(state["pending"]) > TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT:
                state["pending"] = state["pending"][-TELEGRAM_PREPROCESS_TEXT_PREVIEW_LIMIT:]
            emit(force=False)

        def flush_remaining() -> None:
            emit(force=True)

        setattr(callback, "_flush_remaining", flush_remaining)
        return callback

    def _build_sql_bootstrap(self, chat_id: str) -> dict[str, Any]:
        self._ensure_active()
        summary_row = self.session.execute(
            select(
                func.count(TelegramMessage.id),
                func.min(TelegramMessage.sent_at),
                func.max(TelegramMessage.sent_at),
            ).where(
                TelegramMessage.project_id == self.project.id,
                TelegramMessage.chat_id == chat_id,
                TelegramMessage.message_type != "service",
            )
        ).one()
        top_speakers = self.session.execute(
            select(
                TelegramParticipant.id,
                TelegramParticipant.display_name,
                TelegramParticipant.username,
                TelegramParticipant.telegram_user_id,
                func.count(TelegramMessage.id).label("message_count"),
            )
            .join(TelegramMessage, TelegramMessage.participant_id == TelegramParticipant.id)
            .where(
                TelegramParticipant.project_id == self.project.id,
                TelegramParticipant.chat_id == chat_id,
                TelegramMessage.message_type != "service",
            )
            .group_by(
                TelegramParticipant.id,
                TelegramParticipant.display_name,
                TelegramParticipant.username,
                TelegramParticipant.telegram_user_id,
            )
            .order_by(func.count(TelegramMessage.id).desc())
            .limit(10)
        ).all()
        bucket_rows = self.session.execute(
            select(TelegramMessage.sent_at)
            .where(
                TelegramMessage.project_id == self.project.id,
                TelegramMessage.chat_id == chat_id,
                TelegramMessage.message_type != "service",
                TelegramMessage.sent_at.is_not(None),
            )
            .order_by(TelegramMessage.sent_at.asc())
        ).scalars()
        weekly_counts: dict[str, int] = defaultdict(int)
        for sent_at in bucket_rows:
            if sent_at is None:
                continue
            weekly_counts[_iso_week_key(sent_at)] += 1
        return {
            "message_count": int(summary_row[0] or 0),
            "start_at": _safe_iso(summary_row[1]),
            "end_at": _safe_iso(summary_row[2]),
            "top_speakers": [
                {
                    "participant_id": row[0],
                    "display_name": row[1],
                    "username": row[2],
                    "uid": row[3],
                    "message_count": int(row[4] or 0),
                }
                for row in top_speakers
            ],
            "week_buckets": [
                {"week_key": week_key, "message_count": count}
                for week_key, count in sorted(weekly_counts.items())
            ],
        }

    def _materialize_top_users(self, run_id: str, chat_id: str) -> list[TelegramPreprocessTopUser]:
        self._ensure_active()
        rows = self.session.execute(
            select(
                TelegramParticipant.id,
                TelegramParticipant.telegram_user_id,
                TelegramParticipant.username,
                TelegramParticipant.display_name,
                func.count(TelegramMessage.id).label("message_count"),
                func.min(TelegramMessage.sent_at),
                func.max(TelegramMessage.sent_at),
            )
            .join(TelegramMessage, TelegramMessage.participant_id == TelegramParticipant.id)
            .where(
                TelegramParticipant.project_id == self.project.id,
                TelegramParticipant.chat_id == chat_id,
                TelegramMessage.message_type != "service",
                or_(
                    TelegramParticipant.username.is_(None),
                    ~func.lower(TelegramParticipant.username).like("%bot"),
                ),
            )
            .group_by(
                TelegramParticipant.id,
                TelegramParticipant.telegram_user_id,
                TelegramParticipant.username,
                TelegramParticipant.display_name,
            )
            .order_by(func.count(TelegramMessage.id).desc(), TelegramParticipant.display_name.asc())
            .limit(TELEGRAM_ACTIVE_USER_LIMIT)
        ).all()
        payload = [
            {
                "rank": index,
                "participant_id": row[0],
                "uid": row[1],
                "username": row[2],
                "display_name": row[3],
                "message_count": int(row[4] or 0),
                "first_seen_at": row[5],
                "last_seen_at": row[6],
                "metadata_json": {"source": "sql_materialize"},
            }
            for index, row in enumerate(rows, start=1)
        ]
        return repository.replace_telegram_preprocess_top_users(
            self.session,
            run_id=run_id,
            project_id=self.project.id,
            chat_id=chat_id,
            top_users=payload,
        )

    def _materialize_weekly_topic_candidates(
        self,
        run_id: str,
        chat_id: str,
    ) -> list[TelegramPreprocessWeeklyTopicCandidate]:
        self._ensure_active()
        participant_map = {
            participant.id: participant
            for participant in repository.list_telegram_participants(self.session, self.project.id, chat_id=chat_id)
        }
        rows = self.session.execute(
            select(
                TelegramMessage.telegram_message_id,
                TelegramMessage.participant_id,
                TelegramMessage.sender_name,
                TelegramMessage.sent_at,
                TelegramMessage.text_normalized,
            )
            .where(
                TelegramMessage.project_id == self.project.id,
                TelegramMessage.chat_id == chat_id,
                TelegramMessage.message_type != "service",
                TelegramMessage.sent_at.is_not(None),
                TelegramMessage.telegram_message_id.is_not(None),
            )
            .order_by(TelegramMessage.sent_at.asc(), TelegramMessage.telegram_message_id.asc())
        ).all()
        weekly_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            sent_at = row[3]
            message_id = row[0]
            if sent_at is None or message_id is None:
                continue
            item = {
                "message_id": int(message_id),
                "participant_id": row[1],
                "sender_name": row[2],
                "sent_at": _safe_iso(sent_at),
                "sent_at_value": sent_at,
                "text": _compact_text(row[4], limit=360),
            }
            weekly_rows[_iso_week_key(sent_at)].append(item)

        payloads: list[dict[str, Any]] = []
        for week_key, message_rows in sorted(weekly_rows.items()):
            if not message_rows:
                continue
            selected = self._select_densest_segment(message_rows)
            participant_counts: dict[str, int] = defaultdict(int)
            for item in selected:
                participant_id = str(item.get("participant_id") or "").strip()
                if participant_id:
                    participant_counts[participant_id] += 1
            top_participants = []
            for participant_id, count in sorted(
                participant_counts.items(),
                key=lambda pair: (-pair[1], pair[0]),
            )[:8]:
                participant = participant_map.get(participant_id)
                top_participants.append(
                    {
                        "participant_id": participant_id,
                        "display_name": participant.display_name if participant else None,
                        "username": participant.username if participant else None,
                        "message_count": count,
                    }
                )
            payloads.append(
                {
                    "week_key": week_key,
                    "start_at": selected[0]["sent_at_value"],
                    "end_at": selected[-1]["sent_at_value"],
                    "start_message_id": selected[0]["message_id"],
                    "end_message_id": selected[-1]["message_id"],
                    "message_count": len(selected),
                    "participant_count": len(participant_counts),
                    "top_participants_json": top_participants,
                    "sample_messages_json": [
                        _compact_message_payload(item)
                        for item in selected
                    ],
                    "metadata_json": {
                        "source": "sql_materialize",
                        "week_key": week_key,
                        "selected_message_count": len(selected),
                        "week_total_message_count": len(message_rows),
                    },
                }
            )
        return repository.replace_telegram_preprocess_weekly_topic_candidates(
            self.session,
            run_id=run_id,
            project_id=self.project.id,
            chat_id=chat_id,
            weekly_candidates=payloads,
        )

    def _select_densest_segment(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if len(messages) <= TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT:
            return messages
        best_index = 0
        best_span: float | None = None
        width = TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT
        for start_index in range(0, len(messages) - width + 1):
            start_at = messages[start_index]["sent_at_value"]
            end_at = messages[start_index + width - 1]["sent_at_value"]
            span = (end_at - start_at).total_seconds()
            if best_span is None or span < best_span:
                best_span = span
                best_index = start_index
        return messages[best_index: best_index + width]

    def _run_weekly_topic_summary(
        self,
        run_id: str,
        chat_id: str,
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> list[dict[str, Any]]:
        candidates = repository.list_telegram_preprocess_weekly_topic_candidates(
            self.session,
            self.project.id,
            run_id=run_id,
        )
        if not candidates:
            return []
        existing_topics = [
            self._topic_payload_from_model(item)
            for item in repository.list_telegram_preprocess_topics(
                self.session,
                self.project.id,
                run_id=run_id,
            )
        ]
        completed_keys = {self._topic_checkpoint_key(item) for item in existing_topics}
        remaining_candidates = [
            candidate
            for candidate in candidates
            if candidate.id not in completed_keys and candidate.week_key not in completed_keys
        ]
        if existing_topics:
            self._trace(
                "stage_progress",
                stage="weekly_topic_summary",
                message="Resuming weekly topic summaries from a saved checkpoint.",
                completed_week_count=len(existing_topics),
                remaining_week_count=len(remaining_candidates),
                weekly_candidate_count=len(candidates),
            )
            self._progress(
                progress_callback,
                "weekly_topic_summary",
                min(40 + int((len(existing_topics) / max(len(candidates), 1)) * 34), 76),
                {
                    "topic_count": len(existing_topics),
                    "completed_week_count": len(existing_topics),
                    "remaining_week_count": len(remaining_candidates),
                    "weekly_candidate_count": len(candidates),
                    "usage": dict(self.usage_totals),
                },
            )
        if not remaining_candidates:
            self._trace(
                "agent_completed",
                stage="weekly_topic_summary",
                agent="weekly_topic_agent",
                message=f"Weekly topic summaries were already complete for {len(existing_topics)} weeks.",
            )
            return existing_topics
        if not self.client:
            topics = self._development_weekly_topic_summaries(candidates)
            repository.replace_telegram_preprocess_topics(
                self.session,
                run_id=run_id,
                project_id=self.project.id,
                chat_id=chat_id,
                topics=topics,
            )
            self.session.commit()
            return topics

        self._trace(
            "agent_started",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            message=f"Starting one-shot weekly topic summaries for {len(remaining_candidates)} remaining weeks.",
            weekly_summary_concurrency=self.weekly_summary_concurrency,
        )
        topics = list(existing_topics)
        total = max(len(candidates), 1)
        topic_index_by_candidate = {
            candidate.id: index
            for index, candidate in enumerate(candidates, start=1)
        }
        if self.weekly_summary_concurrency <= 1 or len(remaining_candidates) <= 1:
            for candidate in remaining_candidates:
                self._ensure_active()
                topics = self._upsert_topic_payload(
                    topics,
                    self._summarize_weekly_candidate_with_retries(
                        run_id,
                        candidate,
                        topic_index_by_candidate.get(candidate.id, len(topics) + 1),
                    ),
                )
                repository.replace_telegram_preprocess_topics(
                    self.session,
                    run_id=run_id,
                    project_id=self.project.id,
                    chat_id=chat_id,
                    topics=topics,
                )
                self.session.commit()
                self._progress(
                    progress_callback,
                    "weekly_topic_summary",
                    min(40 + int((len(topics) / total) * 34), 76),
                    {
                        "current_week": candidate.week_key,
                        "topic_count": len(topics),
                        "completed_week_count": len(topics),
                        "remaining_week_count": max(len(candidates) - len(topics), 0),
                        "weekly_candidate_count": len(candidates),
                        "weekly_summary_concurrency": self.weekly_summary_concurrency,
                        "usage": dict(self.usage_totals),
                    },
                )
        else:
            self._trace(
                "stage_progress",
                stage="weekly_topic_summary",
                message=f"Running weekly topic summaries with concurrency {self.weekly_summary_concurrency}.",
                weekly_summary_concurrency=self.weekly_summary_concurrency,
                remaining_week_count=len(remaining_candidates),
            )
            with ThreadPoolExecutor(max_workers=self.weekly_summary_concurrency, thread_name_prefix="telegram-weekly-summary") as executor:
                future_map = {
                    executor.submit(
                        self._run_parallel_weekly_topic_task,
                        run_id,
                        candidate,
                        topic_index_by_candidate.get(candidate.id, len(topics) + 1),
                    ): candidate
                    for candidate in remaining_candidates
                }
                for future in as_completed(future_map):
                    self._ensure_active()
                    candidate = future_map[future]
                    result = future.result()
                    self._add_usage(result.get("usage"))
                    topics = self._upsert_topic_payload(topics, dict(result.get("topic") or {}))
                    repository.replace_telegram_preprocess_topics(
                        self.session,
                        run_id=run_id,
                        project_id=self.project.id,
                        chat_id=chat_id,
                        topics=topics,
                    )
                    self.session.commit()
                    self._progress(
                        progress_callback,
                        "weekly_topic_summary",
                        min(40 + int((len(topics) / total) * 34), 76),
                        {
                            "current_week": candidate.week_key,
                            "topic_count": len(topics),
                            "completed_week_count": len(topics),
                            "remaining_week_count": max(len(candidates) - len(topics), 0),
                            "weekly_candidate_count": len(candidates),
                            "weekly_summary_concurrency": self.weekly_summary_concurrency,
                            "usage": dict(self.usage_totals),
                        },
                    )
        self._trace(
            "agent_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            message=f"Completed one-shot weekly topic summaries for {len(topics)} weeks.",
        )
        return topics

    def _run_parallel_weekly_topic_task(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
    ) -> dict[str, Any]:
        worker = TelegramPreprocessWorker(
            self.session,
            self.project,
            llm_config=self.llm_config,
            log_path=self.log_path,
            cancel_checker=self.cancel_checker,
            trace_callback=self._relay_non_persistent_trace,
        )
        topic = worker._summarize_weekly_candidate_with_retries(run_id, candidate, topic_index)
        return {"topic": topic, "usage": dict(worker.usage_totals)}

    def _relay_non_persistent_trace(self, event: dict[str, Any], _persist: bool = True) -> None:
        if self.trace_callback:
            self.trace_callback(dict(event or {}), False)

    def _summarize_weekly_candidate_with_retries(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, TELEGRAM_WEEKLY_AGENT_RETRIES + 1):
            try:
                return self._run_weekly_topic_agent(run_id, candidate, topic_index, attempt=attempt)
            except Exception as exc:
                last_error = exc
                self._trace(
                    "agent_retry",
                    stage="weekly_topic_summary",
                    agent="weekly_topic_agent",
                    week_key=candidate.week_key,
                    attempt=attempt,
                    max_attempts=TELEGRAM_WEEKLY_AGENT_RETRIES,
                    message=f"Weekly topic summary failed for {candidate.week_key}; retrying.",
                    error=str(exc),
                )
        raise RuntimeError(
            f"Weekly topic summary failed for {candidate.week_key}: {last_error}"
        ) from last_error

    def _run_weekly_topic_agent(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
        *,
        attempt: int,
    ) -> dict[str, Any]:
        assert self.client is not None
        del run_id
        self._ensure_active()
        request_key = f"weekly-{candidate.week_key}-attempt-{attempt}"
        label = f"Weekly topic summary {candidate.week_key}"
        stream_callback = self._build_stream_callback(
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            label=label,
            extra={"week_key": candidate.week_key, "candidate_id": candidate.id},
        )
        compact_lines = [
            _compact_message_line(_compact_message_payload(item))
            for item in list(candidate.sample_messages_json or [])
            if item.get("message_id") is not None
        ]
        participant_directory = [
            {
                "participant_id": item.get("participant_id"),
                "display_name": item.get("display_name"),
                "username": item.get("username"),
                "message_count": item.get("message_count"),
            }
            for item in list(candidate.top_participants_json or [])[:10]
        ]
        self._trace(
            "llm_request_started",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            prompt_preview=_preview_text(
                {
                    "week_key": candidate.week_key,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "compact_line_count": len(compact_lines),
                }
            ),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "You are the Telegram weekly topic summary agent.\n"
                        "You will receive one ISO week candidate as compact message lines already prepared from SQLite.\n"
                        "Do not ask for tools. Do not request more data. Summarize only from the provided lines.\n"
                        "Return JSON with keys: title, summary, keywords, evidence_message_ids, participants.\n"
                        "participants must be objects containing participant_id, role_hint, message_count, mention_count.\n"
                        "Use participant_id values from the provided participant directory when possible.\n"
                        "IMPORTANT: You must output all textual content (such as title, summary, keywords, role_hint) in Chinese (中文)."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        json.dumps(
                            {
                                "project": self.project.name,
                                "candidate_id": candidate.id,
                                "week_key": candidate.week_key,
                                "start_at": _safe_iso(candidate.start_at),
                                "end_at": _safe_iso(candidate.end_at),
                                "start_message_id": candidate.start_message_id,
                                "end_message_id": candidate.end_message_id,
                                "message_count": candidate.message_count,
                                "participant_count": candidate.participant_count,
                                "top_participants": participant_directory,
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\nCompact weekly messages:\n"
                        + "\n".join(compact_lines)
                    ),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.2,
            max_tokens=900,
            stream_handler=stream_callback,
        )
        flush_callback = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_callback):
            flush_callback()
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
        )
        parsed = parse_json_response(result.content or "", fallback=True)
        return self._normalize_weekly_topic(topic_index, candidate, parsed)

    @staticmethod
    def _weekly_tool_schemas() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_weekly_candidates",
                    "description": "List compact weekly candidates for the current preprocess run.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_weekly_candidate",
                    "description": "Fetch one weekly candidate and a compact page of message lines.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "candidate_id": {"type": "string"},
                            "week_key": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_top_users",
                    "description": "List the compact top-user snapshot already materialized by SQL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "analyze_database",
                    "description": "Analyze a compact weekly candidate page and return only summary, keywords, anchors, and participant hints.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "prompt": {"type": "string"},
                            "candidate_id": {"type": "string"},
                            "week_key": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                        "required": ["prompt"],
                    },
                },
            },
        ]

    def _execute_weekly_tool(
        self,
        run_id: str,
        default_candidate: TelegramPreprocessWeeklyTopicCandidate,
        name: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        if name == "list_weekly_candidates":
            limit = max(1, min(int(args.get("limit", 12) or 12), 24))
            offset = max(0, int(args.get("offset", 0) or 0))
            candidates = repository.list_telegram_preprocess_weekly_topic_candidates(
                self.session,
                self.project.id,
                run_id=run_id,
            )
            items = candidates[offset: offset + limit]
            return {
                "candidates": [
                    {
                        "candidate_id": item.id,
                        "week_key": item.week_key,
                        "start_at": _safe_iso(item.start_at),
                        "end_at": _safe_iso(item.end_at),
                        "message_count": item.message_count,
                        "participant_count": item.participant_count,
                        "top_participants": list(item.top_participants_json or [])[:6],
                    }
                    for item in items
                ],
                "total": len(candidates),
            }

        if name == "get_weekly_candidate":
            candidate = self._resolve_weekly_candidate(run_id, default_candidate, args)
            limit = max(1, min(int(args.get("limit", 40) or 40), TELEGRAM_WEEKLY_TOOL_MAX_MESSAGES))
            offset = max(0, int(args.get("offset", 0) or 0))
            lines = [
                _compact_message_line(item)
                for item in list(candidate.sample_messages_json or [])[offset: offset + limit]
            ]
            return {
                "candidate": {
                    "candidate_id": candidate.id,
                    "week_key": candidate.week_key,
                    "start_at": _safe_iso(candidate.start_at),
                    "end_at": _safe_iso(candidate.end_at),
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "top_participants": list(candidate.top_participants_json or [])[:8],
                },
                "lines": lines,
                "has_more": offset + limit < len(list(candidate.sample_messages_json or [])),
            }

        if name == "list_top_users":
            limit = max(1, min(int(args.get("limit", 10) or 10), TELEGRAM_ACTIVE_USER_LIMIT))
            offset = max(0, int(args.get("offset", 0) or 0))
            top_users = repository.list_telegram_preprocess_top_users(
                self.session,
                self.project.id,
                run_id=run_id,
            )
            return {
                "top_users": [
                    {
                        "rank": item.rank,
                        "participant_id": item.participant_id,
                        "uid": item.uid,
                        "username": item.username,
                        "display_name": item.display_name,
                        "message_count": item.message_count,
                    }
                    for item in top_users[offset: offset + limit]
                ],
                "total": len(top_users),
            }

        if name == "analyze_database":
            candidate = self._resolve_weekly_candidate(run_id, default_candidate, args)
            limit = max(1, min(int(args.get("limit", 60) or 60), TELEGRAM_WEEKLY_TOOL_MAX_MESSAGES))
            offset = max(0, int(args.get("offset", 0) or 0))
            scope = WeeklyCandidateScope(
                candidate=candidate,
                messages=[
                    _compact_message_payload(item)
                    for item in list(candidate.sample_messages_json or [])[offset: offset + limit]
                ],
            )
            prompt = str(args.get("prompt") or "").strip() or "Summarize this weekly Telegram candidate."
            return self._analyze_compact_scope(
                scope.messages,
                prompt,
                stage="weekly_topic_summary",
                agent="weekly_topic_analysis",
                request_key=f"weekly-analysis-{candidate.week_key}-{offset}-{limit}",
                label=f"Weekly candidate analysis {candidate.week_key}",
                extra={"week_key": candidate.week_key, "candidate_id": candidate.id},
            )

        return {"error": f"Unknown tool: {name}"}

    def _resolve_weekly_candidate(
        self,
        run_id: str,
        default_candidate: TelegramPreprocessWeeklyTopicCandidate,
        args: dict[str, Any],
    ) -> TelegramPreprocessWeeklyTopicCandidate:
        candidate_id = str(args.get("candidate_id") or "").strip()
        if candidate_id:
            candidate = repository.get_telegram_preprocess_weekly_topic_candidate(self.session, candidate_id)
            if candidate and candidate.project_id == self.project.id and candidate.run_id == run_id:
                return candidate
        week_key = str(args.get("week_key") or "").strip()
        if week_key:
            for item in repository.list_telegram_preprocess_weekly_topic_candidates(
                self.session,
                self.project.id,
                run_id=run_id,
            ):
                if item.week_key == week_key:
                    return item
        return default_candidate

    def _normalize_weekly_topic(
        self,
        topic_index: int,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        title = str(parsed.get("title") or "").strip() or f"Week {candidate.week_key}"
        summary = str(parsed.get("summary") or "").strip()
        if not summary:
            summary = (
                f"This weekly Telegram summary covers the busiest compact segment for {candidate.week_key} "
                f"and should be revisited with targeted evidence during downstream analysis."
            )
        evidence_ids = _coerce_message_ids(
            parsed.get("evidence_message_ids")
            or parsed.get("anchor_message_ids")
            or []
        )
        sample_messages = list(candidate.sample_messages_json or [])
        sample_by_id = {
            int(item["message_id"]): item
            for item in sample_messages
            if item.get("message_id") is not None
        }
        if not evidence_ids:
            evidence_ids = [
                int(item["message_id"])
                for item in sample_messages[:3]
                if item.get("message_id") is not None
            ]
        evidence = [
            {
                "message_id": message_id,
                "sender_name": sample_by_id.get(message_id, {}).get("sender_name"),
                "sent_at": sample_by_id.get(message_id, {}).get("sent_at"),
                "quote": sample_by_id.get(message_id, {}).get("text"),
            }
            for message_id in evidence_ids[:8]
            if message_id in sample_by_id
        ]
        keyword_candidates = parsed.get("keywords") or parsed.get("keywords_json") or []
        keywords = _dedupe_strings(list(keyword_candidates), limit=8)
        if not keywords:
            keywords = top_terms(
                "\n".join(str(item.get("text") or "") for item in sample_messages),
                limit=6,
            )
        participant_lookup = {
            str(item.get("participant_id") or ""): item
            for item in list(candidate.top_participants_json or [])
            if str(item.get("participant_id") or "").strip()
        }
        participants: list[dict[str, Any]] = []
        for item in parsed.get("participants") or []:
            if not isinstance(item, dict):
                continue
            participant_id = str(item.get("participant_id") or "").strip()
            if not participant_id:
                continue
            fallback = participant_lookup.get(participant_id, {})
            participants.append(
                {
                    "participant_id": participant_id,
                    "role_hint": str(item.get("role_hint") or "").strip() or None,
                    "message_count": int(item.get("message_count") or fallback.get("message_count") or 0),
                    "mention_count": int(item.get("mention_count") or 0),
                }
            )
        if not participants:
            participants = [
                {
                    "participant_id": participant_id,
                    "role_hint": None,
                    "message_count": int(payload.get("message_count") or 0),
                    "mention_count": 0,
                }
                for participant_id, payload in participant_lookup.items()
            ]
        return {
            "topic_index": topic_index,
            "title": title,
            "summary": summary,
            "start_at": candidate.start_at,
            "end_at": candidate.end_at,
            "start_message_id": candidate.start_message_id,
            "end_message_id": candidate.end_message_id,
            "message_count": candidate.message_count,
            "participant_count": max(candidate.participant_count, len(participants)),
            "keywords_json": keywords,
            "evidence_json": evidence,
            "participants": participants,
            "metadata_json": {
                "week_key": candidate.week_key,
                "source": "weekly_topic_agent",
                "candidate_id": candidate.id,
            },
        }

    def _development_weekly_topic_summaries(
        self,
        candidates: list[TelegramPreprocessWeeklyTopicCandidate],
    ) -> list[dict[str, Any]]:
        topics: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates, start=1):
            sample_messages = list(candidate.sample_messages_json or [])
            top_participants = list(candidate.top_participants_json or [])
            summary = "\n".join(str(item.get("text") or "") for item in sample_messages[:24])
            topics.append(
                {
                    "topic_index": index,
                    "title": f"Week {candidate.week_key}",
                    "summary": _compact_text(summary, limit=420),
                    "start_at": candidate.start_at,
                    "end_at": candidate.end_at,
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "keywords_json": top_terms(summary, limit=6),
                    "evidence_json": [
                        {
                            "message_id": item.get("message_id"),
                            "sender_name": item.get("sender_name"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                        }
                        for item in sample_messages[:3]
                    ],
                    "participants": [
                        {
                            "participant_id": item.get("participant_id"),
                            "role_hint": None,
                            "message_count": int(item.get("message_count") or 0),
                            "mention_count": 0,
                        }
                        for item in top_participants
                        if item.get("participant_id")
                    ],
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "source": "weekly_topic_sql_fallback",
                        "candidate_id": candidate.id,
                    },
                }
            )
        return topics

    def _build_active_users(
        self,
        run_id: str,
        chat_id: str,
        top_users: list[TelegramPreprocessTopUser],
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> list[dict[str, Any]]:
        if not top_users:
            return []
        self._trace(
            "agent_started",
            stage="active_users",
            agent="active_user_alias_agent",
            message=f"Deriving aliases for {len(top_users)} active Telegram users.",
        )
        active_users: list[dict[str, Any]] = []
        total = max(len(top_users), 1)
        for index, top_user in enumerate(top_users, start=1):
            self._ensure_active()
            alias_payload = self._infer_active_user_alias(chat_id, top_user)
            active_users.append(
                {
                    "rank": top_user.rank,
                    "participant_id": top_user.participant_id,
                    "uid": top_user.uid,
                    "username": top_user.username,
                    "display_name": top_user.display_name,
                    "primary_alias": alias_payload["primary_alias"],
                    "aliases_json": alias_payload["aliases_json"],
                    "message_count": top_user.message_count,
                    "first_seen_at": top_user.first_seen_at,
                    "last_seen_at": top_user.last_seen_at,
                    "evidence_json": alias_payload["evidence_json"],
                }
            )
            self._progress(
                progress_callback,
                "active_users",
                min(78 + int((index / total) * 14), 92),
                {
                    "active_user_count": len(active_users),
                    "current_user_rank": top_user.rank,
                    "current_user_label": top_user.display_name or top_user.username or top_user.uid or top_user.participant_id,
                },
            )
        self._trace(
            "agent_completed",
            stage="active_users",
            agent="active_user_alias_agent",
            message=f"Derived aliases for {len(active_users)} active Telegram users.",
        )
        return active_users

    def _infer_active_user_alias(
        self,
        chat_id: str,
        top_user: TelegramPreprocessTopUser,
    ) -> dict[str, Any]:
        if not self.client:
            primary_alias = (
                top_user.display_name
                or top_user.username
                or top_user.uid
                or top_user.participant_id
            )
            return {
                "primary_alias": primary_alias,
                "aliases_json": _dedupe_strings(
                    [primary_alias, top_user.display_name, top_user.username, top_user.uid],
                    limit=6,
                ),
                "evidence_json": [],
            }

        last_error: Exception | None = None
        for attempt in range(1, TELEGRAM_ALIAS_AGENT_RETRIES + 1):
            try:
                return self._run_active_user_alias_agent(chat_id, top_user, attempt=attempt)
            except Exception as exc:
                last_error = exc
                self._trace(
                    "agent_retry",
                    stage="active_users",
                    agent="active_user_alias_agent",
                    participant_id=top_user.participant_id,
                    attempt=attempt,
                    max_attempts=TELEGRAM_ALIAS_AGENT_RETRIES,
                    message=f"Alias inference failed for rank #{top_user.rank}; retrying.",
                    error=str(exc),
                )
        raise RuntimeError(
            f"Alias inference failed for participant {top_user.participant_id}: {last_error}"
        ) from last_error

    def _run_active_user_alias_agent(
        self,
        chat_id: str,
        top_user: TelegramPreprocessTopUser,
        *,
        attempt: int,
    ) -> dict[str, Any]:
        assert self.client is not None
        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": (
                    "You are the Telegram active-user alias agent.\n"
                    "Infer the primary alias and common nicknames for one user.\n"
                    "You must use tools before answering.\n"
                    "Use get_user_slice, get_user_mentions_slice, and analyze_database.\n"
                    "Return JSON with keys primary_alias, aliases, evidence_message_ids."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "project": self.project.name,
                        "rank": top_user.rank,
                        "participant_id": top_user.participant_id,
                        "uid": top_user.uid,
                        "username": top_user.username,
                        "display_name": top_user.display_name,
                        "message_count": top_user.message_count,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            },
        ]
        for round_index in range(1, TELEGRAM_ALIAS_AGENT_MAX_ITERATIONS + 1):
            self._ensure_active()
            request_key = f"active-user-{top_user.rank}-attempt-{attempt}-round-{round_index}"
            self._trace(
                "llm_request_started",
                stage="active_users",
                agent="active_user_alias_agent",
                request_key=request_key,
                round_index=round_index,
                participant_id=top_user.participant_id,
                request_kind="tool_round",
                label=f"Active user alias #{top_user.rank} round {round_index}",
                tool_names=[tool["function"]["name"] for tool in self._active_user_tool_schemas()],
            )
            round_result = self.client.tool_round(
                messages,
                self._active_user_tool_schemas(),
                model=self.llm_config.model if self.llm_config else None,
                temperature=0.2,
                max_tokens=700,
            )
            self._add_usage(round_result.usage)
            self._trace(
                "llm_request_completed",
                stage="active_users",
                agent="active_user_alias_agent",
                request_key=request_key,
                round_index=round_index,
                participant_id=top_user.participant_id,
                request_kind="tool_round",
                label=f"Active user alias #{top_user.rank} round {round_index}",
                usage=round_result.usage,
                response_text_preview=_preview_text(round_result.content),
                tool_calls=[
                    {"name": call.name, "arguments": call.arguments}
                    for call in round_result.tool_calls
                ],
            )
            if not round_result.tool_calls:
                parsed = parse_json_response(round_result.content or "", fallback=True)
                return self._normalize_active_user_alias(chat_id, top_user, parsed)

            messages.append(
                {
                    "role": "assistant",
                    "content": round_result.content,
                    "tool_calls": [
                        {
                            "id": call.id,
                            "name": call.name,
                            "arguments_json": call.arguments_json,
                        }
                        for call in round_result.tool_calls
                    ],
                }
            )
            for call in round_result.tool_calls:
                self._trace(
                    "tool_call",
                    stage="active_users",
                    agent="active_user_alias_agent",
                    request_key=request_key,
                    round_index=round_index,
                    participant_id=top_user.participant_id,
                    tool_name=call.name,
                    arguments_preview=_preview_text(call.arguments),
                )
                output = self._execute_active_user_tool(chat_id, top_user, call.name, call.arguments)
                self._trace(
                    "tool_result",
                    stage="active_users",
                    agent="active_user_alias_agent",
                    request_key=request_key,
                    round_index=round_index,
                    participant_id=top_user.participant_id,
                    tool_name=call.name,
                    output_preview=_preview_text(output),
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "name": call.name,
                        "content": json.dumps(output, ensure_ascii=False),
                    }
                )
        raise LLMError(f"Active user alias agent exceeded the maximum rounds for {top_user.participant_id}.")

    @staticmethod
    def _active_user_tool_schemas() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_user_slice",
                    "description": "Fetch a compact page of the user's own non-service messages.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_user_mentions_slice",
                    "description": "Fetch a compact page of messages where other users appear to mention this user.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "analyze_database",
                    "description": "Analyze a compact alias-evidence slice and return only summary, keywords, anchors, and alias hints.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "prompt": {"type": "string"},
                            "participant_id": {"type": "string"},
                            "slice_kind": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                        "required": ["prompt"],
                    },
                },
            },
        ]

    def _execute_active_user_tool(
        self,
        chat_id: str,
        top_user: TelegramPreprocessTopUser,
        name: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        participant_id = str(args.get("participant_id") or top_user.participant_id).strip() or top_user.participant_id
        if name == "get_user_slice":
            limit = max(1, min(int(args.get("limit", 12) or 12), TELEGRAM_ALIAS_TOOL_MAX_MESSAGES))
            offset = max(0, int(args.get("offset", 0) or 0))
            messages = self._query_user_messages(chat_id, participant_id, limit=limit, offset=offset)
            return {
                "lines": [_compact_message_line(item) for item in messages],
                "has_more": len(messages) == limit,
            }

        if name == "get_user_mentions_slice":
            limit = max(1, min(int(args.get("limit", 12) or 12), TELEGRAM_ALIAS_TOOL_MAX_MESSAGES))
            offset = max(0, int(args.get("offset", 0) or 0))
            messages = self._query_user_mentions(chat_id, top_user, limit=limit, offset=offset)
            return {
                "lines": [_compact_message_line(item) for item in messages],
                "has_more": len(messages) == limit,
            }

        if name == "analyze_database":
            prompt = str(args.get("prompt") or "").strip() or "Infer the user's most common alias."
            slice_kind = str(args.get("slice_kind") or "self").strip().lower()
            limit = max(1, min(int(args.get("limit", 12) or 12), TELEGRAM_ALIAS_TOOL_MAX_MESSAGES))
            offset = max(0, int(args.get("offset", 0) or 0))
            if slice_kind == "mentions":
                messages = self._query_user_mentions(chat_id, top_user, limit=limit, offset=offset)
            else:
                messages = self._query_user_messages(chat_id, participant_id, limit=limit, offset=offset)
            return self._analyze_compact_scope(
                messages,
                prompt,
                stage="active_users",
                agent="active_user_alias_analysis",
                request_key=f"active-user-analysis-{top_user.rank}-{slice_kind}-{offset}-{limit}",
                label=f"Active user alias analysis #{top_user.rank}",
                extra={"participant_id": top_user.participant_id, "slice_kind": slice_kind},
            )

        return {"error": f"Unknown tool: {name}"}

    def _query_user_messages(
        self,
        chat_id: str,
        participant_id: str,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        rows = self.session.execute(
            select(
                TelegramMessage.telegram_message_id,
                TelegramMessage.participant_id,
                TelegramMessage.sender_name,
                TelegramMessage.sent_at,
                TelegramMessage.text_normalized,
            )
            .where(
                TelegramMessage.project_id == self.project.id,
                TelegramMessage.chat_id == chat_id,
                TelegramMessage.participant_id == participant_id,
                TelegramMessage.message_type != "service",
            )
            .order_by(TelegramMessage.telegram_message_id.asc())
            .offset(offset)
            .limit(limit)
        ).all()
        return [
            {
                "message_id": row[0],
                "participant_id": row[1],
                "sender_name": row[2],
                "sent_at": _safe_iso(row[3]),
                "text": _compact_text(row[4]),
            }
            for row in rows
            if row[0] is not None
        ]

    def _query_user_mentions(
        self,
        chat_id: str,
        top_user: TelegramPreprocessTopUser,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        mention_terms = _dedupe_strings(
            [top_user.display_name, top_user.username, top_user.uid],
            limit=4,
        )
        if not mention_terms:
            return []
        conditions = [TelegramMessage.text_normalized.ilike(f"%{term}%") for term in mention_terms]
        rows = self.session.execute(
            select(
                TelegramMessage.telegram_message_id,
                TelegramMessage.participant_id,
                TelegramMessage.sender_name,
                TelegramMessage.sent_at,
                TelegramMessage.text_normalized,
            )
            .where(
                TelegramMessage.project_id == self.project.id,
                TelegramMessage.chat_id == chat_id,
                TelegramMessage.message_type != "service",
                TelegramMessage.participant_id != top_user.participant_id,
                or_(*conditions),
            )
            .order_by(TelegramMessage.telegram_message_id.asc())
            .offset(offset)
            .limit(limit)
        ).all()
        return [
            {
                "message_id": row[0],
                "participant_id": row[1],
                "sender_name": row[2],
                "sent_at": _safe_iso(row[3]),
                "text": _compact_text(row[4]),
            }
            for row in rows
            if row[0] is not None
        ]

    def _normalize_active_user_alias(
        self,
        chat_id: str,
        top_user: TelegramPreprocessTopUser,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        primary_alias = (
            str(parsed.get("primary_alias") or "").strip()
            or top_user.display_name
            or top_user.username
            or top_user.uid
            or top_user.participant_id
        )
        aliases = _dedupe_strings(
            list(parsed.get("aliases") or parsed.get("aliases_json") or [])
            + [primary_alias, top_user.display_name, top_user.username, top_user.uid],
            limit=8,
        )
        evidence_ids = _coerce_message_ids(parsed.get("evidence_message_ids") or [])
        evidence_messages: list[dict[str, Any]] = []
        if evidence_ids:
            for message_id in evidence_ids[:6]:
                message = repository.get_telegram_message_by_telegram_id(self.session, self.project.id, message_id)
                if not message or message.chat_id != chat_id:
                    continue
                evidence_messages.append(
                    {
                        "message_id": message_id,
                        "sender_name": message.sender_name,
                        "sent_at": _safe_iso(message.sent_at),
                        "quote": _compact_text(message.text_normalized),
                    }
                )
        return {
            "primary_alias": primary_alias,
            "aliases_json": aliases,
            "evidence_json": evidence_messages,
        }

    def _analyze_compact_scope(
        self,
        messages: list[dict[str, Any]],
        prompt: str,
        *,
        stage: str,
        agent: str,
        request_key: str,
        label: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_messages = [
            _compact_message_payload(item)
            for item in messages
            if item.get("message_id") is not None
        ]
        if not normalized_messages:
            return {
                "analysis": {
                    "summary": "No matching compact rows were found for this request.",
                    "keywords": [],
                    "anchor_message_ids": [],
                    "participant_hints": [],
                }
            }

        if not self.client:
            joined = "\n".join(_compact_message_line(item) for item in normalized_messages)
            return {
                "analysis": {
                    "summary": _compact_text(f"{prompt} {joined}", limit=320),
                    "keywords": top_terms(joined, limit=6),
                    "anchor_message_ids": [
                        int(item["message_id"])
                        for item in normalized_messages[:5]
                    ],
                    "participant_hints": _dedupe_strings(
                        [item.get("sender_name") for item in normalized_messages],
                        limit=6,
                    ),
                }
            }

        stream_callback = self._build_stream_callback(
            stage=stage,
            agent=agent,
            request_key=request_key,
            label=label,
            extra=extra,
        )
        self._trace(
            "llm_request_started",
            stage=stage,
            agent=agent,
            request_key=request_key,
            request_kind="chat_completion",
            label=label,
            prompt_preview=_preview_text(prompt),
            **dict(extra or {}),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "Analyze a compact Telegram SQL slice.\n"
                        "Return JSON with keys summary, keywords, anchor_message_ids, participant_hints."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Prompt:\n{prompt}\n\n"
                        f"Compact rows:\n" + "\n".join(_compact_message_line(item) for item in normalized_messages)
                    ),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.2,
            max_tokens=600,
            stream_handler=stream_callback,
        )
        flush_callback = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_callback):
            flush_callback()
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage=stage,
            agent=agent,
            request_key=request_key,
            request_kind="chat_completion",
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
            **dict(extra or {}),
        )
        parsed = parse_json_response(result.content or "", fallback=True)
        anchor_ids = _coerce_message_ids(
            parsed.get("anchor_message_ids")
            or parsed.get("evidence_message_ids")
            or []
        )
        if not anchor_ids:
            anchor_ids = [
                int(item["message_id"])
                for item in normalized_messages[:5]
            ]
        return {
            "analysis": {
                "summary": str(parsed.get("summary") or "").strip() or _compact_text(prompt, limit=240),
                "keywords": _dedupe_strings(list(parsed.get("keywords") or []), limit=8),
                "anchor_message_ids": anchor_ids[:8],
                "participant_hints": _dedupe_strings(
                    list(parsed.get("participant_hints") or []),
                    limit=8,
                ),
            }
        }

    def _run_weekly_topic_agent(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
        *,
        attempt: int,
    ) -> dict[str, Any]:
        assert self.client is not None
        del run_id
        self._ensure_active()
        request_key = f"weekly-{candidate.week_key}-attempt-{attempt}"
        label = f"Weekly topic summary {candidate.week_key}"
        stream_callback = self._build_stream_callback(
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            label=label,
            extra={"week_key": candidate.week_key, "candidate_id": candidate.id},
        )
        compact_lines = [
            _compact_message_line(_compact_message_payload(item))
            for item in list(candidate.sample_messages_json or [])
            if item.get("message_id") is not None
        ]
        participant_directory = [
            {
                "participant_id": item.get("participant_id"),
                "display_name": item.get("display_name"),
                "username": item.get("username"),
                "message_count": item.get("message_count"),
            }
            for item in list(candidate.top_participants_json or [])[:10]
        ]
        self._trace(
            "llm_request_started",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            prompt_preview=_preview_text(
                {
                    "week_key": candidate.week_key,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "compact_line_count": len(compact_lines),
                }
            ),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "你是 Telegram 群聊的单周话题总结 Agent。\n"
                        "你会收到一个已经由 SQLite 预聚合好的 ISO week 候选片段。\n"
                        "只能依据提供的紧凑消息行总结，不要请求工具，不要索要额外数据，也不要脑补片段外的信息。\n"
                        "请返回 JSON，包含键：title, summary, keywords, evidence_message_ids, participants。\n"
                        "participants 必须是对象数组，每个对象包含 participant_id, role_hint, message_count, mention_count。\n"
                        "如果 participant directory 里已经给出了 participant_id，优先复用这些 id。\n"
                        "除 JSON 键名外，所有可读文本都必须使用简体中文。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        json.dumps(
                            {
                                "project": self.project.name,
                                "candidate_id": candidate.id,
                                "week_key": candidate.week_key,
                                "start_at": _safe_iso(candidate.start_at),
                                "end_at": _safe_iso(candidate.end_at),
                                "start_message_id": candidate.start_message_id,
                                "end_message_id": candidate.end_message_id,
                                "message_count": candidate.message_count,
                                "participant_count": candidate.participant_count,
                                "top_participants": participant_directory,
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\nCompact weekly messages:\n"
                        + "\n".join(compact_lines)
                    ),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.2,
            max_tokens=900,
            stream_handler=stream_callback,
        )
        flush_callback = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_callback):
            flush_callback()
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
        )
        parsed = parse_json_response(result.content or "", fallback=True)
        return self._normalize_weekly_topic(topic_index, candidate, parsed)

    def _normalize_weekly_topic(
        self,
        topic_index: int,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        title = str(parsed.get("title") or "").strip() or f"{candidate.week_key} 周话题"
        if title.lower().startswith("week "):
            title = f"{candidate.week_key} 周话题"
        summary = str(parsed.get("summary") or "").strip()
        if not summary:
            summary = (
                f"这份周话题总结覆盖了 {candidate.week_key} 最活跃的消息片段，"
                "后续十维分析应再按证据消息做精确回查。"
            )
        evidence_ids = _coerce_message_ids(
            parsed.get("evidence_message_ids")
            or parsed.get("anchor_message_ids")
            or []
        )
        sample_messages = list(candidate.sample_messages_json or [])
        sample_by_id = {
            int(item["message_id"]): item
            for item in sample_messages
            if item.get("message_id") is not None
        }
        if not evidence_ids:
            evidence_ids = [
                int(item["message_id"])
                for item in sample_messages[:3]
                if item.get("message_id") is not None
            ]
        evidence = [
            {
                "message_id": message_id,
                "sender_name": sample_by_id.get(message_id, {}).get("sender_name"),
                "sent_at": sample_by_id.get(message_id, {}).get("sent_at"),
                "quote": sample_by_id.get(message_id, {}).get("text"),
            }
            for message_id in evidence_ids[:8]
            if message_id in sample_by_id
        ]
        keyword_candidates = parsed.get("keywords") or parsed.get("keywords_json") or []
        keywords = _dedupe_strings(list(keyword_candidates), limit=8)
        if not keywords:
            keywords = top_terms(
                "\n".join(str(item.get("text") or "") for item in sample_messages),
                limit=6,
            )
        participant_lookup = {
            str(item.get("participant_id") or ""): item
            for item in list(candidate.top_participants_json or [])
            if str(item.get("participant_id") or "").strip()
        }
        participants: list[dict[str, Any]] = []
        for item in parsed.get("participants") or []:
            if not isinstance(item, dict):
                continue
            participant_id = str(item.get("participant_id") or "").strip()
            if not participant_id:
                continue
            fallback = participant_lookup.get(participant_id, {})
            participants.append(
                {
                    "participant_id": participant_id,
                    "role_hint": str(item.get("role_hint") or "").strip() or None,
                    "message_count": int(item.get("message_count") or fallback.get("message_count") or 0),
                    "mention_count": int(item.get("mention_count") or 0),
                }
            )
        if not participants:
            participants = [
                {
                    "participant_id": participant_id,
                    "role_hint": None,
                    "message_count": int(payload.get("message_count") or 0),
                    "mention_count": 0,
                }
                for participant_id, payload in participant_lookup.items()
            ]
        return {
            "topic_index": topic_index,
            "title": title,
            "summary": summary,
            "start_at": candidate.start_at,
            "end_at": candidate.end_at,
            "start_message_id": candidate.start_message_id,
            "end_message_id": candidate.end_message_id,
            "message_count": candidate.message_count,
            "participant_count": max(candidate.participant_count, len(participants)),
            "keywords_json": keywords,
            "evidence_json": evidence,
            "participants": participants,
            "metadata_json": {
                "week_key": candidate.week_key,
                "source": "weekly_topic_agent",
                "candidate_id": candidate.id,
            },
        }

    def _development_weekly_topic_summaries(
        self,
        candidates: list[TelegramPreprocessWeeklyTopicCandidate],
    ) -> list[dict[str, Any]]:
        topics: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates, start=1):
            sample_messages = list(candidate.sample_messages_json or [])
            top_participants = list(candidate.top_participants_json or [])
            summary = "\n".join(str(item.get("text") or "") for item in sample_messages[:24])
            topics.append(
                {
                    "topic_index": index,
                    "title": f"{candidate.week_key} 周话题",
                    "summary": _compact_text(summary, limit=420),
                    "start_at": candidate.start_at,
                    "end_at": candidate.end_at,
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "keywords_json": top_terms(summary, limit=6),
                    "evidence_json": [
                        {
                            "message_id": item.get("message_id"),
                            "sender_name": item.get("sender_name"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                        }
                        for item in sample_messages[:3]
                    ],
                    "participants": [
                        {
                            "participant_id": item.get("participant_id"),
                            "role_hint": None,
                            "message_count": int(item.get("message_count") or 0),
                            "mention_count": 0,
                        }
                        for item in top_participants
                        if item.get("participant_id")
                    ],
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "source": "weekly_topic_sql_fallback",
                        "candidate_id": candidate.id,
                    },
                }
            )
        return topics

    def _run_weekly_topic_agent(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
        *,
        attempt: int,
    ) -> dict[str, Any]:
        assert self.client is not None
        del run_id
        self._ensure_active()
        request_key = f"weekly-{candidate.week_key}-attempt-{attempt}"
        label = f"Weekly topic summary {candidate.week_key}"
        stream_callback = self._build_stream_callback(
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            label=label,
            extra={"week_key": candidate.week_key, "candidate_id": candidate.id},
        )
        compact_lines = [
            _compact_message_line(_compact_message_payload(item))
            for item in list(candidate.sample_messages_json or [])
            if item.get("message_id") is not None
        ]
        participant_directory = [
            {
                "participant_id": item.get("participant_id"),
                "display_name": item.get("display_name"),
                "username": item.get("username"),
                "message_count": item.get("message_count"),
            }
            for item in list(candidate.top_participants_json or [])[:10]
        ]
        self._trace(
            "llm_request_started",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            prompt_preview=_preview_text(
                {
                    "week_key": candidate.week_key,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "compact_line_count": len(compact_lines),
                }
            ),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "你是 Telegram 群聊的周话题总结 Agent。\n"
                        "你会收到一个已经由 SQLite 预聚合好的单周候选片段，内容是紧凑消息行。\n"
                        "不要请求工具，不要索要更多数据，只能基于当前提供的消息完成总结。\n"
                        "请把总结写得足够详细，让后续十维分析 Agent 仅看话题表就能先判断哪些周值得进一步回原始消息。\n"
                        "尤其要说明每个关键参与者在该话题中的观点、立场、作用、争议点或推进动作。\n"
                        "请只返回 JSON，包含这些键：\n"
                        "title, summary, keywords, evidence_message_ids, participants, participant_viewpoints, subtopics, interaction_patterns。\n"
                        "summary 需要是详细中文摘要，至少覆盖话题主旨、讨论推进、参与者分工或立场、结果或悬而未决的问题。\n"
                        "participants 必须是对象数组，每个对象都要包含 participant_id, role_hint, message_count, mention_count。\n"
                        "participant_viewpoints 必须是对象数组，每个对象尽量包含 participant_id, display_name, stance_summary, notable_points, evidence_message_ids。\n"
                        "subtopics 是该周内部的关键子话题数组；interaction_patterns 是互动模式数组，比如谁在提问、谁在推进、谁在反驳、谁在附和。\n"
                        "如果 participant directory 已提供 participant_id，请优先复用；如果某人没有明确观点，就写他的功能性作用，不要编造。\n"
                        "除 JSON 键名外，所有可读文本都请尽量使用简体中文，不要输出 JSON 之外的说明。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        json.dumps(
                            {
                                "project": self.project.name,
                                "candidate_id": candidate.id,
                                "week_key": candidate.week_key,
                                "start_at": _safe_iso(candidate.start_at),
                                "end_at": _safe_iso(candidate.end_at),
                                "start_message_id": candidate.start_message_id,
                                "end_message_id": candidate.end_message_id,
                                "message_count": candidate.message_count,
                                "participant_count": candidate.participant_count,
                                "top_participants": participant_directory,
                                "task_hint": "请优先梳理每个参与者在本周核心话题中的观点或立场，再总结整体话题脉络。",
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\nCompact weekly messages:\n"
                        + "\n".join(compact_lines)
                    ),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.2,
            max_tokens=1200,
            stream_handler=stream_callback,
        )
        flush_callback = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_callback):
            flush_callback()
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
        )
        parsed = parse_json_response(result.content or "", fallback=True)
        return self._normalize_weekly_topic(topic_index, candidate, parsed)

    def _normalize_weekly_topic(
        self,
        topic_index: int,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        title = str(parsed.get("title") or "").strip() or f"{candidate.week_key} 周话题"
        if title.lower().startswith("week "):
            title = f"{candidate.week_key} 周话题"
        summary = str(parsed.get("summary") or "").strip()
        if not summary:
            summary = (
                f"这份周话题总结覆盖 {candidate.week_key} 的高活跃片段，"
                "后续十维分析应再按证据消息做精确回查。"
            )
        evidence_ids = _coerce_message_ids(
            parsed.get("evidence_message_ids")
            or parsed.get("anchor_message_ids")
            or []
        )
        sample_messages = list(candidate.sample_messages_json or [])
        sample_by_id = {
            int(item["message_id"]): item
            for item in sample_messages
            if item.get("message_id") is not None
        }
        if not evidence_ids:
            evidence_ids = [
                int(item["message_id"])
                for item in sample_messages[:3]
                if item.get("message_id") is not None
            ]
        evidence = [
            {
                "message_id": message_id,
                "sender_name": sample_by_id.get(message_id, {}).get("sender_name"),
                "sent_at": sample_by_id.get(message_id, {}).get("sent_at"),
                "quote": sample_by_id.get(message_id, {}).get("text"),
            }
            for message_id in evidence_ids[:8]
            if message_id in sample_by_id
        ]
        keyword_candidates = parsed.get("keywords") or parsed.get("keywords_json") or []
        keywords = _dedupe_strings(list(keyword_candidates), limit=8)
        if not keywords:
            keywords = top_terms(
                "\n".join(str(item.get("text") or "") for item in sample_messages),
                limit=6,
            )
        participant_lookup = {
            str(item.get("participant_id") or ""): item
            for item in list(candidate.top_participants_json or [])
            if str(item.get("participant_id") or "").strip()
        }
        participant_name_lookup = {
            str(item.get("display_name") or "").strip().lower(): str(item.get("participant_id") or "").strip()
            for item in list(candidate.top_participants_json or [])
            if str(item.get("display_name") or "").strip() and str(item.get("participant_id") or "").strip()
        }
        participant_viewpoints: list[dict[str, Any]] = []
        for item in parsed.get("participant_viewpoints") or []:
            if not isinstance(item, dict):
                continue
            participant_id = str(item.get("participant_id") or "").strip()
            display_name = str(item.get("display_name") or item.get("name") or "").strip()
            if not participant_id and display_name:
                participant_id = participant_name_lookup.get(display_name.lower(), "")
            stance_summary = str(
                item.get("stance_summary")
                or item.get("viewpoint")
                or item.get("opinion")
                or item.get("summary")
                or ""
            ).strip()
            notable_points = _dedupe_strings(
                list(item.get("notable_points") or item.get("points") or []),
                limit=6,
            )
            viewpoint_evidence_ids = _coerce_message_ids(
                item.get("evidence_message_ids")
                or item.get("anchor_message_ids")
                or []
            )[:6]
            if not participant_id and not display_name:
                continue
            if not display_name and participant_id in participant_lookup:
                display_name = str(participant_lookup[participant_id].get("display_name") or "").strip()
            participant_viewpoints.append(
                {
                    "participant_id": participant_id or None,
                    "display_name": display_name or None,
                    "stance_summary": stance_summary or None,
                    "notable_points": notable_points,
                    "evidence_message_ids": viewpoint_evidence_ids,
                }
            )
        participant_viewpoint_by_id = {
            str(item.get("participant_id") or ""): item
            for item in participant_viewpoints
            if str(item.get("participant_id") or "").strip()
        }
        participants: list[dict[str, Any]] = []
        for item in parsed.get("participants") or []:
            if not isinstance(item, dict):
                continue
            participant_id = str(item.get("participant_id") or "").strip()
            if not participant_id:
                continue
            fallback = participant_lookup.get(participant_id, {})
            viewpoint = participant_viewpoint_by_id.get(participant_id, {})
            participants.append(
                {
                    "participant_id": participant_id,
                    "role_hint": (
                        str(item.get("role_hint") or "").strip()
                        or str(viewpoint.get("stance_summary") or "").strip()
                        or None
                    ),
                    "message_count": int(item.get("message_count") or fallback.get("message_count") or 0),
                    "mention_count": int(item.get("mention_count") or 0),
                }
            )
        if not participants:
            participants = [
                {
                    "participant_id": participant_id,
                    "role_hint": (
                        str(participant_viewpoint_by_id.get(participant_id, {}).get("stance_summary") or "").strip()
                        or None
                    ),
                    "message_count": int(payload.get("message_count") or 0),
                    "mention_count": 0,
                }
                for participant_id, payload in participant_lookup.items()
            ]
        subtopics = _dedupe_strings(
            list(parsed.get("subtopics") or parsed.get("topic_segments") or []),
            limit=8,
        )
        interaction_patterns = _dedupe_strings(
            list(parsed.get("interaction_patterns") or parsed.get("interaction_notes") or []),
            limit=8,
        )
        return {
            "topic_index": topic_index,
            "title": title,
            "summary": summary,
            "start_at": candidate.start_at,
            "end_at": candidate.end_at,
            "start_message_id": candidate.start_message_id,
            "end_message_id": candidate.end_message_id,
            "message_count": candidate.message_count,
            "participant_count": max(candidate.participant_count, len(participants)),
            "keywords_json": keywords,
            "evidence_json": evidence,
            "participants": participants,
            "metadata_json": {
                "week_key": candidate.week_key,
                "source": "weekly_topic_agent",
                "candidate_id": candidate.id,
                "participant_viewpoints": participant_viewpoints,
                "subtopics": subtopics,
                "interaction_patterns": interaction_patterns,
                "detailed_summary": summary,
            },
        }

    def _development_weekly_topic_summaries(
        self,
        candidates: list[TelegramPreprocessWeeklyTopicCandidate],
    ) -> list[dict[str, Any]]:
        topics: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates, start=1):
            sample_messages = list(candidate.sample_messages_json or [])
            top_participants = list(candidate.top_participants_json or [])
            summary = "\n".join(str(item.get("text") or "") for item in sample_messages[:24])
            compact_summary = _compact_text(summary, limit=420)
            topics.append(
                {
                    "topic_index": index,
                    "title": f"{candidate.week_key} 周话题",
                    "summary": compact_summary,
                    "start_at": candidate.start_at,
                    "end_at": candidate.end_at,
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "keywords_json": top_terms(summary, limit=6),
                    "evidence_json": [
                        {
                            "message_id": item.get("message_id"),
                            "sender_name": item.get("sender_name"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                        }
                        for item in sample_messages[:3]
                    ],
                    "participants": [
                        {
                            "participant_id": item.get("participant_id"),
                            "role_hint": None,
                            "message_count": int(item.get("message_count") or 0),
                            "mention_count": 0,
                        }
                        for item in top_participants
                        if item.get("participant_id")
                    ],
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "source": "weekly_topic_sql_fallback",
                        "candidate_id": candidate.id,
                        "participant_viewpoints": [
                            {
                                "participant_id": item.get("participant_id"),
                                "display_name": item.get("display_name"),
                                "stance_summary": None,
                                "notable_points": [],
                                "evidence_message_ids": [],
                            }
                            for item in top_participants
                            if item.get("participant_id")
                        ],
                        "subtopics": top_terms(summary, limit=4),
                        "interaction_patterns": [],
                        "detailed_summary": compact_summary,
                    },
                }
            )
        return topics

    def _build_active_users(
        self,
        run_id: str,
        chat_id: str,
        top_users: list[TelegramPreprocessTopUser],
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> list[dict[str, Any]]:
        del run_id, chat_id
        if not top_users:
            return []
        active_users = [self._build_active_user_payload(item) for item in top_users]
        self._trace(
            "stage_progress",
            stage="active_users",
            message="Built final active users directly from the SQL top-user snapshot without alias LLM calls.",
            active_user_count=len(active_users),
        )
        self._progress(
            progress_callback,
            "active_users",
            92,
            {
                "active_user_count": len(active_users),
                "usage": dict(self.usage_totals),
            },
        )
        return active_users

    def _run_weekly_topic_agent(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        topic_index: int,
        *,
        attempt: int,
    ) -> dict[str, Any]:
        assert self.client is not None
        del run_id
        self._ensure_active()
        request_key = f"weekly-{candidate.week_key}-attempt-{attempt}"
        label = f"Weekly topic summary {candidate.week_key}"
        stream_callback = self._build_stream_callback(
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            label=label,
            extra={"week_key": candidate.week_key, "candidate_id": candidate.id},
        )
        compact_lines = [
            _compact_message_line(_compact_message_payload(item))
            for item in list(candidate.sample_messages_json or [])
            if item.get("message_id") is not None
        ]
        participant_directory = [
            {
                "participant_id": item.get("participant_id"),
                "display_name": item.get("display_name"),
                "username": item.get("username"),
                "message_count": item.get("message_count"),
            }
            for item in list(candidate.top_participants_json or [])[:10]
        ]
        self._trace(
            "llm_request_started",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            prompt_preview=_preview_text(
                {
                    "week_key": candidate.week_key,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "compact_line_count": len(compact_lines),
                }
            ),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "你是 Telegram 群聊的周话题总结 Agent。\n"
                        "你会收到一个已经由 SQLite 预聚合好的单周候选片段，内容是紧凑消息行。\n"
                        "不要请求工具，不要索要更多数据，只能根据当前提供的消息进行总结。\n"
                        "请只返回 JSON，包含这些键：title, summary, keywords, evidence_message_ids, participants。\n"
                        "participants 必须是对象数组，每个对象都要包含 participant_id, role_hint, message_count, mention_count。\n"
                        "如果 participant directory 已提供 participant_id，请优先复用。\n"
                        "除 JSON 键名外，所有可读文本都请尽量使用简体中文。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        json.dumps(
                            {
                                "project": self.project.name,
                                "candidate_id": candidate.id,
                                "week_key": candidate.week_key,
                                "start_at": _safe_iso(candidate.start_at),
                                "end_at": _safe_iso(candidate.end_at),
                                "start_message_id": candidate.start_message_id,
                                "end_message_id": candidate.end_message_id,
                                "message_count": candidate.message_count,
                                "participant_count": candidate.participant_count,
                                "top_participants": participant_directory,
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\nCompact weekly messages:\n"
                        + "\n".join(compact_lines)
                    ),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.2,
            max_tokens=900,
            stream_handler=stream_callback,
        )
        flush_callback = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_callback):
            flush_callback()
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            request_key=request_key,
            week_key=candidate.week_key,
            request_kind="chat_completion",
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
        )
        parsed = parse_json_response(result.content or "", fallback=True)
        return self._normalize_weekly_topic(topic_index, candidate, parsed)

    def _normalize_weekly_topic(
        self,
        topic_index: int,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        title = str(parsed.get("title") or "").strip() or f"{candidate.week_key} 周话题"
        if title.lower().startswith("week "):
            title = f"{candidate.week_key} 周话题"
        summary = str(parsed.get("summary") or "").strip()
        if not summary:
            summary = (
                f"这份周话题总结覆盖了 {candidate.week_key} 的高活跃片段，"
                "后续十维分析应再按证据消息做精确回查。"
            )
        evidence_ids = _coerce_message_ids(
            parsed.get("evidence_message_ids")
            or parsed.get("anchor_message_ids")
            or []
        )
        sample_messages = list(candidate.sample_messages_json or [])
        sample_by_id = {
            int(item["message_id"]): item
            for item in sample_messages
            if item.get("message_id") is not None
        }
        if not evidence_ids:
            evidence_ids = [
                int(item["message_id"])
                for item in sample_messages[:3]
                if item.get("message_id") is not None
            ]
        evidence = [
            {
                "message_id": message_id,
                "sender_name": sample_by_id.get(message_id, {}).get("sender_name"),
                "sent_at": sample_by_id.get(message_id, {}).get("sent_at"),
                "quote": sample_by_id.get(message_id, {}).get("text"),
            }
            for message_id in evidence_ids[:8]
            if message_id in sample_by_id
        ]
        keyword_candidates = parsed.get("keywords") or parsed.get("keywords_json") or []
        keywords = _dedupe_strings(list(keyword_candidates), limit=8)
        if not keywords:
            keywords = top_terms(
                "\n".join(str(item.get("text") or "") for item in sample_messages),
                limit=6,
            )
        participant_lookup = {
            str(item.get("participant_id") or ""): item
            for item in list(candidate.top_participants_json or [])
            if str(item.get("participant_id") or "").strip()
        }
        participants: list[dict[str, Any]] = []
        for item in parsed.get("participants") or []:
            if not isinstance(item, dict):
                continue
            participant_id = str(item.get("participant_id") or "").strip()
            if not participant_id:
                continue
            fallback = participant_lookup.get(participant_id, {})
            participants.append(
                {
                    "participant_id": participant_id,
                    "role_hint": str(item.get("role_hint") or "").strip() or None,
                    "message_count": int(item.get("message_count") or fallback.get("message_count") or 0),
                    "mention_count": int(item.get("mention_count") or 0),
                }
            )
        if not participants:
            participants = [
                {
                    "participant_id": participant_id,
                    "role_hint": None,
                    "message_count": int(payload.get("message_count") or 0),
                    "mention_count": 0,
                }
                for participant_id, payload in participant_lookup.items()
            ]
        return {
            "topic_index": topic_index,
            "title": title,
            "summary": summary,
            "start_at": candidate.start_at,
            "end_at": candidate.end_at,
            "start_message_id": candidate.start_message_id,
            "end_message_id": candidate.end_message_id,
            "message_count": candidate.message_count,
            "participant_count": max(candidate.participant_count, len(participants)),
            "keywords_json": keywords,
            "evidence_json": evidence,
            "participants": participants,
            "metadata_json": {
                "week_key": candidate.week_key,
                "source": "weekly_topic_agent",
                "candidate_id": candidate.id,
            },
        }

    def _development_weekly_topic_summaries(
        self,
        candidates: list[TelegramPreprocessWeeklyTopicCandidate],
    ) -> list[dict[str, Any]]:
        topics: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates, start=1):
            sample_messages = list(candidate.sample_messages_json or [])
            top_participants = list(candidate.top_participants_json or [])
            summary = "\n".join(str(item.get("text") or "") for item in sample_messages[:24])
            topics.append(
                {
                    "topic_index": index,
                    "title": f"{candidate.week_key} 周话题",
                    "summary": _compact_text(summary, limit=420),
                    "start_at": candidate.start_at,
                    "end_at": candidate.end_at,
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "keywords_json": top_terms(summary, limit=6),
                    "evidence_json": [
                        {
                            "message_id": item.get("message_id"),
                            "sender_name": item.get("sender_name"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                        }
                        for item in sample_messages[:3]
                    ],
                    "participants": [
                        {
                            "participant_id": item.get("participant_id"),
                            "role_hint": None,
                            "message_count": int(item.get("message_count") or 0),
                            "mention_count": 0,
                        }
                        for item in top_participants
                        if item.get("participant_id")
                    ],
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "source": "weekly_topic_sql_fallback",
                        "candidate_id": candidate.id,
                    },
                }
            )
        return topics

    def _build_active_users(
        self,
        run_id: str,
        chat_id: str,
        top_users: list[TelegramPreprocessTopUser],
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> list[dict[str, Any]]:
        del run_id, chat_id, top_users, progress_callback
        return []

    def _add_usage(self, usage: dict[str, Any] | None) -> None:
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "cache_creation_tokens",
            "cache_read_tokens",
        ):
            self.usage_totals[key] += int((usage or {}).get(key, 0) or 0)

    def _progress(
        self,
        callback: Callable[[str, int, dict[str, Any] | None], None] | None,
        stage: str,
        progress_percent: int,
        summary_patch: dict[str, Any] | None = None,
    ) -> None:
        if callback:
            callback(stage, progress_percent, summary_patch)

    def _ensure_active(self) -> None:
        if self.cancel_checker and self.cancel_checker():
            raise RuntimeError("Telegram preprocess cancelled.")


class TelegramPreprocessManager:
    def __init__(
        self,
        db: Database,
        *,
        llm_log_path: str | None = None,
        max_workers: int = 2,
        run_inline: bool = False,
        stream_hub: AnalysisStreamHub | None = None,
    ) -> None:
        self.db = db
        self.llm_log_path = Path(llm_log_path) if llm_log_path else None
        self.run_inline = run_inline
        self.stream_hub = stream_hub
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="telegram-preprocess")
        self.futures: dict[str, Future[None]] = {}
        self._project_by_future: dict[str, str] = {}
        self._cancelled_projects: set[str] = set()
        self._trace_sequences: dict[str, int] = {}
        self._lock = Lock()

    def resume_interrupted_runs(self) -> None:
        with self.db.session() as session:
            for run in session.scalars(select(TelegramPreprocessRun)):
                if run.status not in {"queued", "running"}:
                    continue
                run.status = "failed"
                run.finished_at = utcnow()
                run.error_message = "Service restarted while Telegram preprocess was still running. Resume this run to continue from the saved checkpoint."
                run.progress_percent = int(run.progress_percent or 0)
                summary = dict(run.summary_json or {})
                summary["current_stage"] = "interrupted"
                summary["resume_available"] = True
                run.summary_json = summary
                self._trace_sequences[run.id] = int(summary.get("trace_event_count") or 0)
            session.commit()

    def submit(self, project_id: str, *, weekly_summary_concurrency: int | None = None) -> Any:
        with self.db.session() as session:
            project = repository.get_project(session, project_id)
            if not project or project.mode != "telegram":
                raise ValueError("Telegram project not found.")
            chat = repository.get_latest_telegram_chat(session, project_id)
            if not chat:
                raise ValueError("Telegram chat not found.")
            chat_config = repository.get_service_config(session, "chat_service")
            normalized_concurrency = TelegramPreprocessWorker._normalize_weekly_summary_concurrency(
                weekly_summary_concurrency
            )
            active_run = repository.get_active_telegram_preprocess_run(session, project_id)
            if active_run and self.is_tracking(active_run.id):
                return active_run
            resumable_run = active_run or repository.get_latest_resumable_telegram_preprocess_run(session, project_id)
            if resumable_run and resumable_run.chat_id == chat.id:
                summary = dict(resumable_run.summary_json or {})
                summary["resume_available"] = True
                summary["resume_count"] = int(summary.get("resume_count") or 0) + 1
                summary["weekly_summary_concurrency"] = normalized_concurrency
                summary["progress_percent"] = int(
                    summary.get("progress_percent") or resumable_run.progress_percent or 0
                )
                resumable_run.status = "queued"
                resumable_run.finished_at = None
                resumable_run.error_message = None
                resumable_run.current_stage = str(summary.get("current_stage") or resumable_run.current_stage or "queued")
                resumable_run.progress_percent = int(summary.get("progress_percent") or 0)
                resumable_run.summary_json = summary
                session.commit()
                run_id = resumable_run.id
                self._trace_sequences[run_id] = int(summary.get("trace_event_count") or 0)
                self._record_trace(
                    session,
                    run_id,
                    {
                        "timestamp": utcnow().isoformat(),
                        "kind": "stage_progress",
                        "stage": "resume",
                        "message": "Resuming Telegram preprocess from the latest saved checkpoint.",
                        "topic_count": int(summary.get("topic_count") or 0),
                        "weekly_candidate_count": int(summary.get("weekly_candidate_count") or 0),
                        "weekly_summary_concurrency": normalized_concurrency,
                    },
                    persist=True,
                )
            else:
                run = repository.create_telegram_preprocess_run(
                    session,
                    project_id=project_id,
                    chat_id=chat.id,
                    status="queued",
                    llm_model=chat_config.model if chat_config else None,
                    summary_json={
                        "current_stage": "queued",
                        "progress_percent": 0,
                        "window_count": 0,
                        "top_user_count": 0,
                        "weekly_candidate_count": 0,
                        "topic_count": 0,
                        "weekly_summary_concurrency": normalized_concurrency,
                        "trace_events": [],
                        "trace_event_count": 0,
                        "resume_count": 0,
                    },
                )
                session.commit()
                run_id = run.id
                self._trace_sequences[run_id] = 0
        if self.run_inline:
            self._execute(run_id)
        else:
            future = self.executor.submit(self._execute, run_id)
            with self._lock:
                self.futures[run_id] = future
                self._project_by_future[run_id] = project_id
        with self.db.session() as session:
            return repository.get_telegram_preprocess_run(session, run_id)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def is_tracking(self, run_id: str) -> bool:
        with self._lock:
            future = self.futures.get(run_id)
        return future is not None and not future.done()

    def cancel_project(self, project_id: str) -> bool:
        with self._lock:
            self._cancelled_projects.add(project_id)
            future_items = [
                (future_key, self.futures.get(future_key))
                for future_key, mapped_project_id in self._project_by_future.items()
                if mapped_project_id == project_id
            ]
        all_cancelled = True
        for future_key, future in future_items:
            if future is None:
                continue
            if not future.cancel():
                all_cancelled = False
                continue
            self._finish_future(future_key)
        return all_cancelled or not self.has_project_activity(project_id)

    def has_project_activity(self, project_id: str) -> bool:
        with self._lock:
            for future_key, mapped_project_id in self._project_by_future.items():
                if mapped_project_id != project_id:
                    continue
                future = self.futures.get(future_key)
                if future is not None and not future.done():
                    return True
            return False

    def _execute(self, run_id: str) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    run = repository.get_telegram_preprocess_run(session, run_id)
                    if not run:
                        return
                    project = repository.get_project(session, run.project_id)
                    if not project:
                        raise ValueError("Project not found.")
                    chat_config = repository.get_service_config(session, "chat_service")
                    existing_summary = dict(run.summary_json or {})
                    existing_progress = int(existing_summary.get("progress_percent") or run.progress_percent or 0)
                    is_resume = bool(
                        existing_summary.get("weekly_candidate_count")
                        or existing_summary.get("topic_count")
                    )
                    run.status = "running"
                    run.started_at = run.started_at or utcnow()
                    run.finished_at = None
                    run.error_message = None
                    run.progress_percent = max(existing_progress, 3)
                    summary = existing_summary
                    summary["current_stage"] = "running"
                    summary["progress_percent"] = run.progress_percent
                    summary["resume_available"] = False
                    run.summary_json = summary
                    session.commit()
                    if is_resume:
                        self._record_trace(
                            session,
                            run_id,
                            {
                                "timestamp": utcnow().isoformat(),
                                "kind": "stage_progress",
                                "stage": "resume",
                                "message": "Continuing Telegram preprocess from previously saved stage outputs.",
                                "topic_count": int(summary.get("topic_count") or 0),
                                "weekly_candidate_count": int(summary.get("weekly_candidate_count") or 0),
                                "weekly_summary_concurrency": int(summary.get("weekly_summary_concurrency") or 1),
                            },
                            persist=True,
                        )
                    self._publish_snapshot(run.id)

                    worker = TelegramPreprocessWorker(
                        session,
                        project,
                        llm_config=chat_config,
                        log_path=str(self.llm_log_path) if self.llm_log_path else None,
                        cancel_checker=lambda: self._is_cancelled(project.id),
                        trace_callback=lambda event, persist=True: self._record_trace(session, run_id, event, persist=persist),
                    )

                    def progress(stage: str, progress_percent: int, summary_patch: dict[str, Any] | None) -> None:
                        live_run = repository.get_telegram_preprocess_run(session, run_id)
                        if not live_run:
                            return
                        live_run.progress_percent = progress_percent
                        live_run.current_stage = stage
                        summary = dict(live_run.summary_json or {})
                        summary["current_stage"] = stage
                        summary["progress_percent"] = progress_percent
                        if summary_patch:
                            summary.update(summary_patch)
                        usage = dict(summary.get("usage") or {})
                        live_run.prompt_tokens = int(usage.get("prompt_tokens") or live_run.prompt_tokens or 0)
                        live_run.completion_tokens = int(usage.get("completion_tokens") or live_run.completion_tokens or 0)
                        live_run.total_tokens = int(usage.get("total_tokens") or live_run.total_tokens or 0)
                        live_run.cache_creation_tokens = int(
                            usage.get("cache_creation_tokens") or live_run.cache_creation_tokens or 0
                        )
                        live_run.cache_read_tokens = int(
                            usage.get("cache_read_tokens") or live_run.cache_read_tokens or 0
                        )
                        live_run.window_count = int(
                            summary.get("weekly_candidate_count") or summary.get("window_count") or live_run.window_count or 0
                        )
                        live_run.topic_count = int(summary.get("topic_count") or live_run.topic_count or 0)
                        live_run.summary_json = summary
                        session.commit()
                        self._publish_snapshot(run_id)

                    result = worker.process(run, progress_callback=progress)
                    live_run = repository.get_telegram_preprocess_run(session, run_id)
                    if not live_run:
                        return
                    final_topics = repository.replace_telegram_preprocess_topics(
                        session,
                        run_id=live_run.id,
                        project_id=project.id,
                        chat_id=live_run.chat_id,
                        topics=list(result.get("topics") or []),
                    )
                    usage = result.get("usage") or {}
                    live_run.status = "completed"
                    live_run.finished_at = utcnow()
                    live_run.progress_percent = 100
                    live_run.current_stage = "completed"
                    live_run.prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
                    live_run.completion_tokens = int(usage.get("completion_tokens", 0) or 0)
                    live_run.total_tokens = int(usage.get("total_tokens", 0) or 0)
                    live_run.cache_creation_tokens = int(usage.get("cache_creation_tokens", 0) or 0)
                    live_run.cache_read_tokens = int(usage.get("cache_read_tokens", 0) or 0)
                    live_run.window_count = int(result.get("window_count", 0) or 0)
                    live_run.topic_count = len(final_topics)
                    live_run.summary_json = {
                        **dict(live_run.summary_json or {}),
                        "current_stage": "completed",
                        "progress_percent": 100,
                        "window_count": live_run.window_count,
                        "top_user_count": int(result.get("top_user_count", 0) or 0),
                        "weekly_candidate_count": int(result.get("weekly_candidate_count", 0) or 0),
                        "topic_count": live_run.topic_count,
                        "weekly_summary_concurrency": int(
                            (live_run.summary_json or {}).get("weekly_summary_concurrency") or 1
                        ),
                        "bootstrap": result.get("bootstrap") or {},
                        "usage": usage,
                        "resume_available": False,
                    }
                    session.commit()
                    self._record_trace(
                        session,
                        run_id,
                        {
                            "timestamp": utcnow().isoformat(),
                            "kind": "run_completed",
                            "stage": "completed",
                            "message": f"Telegram preprocess completed with {live_run.topic_count} weekly topics.",
                        },
                        persist=True,
                    )
                    self._publish_snapshot(run_id)
        except Exception as exc:
            with self.db.session() as session:
                run = repository.get_telegram_preprocess_run(session, run_id)
                if run:
                    self._mark_run_failed(session, run, str(exc).strip() or exc.__class__.__name__)
                    summary = dict(run.summary_json or {})
                    summary["traceback"] = traceback.format_exc()
                    run.summary_json = summary
                    session.commit()
                    self._record_trace(
                        session,
                        run_id,
                        {
                            "timestamp": utcnow().isoformat(),
                            "kind": "run_failed",
                            "stage": "failed",
                            "message": run.error_message or "Telegram preprocess failed.",
                            "error": str(exc),
                        },
                        persist=True,
                    )
                    self._publish_snapshot(run_id)
        finally:
            self._finish_future(run_id)

    def _mark_run_failed(self, session: Session, run, error_message: str) -> None:
        run.status = "failed"
        run.finished_at = utcnow()
        run.error_message = error_message
        summary = dict(run.summary_json or {})
        summary["current_stage"] = "failed"
        summary["progress_percent"] = int(run.progress_percent or 0)
        summary["resume_available"] = True
        run.summary_json = summary

    def _next_trace_seq(self, run_id: str) -> int:
        with self._lock:
            next_value = int(self._trace_sequences.get(run_id, 0) or 0) + 1
            self._trace_sequences[run_id] = next_value
        return next_value

    def _publish_snapshot(self, run_id: str) -> None:
        if self.stream_hub:
            self.stream_hub.publish(run_id, event="snapshot", payload={"run_id": run_id})

    def _record_trace(
        self,
        session: Session,
        run_id: str,
        event: dict[str, Any],
        *,
        persist: bool = True,
    ) -> dict[str, Any]:
        normalized = dict(event or {})
        normalized.setdefault("timestamp", utcnow().isoformat())
        if persist:
            live_run = session.get(TelegramPreprocessRun, run_id)
            if live_run:
                summary = dict(live_run.summary_json or {})
                trace_events = [
                    dict(item)
                    for item in (summary.get("trace_events") or [])
                    if isinstance(item, dict)
                ]
                normalized["seq"] = self._next_trace_seq(run_id)
                trace_events.append(normalized)
                summary["trace_event_count"] = normalized["seq"]
                summary["trace_events"] = trace_events[-TELEGRAM_PREPROCESS_TRACE_LIMIT:]
                live_run.summary_json = summary
                session.commit()
        else:
            normalized.setdefault("seq", None)
        if self.stream_hub:
            self.stream_hub.publish(run_id, event="trace", payload=normalized)
        return normalized

    def _finish_future(self, future_key: str) -> None:
        project_id: str | None
        with self._lock:
            self.futures.pop(future_key, None)
            project_id = self._project_by_future.pop(future_key, None)
            self._trace_sequences.pop(future_key, None)
            if project_id is None:
                return
            if any(
                mapped_project_id == project_id
                and self.futures.get(other_key) is not None
                and not self.futures[other_key].done()
                for other_key, mapped_project_id in self._project_by_future.items()
            ):
                return
            self._cancelled_projects.discard(project_id)

    def _is_cancelled(self, project_id: str) -> bool:
        with self._lock:
            return project_id in self._cancelled_projects
