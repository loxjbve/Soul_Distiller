from __future__ import annotations

from app.telegram.preprocess import helpers as _helpers
from app.telegram.preprocess.relationships import TelegramPreprocessRelationshipsMixin
from app.telegram.preprocess.weekly_topics import TelegramPreprocessWeeklyTopicsMixin

globals().update(
    {
        name: getattr(_helpers, name)
        for name in dir(_helpers)
        if not name.startswith("__")
    }
)

class TelegramPreprocessWorker(TelegramPreprocessRelationshipsMixin, TelegramPreprocessWeeklyTopicsMixin):
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
                **self._empty_topic_progress(),
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
                **self._topic_progress_payload(weekly_candidates, []),
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
                **self._topic_progress_payload(weekly_candidates, topics, completed=True),
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
            **self._topic_progress_payload(weekly_candidates, topics, completed=True),
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
        return max(TELEGRAM_PREPROCESS_MIN_CONCURRENCY, candidate)

    def _resolve_weekly_summary_concurrency(self, run: TelegramPreprocessRun) -> int:
        summary = dict(run.summary_json or {})
        return self._normalize_weekly_summary_concurrency(
            summary.get("weekly_summary_concurrency") or TELEGRAM_PREPROCESS_MIN_CONCURRENCY
        )

    def _topic_payload_from_model(self, topic: Any) -> dict[str, Any]:
        quote_payloads = [
            {
                "participant_id": quote.participant_id,
                "display_name": quote.participant.display_name if quote.participant else None,
                "message_id": quote.telegram_message_id,
                "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
                "quote": quote.quote,
                "rank": int(quote.rank or 0),
            }
            for quote in sorted(
                list(getattr(topic, "quotes", None) or []),
                key=lambda item: (
                    item.participant_id or "",
                    int(item.rank or 0),
                    int(item.telegram_message_id or 0),
                ),
            )
        ]
        return {
            "topic_id": getattr(topic, "id", None),
            "topic_index": int(getattr(topic, "topic_index", 0) or 0),
            "week_key": getattr(topic, "week_key", None) or dict(getattr(topic, "metadata_json", None) or {}).get("week_key"),
            "week_topic_index": int(getattr(topic, "week_topic_index", 0) or 0),
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
                    "stance_summary": getattr(link, "stance_summary", None),
                    "message_count": int(link.message_count or 0),
                    "mention_count": int(link.mention_count or 0),
                }
                for link in list(getattr(topic, "participants", None) or [])
            ],
            "participant_quotes": quote_payloads,
            "metadata_json": dict(getattr(topic, "metadata_json", None) or {}),
        }

    @staticmethod
    def _topic_checkpoint_key(payload: dict[str, Any]) -> str:
        metadata = dict(payload.get("metadata_json") or {})
        candidate_id = str(metadata.get("candidate_id") or "").strip()
        if candidate_id:
            return f"candidate:{candidate_id}"
        week_key = str(metadata.get("week_key") or "").strip()
        week_topic_index = int(payload.get("week_topic_index") or payload.get("topic_index") or 0)
        title = str(payload.get("title") or "").strip().lower()
        return f"topic:{week_key}:{week_topic_index}:{title}"

    def _completed_candidate_keys(self, completed_topics: list[dict[str, Any]]) -> set[str]:
        return {
            checkpoint
            for checkpoint in (self._topic_checkpoint_key(item) for item in completed_topics)
            if checkpoint.startswith("candidate:")
        }

    @staticmethod
    def _empty_topic_progress() -> dict[str, Any]:
        return {
            "current_topic_index": 0,
            "current_topic_total": 0,
            "current_topic_label": "",
        }

    def _topic_progress_payload(
        self,
        candidates: list[TelegramPreprocessWeeklyTopicCandidate],
        completed_topics: list[dict[str, Any]],
        *,
        current_candidate: TelegramPreprocessWeeklyTopicCandidate | None = None,
        completed: bool = False,
    ) -> dict[str, Any]:
        total = len(candidates)
        if total <= 0:
            return self._empty_topic_progress()

        if completed:
            last_topic = completed_topics[-1] if completed_topics else {}
            return {
                "current_topic_index": total,
                "current_topic_total": total,
                "current_topic_label": str(last_topic.get("title") or "").strip(),
            }

        completed_keys = self._completed_candidate_keys(completed_topics)
        topic_index_by_candidate = {
            candidate.id: index
            for index, candidate in enumerate(candidates, start=1)
        }

        next_candidate = current_candidate
        next_candidate_key = f"candidate:{next_candidate.id}" if next_candidate else ""
        if next_candidate and next_candidate_key in completed_keys:
            next_candidate = None

        if not next_candidate:
            for candidate in candidates:
                if f"candidate:{candidate.id}" in completed_keys:
                    continue
                next_candidate = candidate
                break

        completed_count = len(completed_keys)
        if not next_candidate:
            return {
                "current_topic_index": total,
                "current_topic_total": total,
                "current_topic_label": str((completed_topics[-1] if completed_topics else {}).get("title") or "").strip(),
            }

        current_index = topic_index_by_candidate.get(
            next_candidate.id,
            min(completed_count + 1, total),
        )
        return {
            "current_topic_index": int(current_index),
            "current_topic_total": total,
            "current_topic_label": (
                f"{str(next_candidate.week_key or '').strip() or 'Week'}"
                f" window {int(getattr(next_candidate, 'window_index', 1) or 1)}"
            ),
        }

    @staticmethod
    def _topic_sort_key(payload: dict[str, Any]) -> tuple[str, int, str, int]:
        return (
            str(payload.get("week_key") or dict(payload.get("metadata_json") or {}).get("week_key") or ""),
            int(payload.get("week_topic_index") or payload.get("topic_index") or 0),
            _safe_iso(payload.get("start_at")) or "",
            int(payload.get("start_message_id") or 0),
        )

    @staticmethod
    def _topic_score(payload: dict[str, Any]) -> tuple[float, int, int]:
        participant_quotes = list(payload.get("participant_quotes") or [])
        return (
            float(len(payload.get("evidence_json") or [])) + (len(participant_quotes) * 0.35) + (len(payload.get("participants") or []) * 0.2),
            int(payload.get("message_count") or 0),
            int(payload.get("participant_count") or 0),
        )

    def _merge_topic_payload(
        self,
        existing: dict[str, Any],
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = dict(existing.get("metadata_json") or {})
        incoming_metadata = dict(incoming.get("metadata_json") or {})
        metadata["subtopics"] = _dedupe_strings(
            list(metadata.get("subtopics") or []) + list(incoming_metadata.get("subtopics") or []),
            limit=8,
        )
        metadata["interaction_patterns"] = _dedupe_strings(
            list(metadata.get("interaction_patterns") or []) + list(incoming_metadata.get("interaction_patterns") or []),
            limit=8,
        )
        merged = dict(existing)
        merged["summary"] = max(
            [str(existing.get("summary") or "").strip(), str(incoming.get("summary") or "").strip()],
            key=len,
        )
        merged["title"] = str(existing.get("title") or "").strip() or str(incoming.get("title") or "").strip()
        merged["start_at"] = min(
            [item for item in [existing.get("start_at"), incoming.get("start_at")] if item is not None],
            default=existing.get("start_at") or incoming.get("start_at"),
        )
        merged["end_at"] = max(
            [item for item in [existing.get("end_at"), incoming.get("end_at")] if item is not None],
            default=existing.get("end_at") or incoming.get("end_at"),
        )
        merged["start_message_id"] = min(
            [int(item) for item in [existing.get("start_message_id"), incoming.get("start_message_id")] if item is not None],
            default=existing.get("start_message_id") or incoming.get("start_message_id"),
        )
        merged["end_message_id"] = max(
            [int(item) for item in [existing.get("end_message_id"), incoming.get("end_message_id")] if item is not None],
            default=existing.get("end_message_id") or incoming.get("end_message_id"),
        )
        merged["message_count"] = max(int(existing.get("message_count") or 0), int(incoming.get("message_count") or 0))
        merged["participant_count"] = max(int(existing.get("participant_count") or 0), int(incoming.get("participant_count") or 0))
        merged["keywords_json"] = _dedupe_strings(
            list(existing.get("keywords_json") or []) + list(incoming.get("keywords_json") or []),
            limit=8,
        )
        evidence_by_id: dict[int, dict[str, Any]] = {}
        for item in list(existing.get("evidence_json") or []) + list(incoming.get("evidence_json") or []):
            message_id = int(item.get("message_id") or 0)
            if message_id <= 0:
                continue
            evidence_by_id[message_id] = dict(item)
        merged["evidence_json"] = [evidence_by_id[key] for key in sorted(evidence_by_id)[:8]]
        participant_by_id: dict[str, dict[str, Any]] = {}
        for participant in list(existing.get("participants") or []) + list(incoming.get("participants") or []):
            participant_id = str(participant.get("participant_id") or "").strip()
            if not participant_id:
                continue
            row = participant_by_id.get(participant_id, {"participant_id": participant_id})
            row["role_hint"] = str(row.get("role_hint") or "").strip() or str(participant.get("role_hint") or "").strip() or None
            row["stance_summary"] = (
                str(participant.get("stance_summary") or "").strip()
                or str(row.get("stance_summary") or "").strip()
                or None
            )
            row["message_count"] = max(int(row.get("message_count") or 0), int(participant.get("message_count") or 0))
            row["mention_count"] = max(int(row.get("mention_count") or 0), int(participant.get("mention_count") or 0))
            participant_by_id[participant_id] = row
        merged["participants"] = list(participant_by_id.values())
        quote_seen: set[tuple[str, int, str]] = set()
        merged_quotes: list[dict[str, Any]] = []
        for quote in list(existing.get("participant_quotes") or []) + list(incoming.get("participant_quotes") or []):
            participant_id = str(quote.get("participant_id") or "").strip()
            message_id = int(quote.get("message_id") or 0)
            quote_text = str(quote.get("quote") or "").strip()
            if not participant_id or message_id <= 0 or not quote_text:
                continue
            key = (participant_id, message_id, quote_text)
            if key in quote_seen:
                continue
            quote_seen.add(key)
            merged_quotes.append(dict(quote))
        merged_quotes.sort(
            key=lambda item: (
                str(item.get("participant_id") or ""),
                int(item.get("rank") or 0),
                int(item.get("message_id") or 0),
            )
        )
        limited_quotes: list[dict[str, Any]] = []
        per_participant_counts: dict[str, int] = defaultdict(int)
        for quote in merged_quotes:
            participant_id = str(quote.get("participant_id") or "").strip()
            if per_participant_counts[participant_id] >= TELEGRAM_WEEKLY_PARTICIPANT_QUOTE_LIMIT:
                continue
            per_participant_counts[participant_id] += 1
            quote["rank"] = per_participant_counts[participant_id]
            limited_quotes.append(quote)
        merged["participant_quotes"] = limited_quotes
        merged["metadata_json"] = {**incoming_metadata, **metadata}
        merged["week_key"] = merged.get("week_key") or merged["metadata_json"].get("week_key")
        return merged

    def _normalize_topic_collection(
        self,
        topics: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for topic in topics:
            week_key = str(topic.get("week_key") or dict(topic.get("metadata_json") or {}).get("week_key") or "").strip()
            topic["week_key"] = week_key
            grouped[week_key].append(topic)

        normalized_topics: list[dict[str, Any]] = []
        for week_key in sorted(grouped):
            merged_topics: list[dict[str, Any]] = []
            for topic in sorted(grouped[week_key], key=self._topic_sort_key):
                candidate_title = "".join(ch for ch in str(topic.get("title") or "").lower() if ch.isalnum())
                evidence_ids = {
                    int(item.get("message_id") or 0)
                    for item in (topic.get("evidence_json") or [])
                    if int(item.get("message_id") or 0) > 0
                }
                merged_index = None
                for index, existing in enumerate(merged_topics):
                    existing_title = "".join(ch for ch in str(existing.get("title") or "").lower() if ch.isalnum())
                    existing_evidence = {
                        int(item.get("message_id") or 0)
                        for item in (existing.get("evidence_json") or [])
                        if int(item.get("message_id") or 0) > 0
                    }
                    if candidate_title and candidate_title == existing_title:
                        merged_index = index
                        break
                    if evidence_ids and existing_evidence and evidence_ids.intersection(existing_evidence):
                        merged_index = index
                        break
                if merged_index is None:
                    merged_topics.append(topic)
                else:
                    merged_topics[merged_index] = self._merge_topic_payload(merged_topics[merged_index], topic)

            merged_topics = [
                topic
                for topic in merged_topics
                if (topic.get("evidence_json") or []) or (topic.get("participant_quotes") or [])
            ]
            merged_topics.sort(key=self._topic_score, reverse=True)
            merged_topics = merged_topics[:TELEGRAM_WEEKLY_TOPIC_CAP]
            for week_topic_index, topic in enumerate(merged_topics, start=1):
                topic["week_key"] = week_key or None
                topic["week_topic_index"] = week_topic_index
                metadata = dict(topic.get("metadata_json") or {})
                metadata["week_key"] = week_key or None
                topic["metadata_json"] = metadata
                normalized_topics.append(topic)

        normalized_topics.sort(
            key=lambda item: (
                str(item.get("week_key") or ""),
                int(item.get("week_topic_index") or 0),
                _safe_iso(item.get("start_at")) or "",
                int(item.get("start_message_id") or 0),
            )
        )
        for topic_index, topic in enumerate(normalized_topics, start=1):
            topic["topic_index"] = topic_index
        return normalized_topics

    def _merge_candidate_topic_payloads(
        self,
        topics: list[dict[str, Any]],
        payloads: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        checkpoint_keys = {self._topic_checkpoint_key(item) for item in payloads}
        updated = [
            item
            for item in topics
            if self._topic_checkpoint_key(item) not in checkpoint_keys
        ]
        updated.extend(payloads)
        return self._normalize_topic_collection(updated)

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
            for window_index, selected in enumerate(self._select_densest_segments(message_rows), start=1):
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
                        "window_index": window_index,
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
                            "window_index": window_index,
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



__all__ = ["TelegramPreprocessWorker"]

