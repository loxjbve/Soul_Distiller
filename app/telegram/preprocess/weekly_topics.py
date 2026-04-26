from __future__ import annotations

from app.telegram.preprocess import helpers as _helpers

globals().update(
    {
        name: getattr(_helpers, name)
        for name in dir(_helpers)
        if not name.startswith("__")
    }
)

class TelegramPreprocessWeeklyTopicsMixin:
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

    def _select_densest_segments(self, messages: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        remaining = list(messages)
        selected_segments: list[list[dict[str, Any]]] = []
        for _ in range(TELEGRAM_WEEKLY_MAX_WINDOWS):
            if not remaining:
                break
            segment = self._select_densest_segment(remaining)
            if not segment:
                break
            selected_segments.append(segment)
            selected_ids = {int(item["message_id"]) for item in segment if item.get("message_id") is not None}
            remaining = [item for item in remaining if int(item.get("message_id") or 0) not in selected_ids]
        selected_segments.sort(
            key=lambda segment: (
                _safe_iso(segment[0].get("sent_at_value")) or "",
                int(segment[0].get("message_id") or 0),
            )
        )
        return selected_segments

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
        completed_keys = self._completed_candidate_keys(existing_topics)
        remaining_candidates = [
            candidate
            for candidate in candidates
            if f"candidate:{candidate.id}" not in completed_keys
        ]
        completed_candidate_count = len(completed_keys)
        if existing_topics:
            self._trace(
                "stage_progress",
                stage="weekly_topic_summary",
                message="Resuming weekly topic summaries from a saved checkpoint.",
                completed_week_count=completed_candidate_count,
                remaining_week_count=len(remaining_candidates),
                weekly_candidate_count=len(candidates),
            )
            self._progress(
                progress_callback,
                "weekly_topic_summary",
                min(40 + int((completed_candidate_count / max(len(candidates), 1)) * 34), 76),
                {
                    "topic_count": len(existing_topics),
                    "completed_week_count": completed_candidate_count,
                    "remaining_week_count": len(remaining_candidates),
                    "weekly_candidate_count": len(candidates),
                    "usage": dict(self.usage_totals),
                    **self._topic_progress_payload(candidates, existing_topics),
                },
            )
        if not remaining_candidates:
            self._trace(
                "agent_completed",
                stage="weekly_topic_summary",
                agent="weekly_topic_agent",
                message=f"Weekly topic summaries were already complete for {completed_candidate_count} windows.",
            )
            return self._normalize_topic_collection(existing_topics)
        if not self.client:
            topics = self._normalize_topic_collection(self._development_weekly_topic_summaries(candidates))
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
            message=f"Starting weekly topic summaries for {len(remaining_candidates)} remaining windows.",
            weekly_summary_concurrency=self.weekly_summary_concurrency,
        )
        topics = list(existing_topics)
        total = max(len(candidates), 1)

        self._trace(
            "stage_progress",
            stage="weekly_topic_summary",
            message="Running weekly topic summaries concurrently.",
            remaining_week_count=len(remaining_candidates),
        )
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(
            max_workers=self.weekly_summary_concurrency,
            thread_name_prefix="telegram-weekly-summary",
        ) as executor:
            future_map = {
                executor.submit(
                    self._run_parallel_weekly_topic_task,
                    run_id,
                    candidate,
                ): candidate
                for candidate in remaining_candidates
            }
            self._progress(
                progress_callback,
                "weekly_topic_summary",
                40,
                {
                    "topic_count": len(topics),
                    "completed_week_count": completed_candidate_count,
                    "remaining_week_count": max(len(candidates) - completed_candidate_count, 0),
                    "weekly_candidate_count": len(candidates),
                    "weekly_summary_concurrency": self.weekly_summary_concurrency,
                    "active_agents": min(len(remaining_candidates), self.weekly_summary_concurrency),
                    "usage": dict(self.usage_totals),
                    **self._topic_progress_payload(
                        candidates,
                        topics,
                        current_candidate=remaining_candidates[0] if remaining_candidates else None,
                    ),
                },
            )
            for future in as_completed(future_map):
                self._ensure_active()
                candidate = future_map[future]
                try:
                    result = future.result()
                    self._add_usage(result.get("usage"))
                    topics = self._merge_candidate_topic_payloads(topics, list(result.get("topics") or []))
                    repository.replace_telegram_preprocess_topics(
                        self.session,
                        run_id=run_id,
                        project_id=self.project.id,
                        chat_id=chat_id,
                        topics=topics,
                    )
                    self.session.commit()
                except Exception as exc:
                    self._trace(
                        "agent_retry",
                        stage="weekly_topic_summary",
                        agent="weekly_topic_agent",
                        message=f"Weekly topic summary unhandled error: {exc}",
                    )
                completed_candidate_count = len(self._completed_candidate_keys(topics))
                self._progress(
                    progress_callback,
                    "weekly_topic_summary",
                    min(40 + int((completed_candidate_count / total) * 34), 76),
                    {
                        "current_week": candidate.week_key,
                        "current_window": int(candidate.window_index or 1),
                        "topic_count": len(topics),
                        "completed_week_count": completed_candidate_count,
                        "remaining_week_count": max(len(candidates) - completed_candidate_count, 0),
                        "weekly_candidate_count": len(candidates),
                        "weekly_summary_concurrency": self.weekly_summary_concurrency,
                        "active_agents": min(
                            max(len(candidates) - completed_candidate_count, 0),
                            self.weekly_summary_concurrency,
                        ),
                        "usage": dict(self.usage_totals),
                        **self._topic_progress_payload(candidates, topics),
                    },
                )
        self._trace(
            "agent_completed",
            stage="weekly_topic_summary",
            agent="weekly_topic_agent",
            message=f"Completed weekly topic summaries for {len(self._completed_candidate_keys(topics))} windows.",
        )
        return self._normalize_topic_collection(topics)

    def _run_parallel_weekly_topic_task(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
    ) -> dict[str, Any]:
        from app.telegram.preprocess.worker import TelegramPreprocessWorker

        thread_session = Session(bind=self.session.get_bind())
        try:
            thread_project = repository.get_project(thread_session, self.project.id)
            thread_candidate = repository.get_telegram_preprocess_weekly_topic_candidate(thread_session, candidate.id)
            if not thread_project or not thread_candidate:
                raise ValueError("Weekly topic summary context could not be reloaded for the worker thread.")
            worker = TelegramPreprocessWorker(
                thread_session,
                thread_project,
                llm_config=self.llm_config,
                log_path=self.log_path,
                cancel_checker=self.cancel_checker,
                trace_callback=self._relay_non_persistent_trace,
            )
            topics = worker._summarize_weekly_candidate_with_retries(run_id, thread_candidate)
            thread_session.commit()
            return {"topics": topics, "usage": dict(worker.usage_totals)}
        finally:
            thread_session.close()

    def _relay_non_persistent_trace(self, event: dict[str, Any], _persist: bool = True) -> None:
        if self.trace_callback:
            self.trace_callback(dict(event or {}), False)

    def _summarize_weekly_candidate_with_retries(
        self,
        run_id: str,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
    ) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(1, TELEGRAM_WEEKLY_AGENT_RETRIES + 1):
            try:
                try:
                    result = self._run_weekly_topic_agent(run_id, candidate, attempt=attempt)
                except TypeError as exc:
                    if "topic_index" not in str(exc):
                        raise
                    result = self._run_weekly_topic_agent(run_id, candidate, 1, attempt=attempt)  # type: ignore[misc]
                if isinstance(result, dict):
                    return self._normalize_topic_collection([dict(result)])
                return self._normalize_topic_collection(list(result or []))
            except Exception as exc:
                last_error = exc
                self._trace(
                    "agent_retry",
                    stage="weekly_topic_summary",
                    agent="weekly_topic_agent",
                    week_key=candidate.week_key,
                    window_index=int(candidate.window_index or 1),
                    attempt=attempt,
                    max_attempts=TELEGRAM_WEEKLY_AGENT_RETRIES,
                    message=f"Weekly topic summary failed for {candidate.week_key}; retrying.",
                    error=str(exc),
                )
        raise RuntimeError(
            f"Weekly topic summary failed for {candidate.week_key}: {last_error}"
        ) from last_error

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

    def _normalize_weekly_topics(
        self,
        candidate: TelegramPreprocessWeeklyTopicCandidate,
        parsed: dict[str, Any],
    ) -> list[dict[str, Any]]:
        sample_messages = list(candidate.sample_messages_json or [])
        sample_by_id = {
            int(item["message_id"]): item
            for item in sample_messages
            if item.get("message_id") is not None
        }
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

        topics: list[dict[str, Any]] = []
        for raw_index, item in enumerate(parsed.get("topics") or [], start=1):
            if raw_index > TELEGRAM_WEEKLY_TOPIC_CAP or not isinstance(item, dict):
                break
            title = str(item.get("title") or "").strip() or f"{candidate.week_key} topic {raw_index}"
            summary = str(item.get("summary") or "").strip() or _compact_text(
                " ".join(str(message.get("text") or "") for message in sample_messages[:12]),
                limit=140,
            )
            evidence_ids = [
                message_id
                for message_id in _coerce_message_ids(item.get("evidence_message_ids") or [])
                if message_id in sample_by_id
            ][:8]
            if not evidence_ids:
                evidence_ids = [
                    int(message["message_id"])
                    for message in sample_messages[:3]
                    if message.get("message_id") is not None
                ]
            evidence = [
                {
                    "message_id": message_id,
                    "sender_name": sample_by_id.get(message_id, {}).get("sender_name"),
                    "sent_at": sample_by_id.get(message_id, {}).get("sent_at"),
                    "quote": sample_by_id.get(message_id, {}).get("text"),
                }
                for message_id in evidence_ids
                if message_id in sample_by_id
            ]
            keywords = _dedupe_strings(list(item.get("keywords") or []), limit=8)
            if not keywords:
                keywords = top_terms("\n".join(str(message.get("text") or "") for message in sample_messages), limit=6)

            participants: list[dict[str, Any]] = []
            participant_quotes: list[dict[str, Any]] = []
            for participant_item in item.get("participants") or []:
                if not isinstance(participant_item, dict):
                    continue
                participant_id = str(participant_item.get("participant_id") or "").strip()
                display_name = str(participant_item.get("display_name") or "").strip()
                if not participant_id and display_name:
                    participant_id = participant_name_lookup.get(display_name.lower(), "")
                if not participant_id:
                    continue
                fallback = participant_lookup.get(participant_id, {})
                participants.append(
                    {
                        "participant_id": participant_id,
                        "role_hint": str(participant_item.get("role_hint") or "").strip() or None,
                        "stance_summary": str(participant_item.get("stance_summary") or "").strip() or None,
                        "message_count": int(participant_item.get("message_count") or fallback.get("message_count") or 0),
                        "mention_count": int(participant_item.get("mention_count") or 0),
                    }
                )
                quote_ids = [
                    message_id
                    for message_id in _coerce_message_ids(participant_item.get("quote_message_ids") or [])
                    if message_id in sample_by_id
                ]
                filtered_quote_ids = [
                    message_id
                    for message_id in quote_ids
                    if str(sample_by_id.get(message_id, {}).get("participant_id") or "").strip() == participant_id
                ]
                if filtered_quote_ids:
                    quote_ids = filtered_quote_ids
                for quote_rank, message_id in enumerate(quote_ids[:TELEGRAM_WEEKLY_PARTICIPANT_QUOTE_LIMIT], start=1):
                    quote_row = sample_by_id.get(message_id, {})
                    participant_quotes.append(
                        {
                            "participant_id": participant_id,
                            "display_name": display_name or fallback.get("display_name"),
                            "message_id": message_id,
                            "sent_at": quote_row.get("sent_at"),
                            "quote": quote_row.get("text"),
                            "rank": quote_rank,
                        }
                    )

            if not participants and participant_lookup:
                participants = [
                    {
                        "participant_id": participant_id,
                        "role_hint": None,
                        "stance_summary": None,
                        "message_count": int(payload.get("message_count") or 0),
                        "mention_count": 0,
                    }
                    for participant_id, payload in participant_lookup.items()
                ]

            if not evidence and not participant_quotes:
                continue

            topics.append(
                {
                    "week_key": candidate.week_key,
                    "week_topic_index": raw_index,
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
                    "participant_quotes": participant_quotes,
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "window_index": int(candidate.window_index or 1),
                        "source": "weekly_topic_agent",
                        "candidate_id": candidate.id,
                        "subtopics": _dedupe_strings(list(item.get("subtopics") or []), limit=8),
                        "interaction_patterns": _dedupe_strings(list(item.get("interaction_patterns") or []), limit=8),
                    },
                }
            )

        if not topics and sample_messages:
            topics.append(
                {
                    "week_key": candidate.week_key,
                    "week_topic_index": 1,
                    "title": f"{candidate.week_key} dense window {int(candidate.window_index or 1)}",
                    "summary": _compact_text(
                        " ".join(str(message.get("text") or "") for message in sample_messages[:12]),
                        limit=140,
                    ),
                    "start_at": candidate.start_at,
                    "end_at": candidate.end_at,
                    "start_message_id": candidate.start_message_id,
                    "end_message_id": candidate.end_message_id,
                    "message_count": candidate.message_count,
                    "participant_count": candidate.participant_count,
                    "keywords_json": top_terms("\n".join(str(message.get("text") or "") for message in sample_messages), limit=6),
                    "evidence_json": [
                        {
                            "message_id": item.get("message_id"),
                            "sender_name": item.get("sender_name"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                        }
                        for item in sample_messages[:3]
                        if item.get("message_id") is not None
                    ],
                    "participants": [
                        {
                            "participant_id": participant_id,
                            "role_hint": None,
                            "stance_summary": None,
                            "message_count": int(payload.get("message_count") or 0),
                            "mention_count": 0,
                        }
                        for participant_id, payload in participant_lookup.items()
                    ],
                    "participant_quotes": [
                        {
                            "participant_id": item.get("participant_id"),
                            "display_name": item.get("sender_name"),
                            "message_id": item.get("message_id"),
                            "sent_at": item.get("sent_at"),
                            "quote": item.get("text"),
                            "rank": 1,
                        }
                        for item in sample_messages[:4]
                        if item.get("participant_id") and item.get("message_id") is not None
                    ],
                    "metadata_json": {
                        "week_key": candidate.week_key,
                        "window_index": int(candidate.window_index or 1),
                        "source": "weekly_topic_fallback",
                        "candidate_id": candidate.id,
                        "subtopics": [],
                        "interaction_patterns": [],
                    },
                }
            )

        return self._normalize_topic_collection(topics)

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
        del run_id, chat_id
        if not top_users:
            return []
        active_users = [self._build_active_user_payload(item) for item in top_users]
        self._trace(
            "stage_progress",
            stage="active_users",
            message="Built active-user snapshots directly from the SQL top-user materialization.",
            active_user_count=len(active_users),
        )
        self._progress(
            progress_callback,
            "active_users",
            94,
            {
                "active_user_count": len(active_users),
                "usage": dict(self.usage_totals),
            },
        )
        return active_users


__all__ = [
    "TELEGRAM_WEEKLY_AGENT_MAX_ITERATIONS",
    "TELEGRAM_WEEKLY_AGENT_RETRIES",
    "TELEGRAM_WEEKLY_CANDIDATE_MESSAGE_LIMIT",
    "TELEGRAM_WEEKLY_MAX_WINDOWS",
    "TELEGRAM_WEEKLY_PARTICIPANT_QUOTE_LIMIT",
    "TELEGRAM_WEEKLY_TOOL_MAX_MESSAGES",
    "TELEGRAM_WEEKLY_TOPIC_CAP",
    "TelegramPreprocessWeeklyTopicsMixin",
]
