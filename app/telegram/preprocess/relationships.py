from __future__ import annotations

from app.telegram.preprocess import helpers as _helpers

globals().update(
    {
        name: getattr(_helpers, name)
        for name in dir(_helpers)
        if not name.startswith("__")
    }
)

class TelegramPreprocessRelationshipsMixin:
    @staticmethod
    def _active_user_payload_from_model(item: Any) -> dict[str, Any]:
        return {
            "rank": int(getattr(item, "rank", 0) or 0),
            "participant_id": str(getattr(item, "participant_id", "") or "").strip(),
            "uid": getattr(item, "uid", None),
            "username": getattr(item, "username", None),
            "display_name": getattr(item, "display_name", None),
            "primary_alias": getattr(item, "primary_alias", None),
            "aliases_json": list(getattr(item, "aliases_json", None) or []),
            "message_count": int(getattr(item, "message_count", 0) or 0),
            "first_seen_at": getattr(item, "first_seen_at", None),
            "last_seen_at": getattr(item, "last_seen_at", None),
            "evidence_json": list(getattr(item, "evidence_json", None) or []),
        }

    @staticmethod
    def _relationship_pair_ids(participant_a_id: str, participant_b_id: str) -> tuple[str, str]:
        if participant_b_id < participant_a_id:
            return participant_b_id, participant_a_id
        return participant_a_id, participant_b_id

    def _build_relationship_reply_context(
        self,
        messages: list[TelegramMessage],
        index_by_message_id: dict[int, int],
        replied_message_id: int,
        reply_message_id: int,
    ) -> dict[str, Any] | None:
        reply_index = index_by_message_id.get(reply_message_id)
        replied_index = index_by_message_id.get(replied_message_id)
        if reply_index is None or replied_index is None:
            return None
        start = max(min(reply_index, replied_index) - 1, 0)
        end = min(max(reply_index, replied_index) + 2, len(messages))
        window = messages[start:end]
        anchor = next(
            (item for item in window if int(item.telegram_message_id or 0) == reply_message_id),
            None,
        )
        replied = next(
            (item for item in window if int(item.telegram_message_id or 0) == replied_message_id),
            None,
        )
        if not anchor or not replied:
            return None
        payload = {
            "kind": "reply_context",
            "anchor_message_id": int(anchor.telegram_message_id or 0) or None,
            "message_ids": [
                int(item.telegram_message_id or 0)
                for item in window
                if int(item.telegram_message_id or 0) > 0
            ],
            "summary": f"{anchor.sender_name or 'unknown'} replied to {replied.sender_name or 'unknown'}",
            "messages": [
                {
                    "message_id": int(item.telegram_message_id or 0) or None,
                    "participant_id": item.participant_id,
                    "sender_name": item.sender_name,
                    "sent_at": item.sent_at.isoformat() if item.sent_at else None,
                    "text": _compact_text(item.text_normalized, limit=220),
                }
                for item in window
            ],
            "context_text": " ".join(_compact_text(item.text_normalized, limit=160) for item in window if item.text_normalized),
        }
        return payload

    def _build_relationship_topic_payload(
        self,
        topic: Any,
        participant_a_id: str,
        participant_b_id: str,
    ) -> dict[str, Any]:
        metadata = dict(getattr(topic, "metadata_json", None) or {})
        stance_by_id = {
            str(link.participant_id or "").strip(): str(getattr(link, "stance_summary", None) or "").strip()
            for link in list(getattr(topic, "participants", None) or [])
            if str(link.participant_id or "").strip()
        }
        quotes = []
        message_ids: list[int] = []
        for quote in list(getattr(topic, "quotes", None) or []):
            if quote.participant_id not in {participant_a_id, participant_b_id}:
                continue
            message_id = int(quote.telegram_message_id or 0) or None
            if message_id is not None:
                message_ids.append(message_id)
            quotes.append(
                {
                    "participant_id": quote.participant_id,
                    "display_name": quote.participant.display_name if quote.participant else None,
                    "message_id": message_id,
                    "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
                    "quote": quote.quote,
                }
            )
        for evidence in list(getattr(topic, "evidence_json", None) or []):
            if not isinstance(evidence, dict):
                continue
            try:
                message_id = int(evidence.get("message_id"))
            except (TypeError, ValueError):
                message_id = None
            if message_id is not None:
                message_ids.append(message_id)
        return {
            "kind": "topic",
            "topic_id": getattr(topic, "id", None),
            "week_key": getattr(topic, "week_key", None) or metadata.get("week_key"),
            "title": getattr(topic, "title", None),
            "summary": getattr(topic, "summary", None),
            "interaction_patterns": [
                str(item).strip()
                for item in (metadata.get("interaction_patterns") or [])
                if str(item).strip()
            ][:6],
            "participant_a_stance": stance_by_id.get(participant_a_id) or None,
            "participant_b_stance": stance_by_id.get(participant_b_id) or None,
            "message_ids": sorted({item for item in message_ids if item is not None})[:8],
            "quotes": quotes[:6],
        }

    def _build_relationship_candidate_metrics(
        self,
        *,
        selected_users: list[dict[str, Any]],
        topics: list[Any],
        messages: list[TelegramMessage],
    ) -> list[dict[str, Any]]:
        selected_ids = {
            str(item.get("participant_id") or "").strip(): item
            for item in selected_users
            if str(item.get("participant_id") or "").strip()
        }
        pairs: dict[tuple[str, str], dict[str, Any]] = {}

        def ensure_pair(participant_a_id: str, participant_b_id: str) -> dict[str, Any]:
            pair_key = self._relationship_pair_ids(participant_a_id, participant_b_id)
            if pair_key not in pairs:
                pairs[pair_key] = {
                    "participant_a_id": pair_key[0],
                    "participant_b_id": pair_key[1],
                    "reply_total": 0,
                    "reply_a_to_b": 0,
                    "reply_b_to_a": 0,
                    "shared_topic_count": 0,
                    "shared_topics_with_both_quotes": 0,
                    "topic_evidence": [],
                    "reply_contexts": [],
                    "signal_fragments": [],
                }
            return pairs[pair_key]

        index_by_message_id = {
            int(message.telegram_message_id): index
            for index, message in enumerate(messages)
            if message.telegram_message_id is not None
        }
        message_by_id = {
            int(message.telegram_message_id): message
            for message in messages
            if message.telegram_message_id is not None
        }
        for message in messages:
            participant_id = str(message.participant_id or "").strip()
            if not participant_id or participant_id not in selected_ids or message.reply_to_message_id is None:
                continue
            replied = message_by_id.get(int(message.reply_to_message_id))
            if not replied:
                continue
            replied_participant_id = str(replied.participant_id or "").strip()
            if not replied_participant_id or replied_participant_id not in selected_ids or replied_participant_id == participant_id:
                continue
            pair = ensure_pair(participant_id, replied_participant_id)
            pair["reply_total"] += 1
            if participant_id == pair["participant_a_id"]:
                pair["reply_a_to_b"] += 1
            else:
                pair["reply_b_to_a"] += 1
            context_payload = self._build_relationship_reply_context(
                messages,
                index_by_message_id,
                int(replied.telegram_message_id or 0),
                int(message.telegram_message_id or 0),
            )
            if context_payload:
                pair["reply_contexts"].append(context_payload)
                pair["signal_fragments"].append(str(context_payload.get("context_text") or ""))

        for topic in topics:
            topic_participants = [
                link
                for link in list(getattr(topic, "participants", None) or [])
                if str(link.participant_id or "").strip() in selected_ids
            ]
            participant_ids = sorted(
                {
                    str(link.participant_id or "").strip()
                    for link in topic_participants
                    if str(link.participant_id or "").strip()
                }
            )
            quote_participants = {
                str(quote.participant_id or "").strip()
                for quote in list(getattr(topic, "quotes", None) or [])
                if str(quote.participant_id or "").strip() in selected_ids
            }
            for index, participant_a_id in enumerate(participant_ids):
                for participant_b_id in participant_ids[index + 1 :]:
                    pair = ensure_pair(participant_a_id, participant_b_id)
                    pair["shared_topic_count"] += 1
                    if participant_a_id in quote_participants and participant_b_id in quote_participants:
                        pair["shared_topics_with_both_quotes"] += 1
                    topic_payload = self._build_relationship_topic_payload(topic, participant_a_id, participant_b_id)
                    pair["topic_evidence"].append(topic_payload)
                    pair["signal_fragments"].append(
                        " ".join(
                            [
                                str(topic_payload.get("summary") or ""),
                                " ".join(topic_payload.get("interaction_patterns") or []),
                                str(topic_payload.get("participant_a_stance") or ""),
                                str(topic_payload.get("participant_b_stance") or ""),
                                " ".join(str(item.get("quote") or "") for item in topic_payload.get("quotes") or []),
                            ]
                        ).strip()
                    )

        candidates: list[dict[str, Any]] = []
        for metrics in pairs.values():
            reply_total = int(metrics["reply_total"] or 0)
            reply_a_to_b = int(metrics["reply_a_to_b"] or 0)
            reply_b_to_a = int(metrics["reply_b_to_a"] or 0)
            shared_topic_count = int(metrics["shared_topic_count"] or 0)
            if not (
                reply_total >= 2
                or (reply_a_to_b >= 1 and reply_b_to_a >= 1)
                or shared_topic_count >= 2
            ):
                continue
            reply_score = min(reply_total / 6.0, 1.0)
            shared_topic_score = min(shared_topic_count / 5.0, 1.0)
            co_quote_score = min(int(metrics["shared_topics_with_both_quotes"] or 0) / 4.0, 1.0)
            interaction_strength = (0.55 * reply_score) + (0.35 * shared_topic_score) + (0.10 * co_quote_score)
            metrics.update(
                {
                    "reply_score": round(reply_score, 4),
                    "shared_topic_score": round(shared_topic_score, 4),
                    "co_quote_score": round(co_quote_score, 4),
                    "interaction_strength": round(interaction_strength, 4),
                }
            )
            candidates.append(metrics)

        candidates.sort(
            key=lambda item: (
                float(item.get("interaction_strength") or 0.0),
                int(item.get("reply_total") or 0),
                int(item.get("shared_topic_count") or 0),
            ),
            reverse=True,
        )
        return candidates

    @staticmethod
    def _normalize_relationship_label(value: Any) -> str:
        label = str(value or "").strip().lower()
        if label in {"friendly", "neutral", "tense", "unclear"}:
            return label
        if label in {"hostile", "enemy", "opposed"}:
            return "tense"
        return "unclear"

    def _heuristic_relationship_label(self, candidate: dict[str, Any]) -> tuple[str, float]:
        text = " ".join(str(item or "") for item in (candidate.get("signal_fragments") or [])).lower()
        positive_keywords = (
            "agree",
            "agreed",
            "support",
            "supports",
            "supported",
            "thanks",
            "confirm",
            "aligned",
            "协作",
            "支持",
            "附和",
            "认同",
            "配合",
            "补充支持",
        )
        tense_keywords = (
            "disagree",
            "argue",
            "conflict",
            "oppose",
            "hostile",
            "反驳",
            "冲突",
            "对立",
            "拆台",
            "针锋相对",
            "质疑",
        )
        positive_hits = sum(text.count(keyword) for keyword in positive_keywords)
        tense_hits = sum(text.count(keyword) for keyword in tense_keywords)
        interaction_strength = float(candidate.get("interaction_strength") or 0.0)
        if tense_hits > positive_hits and tense_hits > 0:
            return "tense", round(min(max(0.58, interaction_strength), 0.82), 4)
        if positive_hits > tense_hits and positive_hits > 0:
            return "friendly", round(min(max(0.58, interaction_strength), 0.82), 4)
        if interaction_strength >= 0.45:
            return "neutral", round(min(max(0.52, interaction_strength), 0.74), 4)
        return "unclear", round(min(interaction_strength, 0.55), 4)

    def _summarize_relationship_edge(
        self,
        candidate: dict[str, Any],
        participant_lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError("Relationship edge LLM summary requires a chat client.")

        participant_a = participant_lookup.get(candidate["participant_a_id"], {})
        participant_b = participant_lookup.get(candidate["participant_b_id"], {})
        request_key = f"relationship-{candidate['participant_a_id'][:8]}-{candidate['participant_b_id'][:8]}"
        label = f"Relationship edge {participant_a.get('primary_alias') or participant_a.get('display_name') or candidate['participant_a_id']} / {participant_b.get('primary_alias') or participant_b.get('display_name') or candidate['participant_b_id']}"
        payload = {
            "participant_a": {
                "participant_id": candidate["participant_a_id"],
                "label": participant_a.get("primary_alias") or participant_a.get("display_name") or participant_a.get("username") or candidate["participant_a_id"],
                "username": participant_a.get("username"),
                "message_count": participant_a.get("message_count"),
            },
            "participant_b": {
                "participant_id": candidate["participant_b_id"],
                "label": participant_b.get("primary_alias") or participant_b.get("display_name") or participant_b.get("username") or candidate["participant_b_id"],
                "username": participant_b.get("username"),
                "message_count": participant_b.get("message_count"),
            },
            "interaction_strength": candidate.get("interaction_strength"),
            "metrics": {
                "reply_total": candidate.get("reply_total"),
                "reply_a_to_b": candidate.get("reply_a_to_b"),
                "reply_b_to_a": candidate.get("reply_b_to_a"),
                "shared_topic_count": candidate.get("shared_topic_count"),
                "shared_topics_with_both_quotes": candidate.get("shared_topics_with_both_quotes"),
                "heuristic_label": candidate.get("heuristic_label"),
            },
            "shared_topics": list(candidate.get("topic_evidence") or [])[:TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE],
            "reply_contexts": list(candidate.get("reply_contexts") or [])[:TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS],
            "counterevidence": list(candidate.get("counterevidence_json") or [])[:TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE],
        }
        self._trace(
            "llm_request_started",
            stage="relationship_snapshot",
            agent="relationship_edge_agent",
            request_key=request_key,
            label=label,
            prompt_preview=_preview_text(payload),
        )
        result = self.client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "你是 Telegram 群聊关系快照分析代理。\n"
                        "请基于结构化证据判断两位参与者的关系。\n"
                        "仅返回 JSON，键必须是 relation_label, confidence, summary, supporting_signals, counter_signals, evidence_message_ids。\n"
                        "relation_label 只允许 friendly, neutral, tense, unclear。\n"
                        "friendly 表示持续支持、协作、积极接力。\n"
                        "neutral 表示互动稳定，但不明显亲近或敌对。\n"
                        "tense 表示持续反驳、对立、冲突拉扯。\n"
                        "unclear 表示证据混杂或不足。\n"
                        "summary、supporting_signals、counter_signals 必须使用中文。\n"
                        "不要编造证据。"
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False, indent=2),
                },
            ],
            model=self.llm_config.model if self.llm_config else None,
            temperature=0.1,
            max_tokens=520,
        )
        self._add_usage(result.usage)
        self._trace(
            "llm_request_completed",
            stage="relationship_snapshot",
            agent="relationship_edge_agent",
            request_key=request_key,
            label=label,
            usage=result.usage,
            response_text_preview=_preview_text(result.content),
        )
        parsed = parse_json_response(str(result.content or ""), fallback=True)
        try:
            confidence = float(parsed.get("confidence"))
        except (TypeError, ValueError):
            confidence = float(candidate.get("interaction_strength") or 0.0)
        return {
            "relation_label": self._normalize_relationship_label(parsed.get("relation_label")),
            "confidence": round(min(max(confidence, 0.0), 1.0), 4),
            "summary": str(parsed.get("summary") or "").strip() or None,
            "supporting_signals": _dedupe_strings(list(parsed.get("supporting_signals") or []), limit=6),
            "counter_signals": _dedupe_strings(list(parsed.get("counter_signals") or []), limit=6),
            "evidence_message_ids": _coerce_message_ids(parsed.get("evidence_message_ids") or [])[:8],
        }

    def build_relationship_snapshot(
        self,
        run: TelegramPreprocessRun,
        *,
        progress_callback: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> dict[str, Any]:
        top_users = repository.list_telegram_preprocess_top_users(self.session, self.project.id, run_id=run.id)
        existing_active_users = repository.list_telegram_preprocess_active_users(self.session, self.project.id, run_id=run.id)
        active_users = (
            [self._active_user_payload_from_model(item) for item in existing_active_users]
            if existing_active_users
            else self._build_active_users(run.id, run.chat_id or "", top_users, progress_callback=progress_callback)
        )
        if not existing_active_users and active_users:
            repository.replace_telegram_preprocess_active_users(
                self.session,
                run_id=run.id,
                project_id=self.project.id,
                chat_id=run.chat_id,
                active_users=active_users,
            )
            self.session.flush()

        selected_users = active_users[:TELEGRAM_RELATIONSHIP_USER_LIMIT]
        snapshot = repository.create_or_replace_telegram_relationship_snapshot(
            self.session,
            run_id=run.id,
            project_id=self.project.id,
            chat_id=run.chat_id,
            status="running",
            analyzed_user_count=len(selected_users),
            candidate_pair_count=0,
            llm_pair_count=0,
            started_at=utcnow(),
            summary_json={
                "friendly_count": 0,
                "neutral_count": 0,
                "tense_count": 0,
                "unclear_count": 0,
                "edge_count": 0,
                "central_users": [],
                "isolated_users": [
                    {
                        "participant_id": item.get("participant_id"),
                        "label": item.get("primary_alias") or item.get("display_name") or item.get("username") or item.get("participant_id"),
                    }
                    for item in selected_users
                ],
                "snapshot_notes": [],
            },
        )
        self._trace(
            "agent_started",
            stage="relationship_snapshot",
            agent="relationship_snapshot_agent",
            message=f"Building Telegram relationship snapshot for {len(selected_users)} active users.",
            analyzed_user_count=len(selected_users),
        )
        if not selected_users:
            snapshot.status = "completed"
            snapshot.finished_at = utcnow()
            snapshot.summary_json = {
                **dict(snapshot.summary_json or {}),
                "snapshot_notes": ["No active users were available for relationship analysis."],
            }
            self.session.flush()
            self._progress(
                progress_callback,
                "relationship_snapshot",
                98,
                {
                    "active_user_count": 0,
                    "relationship_snapshot_id": snapshot.id,
                    "relationship_status": snapshot.status,
                    "relationship_edge_count": 0,
                },
            )
            return {
                "snapshot_id": snapshot.id,
                "status": snapshot.status,
                "active_user_count": 0,
                "candidate_pair_count": 0,
                "edge_count": 0,
                "summary": dict(snapshot.summary_json or {}),
            }

        topics = repository.list_telegram_preprocess_topics(self.session, self.project.id, run_id=run.id)
        messages = repository.list_telegram_messages(
            self.session,
            self.project.id,
            chat_id=run.chat_id,
            ascending=True,
        )
        participant_lookup = {
            str(item.get("participant_id") or "").strip(): item
            for item in selected_users
            if str(item.get("participant_id") or "").strip()
        }
        candidate_pairs = self._build_relationship_candidate_metrics(
            selected_users=selected_users,
            topics=topics,
            messages=messages,
        )
        filtered_pairs = [
            item
            for item in candidate_pairs
            if float(item.get("interaction_strength") or 0.0) >= TELEGRAM_RELATIONSHIP_MIN_STRENGTH
        ]
        llm_candidates = filtered_pairs[:TELEGRAM_RELATIONSHIP_LLM_EDGE_LIMIT]
        llm_candidate_keys = {
            (item["participant_a_id"], item["participant_b_id"])
            for item in llm_candidates
        }
        llm_results_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        llm_errors_by_key: dict[tuple[str, str], str] = {}
        llm_max_workers = max(
            TELEGRAM_PREPROCESS_MIN_CONCURRENCY,
            min(len(llm_candidates), self.weekly_summary_concurrency),
        )
        if self.client and llm_candidates:
            self._trace(
                "stage_progress",
                stage="relationship_snapshot",
                message=(
                    f"Running relationship edge analysis with concurrency "
                    f"{llm_max_workers} for {len(llm_candidates)} candidate pairs."
                ),
                llm_pair_count=len(llm_candidates),
                weekly_summary_concurrency=self.weekly_summary_concurrency,
                relationship_concurrency=llm_max_workers,
            )
            with ThreadPoolExecutor(
                max_workers=llm_max_workers,
                thread_name_prefix="telegram-relationship-edge",
            ) as executor:
                future_map = {
                    executor.submit(self._summarize_relationship_edge, candidate, participant_lookup): (
                        candidate["participant_a_id"],
                        candidate["participant_b_id"],
                    )
                    for candidate in llm_candidates
                }
                for future in as_completed(future_map):
                    pair_key = future_map[future]
                    try:
                        llm_results_by_key[pair_key] = future.result()
                    except Exception as exc:
                        llm_errors_by_key[pair_key] = str(exc)
                        self._trace(
                            "agent_retry",
                            stage="relationship_snapshot",
                            agent="relationship_edge_agent",
                            participant_a_id=pair_key[0],
                            participant_b_id=pair_key[1],
                            message="Relationship edge summary fell back to rule-only handling.",
                            error=str(exc),
                        )
        partial_snapshot = False
        snapshot_notes: list[str] = []
        edge_payloads: list[dict[str, Any]] = []
        for candidate in filtered_pairs:
            heuristic_label, heuristic_confidence = self._heuristic_relationship_label(candidate)
            candidate["heuristic_label"] = heuristic_label
            evidence_pool = (list(candidate.get("topic_evidence") or [])[:TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE]) + (
                list(candidate.get("reply_contexts") or [])[:TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS]
            )
            counterevidence_pool = list(candidate.get("reply_contexts") or [])[TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS:]
            if len(counterevidence_pool) < TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE:
                counterevidence_pool.extend(list(candidate.get("topic_evidence") or [])[TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE:])
            counterevidence_pool = counterevidence_pool[:TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE]
            candidate["counterevidence_json"] = counterevidence_pool
            message_evidence_by_id: dict[int, dict[str, Any]] = {}
            for item in evidence_pool:
                for message_id in item.get("message_ids") or []:
                    try:
                        normalized_message_id = int(message_id)
                    except (TypeError, ValueError):
                        continue
                    message_evidence_by_id.setdefault(normalized_message_id, item)
                anchor_message_id = item.get("anchor_message_id")
                if anchor_message_id is not None:
                    try:
                        message_evidence_by_id.setdefault(int(anchor_message_id), item)
                    except (TypeError, ValueError):
                        pass

            relation_label = heuristic_label
            confidence = heuristic_confidence
            summary = None
            supporting_signals: list[str] = []
            counter_signals: list[str] = []
            pair_key = (candidate["participant_a_id"], candidate["participant_b_id"])
            if pair_key in llm_candidate_keys:
                if self.client:
                    try:
                        if pair_key in llm_errors_by_key:
                            raise RuntimeError(llm_errors_by_key[pair_key])
                        llm_payload = llm_results_by_key[pair_key]
                        relation_label = llm_payload["relation_label"] or heuristic_label
                        confidence = llm_payload["confidence"] or heuristic_confidence
                        summary = llm_payload["summary"]
                        supporting_signals = list(llm_payload.get("supporting_signals") or [])
                        counter_signals = list(llm_payload.get("counter_signals") or [])
                        evidence_ids = list(llm_payload.get("evidence_message_ids") or [])
                        if evidence_ids:
                            selected_evidence: list[dict[str, Any]] = []
                            seen_evidence: set[int] = set()
                            for message_id in evidence_ids:
                                item = message_evidence_by_id.get(message_id)
                                if not item or id(item) in seen_evidence:
                                    continue
                                seen_evidence.add(id(item))
                                selected_evidence.append(item)
                            if selected_evidence:
                                evidence_pool = selected_evidence
                    except Exception as exc:
                        partial_snapshot = True
                        relation_label = "unclear"
                        confidence = round(min(float(candidate.get("interaction_strength") or 0.0), 0.6), 4)
                        summary = None
                        snapshot_notes.append(
                            f"LLM summary fallback for pair {candidate['participant_a_id']} / {candidate['participant_b_id']}: {exc}"
                        )
                else:
                    partial_snapshot = True
                    relation_label = "unclear"
                    confidence = round(min(float(candidate.get("interaction_strength") or 0.0), 0.6), 4)
                    snapshot_notes.append("Chat LLM was unavailable; top relationship edges fell back to rule-only evidence.")

            edge_payloads.append(
                {
                    "participant_a_id": candidate["participant_a_id"],
                    "participant_b_id": candidate["participant_b_id"],
                    "interaction_strength": round(float(candidate.get("interaction_strength") or 0.0), 4),
                    "confidence": confidence,
                    "relation_label": self._normalize_relationship_label(relation_label),
                    "summary": summary,
                    "evidence_json": evidence_pool[:8],
                    "counterevidence_json": counterevidence_pool,
                    "metrics_json": {
                        "reply_total": int(candidate.get("reply_total") or 0),
                        "reply_a_to_b": int(candidate.get("reply_a_to_b") or 0),
                        "reply_b_to_a": int(candidate.get("reply_b_to_a") or 0),
                        "shared_topic_count": int(candidate.get("shared_topic_count") or 0),
                        "shared_topics_with_both_quotes": int(candidate.get("shared_topics_with_both_quotes") or 0),
                        "reply_score": float(candidate.get("reply_score") or 0.0),
                        "shared_topic_score": float(candidate.get("shared_topic_score") or 0.0),
                        "co_quote_score": float(candidate.get("co_quote_score") or 0.0),
                        "heuristic_label": heuristic_label,
                        "supporting_signals": supporting_signals,
                        "counter_signals": counter_signals,
                    },
                }
            )

        repository.replace_telegram_relationship_edges(
            self.session,
            snapshot_id=snapshot.id,
            project_id=self.project.id,
            edges=edge_payloads,
        )

        relationship_counts = {"friendly": 0, "neutral": 0, "tense": 0, "unclear": 0}
        weighted_degree: dict[str, float] = defaultdict(float)
        edge_count_by_participant: dict[str, int] = defaultdict(int)
        connected_ids: set[str] = set()
        for edge in edge_payloads:
            relation_label = self._normalize_relationship_label(edge.get("relation_label"))
            relationship_counts[relation_label] += 1
            participant_a_id = edge["participant_a_id"]
            participant_b_id = edge["participant_b_id"]
            connected_ids.update({participant_a_id, participant_b_id})
            weight = float(edge.get("interaction_strength") or 0.0)
            weighted_degree[participant_a_id] += weight
            weighted_degree[participant_b_id] += weight
            edge_count_by_participant[participant_a_id] += 1
            edge_count_by_participant[participant_b_id] += 1

        central_users = sorted(
            (
                {
                    "participant_id": participant_id,
                    "label": participant_lookup.get(participant_id, {}).get("primary_alias")
                    or participant_lookup.get(participant_id, {}).get("display_name")
                    or participant_lookup.get(participant_id, {}).get("username")
                    or participant_id,
                    "weighted_degree": round(score, 4),
                    "edge_count": edge_count_by_participant.get(participant_id, 0),
                }
                for participant_id, score in weighted_degree.items()
            ),
            key=lambda item: (float(item.get("weighted_degree") or 0.0), int(item.get("edge_count") or 0)),
            reverse=True,
        )[:5]
        isolated_users = [
            {
                "participant_id": participant_id,
                "label": participant_lookup.get(participant_id, {}).get("primary_alias")
                or participant_lookup.get(participant_id, {}).get("display_name")
                or participant_lookup.get(participant_id, {}).get("username")
                or participant_id,
            }
            for participant_id in participant_lookup
            if participant_id not in connected_ids
        ]
        if not edge_payloads:
            snapshot_notes.append("No participant pairs met the minimum interaction threshold.")

        snapshot.status = "partial" if partial_snapshot else "completed"
        snapshot.finished_at = utcnow()
        snapshot.analyzed_user_count = len(selected_users)
        snapshot.candidate_pair_count = len(candidate_pairs)
        snapshot.llm_pair_count = len(llm_candidates) if self.client else 0
        snapshot.error_message = None
        snapshot.summary_json = {
            "friendly_count": relationship_counts["friendly"],
            "neutral_count": relationship_counts["neutral"],
            "tense_count": relationship_counts["tense"],
            "unclear_count": relationship_counts["unclear"],
            "edge_count": len(edge_payloads),
            "central_users": central_users,
            "isolated_users": isolated_users,
            "snapshot_notes": _dedupe_strings(snapshot_notes, limit=8),
        }
        self._trace(
            "agent_completed",
            stage="relationship_snapshot",
            agent="relationship_snapshot_agent",
            message=f"Built Telegram relationship snapshot with {len(edge_payloads)} edges.",
            relationship_snapshot_id=snapshot.id,
            relationship_status=snapshot.status,
            relationship_edge_count=len(edge_payloads),
        )
        self._progress(
            progress_callback,
            "relationship_snapshot",
            98,
            {
                "active_user_count": len(selected_users),
                "relationship_snapshot_id": snapshot.id,
                "relationship_status": snapshot.status,
                "relationship_edge_count": len(edge_payloads),
                "relationship_summary": dict(snapshot.summary_json or {}),
                "usage": dict(self.usage_totals),
            },
        )
        return {
            "snapshot_id": snapshot.id,
            "status": snapshot.status,
            "active_user_count": len(selected_users),
            "candidate_pair_count": len(candidate_pairs),
            "edge_count": len(edge_payloads),
            "summary": dict(snapshot.summary_json or {}),
        }


__all__ = [
    "TELEGRAM_RELATIONSHIP_LLM_EDGE_LIMIT",
    "TELEGRAM_RELATIONSHIP_MAX_COUNTEREVIDENCE",
    "TELEGRAM_RELATIONSHIP_MAX_REPLY_CONTEXTS",
    "TELEGRAM_RELATIONSHIP_MAX_TOPIC_EVIDENCE",
    "TELEGRAM_RELATIONSHIP_MIN_STRENGTH",
    "TELEGRAM_RELATIONSHIP_USER_LIMIT",
    "TelegramPreprocessRelationshipsMixin",
]
