from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.analysis.facets import FacetDefinition
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import Project, TelegramMessage, TelegramParticipant
from app.schemas import ServiceConfig
from app.storage import repository

TELEGRAM_TOOL_LOOP_MAX_STEPS = 7
TELEGRAM_TOOL_MAX_MESSAGES = 18


def _compact_message(message: TelegramMessage) -> dict[str, Any]:
    text = " ".join((message.text_normalized or "").split()).strip()
    return {
        "message_id": message.telegram_message_id,
        "participant_id": message.participant_id,
        "sender_name": message.sender_name,
        "sent_at": message.sent_at.isoformat() if message.sent_at else None,
        "reply_to_message_id": message.reply_to_message_id,
        "text": text[:260],
    }


@dataclass(slots=True)
class TelegramFacetAnalysisResult:
    payload: dict[str, Any]
    retrieval_trace: dict[str, Any]
    hit_count: int


class TelegramAnalysisAgent:
    def __init__(
        self,
        session: Session,
        project: Project,
        *,
        llm_config: ServiceConfig | None,
        log_path: str | None = None,
    ) -> None:
        self.session = session
        self.project = project
        self.llm_config = llm_config
        self.log_path = log_path
        self.client = OpenAICompatibleClient(llm_config, log_path=log_path) if llm_config else None

    def resolve_target_user(
        self,
        *,
        target_user_query: str | None,
        participant_id: str | None,
        preprocess_run_id: str | None = None,
    ) -> dict[str, Any]:
        preprocess_run = (
            repository.get_telegram_preprocess_run(self.session, preprocess_run_id)
            if preprocess_run_id
            else repository.get_latest_successful_telegram_preprocess_run(self.session, self.project.id)
        )
        if not preprocess_run or preprocess_run.project_id != self.project.id or preprocess_run.status != "completed":
            raise ValueError("A successful Telegram preprocess run is required before analysis.")

        active_users = repository.list_telegram_preprocess_active_users(
            self.session,
            self.project.id,
            run_id=preprocess_run.id,
        )
        query = (target_user_query or "").strip().lower()

        if participant_id:
            explicit = next((item for item in active_users if item.participant_id == participant_id), None)
            if explicit:
                return self._active_user_snapshot(explicit, preprocess_run.id)
            participant = repository.get_telegram_participant(self.session, participant_id)
            if participant:
                return self._participant_snapshot(participant, preprocess_run.id)

        if query:
            matched_active = [
                item
                for item in active_users
                if any(
                    query in value.lower()
                    for value in [
                        item.uid or "",
                        item.username or "",
                        item.display_name or "",
                        item.primary_alias or "",
                        " ".join(item.aliases_json or []),
                    ]
                    if value
                )
            ]
            if matched_active:
                return self._active_user_snapshot(matched_active[0], preprocess_run.id)
            matched_participants = repository.search_telegram_participants(self.session, self.project.id, query, limit=12)
            if matched_participants:
                return self._participant_snapshot(matched_participants[0], preprocess_run.id)

        if active_users:
            return self._active_user_snapshot(active_users[0], preprocess_run.id)
        participants = repository.list_telegram_participants(self.session, self.project.id, limit=1)
        if participants:
            return self._participant_snapshot(participants[0], preprocess_run.id)
        raise ValueError("No Telegram participants are available for analysis.")

    def analyze_facet(
        self,
        facet: FacetDefinition,
        *,
        target_user_query: str | None,
        participant_id: str | None,
        analysis_context: str | None,
        preprocess_run_id: str | None = None,
    ) -> TelegramFacetAnalysisResult:
        target_user = self.resolve_target_user(
            target_user_query=target_user_query,
            participant_id=participant_id,
            preprocess_run_id=preprocess_run_id,
        )
        preprocess_run_id = str(target_user["preprocess_run_id"])
        related_topics = repository.list_telegram_preprocess_topics(
            self.session,
            self.project.id,
            run_id=preprocess_run_id,
        )
        related_topics = [
            topic
            for topic in related_topics
            if any(link.participant_id == target_user["participant_id"] for link in topic.participants)
        ]

        if not self.client:
            return self._heuristic_facet_result(facet, target_user, related_topics, preprocess_run_id)

        system_prompt = (
            "You are analyzing a Telegram export stored in SQL.\n"
            "This mode NEVER uses embeddings.\n"
            "Follow this order:\n"
            "1. Inspect the target user profile.\n"
            "2. Query related preprocess topics.\n"
            "3. Fetch exact raw messages as evidence.\n"
            "4. Use analyze_database to keep context compact.\n"
            "Return JSON with keys summary, bullets, confidence, evidence, conflicts, notes.\n"
            "Each evidence item must contain message_id, sender_name, sent_at, quote, reason.\n"
        )
        user_prompt = json.dumps(
            {
                "project": self.project.name,
                "facet_key": facet.key,
                "facet_label": facet.label,
                "facet_purpose": facet.purpose,
                "target_user": target_user,
                "analysis_context": analysis_context or "",
            },
            ensure_ascii=False,
            indent=2,
        )
        result = self._run_tool_loop(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            target_user=target_user,
            preprocess_run_id=preprocess_run_id,
        )
        parsed = parse_json_response(str(result["content"] or ""), fallback=True)
        evidence = self._normalize_agent_evidence(parsed.get("evidence"), fallback=result["fallback_evidence"])
        payload = {
            "summary": str(parsed.get("summary") or "").strip() or f"Telegram evidence around {facet.label} remains concentrated in the selected user's related topics.",
            "bullets": [
                str(item).strip()
                for item in (parsed.get("bullets") or [])
                if str(item).strip()
            ][:8],
            "confidence": self._parse_confidence(parsed.get("confidence"), default=0.68),
            "evidence": evidence,
            "conflicts": [
                {
                    "title": str(item.get("title") or "").strip(),
                    "detail": str(item.get("detail") or "").strip(),
                }
                for item in (parsed.get("conflicts") or [])
                if isinstance(item, dict)
            ][:5],
            "notes": str(parsed.get("notes") or "").strip() or None,
            "_meta": {
                "llm_called": True,
                "llm_success": True,
                "llm_attempts": int(result["iterations"]),
                "provider_kind": self.llm_config.provider_kind if self.llm_config else None,
                "api_mode": self.llm_config.api_mode if self.llm_config else None,
                "llm_model": result["model"] or (self.llm_config.model if self.llm_config else None),
                "prompt_tokens": int(result["usage"].get("prompt_tokens", 0) or 0),
                "completion_tokens": int(result["usage"].get("completion_tokens", 0) or 0),
                "total_tokens": int(result["usage"].get("total_tokens", 0) or 0),
                "cache_creation_tokens": int(result["usage"].get("cache_creation_tokens", 0) or 0),
                "cache_read_tokens": int(result["usage"].get("cache_read_tokens", 0) or 0),
                "request_url": self.client.endpoint_url("/responses") if self.client else None,
                "request_payload": {"mode": "telegram_user_analysis", "facet": facet.key},
                "raw_text": str(result["content"] or ""),
                "llm_error": None,
                "log_path": self.log_path,
            },
        }
        retrieval_trace = {
            "mode": "telegram_agent",
            "evidence_kind": "telegram_messages",
            "tool_calls": result["tool_trace"],
            "preprocess_run_id": preprocess_run_id,
            "target_user": target_user,
            "topic_ids": sorted(result["used_topic_ids"]),
            "queried_message_ids": sorted(result["queried_message_ids"])[:96],
            "topic_count_used": len(result["used_topic_ids"]),
        }
        return TelegramFacetAnalysisResult(
            payload=payload,
            retrieval_trace=retrieval_trace,
            hit_count=len(evidence),
        )

    def _run_tool_loop(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        target_user: dict[str, Any],
        preprocess_run_id: str,
    ) -> dict[str, Any]:
        assert self.client is not None
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 0,
        }
        tool_trace: list[dict[str, Any]] = []
        used_topic_ids: set[str] = set()
        queried_message_ids: set[int] = set()
        fallback_evidence: list[dict[str, Any]] = []
        model_name = self.llm_config.model if self.llm_config else None

        for iteration in range(1, TELEGRAM_TOOL_LOOP_MAX_STEPS + 1):
            round_result = self.client.tool_round(
                messages,
                self._tool_schemas(),
                model=self.llm_config.model if self.llm_config else None,
                temperature=0.2,
                max_tokens=1200,
            )
            model_name = round_result.model or model_name
            for key in usage:
                usage[key] += int(round_result.usage.get(key, 0) or 0)
            if not round_result.tool_calls:
                return {
                    "content": round_result.content,
                    "usage": usage,
                    "tool_trace": tool_trace,
                    "used_topic_ids": used_topic_ids,
                    "queried_message_ids": queried_message_ids,
                    "fallback_evidence": fallback_evidence,
                    "iterations": iteration,
                    "model": model_name,
                }
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
                output, state = self._execute_tool(call.name, call.arguments, target_user, preprocess_run_id)
                used_topic_ids.update(state.get("topic_ids", []))
                queried_message_ids.update(state.get("message_ids", []))
                for key in usage:
                    usage[key] += int((state.get("usage") or {}).get(key, 0) or 0)
                if state.get("messages"):
                    fallback_evidence = self._messages_to_evidence(state.get("messages", []))[:8]
                tool_trace.append(
                    {
                        "tool": call.name,
                        "arguments": call.arguments,
                        "topic_ids": sorted(state.get("topic_ids", [])),
                        "message_ids": sorted(state.get("message_ids", []))[:32],
                    }
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "name": call.name,
                        "content": json.dumps(output, ensure_ascii=False),
                    }
                )
        raise LLMError("Telegram analysis exceeded the maximum tool iterations.")

    def _execute_tool(
        self,
        name: str,
        args: dict[str, Any],
        target_user: dict[str, Any],
        preprocess_run_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if name == "list_target_user_candidates":
            query = str(args.get("query") or "").strip()
            candidates: list[dict[str, Any]] = []
            active_users = repository.list_telegram_preprocess_active_users(
                self.session,
                self.project.id,
                run_id=preprocess_run_id,
            )
            query_lower = query.lower()
            for item in active_users:
                if query and not any(
                    query_lower in value.lower()
                    for value in [
                        item.primary_alias or "",
                        item.display_name or "",
                        item.username or "",
                        item.uid or "",
                        " ".join(item.aliases_json or []),
                    ]
                    if value
                ):
                    continue
                candidates.append(self._active_user_snapshot(item, preprocess_run_id))
            if not candidates and query:
                candidates.extend(
                    self._participant_snapshot(item, preprocess_run_id)
                    for item in repository.search_telegram_participants(self.session, self.project.id, query, limit=10)
                )
            return {"candidates": candidates[:10]}, {}

        if name == "get_target_user_profile":
            return {"target_user": target_user}, {}

        if name == "list_related_topics":
            participant_id = str(args.get("participant_id") or target_user["participant_id"]).strip()
            query = str(args.get("query") or "").strip().lower()
            topics = repository.list_telegram_preprocess_topics(
                self.session,
                self.project.id,
                run_id=preprocess_run_id,
            )
            matched = []
            for topic in topics:
                if not any(link.participant_id == participant_id for link in topic.participants):
                    continue
                if query and query not in str(topic.title or "").lower() and query not in str(topic.summary or "").lower():
                    continue
                matched.append(
                    {
                        "topic_id": topic.id,
                        "title": topic.title,
                        "summary": topic.summary,
                        "start_message_id": topic.start_message_id,
                        "end_message_id": topic.end_message_id,
                        "message_count": topic.message_count,
                        "participant_count": topic.participant_count,
                        "keywords": topic.keywords_json or [],
                        "participants": [
                            {
                                "participant_id": link.participant_id,
                                "display_name": link.participant.display_name if link.participant else None,
                                "role_hint": link.role_hint,
                                "message_count": link.message_count,
                                "mention_count": link.mention_count,
                            }
                            for link in topic.participants
                        ],
                    }
                )
            return {"topics": matched[:12]}, {"topic_ids": {item["topic_id"] for item in matched[:12]}}

        if name == "query_telegram_messages":
            participant_id = str(args.get("participant_id") or target_user["participant_id"]).strip()
            topic_ids = [str(item) for item in (args.get("topic_ids") or []) if str(item).strip()]
            message_id_start = args.get("message_id_start")
            message_id_end = args.get("message_id_end")
            if topic_ids:
                topics = repository.list_telegram_preprocess_topics(
                    self.session,
                    self.project.id,
                    run_id=preprocess_run_id,
                )
                scoped_topics = [item for item in topics if item.id in topic_ids]
                if scoped_topics:
                    start_candidates = [item.start_message_id for item in scoped_topics if item.start_message_id is not None]
                    end_candidates = [item.end_message_id for item in scoped_topics if item.end_message_id is not None]
                    if start_candidates:
                        message_id_start = min(start_candidates) if message_id_start is None else min(min(start_candidates), int(message_id_start))
                    if end_candidates:
                        message_id_end = max(end_candidates) if message_id_end is None else max(max(end_candidates), int(message_id_end))
            messages = repository.list_telegram_messages(
                self.session,
                self.project.id,
                participant_ids=[participant_id] if participant_id else None,
                text_query=str(args.get("query") or "").strip() or None,
                message_id_start=int(message_id_start) if message_id_start is not None else None,
                message_id_end=int(message_id_end) if message_id_end is not None else None,
                limit=max(1, min(int(args.get("limit", TELEGRAM_TOOL_MAX_MESSAGES) or TELEGRAM_TOOL_MAX_MESSAGES), TELEGRAM_TOOL_MAX_MESSAGES)),
                ascending=True,
            )
            payload = {"messages": [_compact_message(item) for item in messages]}
            return payload, {
                "topic_ids": set(topic_ids),
                "message_ids": {int(item.telegram_message_id) for item in messages if item.telegram_message_id is not None},
                "messages": messages,
            }

        if name == "lookup_messages":
            message_ids = [int(item) for item in (args.get("message_ids") or []) if str(item).strip().isdigit()]
            messages = [
                repository.get_telegram_message_by_telegram_id(self.session, self.project.id, message_id)
                for message_id in message_ids[:TELEGRAM_TOOL_MAX_MESSAGES]
            ]
            found = [item for item in messages if item]
            return {"messages": [_compact_message(item) for item in found]}, {
                "message_ids": {int(item.telegram_message_id) for item in found if item.telegram_message_id is not None},
                "messages": found,
            }

        if name == "query_message_context":
            message_id = int(args.get("message_id"))
            context = repository.get_telegram_message_context(
                self.session,
                self.project.id,
                message_id,
                before=max(0, min(int(args.get("before", 3) or 3), 8)),
                after=max(0, min(int(args.get("after", 3) or 3), 8)),
            )
            return {"messages": [_compact_message(item) for item in context]}, {
                "message_ids": {int(item.telegram_message_id) for item in context if item.telegram_message_id is not None},
                "messages": context,
            }

        if name == "query_reply_chain":
            current = repository.get_telegram_message_by_telegram_id(
                self.session,
                self.project.id,
                int(args.get("message_id")),
            )
            chain: list[TelegramMessage] = []
            depth = max(1, min(int(args.get("depth", 6) or 6), 12))
            while current and len(chain) < depth:
                chain.append(current)
                if current.reply_to_message_id is None:
                    break
                current = repository.get_telegram_message_by_telegram_id(
                    self.session,
                    self.project.id,
                    int(current.reply_to_message_id),
                )
            chain.reverse()
            return {"messages": [_compact_message(item) for item in chain]}, {
                "message_ids": {int(item.telegram_message_id) for item in chain if item.telegram_message_id is not None},
                "messages": chain,
            }

        if name == "analyze_database":
            prompt = str(args.get("prompt") or "").strip() or "Summarize the evidence slice."
            topic_ids = [str(item) for item in (args.get("topic_ids") or []) if str(item).strip()]
            message_ids = [int(item) for item in (args.get("message_ids") or []) if str(item).strip().isdigit()]
            messages = []
            if message_ids:
                messages = [
                    repository.get_telegram_message_by_telegram_id(self.session, self.project.id, message_id)
                    for message_id in message_ids[:TELEGRAM_TOOL_MAX_MESSAGES]
                ]
                messages = [item for item in messages if item]
            elif topic_ids:
                topics = repository.list_telegram_preprocess_topics(
                    self.session,
                    self.project.id,
                    run_id=preprocess_run_id,
                )
                for topic in topics:
                    if topic.id not in topic_ids:
                        continue
                    messages.extend(
                        repository.list_telegram_messages(
                            self.session,
                            self.project.id,
                            participant_ids=[target_user["participant_id"]],
                            message_id_start=topic.start_message_id,
                            message_id_end=topic.end_message_id,
                            limit=8,
                            ascending=True,
                        )
                    )
            if not messages:
                return {"analysis": {"summary": "No messages matched the requested slice.", "evidence": []}}, {
                    "topic_ids": set(topic_ids),
                }
            if not self.client:
                analysis = {
                    "summary": prompt,
                    "evidence": self._messages_to_evidence(messages)[:5],
                }
                return {"analysis": analysis}, {
                    "topic_ids": set(topic_ids),
                    "message_ids": {int(item.telegram_message_id) for item in messages if item.telegram_message_id is not None},
                    "messages": messages,
                }
            compact_slice = "\n".join(
                f"[{item.telegram_message_id}] {item.sender_name or 'unknown'}: {' '.join((item.text_normalized or '').split())[:240]}"
                for item in messages[:TELEGRAM_TOOL_MAX_MESSAGES]
            )
            response = self.client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": "Analyze a small exact Telegram slice. Return JSON with keys summary and evidence.",
                    },
                    {
                        "role": "user",
                        "content": f"Prompt: {prompt}\n\nSlice:\n{compact_slice}",
                    },
                ],
                model=self.llm_config.model if self.llm_config else None,
                temperature=0.2,
                max_tokens=600,
            )
            parsed = parse_json_response(response.content, fallback=True)
            return {"analysis": parsed}, {
                "topic_ids": set(topic_ids),
                "message_ids": {int(item.telegram_message_id) for item in messages if item.telegram_message_id is not None},
                "messages": messages,
                "usage": response.usage,
            }

        return {"error": f"Unknown Telegram tool: {name}"}, {}

    @staticmethod
    def _tool_schemas() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_target_user_candidates",
                    "description": "List candidate Telegram users from the latest preprocess active-user snapshot, with participant fallback.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_target_user_profile",
                    "description": "Return the resolved Telegram target user's identity, aliases, and preprocess snapshot.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_related_topics",
                    "description": "List preprocess topics involving the target user before reading raw messages.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                            "query": {"type": "string"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "query_telegram_messages",
                    "description": "Fetch exact Telegram raw messages for the target user using topic ids, id ranges, or text filters.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                            "topic_ids": {"type": "array", "items": {"type": "string"}},
                            "query": {"type": "string"},
                            "message_id_start": {"type": "integer"},
                            "message_id_end": {"type": "integer"},
                            "limit": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "lookup_messages",
                    "description": "Fetch exact Telegram messages by message id.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "message_ids": {"type": "array", "items": {"type": "integer"}},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "query_message_context",
                    "description": "Fetch a short local context around one Telegram message id.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "message_id": {"type": "integer"},
                            "before": {"type": "integer"},
                            "after": {"type": "integer"},
                        },
                        "required": ["message_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "query_reply_chain",
                    "description": "Walk a Telegram reply chain backwards to gather the conversational anchor.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "message_id": {"type": "integer"},
                            "depth": {"type": "integer"},
                        },
                        "required": ["message_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "analyze_database",
                    "description": "Run a compact sub-analysis over a small exact raw-message slice so the parent loop sees only a tight summary.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "topic_ids": {"type": "array", "items": {"type": "string"}},
                            "message_ids": {"type": "array", "items": {"type": "integer"}},
                            "prompt": {"type": "string"},
                        },
                        "required": ["prompt"],
                    },
                },
            },
        ]

    def _heuristic_facet_result(
        self,
        facet: FacetDefinition,
        target_user: dict[str, Any],
        topics: list[Any],
        preprocess_run_id: str,
    ) -> TelegramFacetAnalysisResult:
        selected_topics = topics[:3]
        bullets = [f"{item.title}: {item.summary}" for item in selected_topics if item.summary][:8]
        evidence: list[dict[str, Any]] = []
        for topic in selected_topics:
            evidence.extend((topic.evidence_json or [])[:2])
        payload = {
            "summary": (
                f"Telegram analysis for {target_user.get('label') or target_user.get('display_name') or target_user['participant_id']} "
                f"currently relies on {len(selected_topics)} related preprocess topics for facet {facet.label}."
            ),
            "bullets": bullets,
            "confidence": 0.52,
            "evidence": self._normalize_agent_evidence(evidence, fallback=evidence),
            "conflicts": [],
            "notes": "Chat LLM is not configured; Telegram facet analysis used preprocess-topic heuristics only.",
            "_meta": {
                "llm_called": False,
                "llm_success": False,
                "llm_attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "cache_creation_tokens": 0,
                "cache_read_tokens": 0,
                "duration_ms": 0,
            },
        }
        return TelegramFacetAnalysisResult(
            payload=payload,
            retrieval_trace={
                "mode": "telegram_agent_heuristic",
                "preprocess_run_id": preprocess_run_id,
                "target_user": target_user,
                "topic_ids": [item.id for item in selected_topics],
                "topic_count_used": len(selected_topics),
            },
            hit_count=len(payload["evidence"]),
        )

    @staticmethod
    def _normalize_agent_evidence(raw_items: Any, *, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in raw_items or []:
            if not isinstance(item, dict):
                continue
            message_id = item.get("message_id") or item.get("telegram_message_id")
            quote = str(item.get("quote") or "").strip()
            if message_id is None or not quote:
                continue
            try:
                message_id = int(message_id)
            except (TypeError, ValueError):
                continue
            normalized.append(
                {
                    "message_id": message_id,
                    "sender_name": str(item.get("sender_name") or "").strip() or None,
                    "sent_at": str(item.get("sent_at") or "").strip() or None,
                    "quote": quote,
                    "reason": str(item.get("reason") or "").strip() or "Direct Telegram evidence",
                }
            )
        return normalized[:20] or fallback[:20]

    @staticmethod
    def _parse_confidence(value: Any, *, default: float) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return default

    def _messages_to_evidence(self, messages: list[TelegramMessage]) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for item in messages:
            if item.telegram_message_id is None:
                continue
            text = " ".join((item.text_normalized or "").split()).strip()
            if not text:
                continue
            evidence.append(
                {
                    "message_id": int(item.telegram_message_id),
                    "sender_name": item.sender_name,
                    "sent_at": item.sent_at.isoformat() if item.sent_at else None,
                    "quote": text[:220],
                    "reason": "Direct Telegram evidence",
                }
            )
        return evidence

    def _active_user_snapshot(self, active_user, preprocess_run_id: str) -> dict[str, Any]:
        return {
            "participant_id": active_user.participant_id,
            "uid": active_user.uid,
            "username": active_user.username,
            "display_name": active_user.display_name,
            "primary_alias": active_user.primary_alias,
            "aliases_json": active_user.aliases_json or [],
            "message_count": active_user.message_count,
            "label": active_user.primary_alias or active_user.display_name or active_user.username or active_user.uid,
            "source": "preprocess_active_user",
            "preprocess_run_id": preprocess_run_id,
        }

    def _participant_snapshot(self, participant: TelegramParticipant, preprocess_run_id: str) -> dict[str, Any]:
        aliases = [
            item
            for item in [participant.display_name, participant.username, participant.telegram_user_id, participant.participant_key]
            if item
        ]
        return {
            "participant_id": participant.id,
            "uid": participant.telegram_user_id,
            "username": participant.username,
            "display_name": participant.display_name,
            "primary_alias": participant.display_name or participant.username or participant.participant_key,
            "aliases_json": list(dict.fromkeys(aliases))[:8],
            "message_count": participant.message_count,
            "label": participant.display_name or participant.username or participant.participant_key,
            "source": "telegram_participant",
            "preprocess_run_id": preprocess_run_id,
        }
