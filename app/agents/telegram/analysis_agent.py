from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.analysis.facets import FacetDefinition
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import Project, TelegramMessage, TelegramParticipant
from app.schemas import ServiceConfig
from app.storage import repository

TELEGRAM_TOOL_LOOP_MAX_STEPS = 25
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
        trace_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.session = session
        self.project = project
        self.llm_config = llm_config
        self.log_path = log_path
        self.trace_callback = trace_callback
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

        top_users = repository.list_telegram_preprocess_top_users(
            self.session,
            self.project.id,
            run_id=preprocess_run.id,
        )
        query = (target_user_query or "").strip().lower()

        if participant_id:
            explicit = next((item for item in top_users if item.participant_id == participant_id), None)
            if explicit:
                return self._top_user_snapshot(explicit, preprocess_run.id)
            participant = repository.get_telegram_participant(self.session, participant_id)
            if participant:
                return self._participant_snapshot(participant, preprocess_run.id)

        if query:
            matched_top_users = [
                item
                for item in top_users
                if any(
                    query in value.lower()
                    for value in [
                        item.uid or "",
                        item.username or "",
                        item.display_name or "",
                    ]
                    if value
                )
            ]
            if matched_top_users:
                return self._top_user_snapshot(matched_top_users[0], preprocess_run.id)
            matched_participants = repository.search_telegram_participants(self.session, self.project.id, query, limit=12)
            if matched_participants:
                return self._participant_snapshot(matched_participants[0], preprocess_run.id)

        if top_users:
            return self._top_user_snapshot(top_users[0], preprocess_run.id)
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
        topic_preview = self._build_topic_catalog_preview(related_topics)

        if not self.client:
            return self._heuristic_facet_result(facet, target_user, related_topics, preprocess_run_id)


        system_prompt = (
            "你正在分析已经存入 SQL 的 Telegram 群聊记录。\n"
            "这个模式绝对不能使用 embedding、chunk retrieval 或 retrieval.search。\n"
            "请严格按这个顺序工作：\n"
            "1. 先读取目标用户。\n"
            "2. 再查询与目标用户相关的话题表。\n"
            "3. 根据用户参与的话题，抓取原始消息作为证据。\n"
            "4. 优先使用 analyze_database 保持上下文紧凑。\n"
            "请只返回 JSON，包含 summary, bullets, confidence, fewshots, conflicts, notes。\n"
            "除 JSON 键名外，所有可读文本都尽量使用简体中文。\n"
            "fewshots 必须优先围绕目标用户自己的消息展开，但每条都要补上前后几条上下文。\n"
            "每条 fewshot 都必须包含 message_id, sender_name, sent_at, situation, expression, quote, context_before, context_after。"
        )
        system_prompt = (
            "你正在分析已经写入 SQL 的 Telegram 群聊记录。\n"
            "这个模式绝对不能使用 embedding、chunk retrieval 或 retrieval.search。\n"
            "请严格按这个顺序工作：\n"
            "1. 先读取目标用户信息。\n"
            "2. 再查询与目标用户相关的话题表。\n"
            "3. 根据目标用户参与的话题，抓取原始消息作为 few-shot 证据。\n"
            "4. 优先使用 analyze_database 保持上下文紧凑。\n"
            "只返回 JSON，必须包含 summary, bullets, confidence, fewshots, conflicts, notes。\n"
            "除 JSON 键名外，所有可读文本尽量使用简体中文。\n"
            "fewshots 必须优先围绕目标用户自己的消息展开，但每条都要补上前后几条上下文。\n"
            "每条 fewshot 都必须包含 message_id, sender_name, sent_at, situation, expression, quote, context_before, context_after。\n"
            "其中 situation 要明确说明目标用户当时面对什么情况；expression 要概括其表达方式；quote 必须保留目标用户原话。\n"
            "context_before 和 context_after 必须优先提供非目标用户的前后文，帮助理解回应对象、话题延续和互动关系。"
        )
        user_prompt = json.dumps(
            {
                "project": self.project.name,
                "facet_key": facet.key,
                "facet_label": facet.label,
                "facet_purpose": facet.purpose,
                "target_user": target_user,
                "related_topic_count": len(related_topics),
                "related_topic_preview": topic_preview,
                "analysis_context": analysis_context or "",
            },
            ensure_ascii=False,
            indent=2,
        )

        self._trace(
            "agent_started",
            agent="telegram_facet_agent",
            facet_key=facet.key,
            label=facet.label,
            preprocess_run_id=preprocess_run_id,
            related_topic_count=len(related_topics),
            topic_preview_count=len(topic_preview),
            target_user=target_user,
        )
        result = self._run_tool_loop(
            facet=facet,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            target_user=target_user,
            preprocess_run_id=preprocess_run_id,
        )
        parsed = parse_json_response(str(result["content"] or ""), fallback=True)
        fewshots = self._normalize_agent_fewshots(
            parsed.get("fewshots") or parsed.get("evidence"),
            fallback=result["fallback_fewshots"],
        )
        payload = {
            "summary": str(parsed.get("summary") or "").strip() or f"Telegram evidence around {facet.label} remains concentrated in the selected user's related topics.",
            "bullets": [
                str(item).strip()
                for item in (parsed.get("bullets") or [])
                if str(item).strip()
            ][:8],
            "confidence": self._parse_confidence(parsed.get("confidence"), default=0.68),
            "fewshots": fewshots,
            "evidence": fewshots,
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
            "evidence_kind": "telegram_fewshots",
            "tool_calls": result["tool_trace"],
            "preprocess_run_id": preprocess_run_id,
            "target_user": target_user,
            "topic_ids": sorted(result["used_topic_ids"]),
            "topic_weeks_used": sorted(result["used_week_keys"]),
            "queried_message_ids": sorted(result["queried_message_ids"])[:96],
            "topic_count_used": len(result["used_topic_ids"]),
        }
        self._trace(
            "agent_completed",
            agent="telegram_facet_agent",
            facet_key=facet.key,
            label=facet.label,
            topic_count_used=len(result["used_topic_ids"]),
            queried_message_count=len(result["queried_message_ids"]),
        )
        return TelegramFacetAnalysisResult(
            payload=payload,
            retrieval_trace=retrieval_trace,
            hit_count=len(fewshots),
        )

    def _run_tool_loop(
        self,
        *,
        facet: FacetDefinition,
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
        used_week_keys: set[str] = set()
        queried_message_ids: set[int] = set()
        fallback_fewshots: list[dict[str, Any]] = []
        model_name = self.llm_config.model if self.llm_config else None
        has_topic_overview = False

        for iteration in range(1, TELEGRAM_TOOL_LOOP_MAX_STEPS + 1):
            request_key = f"{facet.key}-round-{iteration}"
            self._trace(
                "llm_request_started",
                agent="telegram_facet_agent",
                facet_key=facet.key,
                round_index=iteration,
                request_key=request_key,
                request_kind="tool_round",
                label=f"{facet.label} round {iteration}",
                tool_names=[tool["function"]["name"] for tool in self._tool_schemas()],
            )
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
            self._trace(
                "llm_request_completed",
                agent="telegram_facet_agent",
                facet_key=facet.key,
                round_index=iteration,
                request_key=request_key,
                request_kind="tool_round",
                label=f"{facet.label} round {iteration}",
                usage=round_result.usage,
                response_text_preview=self._preview(round_result.content),
                tool_calls=[
                    {"name": call.name, "arguments": call.arguments}
                    for call in round_result.tool_calls
                ],
            )
            if not round_result.tool_calls:
                return {
                    "content": round_result.content,
                    "usage": usage,
                    "tool_trace": tool_trace,
                    "used_topic_ids": used_topic_ids,
                    "used_week_keys": used_week_keys,
                    "queried_message_ids": queried_message_ids,
                    "fallback_fewshots": fallback_fewshots,
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
                self._trace(
                    "tool_call",
                    agent="telegram_facet_agent",
                    facet_key=facet.key,
                    round_index=iteration,
                    request_key=request_key,
                    tool_name=call.name,
                    arguments_preview=self._preview(call.arguments),
                )
                raw_evidence_tools = {
                    "query_telegram_messages",
                    "lookup_messages",
                    "query_message_context",
                    "query_reply_chain",
                    "analyze_database",
                }
                if call.name in raw_evidence_tools and not has_topic_overview:
                    output = {
                        "error": "请先调用 list_related_topics，先查看目标用户相关的话题概要，再决定是否读取原始消息证据。"
                    }
                    state = {}
                else:
                    output, state = self._execute_tool(call.name, call.arguments, target_user, preprocess_run_id)
                if call.name == "list_related_topics" and not output.get("error"):
                    has_topic_overview = True
                used_topic_ids.update(state.get("topic_ids", []))
                used_week_keys.update(state.get("week_keys", []))
                queried_message_ids.update(state.get("message_ids", []))
                for key in usage:
                    usage[key] += int((state.get("usage") or {}).get(key, 0) or 0)
                if state.get("messages"):
                    fallback_fewshots = self._messages_to_fewshots(
                        state.get("messages", []),
                        participant_id=str(target_user.get("participant_id") or "").strip() or None,
                    )[:8]
                tool_entry = {
                    "tool": call.name,
                    "arguments": call.arguments,
                    "topic_ids": sorted(state.get("topic_ids", [])),
                    "week_keys": sorted(state.get("week_keys", [])),
                    "message_ids": sorted(state.get("message_ids", []))[:32],
                    "result_preview": self._preview(output),
                }
                tool_trace.append(
                    tool_entry
                )
                self._trace(
                    "tool_result",
                    agent="telegram_facet_agent",
                    facet_key=facet.key,
                    round_index=iteration,
                    request_key=request_key,
                    tool_name=call.name,
                    topic_ids=tool_entry["topic_ids"],
                    week_keys=tool_entry["week_keys"],
                    message_ids=tool_entry["message_ids"],
                    output_preview=tool_entry["result_preview"],
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
            top_users = repository.list_telegram_preprocess_top_users(
                self.session,
                self.project.id,
                run_id=preprocess_run_id,
            )
            query_lower = query.lower()
            for item in top_users:
                if query and not any(
                    query_lower in value.lower()
                    for value in [
                        item.display_name or "",
                        item.username or "",
                        item.uid or "",
                    ]
                    if value
                ):
                    continue
                candidates.append(self._top_user_snapshot(item, preprocess_run_id))
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
            limit = max(1, min(int(args.get("limit", 12) or 12), 24))
            offset = max(0, int(args.get("offset", 0) or 0))
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
                        "start_at": topic.start_at.isoformat() if topic.start_at else None,
                        "end_at": topic.end_at.isoformat() if topic.end_at else None,
                        "start_message_id": topic.start_message_id,
                        "end_message_id": topic.end_message_id,
                        "message_count": topic.message_count,
                        "participant_count": topic.participant_count,
                        "keywords": topic.keywords_json or [],
                        "week_key": str((topic.metadata_json or {}).get("week_key") or ""),
                        "evidence_message_ids": [
                            int(item.get("message_id"))
                            for item in (topic.evidence_json or [])
                            if isinstance(item, dict) and item.get("message_id") is not None
                        ][:8],
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
            page = matched[offset: offset + limit]
            return {
                "topics": page,
                "total": len(matched),
                "offset": offset,
                "limit": limit,
            }, {
                "topic_ids": {item["topic_id"] for item in page},
                "week_keys": {item.get("week_key") for item in page if item.get("week_key")},
            }

        if name == "query_telegram_messages":
            participant_id = str(args.get("participant_id") or target_user["participant_id"]).strip()
            topic_ids = [str(item) for item in (args.get("topic_ids") or []) if str(item).strip()]
            if not topic_ids:
                return {
                    "error": "query_telegram_messages 必须基于已选话题执行，请先调用 list_related_topics 并传入 topic_ids。"
                }, {}
            message_id_start = args.get("message_id_start")
            message_id_end = args.get("message_id_end")
            text_query = str(args.get("query") or "").strip() or None
            limit = max(1, min(int(args.get("limit", TELEGRAM_TOOL_MAX_MESSAGES) or TELEGRAM_TOOL_MAX_MESSAGES), TELEGRAM_TOOL_MAX_MESSAGES))
            messages, scoped_topics = self._collect_topic_scoped_messages(
                preprocess_run_id=preprocess_run_id,
                participant_id=participant_id,
                topic_ids=topic_ids,
                text_query=text_query,
                message_id_start=int(message_id_start) if message_id_start is not None else None,
                message_id_end=int(message_id_end) if message_id_end is not None else None,
                limit=limit,
            )
            payload = {"messages": [_compact_message(item) for item in messages]}
            return payload, {
                "topic_ids": set(topic_ids),
                "week_keys": {
                    str((topic.metadata_json or {}).get("week_key") or "")
                    for topic in scoped_topics
                    if str((topic.metadata_json or {}).get("week_key") or "").strip()
                },
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
                return {"analysis": {"summary": "No messages matched the requested slice.", "fewshots": []}}, {
                    "topic_ids": set(topic_ids),
                }
            expanded_messages = self._expand_with_message_context(messages)
            if not self.client:
                analysis = {
                    "summary": prompt,
                    "fewshots": self._messages_to_fewshots(
                        expanded_messages,
                        participant_id=str(target_user.get("participant_id") or "").strip() or None,
                    )[:5],
                }
                return {"analysis": analysis}, {
                    "topic_ids": set(topic_ids),
                    "message_ids": {int(item.telegram_message_id) for item in expanded_messages if item.telegram_message_id is not None},
                    "messages": expanded_messages,
                }
            compact_slice = "\n".join(
                f"[{item.telegram_message_id}] {item.sender_name or 'unknown'}: {' '.join((item.text_normalized or '').split())[:240]}"
                for item in expanded_messages[:TELEGRAM_TOOL_MAX_MESSAGES]
            )
            response = self.client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": "分析一小段精确的 Telegram 原始消息切片，只返回 JSON，包含 summary 和 fewshots。fewshots 需要标注情境、表达方式、原话以及前后文。除键名外，正文尽量使用简体中文。",
                    },
                    {
                        "role": "user",
                        "content": f"分析任务：{prompt}\n\n请围绕目标用户自己的消息整理 fewshots，并补上前后几条上下文。\n\n消息切片：\n{compact_slice}",
                    },
                ],
                model=self.llm_config.model if self.llm_config else None,
                temperature=0.2,
                max_tokens=600,
            )
            parsed = parse_json_response(response.content, fallback=True)
            return {"analysis": parsed}, {
                "topic_ids": set(topic_ids),
                "message_ids": {int(item.telegram_message_id) for item in expanded_messages if item.telegram_message_id is not None},
                "messages": expanded_messages,
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
        fewshots: list[dict[str, Any]] = []
        for topic in selected_topics:
            fewshots.extend((topic.evidence_json or [])[:2])
        normalized_fewshots = self._normalize_agent_fewshots(fewshots, fallback=fewshots)
        payload = {
            "summary": (
                f"Telegram analysis for {target_user.get('label') or target_user.get('display_name') or target_user['participant_id']} "
                f"currently relies on {len(selected_topics)} related preprocess topics for facet {facet.label}."
            ),
            "bullets": bullets,
            "confidence": 0.52,
            "fewshots": normalized_fewshots,
            "evidence": normalized_fewshots,
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
            hit_count=len(payload["fewshots"]),
        )

    @staticmethod
    def _normalize_agent_fewshots(raw_items: Any, *, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            situation = str(item.get("situation") or item.get("reason") or "").strip() or "目标用户在当前话题中的直接回应"
            expression = str(item.get("expression") or "").strip() or "直接回应"
            context_before = str(item.get("context_before") or "").strip()
            context_after = str(item.get("context_after") or "").strip()
            normalized.append(
                {
                    "message_id": message_id,
                    "sender_name": str(item.get("sender_name") or "").strip() or None,
                    "sent_at": str(item.get("sent_at") or "").strip() or None,
                    "situation": situation,
                    "expression": expression,
                    "quote": quote,
                    "context_before": context_before or None,
                    "context_after": context_after or None,
                    "reason": str(item.get("reason") or "").strip() or situation,
                }
            )
        return normalized[:20] or fallback[:20]

    @staticmethod
    def _parse_confidence(value: Any, *, default: float) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return default

    def _messages_to_fewshots(
        self,
        messages: list[TelegramMessage],
        *,
        participant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        target_messages = [
            item
            for item in messages
            if item.telegram_message_id is not None
            and " ".join((item.text_normalized or "").split()).strip()
            and (not participant_id or item.participant_id == participant_id)
        ]
        if not target_messages:
            target_messages = [
                item
                for item in messages
                if item.telegram_message_id is not None and " ".join((item.text_normalized or "").split()).strip()
            ]

        fewshots: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for item in sorted(target_messages, key=lambda row: int(row.telegram_message_id or 0)):
            message_id = int(item.telegram_message_id or 0)
            if not message_id or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            window = repository.get_telegram_message_context(
                self.session,
                self.project.id,
                message_id,
                before=6,
                after=6,
            )
            before_rows: list[dict[str, Any]] = []
            after_rows: list[dict[str, Any]] = []
            for row in window:
                row_id = int(row.telegram_message_id or 0)
                text = " ".join((row.text_normalized or "").split()).strip()
                if not row_id or not text or row_id == message_id:
                    continue
                line = f"{row.sender_name or 'unknown'}: {text[:120]}"
                entry = {
                    "message_id": row_id,
                    "line": line,
                    "is_target": bool(item.participant_id and row.participant_id == item.participant_id),
                }
                if row_id < message_id:
                    before_rows.append(entry)
                else:
                    after_rows.append(entry)
            before_lines = self._pick_context_lines(before_rows, limit=3, from_end=True)
            after_lines = self._pick_context_lines(after_rows, limit=3, from_end=False)
            quote = " ".join((item.text_normalized or "").split()).strip()[:220]
            fewshots.append(
                {
                    "message_id": message_id,
                    "sender_name": item.sender_name,
                    "sent_at": item.sent_at.isoformat() if item.sent_at else None,
                    "situation": self._describe_message_situation_v2(before_lines, after_lines),
                    "expression": self._describe_expression_style(quote),
                    "quote": quote,
                    "context_before": " | ".join(before_lines) or None,
                    "context_after": " | ".join(after_lines) or None,
                    "reason": "Target-user few-shot with local Telegram context",
                }
            )
            if len(fewshots) >= 12:
                break
        return fewshots

    @staticmethod
    def _pick_context_lines(
        rows: list[dict[str, Any]],
        *,
        limit: int,
        from_end: bool,
    ) -> list[str]:
        if not rows:
            return []
        selected = list(rows[-limit:] if from_end else rows[:limit])
        if not any(not bool(item.get("is_target")) for item in selected):
            fallback = None
            pool = list(reversed(rows)) if from_end else rows
            for item in pool:
                if not bool(item.get("is_target")):
                    fallback = item
                    break
            if fallback and fallback not in selected:
                if from_end:
                    selected = [fallback, *selected[1:]]
                else:
                    selected = [*selected[:-1], fallback]
                selected.sort(key=lambda item: int(item.get("message_id") or 0))
        return [str(item.get("line") or "").strip() for item in selected if str(item.get("line") or "").strip()]

    @staticmethod
    def _describe_message_situation_v2(before_lines: list[str], after_lines: list[str]) -> str:
        if before_lines and after_lines:
            return f"目标用户是在回应“{before_lines[-1]}”这条上文，给出自己的表态后，对话继续推进到“{after_lines[0]}”。"
        if before_lines:
            return f"目标用户是在“{before_lines[-1]}”这条上文之后直接接话回应。"
        if after_lines:
            return f"目标用户先抛出这句回应，随后对话继续接到“{after_lines[0]}”。"
        return "目标用户在当前话题节点里直接给出了一句独立回应。"

    @staticmethod
    def _describe_message_situation(before_lines: list[str], after_lines: list[str]) -> str:
        if before_lines and after_lines:
            return f"前文在讨论 {before_lines[-1]}，目标用户给出回应后，后续继续延展到 {after_lines[0]}"
        if before_lines:
            return f"前文语境为 {before_lines[-1]}，目标用户在此基础上直接回应"
        if after_lines:
            return f"目标用户先抛出回应，随后话题继续延展到 {after_lines[0]}"
        return "目标用户在当前话题中的直接表达"

    @staticmethod
    def _describe_expression_style(text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return "直接回应"
        if "?" in normalized or "？" in normalized:
            return "追问或反问式表达"
        if len(normalized) <= 24:
            return "短句直给式表达"
        if len(normalized) >= 90:
            return "展开解释式表达"
        if any(token in normalized for token in ("但是", "不过", "其实", "只是")):
            return "带转折保留的克制表达"
        return "判断先行的陈述式表达"

    def _expand_with_message_context(
        self,
        messages: list[TelegramMessage],
        *,
        before: int = 2,
        after: int = 2,
    ) -> list[TelegramMessage]:
        expanded: list[TelegramMessage] = []
        seen_ids: set[int] = set()
        for item in sorted(messages, key=lambda row: int(row.telegram_message_id or 0)):
            message_id = int(item.telegram_message_id or 0)
            if not message_id:
                continue
            window = repository.get_telegram_message_context(
                self.session,
                self.project.id,
                message_id,
                before=before,
                after=after,
            )
            for row in window:
                row_id = int(row.telegram_message_id or 0)
                if not row_id or row_id in seen_ids:
                    continue
                seen_ids.add(row_id)
                expanded.append(row)
        return sorted(expanded, key=lambda row: int(row.telegram_message_id or 0))

    def _top_user_snapshot(self, top_user, preprocess_run_id: str) -> dict[str, Any]:
        return {
            "participant_id": top_user.participant_id,
            "uid": top_user.uid,
            "username": top_user.username,
            "display_name": top_user.display_name,
            "primary_alias": top_user.display_name or top_user.username or top_user.uid,
            "aliases_json": [
                item
                for item in [top_user.display_name, top_user.username, top_user.uid]
                if item
            ],
            "message_count": top_user.message_count,
            "label": top_user.display_name or top_user.username or top_user.uid,
            "source": "preprocess_top_user",
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

    def _trace(self, kind: str, **payload: Any) -> None:
        if not self.trace_callback:
            return
        self.trace_callback(
            {
                "kind": kind,
                "timestamp": datetime.now().astimezone().isoformat(),
                **payload,
            }
        )

    def _preview(self, value: Any, limit: int = 800) -> str:
        if isinstance(value, str):
            text = value
        else:
            try:
                text = json.dumps(value, ensure_ascii=False, indent=2)
            except TypeError:
                text = str(value)
        compact = " ".join(str(text or "").split()).strip()
        if len(compact) <= limit:
            return compact
        return f"{compact[:limit]}..."

    def _build_stream_trace_callback(
        self,
        *,
        request_key: str,
        agent: str,
        extra: dict[str, Any] | None = None,
    ):
        state = {"text": "", "pending": ""}

        def callback(delta: str) -> None:
            if not delta:
                return
            state["text"] = f"{state['text']}{delta}"
            state["pending"] = f"{state['pending']}{delta}"
            if len(state["pending"]) < 80 and not delta.endswith(("\n", ".", "}", "]", "。", "！", "？")):
                return
            self._trace(
                "llm_delta",
                agent=agent,
                request_key=request_key,
                text_preview=state["text"][-4000:],
                **dict(extra or {}),
            )
            state["pending"] = ""

        def flush_remaining() -> None:
            if not state["pending"]:
                return
            self._trace(
                "llm_delta",
                agent=agent,
                request_key=request_key,
                text_preview=state["text"][-4000:],
                **dict(extra or {}),
            )
            state["pending"] = ""

        setattr(callback, "_flush_remaining", flush_remaining)
        return callback

    def _topic_week_key(self, topic: Any) -> str | None:
        week_key = str(getattr(topic, "week_key", None) or (getattr(topic, "metadata_json", None) or {}).get("week_key") or "").strip()
        return week_key or None

    def _sort_topics_chronologically(self, topics: list[Any]) -> list[Any]:
        return sorted(
            list(topics or []),
            key=lambda item: (
                item.start_at.isoformat() if getattr(item, "start_at", None) else "",
                int(getattr(item, "topic_index", 0) or 0),
                str(getattr(item, "id", "")),
            ),
        )

    def _select_evenly_spaced_topics(self, topics: list[Any], limit: int) -> list[Any]:
        ordered = self._sort_topics_chronologically(topics)
        if len(ordered) <= limit:
            return ordered
        if limit <= 1:
            return [ordered[len(ordered) // 2]]
        picks: list[Any] = []
        used_indexes: set[int] = set()
        last_index = len(ordered) - 1
        for slot in range(limit):
            index = round((slot * last_index) / (limit - 1))
            if index in used_indexes:
                continue
            used_indexes.add(index)
            picks.append(ordered[index])
        if len(picks) < limit:
            for index, item in enumerate(ordered):
                if index in used_indexes:
                    continue
                picks.append(item)
                if len(picks) >= limit:
                    break
        return self._sort_topics_chronologically(picks[:limit])

    def _topic_metadata(self, topic: Any) -> dict[str, Any]:
        metadata = getattr(topic, "metadata_json", None) or {}
        return dict(metadata) if isinstance(metadata, dict) else {}

    def _topic_participant_viewpoints(self, topic: Any, limit: int = 6) -> list[dict[str, Any]]:
        metadata = self._topic_metadata(topic)
        metadata_viewpoints_by_id = {
            str(item.get("participant_id") or "").strip(): item
            for item in (metadata.get("participant_viewpoints") or [])
            if isinstance(item, dict) and str(item.get("participant_id") or "").strip()
        }
        quotes_by_participant: dict[str, list[dict[str, Any]]] = {}
        for quote in sorted(
            list(getattr(topic, "quotes", None) or []),
            key=lambda item: (
                item.participant_id or "",
                int(item.rank or 0),
                int(item.telegram_message_id or 0),
            ),
        ):
            participant_id = str(quote.participant_id or "").strip()
            if not participant_id:
                continue
            quotes_by_participant.setdefault(participant_id, []).append(
                {
                    "message_id": int(quote.telegram_message_id or 0) or None,
                    "quote": quote.quote,
                    "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
                }
            )

        viewpoints: list[dict[str, Any]] = []
        for link in list(getattr(topic, "participants", None) or []):
            participant_id = str(link.participant_id or "").strip()
            if not participant_id:
                continue
            viewpoints.append(
                {
                    "participant_id": participant_id,
                    "display_name": link.participant.display_name if link.participant else None,
                    "stance_summary": (
                        str(getattr(link, "stance_summary", None) or "").strip()
                        or str(metadata_viewpoints_by_id.get(participant_id, {}).get("stance_summary") or "").strip()
                        or None
                    ),
                    "notable_points": [],
                    "evidence_message_ids": [
                        int(item.get("message_id"))
                        for item in quotes_by_participant.get(participant_id, [])
                        if item.get("message_id") is not None
                    ][:6],
                }
            )
            if len(viewpoints) >= limit:
                break

        if viewpoints:
            return viewpoints[:limit]
        fallback_viewpoints: list[dict[str, Any]] = []
        for item in metadata.get("participant_viewpoints") or []:
            if not isinstance(item, dict):
                continue
            fallback_viewpoints.append(
                {
                    "participant_id": str(item.get("participant_id") or "").strip() or None,
                    "display_name": str(item.get("display_name") or "").strip() or None,
                    "stance_summary": str(item.get("stance_summary") or "").strip() or None,
                    "notable_points": [
                        str(point).strip()
                        for point in (item.get("notable_points") or [])
                        if str(point).strip()
                    ][:5],
                    "evidence_message_ids": [
                        int(message_id)
                        for message_id in (item.get("evidence_message_ids") or [])
                        if str(message_id).strip().isdigit()
                    ][:6],
                }
            )
            if len(fallback_viewpoints) >= limit:
                break
        return fallback_viewpoints

    def _topic_quote_payloads(
        self,
        topic: Any,
        *,
        participant_id: str | None = None,
        limit: int = 16,
    ) -> list[dict[str, Any]]:
        quotes = sorted(
            list(getattr(topic, "quotes", None) or []),
            key=lambda item: (
                0 if participant_id and item.participant_id == participant_id else 1,
                item.participant_id or "",
                int(item.rank or 0),
                int(item.telegram_message_id or 0),
            ),
        )
        payloads: list[dict[str, Any]] = []
        for quote in quotes:
            payloads.append(
                {
                    "participant_id": quote.participant_id,
                    "display_name": quote.participant.display_name if quote.participant else None,
                    "username": quote.participant.username if quote.participant else None,
                    "rank": int(quote.rank or 0),
                    "message_id": int(quote.telegram_message_id or 0) or None,
                    "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
                    "quote": quote.quote,
                }
            )
            if len(payloads) >= limit:
                break
        return payloads

    def _serialize_related_topic(
        self,
        topic: Any,
        *,
        participant_id: str,
    ) -> dict[str, Any]:
        metadata = self._topic_metadata(topic)
        participant_quotes = self._topic_quote_payloads(topic, participant_id=participant_id, limit=16)
        quotes_by_participant: dict[str, list[dict[str, Any]]] = {}
        for quote in participant_quotes:
            quote_participant_id = str(quote.get("participant_id") or "").strip()
            if not quote_participant_id:
                continue
            quotes_by_participant.setdefault(quote_participant_id, []).append(quote)
        participants = sorted(
            list(topic.participants or []),
            key=lambda link: (
                0 if link.participant_id == participant_id else 1,
                int(link.message_count or 0) * -1,
                link.participant_id or "",
            ),
        )
        return {
            "topic_id": topic.id,
            "title": topic.title,
            "summary": topic.summary,
            "week_key": self._topic_week_key(topic),
            "week_topic_index": int(getattr(topic, "week_topic_index", 0) or 0),
            "start_at": topic.start_at.isoformat() if topic.start_at else None,
            "end_at": topic.end_at.isoformat() if topic.end_at else None,
            "start_message_id": topic.start_message_id,
            "end_message_id": topic.end_message_id,
            "message_count": topic.message_count,
            "participant_count": topic.participant_count,
            "keywords": list(topic.keywords_json or [])[:8],
            "evidence_message_ids": [
                int(item.get("message_id"))
                for item in (topic.evidence_json or [])
                if isinstance(item, dict) and item.get("message_id") is not None
            ][:8],
            "participants": [
                {
                    "participant_id": link.participant_id,
                    "display_name": link.participant.display_name if link.participant else None,
                    "username": link.participant.username if link.participant else None,
                    "role_hint": link.role_hint,
                    "stance_summary": getattr(link, "stance_summary", None),
                    "message_count": link.message_count,
                    "mention_count": link.mention_count,
                    "quotes": quotes_by_participant.get(link.participant_id, []),
                }
                for link in participants
            ],
            "participant_quotes": participant_quotes,
            "subtopics": [
                str(item).strip()
                for item in (metadata.get("subtopics") or [])
                if str(item).strip()
            ][:8],
            "interaction_patterns": [
                str(item).strip()
                for item in (metadata.get("interaction_patterns") or [])
                if str(item).strip()
            ][:8],
            "participant_viewpoints": self._topic_participant_viewpoints(topic, limit=6),
        }

    def _build_topic_catalog_preview(self, topics: list[Any], limit: int = 6) -> list[dict[str, Any]]:
        preview: list[dict[str, Any]] = []
        for topic in self._select_evenly_spaced_topics(topics, limit):
            metadata = self._topic_metadata(topic)
            preview.append(
                {
                    "topic_id": topic.id,
                    "title": topic.title,
                    "summary": topic.summary,
                    "week_key": self._topic_week_key(topic),
                    "start_at": topic.start_at.isoformat() if topic.start_at else None,
                    "end_at": topic.end_at.isoformat() if topic.end_at else None,
                    "message_count": topic.message_count,
                    "participant_count": topic.participant_count,
                    "keywords": list(topic.keywords_json or [])[:6],
                    "evidence_message_ids": [
                        int(item.get("message_id"))
                        for item in (topic.evidence_json or [])
                        if isinstance(item, dict) and item.get("message_id") is not None
                    ][:6],
                    "subtopics": [str(item).strip() for item in (metadata.get("subtopics") or []) if str(item).strip()][:6],
                    "interaction_patterns": [
                        str(item).strip()
                        for item in (metadata.get("interaction_patterns") or [])
                        if str(item).strip()
                    ][:6],
                    "participant_viewpoints": self._topic_participant_viewpoints(topic, limit=4),
                }
            )
        return preview

    def _list_matching_related_topics(
        self,
        preprocess_run_id: str,
        participant_id: str,
        *,
        query: str | None = None,
    ) -> list[Any]:
        query_lower = str(query or "").strip().lower()
        topics = repository.list_telegram_preprocess_topics_for_participant(
            self.session,
            self.project.id,
            run_id=preprocess_run_id,
            participant_id=participant_id,
        )
        matched: list[Any] = []
        for topic in topics:
            if query_lower:
                haystacks = [
                    str(topic.title or "").lower(),
                    str(topic.summary or "").lower(),
                    " ".join(str(item) for item in (topic.keywords_json or [])).lower(),
                    str(self._topic_week_key(topic) or "").lower(),
                    " ".join(
                        str(item.get("stance_summary") or "")
                        for item in self._topic_participant_viewpoints(topic, limit=8)
                    ).lower(),
                    " ".join(
                        str(item.get("quote") or "")
                        for item in self._topic_quote_payloads(topic, participant_id=participant_id, limit=12)
                    ).lower(),
                ]
                if not any(query_lower in haystack for haystack in haystacks):
                    continue
            matched.append(topic)
        return self._sort_topics_chronologically(matched)

    def _collect_topic_scoped_messages(
        self,
        *,
        preprocess_run_id: str,
        participant_id: str | None,
        topic_ids: list[str],
        text_query: str | None,
        message_id_start: int | None,
        message_id_end: int | None,
        limit: int,
    ) -> tuple[list[TelegramMessage], list[Any]]:
        scoped_topics: list[Any] = []
        if topic_ids:
            all_topics = repository.list_telegram_preprocess_topics(
                self.session,
                self.project.id,
                run_id=preprocess_run_id,
            )
            topic_map = {str(topic.id): topic for topic in all_topics}
            scoped_topics = [topic_map[topic_id] for topic_id in topic_ids if topic_id in topic_map]
            scoped_topics = self._sort_topics_chronologically(scoped_topics)
            if scoped_topics and len(scoped_topics) > limit:
                scoped_topics = self._select_evenly_spaced_topics(scoped_topics, limit)
        if not scoped_topics:
            return (
                repository.list_telegram_messages(
                    self.session,
                    self.project.id,
                    participant_ids=[participant_id] if participant_id else None,
                    text_query=text_query,
                    message_id_start=message_id_start,
                    message_id_end=message_id_end,
                    limit=limit,
                    ascending=True,
                ),
                [],
            )

        messages: list[TelegramMessage] = []
        topic_count = len(scoped_topics)
        base = max(1, limit // topic_count)
        remainder = max(0, limit - (base * topic_count))
        for index, topic in enumerate(scoped_topics):
            topic_limit = base + (1 if index < remainder else 0)
            if topic_limit <= 0:
                continue
            topic_start = topic.start_message_id
            topic_end = topic.end_message_id
            if message_id_start is not None:
                topic_start = max(int(topic_start or message_id_start), int(message_id_start))
            if message_id_end is not None:
                topic_end = min(int(topic_end or message_id_end), int(message_id_end))
            topic_messages = repository.list_telegram_messages(
                self.session,
                self.project.id,
                participant_ids=[participant_id] if participant_id else None,
                text_query=text_query,
                message_id_start=topic_start,
                message_id_end=topic_end,
                limit=topic_limit,
                ascending=True,
            )
            messages.extend(topic_messages)

        deduped: list[TelegramMessage] = []
        seen_ids: set[int] = set()
        for message in sorted(
            messages,
            key=lambda item: int(item.telegram_message_id or 0),
        ):
            message_id = int(message.telegram_message_id or 0)
            if not message_id or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            deduped.append(message)
            if len(deduped) >= limit:
                break
        return deduped, scoped_topics

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
            top_users = repository.list_telegram_preprocess_top_users(
                self.session,
                self.project.id,
                run_id=preprocess_run_id,
            )
            query_lower = query.lower()
            for item in top_users:
                if query and not any(
                    query_lower in value.lower()
                    for value in [item.display_name or "", item.username or "", item.uid or ""]
                    if value
                ):
                    continue
                candidates.append(self._top_user_snapshot(item, preprocess_run_id))
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
            query = str(args.get("query") or "").strip()
            limit = max(1, min(int(args.get("limit", 12) or 12), 24))
            offset = max(0, int(args.get("offset", 0) or 0))
            matched_topics = self._list_matching_related_topics(
                preprocess_run_id,
                participant_id,
                query=query,
            )
            page = matched_topics[offset: offset + limit]
            serialized = [
                self._serialize_related_topic(topic, participant_id=participant_id)
                for topic in page
            ]
            return {
                "topics": serialized,
                "total": len(matched_topics),
                "offset": offset,
                "limit": limit,
            }, {
                "topic_ids": {topic.id for topic in page},
                "week_keys": {self._topic_week_key(topic) for topic in page if self._topic_week_key(topic)},
            }

        if name == "query_telegram_messages":
            participant_id = str(args.get("participant_id") or target_user["participant_id"]).strip()
            topic_ids = [str(item) for item in (args.get("topic_ids") or []) if str(item).strip()]
            if not topic_ids:
                return {
                    "error": "query_telegram_messages requires topic_ids from list_related_topics."
                }, {}
            messages, scoped_topics = self._collect_topic_scoped_messages(
                preprocess_run_id=preprocess_run_id,
                participant_id=participant_id,
                topic_ids=topic_ids,
                text_query=str(args.get("query") or "").strip() or None,
                message_id_start=(int(args.get("message_id_start")) if args.get("message_id_start") is not None else None),
                message_id_end=(int(args.get("message_id_end")) if args.get("message_id_end") is not None else None),
                limit=max(1, min(int(args.get("limit", TELEGRAM_TOOL_MAX_MESSAGES) or TELEGRAM_TOOL_MAX_MESSAGES), TELEGRAM_TOOL_MAX_MESSAGES)),
            )
            return {"messages": [_compact_message(item) for item in messages]}, {
                "topic_ids": set(topic_ids),
                "week_keys": {self._topic_week_key(topic) for topic in scoped_topics if self._topic_week_key(topic)},
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
            messages: list[TelegramMessage] = []
            scoped_topics: list[Any] = []
            if message_ids:
                messages = [
                    repository.get_telegram_message_by_telegram_id(self.session, self.project.id, message_id)
                    for message_id in message_ids[:TELEGRAM_TOOL_MAX_MESSAGES]
                ]
                messages = [item for item in messages if item]
            elif topic_ids:
                messages, scoped_topics = self._collect_topic_scoped_messages(
                    preprocess_run_id=preprocess_run_id,
                    participant_id=str(target_user["participant_id"]),
                    topic_ids=topic_ids,
                    text_query=None,
                    message_id_start=None,
                    message_id_end=None,
                    limit=TELEGRAM_TOOL_MAX_MESSAGES,
                )
            if not messages:
                return {"analysis": {"summary": "No messages matched the requested slice.", "fewshots": []}}, {
                    "topic_ids": set(topic_ids),
                }
            expanded_messages = self._expand_with_message_context(messages)
            if not self.client:
                analysis = {
                    "summary": prompt,
                    "fewshots": self._messages_to_fewshots(
                        expanded_messages,
                        participant_id=str(target_user.get("participant_id") or "").strip() or None,
                    )[:5],
                }
                return {"analysis": analysis}, {
                    "topic_ids": set(topic_ids),
                    "week_keys": {self._topic_week_key(topic) for topic in scoped_topics if self._topic_week_key(topic)},
                    "message_ids": {int(item.telegram_message_id) for item in expanded_messages if item.telegram_message_id is not None},
                    "messages": expanded_messages,
                }
            compact_slice = "\n".join(
                f"[{item.telegram_message_id}] {item.sender_name or 'unknown'}: {' '.join((item.text_normalized or '').split())[:240]}"
                for item in expanded_messages[:TELEGRAM_TOOL_MAX_MESSAGES]
            )
            request_key = f"telegram-analysis-db-{target_user['participant_id']}-{len(topic_ids)}-{len(message_ids)}"
            stream_handler = self._build_stream_trace_callback(
                request_key=request_key,
                agent="telegram_facet_subanalysis",
                extra={"tool_name": "analyze_database", "topic_ids": topic_ids[:8], "message_ids": message_ids[:8]},
            )
            self._trace(
                "llm_request_started",
                agent="telegram_facet_subanalysis",
                request_key=request_key,
                request_kind="chat_completion",
                tool_name="analyze_database",
                prompt_preview=self._preview(prompt),
                topic_ids=topic_ids[:8],
                message_ids=message_ids[:8],
            )
            response = self.client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": "分析一小段精确的 Telegram 原始消息切片，只返回 JSON，包含 summary 和 fewshots。fewshots 需要标注情境、表达方式、原话以及前后文。除键名外，正文尽量使用简体中文。",
                    },
                    {
                        "role": "user",
                        "content": f"分析任务：{prompt}\n\n请围绕目标用户自己的消息整理 fewshots，并补上前后几条上下文。\n\n消息切片：\n{compact_slice}",
                    },
                ],
                model=self.llm_config.model if self.llm_config else None,
                temperature=0.2,
                max_tokens=600,
                stream_handler=stream_handler,
            )
            flush_callback = getattr(stream_handler, "_flush_remaining", None)
            if callable(flush_callback):
                flush_callback()
            self._trace(
                "llm_request_completed",
                agent="telegram_facet_subanalysis",
                request_key=request_key,
                request_kind="chat_completion",
                tool_name="analyze_database",
                usage=response.usage,
                response_text_preview=self._preview(response.content),
                topic_ids=topic_ids[:8],
                message_ids=message_ids[:8],
            )
            parsed = parse_json_response(response.content, fallback=True)
            return {"analysis": parsed}, {
                "topic_ids": set(topic_ids),
                "week_keys": {self._topic_week_key(topic) for topic in scoped_topics if self._topic_week_key(topic)},
                "message_ids": {int(item.telegram_message_id) for item in expanded_messages if item.telegram_message_id is not None},
                "messages": expanded_messages,
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
                    "description": "List candidate Telegram users from preprocess top-users first, then participant fallback.",
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
                    "description": "Return the resolved Telegram target user's identity and snapshot.",
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
                    "description": "Step 1: read all related topic summaries, participant stances, and exact quotes for the target user before deciding whether raw messages are needed.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "participant_id": {"type": "string"},
                            "query": {"type": "string"},
                            "limit": {"type": "integer"},
                            "offset": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "query_telegram_messages",
                    "description": "Step 2: fetch a small balanced raw-message sample for selected topic_ids only after topic summaries show that deeper evidence is needed.",
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
                    "description": "Run a compact sub-analysis over a very small exact raw-message slice and return only a tight summary.",
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
