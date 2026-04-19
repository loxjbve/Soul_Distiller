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
            "1. 先读取目标用户画像。\n"
            "2. 再查询与目标用户相关的话题表。\n"
            "3. 仅在必要时抓取少量原始消息作为证据。\n"
            "4. 优先使用 analyze_database 保持上下文紧凑。\n"
            "请只返回 JSON，包含 summary, bullets, confidence, evidence, conflicts, notes。\n"
            "除 JSON 键名外，所有文字内容尽量使用简体中文。\n"
            "每条 evidence 都必须包含 message_id, sender_name, sent_at, quote, reason。\n"
        )
        system_prompt = (
            "你正在分析已经存入 SQL 的 Telegram 群聊记录。\n"
            "这个模式绝对不能使用 embedding、chunk retrieval 或 retrieval.search。\n"
            "请严格按这个顺序工作：\n"
            "1. 先读取目标用户画像。\n"
            "2. 再查询与目标用户相关的话题表。\n"
            "3. 仅在必要时抓取少量原始消息作为证据。\n"
            "4. 优先使用 analyze_database 保持上下文紧凑。\n"
            "请只返回 JSON，包含 summary, bullets, confidence, evidence, conflicts, notes。\n"
            "除 JSON 键名外，所有可读文本都尽量使用简体中文。\n"
            "每条 evidence 都必须包含 message_id, sender_name, sent_at, quote, reason。"
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
        system_prompt = (
            "你正在分析已经存入 SQL 的 Telegram 群聊记录。\n"
            "这个模式绝对不能使用 embedding、chunk retrieval 或 retrieval.search。\n"
            "你必须先理解目标用户，再查询该用户是否出现在相关周话题里，先阅读话题概要，再决定是否回原始消息取证。\n"
            "如果存在多个相关话题或跨多个 week，优先覆盖时间上分离的多个话题，不要只停留在单一短时间片。\n"
            "只有在周话题总结不足以支撑结论时，才去抓取少量原始消息、上下文或 reply chain。\n"
            "优先使用 analyze_database 保持上下文紧凑。\n"
            "请只返回 JSON，包含 summary, bullets, confidence, evidence, conflicts, notes。\n"
            "除 JSON 键名外，所有可读文本都尽量使用简体中文。\n"
            "每条 evidence 都必须包含 message_id, sender_name, sent_at, quote, reason。"
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
            hit_count=len(evidence),
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
        fallback_evidence: list[dict[str, Any]] = []
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
                    fallback_evidence = self._messages_to_evidence(state.get("messages", []))[:8]
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
                        "content": "分析一小段精确的 Telegram 原始消息切片，只返回 JSON，包含 summary 和 evidence。除键名外，正文尽量使用简体中文。",
                    },
                    {
                        "role": "user",
                        "content": f"分析任务：{prompt}\n\n消息切片：\n{compact_slice}",
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
        week_key = str((getattr(topic, "metadata_json", None) or {}).get("week_key") or "").strip()
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
        viewpoints: list[dict[str, Any]] = []
        for item in metadata.get("participant_viewpoints") or []:
            if not isinstance(item, dict):
                continue
            viewpoints.append(
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
            if len(viewpoints) >= limit:
                break
        return viewpoints

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
        topics = repository.list_telegram_preprocess_topics(
            self.session,
            self.project.id,
            run_id=preprocess_run_id,
        )
        matched: list[Any] = []
        for topic in topics:
            if not any(link.participant_id == participant_id for link in topic.participants):
                continue
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
                {
                    "topic_id": topic.id,
                    "title": topic.title,
                    "summary": topic.summary,
                    "week_key": self._topic_week_key(topic),
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
                            "role_hint": link.role_hint,
                            "message_count": link.message_count,
                            "mention_count": link.mention_count,
                        }
                        for link in topic.participants
                    ],
                    "subtopics": [
                        str(item).strip()
                        for item in (self._topic_metadata(topic).get("subtopics") or [])
                        if str(item).strip()
                    ][:8],
                    "interaction_patterns": [
                        str(item).strip()
                        for item in (self._topic_metadata(topic).get("interaction_patterns") or [])
                        if str(item).strip()
                    ][:8],
                    "participant_viewpoints": self._topic_participant_viewpoints(topic, limit=6),
                }
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
                    "week_keys": {self._topic_week_key(topic) for topic in scoped_topics if self._topic_week_key(topic)},
                    "message_ids": {int(item.telegram_message_id) for item in messages if item.telegram_message_id is not None},
                    "messages": messages,
                }
            compact_slice = "\n".join(
                f"[{item.telegram_message_id}] {item.sender_name or 'unknown'}: {' '.join((item.text_normalized or '').split())[:240]}"
                for item in messages[:TELEGRAM_TOOL_MAX_MESSAGES]
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
                        "content": "分析一小段精确的 Telegram 原始消息切片，只返回 JSON，包含 summary 和 evidence。除键名外，正文尽量使用简体中文。",
                    },
                    {
                        "role": "user",
                        "content": f"分析任务：{prompt}\n\n消息切片：\n{compact_slice}",
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
                    "description": "Step 1: read topic summaries for the target user before deciding whether raw messages are needed.",
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
                    "description": "Step 2: fetch a small balanced raw-message sample only after topic summaries show that deeper evidence is needed.",
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
