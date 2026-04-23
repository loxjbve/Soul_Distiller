from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analysis.facets import FacetDefinition
from app.analysis.stone import build_stone_facet_messages, expand_stone_profile_for_analysis
from app.llm.client import LLMError, OpenAICompatibleClient, normalize_api_mode, parse_json_response
from app.models import DocumentRecord, Project
from app.schemas import ServiceConfig
from app.storage import repository
from app.utils.text import normalize_whitespace

STONE_TOOL_LOOP_MAX_STEPS = 16
STONE_TOOL_MAX_DOCUMENTS = 8
STONE_TOOL_READ_LIMIT = 5000
STONE_LARGE_CORPUS_THRESHOLD = 24
STONE_TOOL_MAX_PROFILE_BUDGET = 24
STONE_TOOL_MIN_PROFILE_BUDGET = 12
STONE_TOOL_TOTAL_TEXT_BUDGET = 18000
STONE_TOOL_PREVIEW_LIMIT = 220
STONE_TOOL_PASSAGE_PREVIEW_LIMIT = 180


@dataclass(slots=True)
class StoneFacetAnalysisResult:
    payload: dict[str, Any]
    retrieval_trace: dict[str, Any]
    hit_count: int


class StoneAnalysisAgent:
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

    def analyze_facet(
        self,
        facet: FacetDefinition,
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> StoneFacetAnalysisResult:
        documents = self._load_ready_documents()
        profiles = [self._profile_snapshot(document) for document in documents]
        if not documents:
            raise ValueError("Stone 分析需要至少一篇已就绪文章。")
        corpus_overview = self._build_corpus_overview(profiles)
        if not self.client or not self.llm_config:
            payload = self._heuristic_facet_result(facet, profiles)
            return StoneFacetAnalysisResult(
                payload=payload,
                retrieval_trace=self._base_retrieval_trace(
                    documents=documents,
                    profiles=profiles,
                    corpus_overview=corpus_overview,
                    queried_document_ids=[],
                    tool_trace=[],
                ),
                hit_count=len(payload.get("fewshots") or []),
            )

        started = perf_counter()
        request_url = self.client.endpoint_url(
            "/responses" if normalize_api_mode(self.llm_config.api_mode) == "responses" else "/chat/completions"
        )
        messages = build_stone_facet_messages(
            self.project.name,
            facet.label,
            facet.key,
            facet.purpose,
            self._render_corpus_overview(corpus_overview),
            target_role=target_role,
            analysis_context=analysis_context,
        )
        tool_result = self._run_tool_loop(
            facet=facet,
            messages=messages,
            documents=documents,
            profiles=profiles,
            corpus_overview=corpus_overview,
        )
        llm_success = True
        llm_error_text: str | None = None
        try:
            parsed = parse_json_response(tool_result["content"], fallback=False)
        except LLMError as exc:
            parsed = parse_json_response(tool_result["content"], fallback=True)
            llm_success = False
            llm_error_text = str(exc)
        payload = self._normalize_payload(
            parsed,
            facet,
            documents=documents,
            profiles=profiles,
            fallback_evidence=tool_result["fallback_evidence"],
        )
        notes = [str(payload.get("notes") or "").strip()] if payload.get("notes") else []
        if not llm_success:
            notes.append("LLM 返回的不是标准 JSON，系统已用回退解析尽量恢复结果。")
        payload["notes"] = "\n".join(item for item in notes if item) or None
        payload["_meta"] = {
            "llm_called": True,
            "llm_success": llm_success,
            "llm_attempts": max(1, int(tool_result.get("iterations") or 1)),
            "provider_kind": self.llm_config.provider_kind,
            "api_mode": normalize_api_mode(self.llm_config.api_mode),
            "llm_model": tool_result.get("model") or self.llm_config.model,
            "prompt_tokens": int((tool_result.get("usage") or {}).get("prompt_tokens", 0) or 0),
            "completion_tokens": int((tool_result.get("usage") or {}).get("completion_tokens", 0) or 0),
            "total_tokens": int((tool_result.get("usage") or {}).get("total_tokens", 0) or 0),
            "duration_ms": int((perf_counter() - started) * 1000),
            "request_url": request_url,
            "request_payload": {
                "messages": messages,
                "tools": self._tool_schemas(),
                "model": self.llm_config.model,
                "api_mode": self.llm_config.api_mode,
                "endpoint_url": request_url,
            },
            "raw_text": str(tool_result.get("content") or "")[:3200],
            "llm_error": llm_error_text,
            "log_path": self.log_path,
        }
        retrieval_trace = self._base_retrieval_trace(
            documents=documents,
            profiles=profiles,
            corpus_overview=corpus_overview,
            queried_document_ids=tool_result["queried_document_ids"],
            tool_trace=tool_result["tool_trace"],
        )
        return StoneFacetAnalysisResult(
            payload=payload,
            retrieval_trace=retrieval_trace,
            hit_count=len(payload.get("fewshots") or []),
        )

    def _load_ready_documents(self) -> list[DocumentRecord]:
        target_project_id = repository.get_target_project_id(self.session, self.project.id)
        stmt = (
            select(DocumentRecord)
            .where(
                DocumentRecord.project_id == target_project_id,
                DocumentRecord.ingest_status == "ready",
            )
            .order_by(DocumentRecord.created_at.asc())
        )
        return list(self.session.scalars(stmt))

    @staticmethod
    def _profile_snapshot(document: DocumentRecord) -> dict[str, Any]:
        profile = dict((document.metadata_json or {}).get("stone_profile") or {})
        expanded = expand_stone_profile_for_analysis(
            profile,
            title=document.title or document.filename,
        )
        return {
            "document_id": document.id,
            "title": document.title or document.filename,
            **expanded,
        }

    @staticmethod
    def _distribution_snapshot(counter: Counter[str], *, limit: int = 12) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for label, count in counter.most_common(limit):
            normalized = normalize_whitespace(label) or "未标注"
            rows.append({"label": normalized, "count": int(count)})
        return rows

    def _build_corpus_overview(self, profiles: list[dict[str, Any]]) -> dict[str, Any]:
        content_type_counter = Counter(
            normalize_whitespace(str(profile.get("content_type") or "")) or "未标注"
            for profile in profiles
        )
        emotion_counter = Counter(
            normalize_whitespace(str(profile.get("emotion_label") or "")) or "未标注"
            for profile in profiles
        )
        length_counter = Counter(
            normalize_whitespace(str(profile.get("length_label") or "")) or "未标注"
            for profile in profiles
        )
        return {
            "project_name": self.project.name,
            "total_documents": len(profiles),
            "profile_count": len(profiles),
            "length_distribution": self._distribution_snapshot(length_counter, limit=4),
            "content_type_distribution": self._distribution_snapshot(content_type_counter, limit=12),
            "emotion_distribution": self._distribution_snapshot(emotion_counter, limit=12),
            "sample_titles": [
                str(profile.get("title") or "（未命名）").strip()
                for profile in profiles[: min(6, len(profiles))]
            ],
            "paging_policy": {
                "page_limit_max": STONE_TOOL_MAX_DOCUMENTS,
                "profile_budget": self._profile_read_budget(len(profiles)),
                "large_corpus_threshold": STONE_LARGE_CORPUS_THRESHOLD,
            },
        }

    @staticmethod
    def _render_corpus_overview(overview: dict[str, Any]) -> str:
        def _render_distribution(label: str, items: list[dict[str, Any]]) -> str:
            if not items:
                return f"{label}：暂无"
            return f"{label}：" + "，".join(
                f"{item.get('label') or '未标注'} {int(item.get('count') or 0)} 篇"
                for item in items
            )

        lines = [
            f"作品总数：{int(overview.get('total_documents') or 0)}",
            _render_distribution("长短分布", list(overview.get("length_distribution") or [])),
            _render_distribution("性质分布", list(overview.get("content_type_distribution") or [])),
            _render_distribution("情绪分布", list(overview.get("emotion_distribution") or [])),
        ]
        sample_titles = [str(item or "").strip() for item in (overview.get("sample_titles") or []) if str(item or "").strip()]
        if sample_titles:
            lines.append("样本标题预览：" + "｜".join(sample_titles))
        paging_policy = dict(overview.get("paging_policy") or {})
        lines.append(
            "分页策略：每次最多读取 "
            f"{int(paging_policy.get('page_limit_max') or STONE_TOOL_MAX_DOCUMENTS)} 篇画像，"
            f"单个 facet 最多分页读取 {int(paging_policy.get('profile_budget') or 0)} 篇。"
        )
        return "\n".join(lines).strip()

    @staticmethod
    def _profile_read_budget(profile_count: int) -> int:
        if profile_count <= STONE_LARGE_CORPUS_THRESHOLD:
            return max(1, profile_count)
        return min(
            STONE_TOOL_MAX_PROFILE_BUDGET,
            max(STONE_TOOL_MIN_PROFILE_BUDGET, (profile_count + 3) // 4),
        )

    @staticmethod
    def _compact_profile(profile: dict[str, Any], *, preview: bool) -> dict[str, Any]:
        def _trim(value: Any, limit: int) -> str:
            text = normalize_whitespace(str(value or ""))
            if len(text) <= limit:
                return text
            return f"{text[: max(0, limit - 1)]}…"

        selected_passages = [
            _trim(item, STONE_TOOL_PASSAGE_PREVIEW_LIMIT if preview else 3200)
            for item in (profile.get("selected_passages") or [])
            if normalize_whitespace(str(item or ""))
        ][:3]
        return {
            "document_id": str(profile.get("document_id") or ""),
            "title": str(profile.get("title") or "（未命名）").strip() or "（未命名）",
            "content_summary": _trim(profile.get("content_summary"), STONE_TOOL_PREVIEW_LIMIT if preview else 3200),
            "content_type": normalize_whitespace(str(profile.get("content_type") or "")),
            "length_label": normalize_whitespace(str(profile.get("length_label") or "")),
            "emotion_label": normalize_whitespace(str(profile.get("emotion_label") or "")),
            "selected_passages": selected_passages,
        }

    def _run_tool_loop(
        self,
        *,
        facet: FacetDefinition,
        messages: list[dict[str, Any]],
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
        corpus_overview: dict[str, Any],
    ) -> dict[str, Any]:
        assert self.client is not None
        usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 0,
        }
        tool_trace: list[dict[str, Any]] = []
        queried_document_ids: set[str] = set()
        fallback_evidence = self._fallback_evidence(profiles)
        model_name = self.llm_config.model if self.llm_config else None
        tool_state = {
            "profile_reads": 0,
            "profile_budget": self._profile_read_budget(len(profiles)),
            "text_chars_read": 0,
        }

        for iteration in range(1, STONE_TOOL_LOOP_MAX_STEPS + 1):
            request_key = f"{facet.key}-stone-round-{iteration}"
            self._trace(
                "llm_request_started",
                agent="stone_facet_agent",
                facet_key=facet.key,
                round_index=iteration,
                request_key=request_key,
                request_kind="tool_round",
                label=f"{facet.label} 第 {iteration} 轮",
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
                agent="stone_facet_agent",
                facet_key=facet.key,
                round_index=iteration,
                request_key=request_key,
                request_kind="tool_round",
                label=f"{facet.label} 第 {iteration} 轮",
                usage=round_result.usage,
                response_text_preview=self._preview(round_result.content),
                tool_calls=[{"name": call.name, "arguments": call.arguments} for call in round_result.tool_calls],
            )
            if not round_result.tool_calls:
                return {
                    "content": round_result.content,
                    "usage": usage,
                    "tool_trace": tool_trace,
                    "queried_document_ids": sorted(queried_document_ids),
                    "fallback_evidence": fallback_evidence,
                    "iterations": iteration,
                    "model": model_name,
                    "tool_state": tool_state,
                    "corpus_overview": corpus_overview,
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
                    agent="stone_facet_agent",
                    facet_key=facet.key,
                    round_index=iteration,
                    request_key=request_key,
                    tool_name=call.name,
                    arguments_preview=self._preview(call.arguments),
                )
                output, state = self._execute_tool(
                    call.name,
                    call.arguments,
                    documents=documents,
                    profiles=profiles,
                    corpus_overview=corpus_overview,
                    tool_state=tool_state,
                )
                queried_document_ids.update(state.get("document_ids", []))
                tool_entry = {
                    "tool": call.name,
                    "arguments": call.arguments,
                    "document_ids": sorted(state.get("document_ids", [])),
                    "request_key": request_key,
                    "result_preview": self._preview(output),
                }
                tool_trace.append(tool_entry)
                self._trace(
                    "tool_result",
                    agent="stone_facet_agent",
                    facet_key=facet.key,
                    round_index=iteration,
                    request_key=request_key,
                    tool_name=call.name,
                    document_ids=tool_entry["document_ids"],
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
        raise LLMError("Stone 分析超过了最大工具调用轮数。")

    def _execute_tool(
        self,
        name: str,
        args: dict[str, Any],
        *,
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
        corpus_overview: dict[str, Any],
        tool_state: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        documents_by_id = {document.id: document for document in documents}
        profiles_by_id = {profile["document_id"]: profile for profile in profiles}

        if name == "get_corpus_overview":
            return {
                **corpus_overview,
                "remaining_profile_budget": max(
                    0,
                    int(tool_state.get("profile_budget", 0) or 0) - int(tool_state.get("profile_reads", 0) or 0),
                ),
                "remaining_text_budget": max(
                    0,
                    STONE_TOOL_TOTAL_TEXT_BUDGET - int(tool_state.get("text_chars_read", 0) or 0),
                ),
            }, {}

        if name == "list_article_profiles_page":
            query = normalize_whitespace(str(args.get("query") or "")).lower()
            content_type = normalize_whitespace(str(args.get("content_type") or ""))
            emotion_label = normalize_whitespace(str(args.get("emotion_label") or ""))
            length_label = normalize_whitespace(str(args.get("length_label") or ""))
            offset = max(0, int(args.get("offset", 0) or 0))
            limit = max(1, min(int(args.get("limit", STONE_TOOL_MAX_DOCUMENTS) or STONE_TOOL_MAX_DOCUMENTS), STONE_TOOL_MAX_DOCUMENTS))
            remaining_budget = max(
                0,
                int(tool_state.get("profile_budget", 0) or 0) - int(tool_state.get("profile_reads", 0) or 0),
            )
            if remaining_budget <= 0:
                return {
                    "error": "当前 facet 的分页读取上限已经用完，请基于已读取范围继续总结，不要继续穷举全部文章。",
                    "remaining_profile_budget": 0,
                }, {}

            matched: list[dict[str, Any]] = []
            for profile in profiles:
                if content_type and normalize_whitespace(str(profile.get("content_type") or "")) != content_type:
                    continue
                if emotion_label and normalize_whitespace(str(profile.get("emotion_label") or "")) != emotion_label:
                    continue
                if length_label and normalize_whitespace(str(profile.get("length_label") or "")) != length_label:
                    continue
                haystack = normalize_whitespace(
                    " ".join(
                        [
                            str(profile.get("title") or ""),
                            str(profile.get("content_summary") or ""),
                            str(profile.get("content_type") or ""),
                            str(profile.get("length_label") or ""),
                            str(profile.get("emotion_label") or ""),
                            str(profile.get("article_theme") or ""),
                            str(profile.get("tone") or ""),
                            str(profile.get("structure_template") or ""),
                            " ".join(profile.get("selected_passages") or []),
                            " ".join(profile.get("lexical_markers") or []),
                            " ".join(profile.get("nonclinical_signals") or []),
                            " ".join(profile.get("representative_lines") or []),
                        ]
                    )
                ).lower()
                if query and query not in haystack:
                    continue
                matched.append(profile)
            paged = matched[offset:offset + min(limit, remaining_budget)]
            tool_state["profile_reads"] = int(tool_state.get("profile_reads", 0) or 0) + len(paged)
            return (
                {
                    "offset": offset,
                    "limit": limit,
                    "returned": len(paged),
                    "total_profiles": len(matched),
                    "has_more": offset + len(paged) < len(matched),
                    "remaining_profile_budget": max(
                        0,
                        int(tool_state.get("profile_budget", 0) or 0) - int(tool_state.get("profile_reads", 0) or 0),
                    ),
                    "filters": {
                        "query": query or None,
                        "content_type": content_type or None,
                        "emotion_label": emotion_label or None,
                        "length_label": length_label or None,
                    },
                    "profiles": [self._compact_profile(item, preview=True) for item in paged],
                },
                {"document_ids": {item["document_id"] for item in paged}},
            )

        if name == "read_article_text":
            document_id = str(args.get("document_id") or "").strip()
            if not document_id or document_id not in documents_by_id:
                return {"error": "未知 document_id。"}, {}
            document = documents_by_id[document_id]
            full_text = str(document.clean_text or document.raw_text or "")
            start_offset = max(0, int(args.get("start_offset", 0) or 0))
            max_chars = max(200, min(int(args.get("max_chars", STONE_TOOL_READ_LIMIT) or STONE_TOOL_READ_LIMIT), 12000))
            remaining_text_budget = max(
                0,
                STONE_TOOL_TOTAL_TEXT_BUDGET - int(tool_state.get("text_chars_read", 0) or 0),
            )
            if remaining_text_budget <= 0:
                return {"error": "当前 facet 的原文读取额度已经用完，请基于已有证据继续总结。"}, {}
            max_chars = min(max_chars, remaining_text_budget)
            excerpt = full_text[start_offset:start_offset + max_chars]
            tool_state["text_chars_read"] = int(tool_state.get("text_chars_read", 0) or 0) + len(excerpt)
            return (
                {
                    "document_id": document.id,
                    "document_title": document.title or document.filename,
                    "start_offset": start_offset,
                    "returned_chars": len(excerpt),
                    "total_chars": len(full_text),
                    "has_more": start_offset + len(excerpt) < len(full_text),
                    "remaining_text_budget": max(
                        0,
                        STONE_TOOL_TOTAL_TEXT_BUDGET - int(tool_state.get("text_chars_read", 0) or 0),
                    ),
                    "text": excerpt,
                },
                {"document_ids": {document.id}},
            )

        if name == "search_article_text":
            query = normalize_whitespace(str(args.get("query") or ""))
            if not query:
                return {"error": "query 不能为空。"}, {}
            limit = max(1, min(int(args.get("limit", 6) or 6), 12))
            matches: list[dict[str, Any]] = []
            matched_ids: set[str] = set()
            for document in documents:
                full_text = str(document.clean_text or document.raw_text or "")
                offset = full_text.lower().find(query.lower())
                if offset < 0:
                    continue
                left = max(0, offset - 180)
                right = min(len(full_text), offset + max(len(query), 1) + 180)
                matches.append(
                    {
                        "document_id": document.id,
                        "document_title": document.title or document.filename,
                        "offset": offset,
                        "snippet": full_text[left:right],
                    }
                )
                matched_ids.add(document.id)
                if len(matches) >= limit:
                    break
            return {"query": query, "matches": matches, "count": len(matches)}, {"document_ids": matched_ids}

        if name == "get_article_profile":
            document_id = str(args.get("document_id") or "").strip()
            profile = profiles_by_id.get(document_id)
            if not profile:
                return {"error": "未知 document_id。"}, {}
            remaining_budget = max(
                0,
                int(tool_state.get("profile_budget", 0) or 0) - int(tool_state.get("profile_reads", 0) or 0),
            )
            if len(profiles) > STONE_LARGE_CORPUS_THRESHOLD and remaining_budget <= 0:
                return {"error": "当前 facet 的画像读取上限已经用完，请基于已读取范围继续总结。"}, {}
            if len(profiles) > STONE_LARGE_CORPUS_THRESHOLD:
                tool_state["profile_reads"] = int(tool_state.get("profile_reads", 0) or 0) + 1
            return (
                {
                    "profile": self._compact_profile(profile, preview=False),
                    "remaining_profile_budget": max(
                        0,
                        int(tool_state.get("profile_budget", 0) or 0) - int(tool_state.get("profile_reads", 0) or 0),
                    ),
                },
                {"document_ids": {document_id}},
            )

        return {"error": f"未知 Stone 工具：{name}"}, {}

    @staticmethod
    def _tool_schemas() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_corpus_overview",
                    "description": "返回 Stone 语料总览，包括作品总数、性质分布、情绪分布和分页额度。",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_article_profiles_page",
                    "description": "按范围分页读取一部分文章画像预览，可按性质、情绪、长短或关键词过滤。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "offset": {"type": "integer"},
                            "limit": {"type": "integer"},
                            "query": {"type": "string"},
                            "content_type": {"type": "string"},
                            "emotion_label": {"type": "string"},
                            "length_label": {"type": "string"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_article_profile",
                    "description": "按 document_id 获取单篇文章的完整画像。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "document_id": {"type": "string"},
                        },
                        "required": ["document_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "read_article_text",
                    "description": "按 document_id 读取文章原文，可指定起始位置和最大字符数。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "document_id": {"type": "string"},
                            "start_offset": {"type": "integer"},
                            "max_chars": {"type": "integer"},
                        },
                        "required": ["document_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_article_text",
                    "description": "在原文语料中搜索关键词，返回命中的片段预览。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                        "required": ["query"],
                    },
                },
            },
        ]

    def _normalize_payload(
        self,
        payload: dict[str, Any],
        facet: FacetDefinition,
        *,
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
        fallback_evidence: list[dict[str, Any]],
    ) -> dict[str, Any]:
        documents_by_id = {document.id: document for document in documents}
        profiles_by_id = {profile["document_id"]: profile for profile in profiles}
        bullets: list[str] = []
        for item in payload.get("bullets") or []:
            text = normalize_whitespace(str(item or ""))
            if text and text not in bullets:
                bullets.append(text)
            if len(bullets) >= 8:
                break

        evidence: list[dict[str, Any]] = []
        for item in (payload.get("fewshots") or payload.get("evidence") or [])[:12]:
            if not isinstance(item, dict):
                continue
            document_id = str(item.get("document_id") or "").strip()
            if not document_id and item.get("document_title"):
                document_id = next(
                    (
                        profile["document_id"]
                        for profile in profiles
                        if str(profile.get("title") or "").strip() == str(item.get("document_title") or "").strip()
                    ),
                    "",
                )
            if not document_id or document_id not in documents_by_id:
                continue
            document = documents_by_id[document_id]
            profile = profiles_by_id.get(document_id) or {}
            quote = normalize_whitespace(str(item.get("quote") or "")) or (
                " / ".join(profile.get("representative_lines") or [])[:220]
            ) or str(document.clean_text or document.raw_text or "")[:220]
            evidence.append(
                {
                    "document_id": document_id,
                    "document_title": document.title or document.filename,
                    "situation": normalize_whitespace(str(item.get("situation") or item.get("reason") or "")) or f"{facet.label} 相关证据",
                    "expression": normalize_whitespace(str(item.get("expression") or "")) or "基于文章画像的表达特征",
                    "reason": normalize_whitespace(str(item.get("reason") or "")) or f"用于支撑 {facet.label} 判断",
                    "quote": quote,
                }
            )
        if not evidence:
            evidence = fallback_evidence[:8]

        conflicts: list[dict[str, Any]] = []
        for item in (payload.get("conflicts") or [])[:5]:
            if not isinstance(item, dict):
                continue
            title = normalize_whitespace(str(item.get("title") or ""))
            detail = normalize_whitespace(str(item.get("detail") or ""))
            if title or detail:
                conflicts.append({"title": title, "detail": detail})

        summary = normalize_whitespace(str(payload.get("summary") or ""))
        if not summary:
            if bullets:
                summary = f"围绕 {facet.label}，当前抽样文章主要显示出这些共同特征：{'；'.join(bullets[:2])}。"
            else:
                summary = f"围绕 {facet.label}，当前样本中已经出现可归纳信号，但还需要结合证据一起阅读。"

        return {
            "summary": summary,
            "bullets": bullets,
            "confidence": self._parse_confidence(payload.get("confidence"), default=0.68),
            "fewshots": evidence,
            "evidence": evidence,
            "conflicts": conflicts,
            "notes": normalize_whitespace(str(payload.get("notes") or "")) or None,
        }

    def _heuristic_facet_result(self, facet: FacetDefinition, profiles: list[dict[str, Any]]) -> dict[str, Any]:
        evidence = self._fallback_evidence(profiles)
        bullets: list[str] = []
        for profile in profiles[:4]:
            pieces = [
                profile.get("content_summary"),
                profile.get("content_type"),
                profile.get("emotion_label"),
            ]
            rendered = "｜".join(piece for piece in pieces if piece)
            if rendered:
                bullets.append(f"{profile.get('title') or '（未命名）'}：{rendered}")
        return {
            "summary": (
                f"当前未配置 Chat LLM，因此 {facet.label} 只能先基于逐篇文章画像做启发式归纳。"
                "结果侧重提炼重复出现的主题、性质和情绪信号。"
            ),
            "bullets": bullets[:8],
            "confidence": 0.52,
            "fewshots": evidence,
            "evidence": evidence,
            "conflicts": [],
            "notes": "未配置 Chat LLM，本次 Stone 维度分析使用了仅基于文章画像的启发式结果。",
            "_meta": {
                "llm_called": False,
                "llm_success": False,
                "llm_attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "duration_ms": 0,
            },
        }

    @staticmethod
    def _fallback_evidence(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for profile in profiles:
            document_id = str(profile.get("document_id") or "").strip()
            if not document_id:
                continue
            quote = " / ".join(profile.get("representative_lines") or [])[:220] or str(profile.get("article_theme") or "")
            evidence.append(
                {
                    "document_id": document_id,
                    "document_title": profile.get("title") or "（未命名）",
                    "situation": "文章画像提取",
                    "expression": profile.get("tone") or "画像摘要",
                    "reason": profile.get("article_theme") or "代表性文章信号",
                    "quote": quote,
                }
            )
            if len(evidence) >= 8:
                break
        return evidence

    @staticmethod
    def _parse_confidence(value: Any, *, default: float) -> float:
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return default

    def _base_retrieval_trace(
        self,
        *,
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
        corpus_overview: dict[str, Any],
        queried_document_ids: list[str],
        tool_trace: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "mode": "stone_agent",
            "evidence_kind": "stone_articles",
            "embedding_configured": False,
            "embedding_attempted": False,
            "embedding_api_called": False,
            "embedding_success": False,
            "embedding_skip_reason": "stone_direct_article_mode",
            "document_count": len(documents),
            "document_profile_count": len(profiles),
            "document_ids": [document.id for document in documents],
            "corpus_overview": corpus_overview,
            "queried_document_ids": queried_document_ids,
            "tool_calls": tool_trace,
        }

    @staticmethod
    def _preview(value: Any, *, limit: int = 240) -> str:
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        text = normalize_whitespace(text)
        return text[:limit]

    def _trace(self, event_type: str, **payload: Any) -> None:
        if not self.trace_callback:
            return
        event = {"type": event_type, **payload}
        self.trace_callback(event)
