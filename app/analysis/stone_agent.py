from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analysis.facets import FacetDefinition
from app.analysis.stone import build_stone_facet_messages, expand_stone_profile_for_analysis, summarize_stone_profiles
from app.llm.client import LLMError, OpenAICompatibleClient, normalize_api_mode, parse_json_response
from app.models import DocumentRecord, Project
from app.schemas import ServiceConfig
from app.storage import repository
from app.utils.text import normalize_whitespace

STONE_TOOL_LOOP_MAX_STEPS = 16
STONE_TOOL_MAX_DOCUMENTS = 12
STONE_TOOL_READ_LIMIT = 5000


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
            raise ValueError("Stone analysis requires ready documents.")
        if not self.client or not self.llm_config:
            payload = self._heuristic_facet_result(facet, profiles)
            return StoneFacetAnalysisResult(
                payload=payload,
                retrieval_trace=self._base_retrieval_trace(
                    documents=documents,
                    profiles=profiles,
                    queried_document_ids=[],
                    tool_trace=[],
                ),
                hit_count=len(payload.get("fewshots") or []),
            )

        started = perf_counter()
        request_url = self.client.endpoint_url(
            "/responses" if normalize_api_mode(self.llm_config.api_mode) == "responses" else "/chat/completions"
        )
        profile_dump = summarize_stone_profiles(profiles)
        messages = build_stone_facet_messages(
            self.project.name,
            facet.label,
            facet.key,
            facet.purpose,
            profile_dump,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        tool_result = self._run_tool_loop(
            facet=facet,
            messages=messages,
            documents=documents,
            profiles=profiles,
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
            notes.append("LLM returned non-JSON text, so the facet was recovered with fallback parsing.")
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

    def _run_tool_loop(
        self,
        *,
        facet: FacetDefinition,
        messages: list[dict[str, Any]],
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
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

        for iteration in range(1, STONE_TOOL_LOOP_MAX_STEPS + 1):
            request_key = f"{facet.key}-stone-round-{iteration}"
            self._trace(
                "llm_request_started",
                agent="stone_facet_agent",
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
                agent="stone_facet_agent",
                facet_key=facet.key,
                round_index=iteration,
                request_key=request_key,
                request_kind="tool_round",
                label=f"{facet.label} round {iteration}",
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
                output, state = self._execute_tool(call.name, call.arguments, documents=documents, profiles=profiles)
                queried_document_ids.update(state.get("document_ids", []))
                tool_entry = {
                    "tool": call.name,
                    "arguments": call.arguments,
                    "document_ids": sorted(state.get("document_ids", [])),
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
        raise LLMError("Stone analysis exceeded the maximum tool iterations.")

    def _execute_tool(
        self,
        name: str,
        args: dict[str, Any],
        *,
        documents: list[DocumentRecord],
        profiles: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        documents_by_id = {document.id: document for document in documents}
        profiles_by_id = {profile["document_id"]: profile for profile in profiles}

        if name == "list_article_profiles":
            query = normalize_whitespace(str(args.get("query") or "")).lower()
            requested_ids = {
                str(item).strip()
                for item in (args.get("document_ids") or [])
                if str(item).strip()
            }
            limit = max(1, min(int(args.get("limit", STONE_TOOL_MAX_DOCUMENTS) or STONE_TOOL_MAX_DOCUMENTS), 24))
            matched: list[dict[str, Any]] = []
            for profile in profiles:
                document_id = str(profile.get("document_id") or "")
                if requested_ids and document_id not in requested_ids:
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
                if len(matched) >= limit:
                    break
            return {"profiles": matched, "count": len(matched)}, {"document_ids": {item["document_id"] for item in matched}}

        if name == "read_article_text":
            document_id = str(args.get("document_id") or "").strip()
            if not document_id or document_id not in documents_by_id:
                return {"error": "Unknown document_id."}, {}
            document = documents_by_id[document_id]
            full_text = str(document.clean_text or document.raw_text or "")
            start_offset = max(0, int(args.get("start_offset", 0) or 0))
            max_chars = max(200, min(int(args.get("max_chars", STONE_TOOL_READ_LIMIT) or STONE_TOOL_READ_LIMIT), 12000))
            excerpt = full_text[start_offset:start_offset + max_chars]
            return (
                {
                    "document_id": document.id,
                    "document_title": document.title or document.filename,
                    "start_offset": start_offset,
                    "returned_chars": len(excerpt),
                    "total_chars": len(full_text),
                    "has_more": start_offset + len(excerpt) < len(full_text),
                    "text": excerpt,
                },
                {"document_ids": {document.id}},
            )

        if name == "search_article_text":
            query = normalize_whitespace(str(args.get("query") or ""))
            if not query:
                return {"error": "Query is required."}, {}
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
                return {"error": "Unknown document_id."}, {}
            return {"profile": profile}, {"document_ids": {document_id}}

        return {"error": f"Unknown stone tool: {name}"}, {}

    @staticmethod
    def _tool_schemas() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_article_profiles",
                    "description": "List per-article profiles, optionally filtered by query or document ids.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "document_ids": {"type": "array", "items": {"type": "string"}},
                            "limit": {"type": "integer"},
                        },
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_article_profile",
                    "description": "Return one exact per-article profile by document id.",
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
                    "description": "Read original article text for one document id, with optional offset and char limit.",
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
                    "description": "Search original article text across the corpus and return matching snippets.",
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
                    "situation": normalize_whitespace(str(item.get("situation") or item.get("reason") or "")) or f"{facet.label} evidence",
                    "expression": normalize_whitespace(str(item.get("expression") or "")) or "Profile-led evidence",
                    "reason": normalize_whitespace(str(item.get("reason") or "")) or f"Supports {facet.label}",
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

        return {
            "summary": normalize_whitespace(str(payload.get("summary") or "")),
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
                profile.get("article_theme"),
                profile.get("tone"),
                profile.get("structure_template"),
            ]
            rendered = " | ".join(piece for piece in pieces if piece)
            if rendered:
                bullets.append(f"{profile.get('title') or '(untitled)'}: {rendered}")
        return {
            "summary": (
                f"{facet.label} currently relies on per-article profiles because chat LLM is not configured. "
                f"The output summarizes repeated signals already captured during article profiling."
            ),
            "bullets": bullets[:8],
            "confidence": 0.52,
            "fewshots": evidence,
            "evidence": evidence,
            "conflicts": [],
            "notes": "Chat LLM is not configured; stone facet analysis used profile-only heuristics.",
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
                    "document_title": profile.get("title") or "(untitled)",
                    "situation": "Per-article profile",
                    "expression": profile.get("tone") or "Profile summary",
                    "reason": profile.get("article_theme") or "Representative article signal",
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
