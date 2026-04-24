from __future__ import annotations

import json
import re
from typing import Any

from app.analysis.stone_v2 import expand_stone_profile_v2_for_analysis
from app.analysis.prompts import build_asset_messages, build_cc_skill_messages
from app.analysis.writing_guide import (
    build_writing_guide_payload_from_facets as _build_writing_guide_payload_from_facets,
    guide_facet_material as _guide_facet_material,
    guide_profile_terms as _guide_profile_terms,
    normalize_fewshot_anchors as _normalize_fewshot_anchors,
    normalize_external_slots as _normalize_external_slots,
    normalize_guide_object as _normalize_guide_object,
    normalize_string_list as _normalize_string_list,
    string_block as _string_block,
)
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import AnalysisFacet, Project
from app.schemas import ASSET_KINDS, AssetBundle, ServiceConfig

SYNTHESIS_SUMMARY_LIMIT = 360
SYNTHESIS_BULLET_LIMIT = 5
SYNTHESIS_BULLET_TEXT_LIMIT = 180
SYNTHESIS_CONFLICT_LIMIT = 3
SYNTHESIS_CONFLICT_TITLE_LIMIT = 80
SYNTHESIS_CONFLICT_DETAIL_LIMIT = 180
SYNTHESIS_SEARCH_CHUNK_LIMIT = 420
SYNTHESIS_SEARCH_RESULT_LIMIT = 5
SKILL_SUPPORT_QUERIES = {
    "skill": "话题总结 高频表达 决策方式 互动模式 原话 证据 语料",
    "personality": "话题总结 性格特质 精神状态 自我认知 核心身份 内在张力 原话 证据",
    "memories": "话题总结 核心记忆 经历 过往重要事件 长期背景 时间线 原话 证据",
}
SKILL_DOCUMENT_FILENAMES = {
    "skill": "Skill.md",
    "personality": "personality.md",
    "memories": "memories.md",
    "merge": "Skill_merge.md",
}
CC_SKILL_DOCUMENT_FILENAMES = {
    "skill": "SKILL.md",
    "personality": "references/personality.md",
    "memories": "references/memories.md",
    "analysis": "references/analysis.md",
}


class AssetSynthesizer:
    def __init__(self, *, log_path: str | None = None) -> None:
        self.log_path = log_path

    def build(
        self,
        asset_kind: str,
        project: Project,
        facets: list[AnalysisFacet],
        config: ServiceConfig | None,
        *,
        target_role: str | None = None,
        analysis_context: str | None = None,
        stream_callback: Any | None = None,
        progress_callback: Any | None = None,
        session: Any | None = None,
        retrieval_service: Any | None = None,
    ) -> AssetBundle:
        normalized_kind = "cc_skill" if asset_kind == "skill" else (asset_kind if asset_kind in ASSET_KINDS else "cc_skill")
        if normalized_kind in {"stone_author_model_v2", "stone_prototype_index_v2"}:
            raise ValueError("Stone V2 assets must be generated from Stone preprocess output, not the generic synthesizer.")
        self._emit_progress(
            progress_callback,
            phase="prepare",
            progress_percent=12,
            message="Reading multi-facet analysis results.",
        )
        structured = (
            self._with_llm(
                normalized_kind,
                project,
                facets,
                config,
                target_role=target_role,
                analysis_context=analysis_context,
                stream_callback=stream_callback,
                progress_callback=progress_callback,
                session=session,
                retrieval_service=retrieval_service,
            )
            if config
            else self._heuristic(
                normalized_kind,
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
                session=session,
            )
        )
        if not config:
            self._emit_progress(
                progress_callback,
                phase="heuristic",
                progress_percent=72,
                message="LLM is not configured; using the local heuristic draft.",
            )
        self._emit_progress(
            progress_callback,
            phase="render",
            progress_percent=86,
            message="Formatting structured fields.",
        )
        if normalized_kind == "skill":
            markdown = self._get_skill_merge_markdown(structured)
            prompt_text = markdown
        elif normalized_kind == "cc_skill":
            markdown = self._get_cc_skill_markdown(structured)
            prompt_text = markdown
        elif normalized_kind == "writing_guide":
            markdown = self._render_writing_guide_markdown(project.name, structured)
            prompt_text = self._render_writing_guide_prompt(project.name, structured)
        else:
            markdown = self._render_profile_report_markdown(project.name, structured)
            prompt_text = self._render_profile_report_prompt(project.name, structured)
        self._emit_progress(
            progress_callback,
            phase="bundle",
            progress_percent=92,
            message="Building Markdown and prompt.",
        )
        return AssetBundle(
            asset_kind=normalized_kind,
            markdown_text=markdown,
            json_payload=structured,
            prompt_text=prompt_text,
        )

    def _with_llm(
        self,
        asset_kind: str,
        project: Project,
        facets: list[AnalysisFacet],
        config: ServiceConfig | None,
        *,
        target_role: str | None,
        analysis_context: str | None,
        stream_callback: Any | None = None,
        progress_callback: Any | None = None,
        session: Any | None = None,
        retrieval_service: Any | None = None,
    ) -> dict[str, Any]:
        if not config:
            return self._heuristic(
                asset_kind,
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        client = OpenAICompatibleClient(config, log_path=self.log_path)
        facet_dump = self._build_facet_dump(facets)

        if asset_kind == "skill":
            try:
                return self._build_skill_documents_with_llm(
                    client,
                    config,
                    project,
                    facet_dump,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    stream_callback=stream_callback,
                    progress_callback=progress_callback,
                    session=session,
                    retrieval_service=retrieval_service,
                )
            except (LLMError, ValueError, KeyError, TypeError):
                self._emit_progress(
                    progress_callback,
                    phase="fallback",
                    progress_percent=68,
                    message="Model output was unusable, falling back to local heuristics.",
                )
                return self._heuristic(
                    asset_kind,
                    project,
                    facets,
                    target_role=target_role,
                    analysis_context=analysis_context,
                )

        if asset_kind == "cc_skill":
            try:
                return self._build_cc_skill_documents_with_llm(
                    client,
                    config,
                    project,
                    facet_dump,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    stream_callback=stream_callback,
                    progress_callback=progress_callback,
                    session=session,
                    retrieval_service=retrieval_service,
                )
            except (LLMError, ValueError, KeyError, TypeError):
                self._emit_progress(
                    progress_callback,
                    phase="fallback",
                    progress_percent=68,
                    message="Model output was unusable, falling back to local heuristics.",
                )
                return self._heuristic(
                    asset_kind,
                    project,
                    facets,
                    target_role=target_role,
                    analysis_context=analysis_context,
                )

        if asset_kind == "writing_guide":
            try:
                return self._build_writing_guide_with_llm(
                    client,
                    config,
                    project,
                    facet_dump,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    stream_callback=stream_callback,
                    progress_callback=progress_callback,
                    session=session,
                )
            except (LLMError, ValueError, KeyError, TypeError):
                self._emit_progress(
                    progress_callback,
                    phase="fallback",
                    progress_percent=68,
                    message="Model output was unusable, falling back to local heuristics.",
                )
                return self._heuristic(
                    asset_kind,
                    project,
                    facets,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    session=session,
                )

        messages = build_asset_messages(
            asset_kind,
            project.name,
            facet_dump,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        try:
            self._emit_progress(
                progress_callback,
                phase="synthesis",
                progress_percent=52,
                message="LLM is generating the structured draft.",
            )
            response = client.chat_completion_result(
                messages,
                model=config.model,
                temperature=0.2,
                max_tokens=None,
                stream_handler=stream_callback,
            )
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            self._emit_progress(
                progress_callback,
                phase="normalize",
                progress_percent=78,
                message="Normalizing model output.",
            )
            parsed = parse_json_response(response.content, fallback=True)
            return self._normalize_profile_report_payload(
                parsed,
                project.name,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        except (LLMError, ValueError, KeyError, TypeError):
            self._emit_progress(
                progress_callback,
                phase="fallback",
                progress_percent=68,
                message="Model output was unusable, falling back to local heuristics.",
            )
            return self._heuristic(
                asset_kind,
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )

    @staticmethod
    def _emit_progress(
        progress_callback: Any | None,
        *,
        phase: str,
        progress_percent: int,
        message: str,
        document_key: str | None = None,
    ) -> None:
        if not callable(progress_callback):
            return
        payload = {
            "phase": phase,
            "progress_percent": progress_percent,
            "message": message,
        }
        if document_key:
            payload["document_key"] = document_key
        progress_callback(payload)

    def _build_facet_dump(self, facets: list[AnalysisFacet]) -> str:
        compact_facets = [self._compact_facet_for_prompt(facet) for facet in facets]
        return json.dumps(compact_facets, ensure_ascii=False, indent=2)

    def _compact_facet_for_prompt(self, facet: AnalysisFacet) -> dict[str, Any]:
        findings = dict(facet.findings_json or {})
        conflicts = list(facet.conflicts_json or [])
        fewshots: list[dict[str, str]] = []
        for item in list(facet.evidence_json or [])[:3]:
            if not isinstance(item, dict):
                continue
            quote = self._truncate_text(item.get("quote"), 160)
            if not quote:
                continue
            fewshots.append(
                {
                    "situation": self._truncate_text(item.get("situation") or item.get("reason"), 120),
                    "expression": self._truncate_text(item.get("expression"), 80),
                    "quote": quote,
                    "context_before": self._truncate_text(item.get("context_before"), 220),
                    "context_after": self._truncate_text(item.get("context_after"), 220),
                    "sender_name": self._truncate_text(item.get("sender_name"), 60),
                    "sent_at": self._truncate_text(item.get("sent_at"), 60),
                    "message_id": item.get("message_id"),
                }
            )
        return {
            "facet_key": facet.facet_key,
            "label": str(findings.get("label") or facet.facet_key),
            "status": str(facet.status or ""),
            "confidence": round(float(facet.confidence or 0.0), 3),
            "summary": self._truncate_text(findings.get("summary"), SYNTHESIS_SUMMARY_LIMIT),
            "bullets": [
                self._truncate_text(item, SYNTHESIS_BULLET_TEXT_LIMIT)
                for item in (findings.get("bullets") or [])[:SYNTHESIS_BULLET_LIMIT]
                if str(item or "").strip()
            ],
            "conflicts": [
                {
                    "title": self._truncate_text(item.get("title"), SYNTHESIS_CONFLICT_TITLE_LIMIT),
                    "detail": self._truncate_text(item.get("detail"), SYNTHESIS_CONFLICT_DETAIL_LIMIT),
                }
                for item in conflicts[:SYNTHESIS_CONFLICT_LIMIT]
                if isinstance(item, dict)
            ],
            "fewshots": fewshots,
        }

    def _build_search_context(self, chunks: list[Any]) -> str:
        lines = ["Retrieved evidence corpus:"]
        for index, chunk in enumerate(chunks[:SYNTHESIS_SEARCH_RESULT_LIMIT], start=1):
            content = self._truncate_text(getattr(chunk, "content", ""), SYNTHESIS_SEARCH_CHUNK_LIMIT)
            if not content:
                continue
            source = getattr(chunk, "document_title", None) or getattr(chunk, "filename", None) or "source"
            chunk_id = str(getattr(chunk, "chunk_id", "") or "").strip()
            lines.append(f"{index}. [{source}]")
            if chunk_id:
                lines.append(f"   chunk_id: {chunk_id}")
            lines.append(f"   excerpt: {content}")
        if len(lines) == 1:
            lines.append("1. No retrieved evidence is available; rely conservatively on the facet dump.")
        return "\n".join(lines)

    @staticmethod
    def _document_stream_callback(stream_callback: Any | None, document_key: str) -> Any | None:
        if not callable(stream_callback):
            return None

        def callback(chunk: str) -> None:
            if not chunk:
                return
            stream_callback({"document_key": document_key, "chunk": chunk})

        flush_remaining = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_remaining):
            setattr(callback, "_flush_remaining", flush_remaining)
        return callback

    @staticmethod
    def _flush_stream_callback(stream_callback: Any | None) -> None:
        flush_remaining = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_remaining):
            flush_remaining()

    def _build_retrieved_support_context(
        self,
        *,
        session: Any,
        retrieval_service: Any,
        embedding_config: ServiceConfig | None,
        project_id: str,
        query: str,
    ) -> str:
        chunks, _, _ = retrieval_service.search(
            session,
            project_id=project_id,
            query=query,
            embedding_config=embedding_config,
            limit=7,
        )
        return self._build_search_context(chunks)

    @staticmethod
    def _truncate_text(value: Any, limit: int) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        if limit <= 3:
            return text[:limit]
        return f"{text[: limit - 3]}..."

    def _build_skill_documents_with_llm(
        self,
        client: OpenAICompatibleClient,
        config: ServiceConfig,
        project: Project,
        facet_dump: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
        stream_callback: Any | None,
        progress_callback: Any | None,
        session: Any | None,
        retrieval_service: Any | None,
    ) -> dict[str, Any]:
        from app.analysis.prompts import build_memories_messages, build_personality_messages
        from app.storage import repository

        personality_markdown = ""
        memories_markdown = ""
        analysis_markdown = self._render_analysis_reference_markdown_v2(facet_dump)
        skill_context = ""
        embedding_config = repository.get_service_config(session, "embedding_service") if session else None

        if session and retrieval_service and project.mode == "telegram":
            telegram_context = self._build_telegram_skill_context(
                session=session,
                project=project,
                target_role=target_role,
                analysis_context=analysis_context,
            )
            skill_context = telegram_context
            personality_markdown = self._build_contextual_skill_document(
                client,
                config,
                project_name=project.name,
                facet_dump=facet_dump,
                context=telegram_context,
                document_key="personality",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "personality"),
            )
            memories_markdown = self._build_contextual_skill_document(
                client,
                config,
                project_name=project.name,
                facet_dump=facet_dump,
                context=telegram_context,
                document_key="memories",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "memories"),
            )
        elif session and retrieval_service and project.mode != "telegram":
            skill_context = self._build_retrieved_support_context(
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
                project_id=project.id,
                query=SKILL_SUPPORT_QUERIES["skill"],
            )
            personality_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query=SKILL_SUPPORT_QUERIES["personality"],
                document_key="personality",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "personality"),
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )
            memories_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query=SKILL_SUPPORT_QUERIES["memories"],
                document_key="memories",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "memories"),
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )

        self._emit_progress(
            progress_callback,
            phase="synthesis",
            progress_percent=52,
            message="LLM is generating Skill.md",
            document_key="skill",
        )
        messages = build_asset_messages(
            "skill",
            project.name,
            facet_dump,
            evidence_context=skill_context,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        skill_stream_callback = self._document_stream_callback(stream_callback, "skill")
        response = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.2,
            max_tokens=None,
            stream_handler=skill_stream_callback,
        )
        self._flush_stream_callback(skill_stream_callback)

        self._emit_progress(
            progress_callback,
            phase="merge",
            progress_percent=78,
            message="Merging Skill_merge.md",
            document_key="merge",
        )
        return self._normalize_skill_payload(
            {
                "target_role": target_role or project.name,
                "source_context": analysis_context or "",
                "skill_markdown": response.content,
                "personality_markdown": personality_markdown,
                "memories_markdown": memories_markdown,
            },
            project.name,
            target_role=target_role,
            analysis_context=analysis_context,
        )

    def _build_cc_skill_documents_with_llm(
        self,
        client: OpenAICompatibleClient,
        config: ServiceConfig,
        project: Project,
        facet_dump: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
        stream_callback: Any | None,
        progress_callback: Any | None,
        session: Any | None,
        retrieval_service: Any | None,
    ) -> dict[str, Any]:
        from app.analysis.prompts import build_memories_messages, build_personality_messages
        from app.storage import repository

        personality_markdown = ""
        memories_markdown = ""
        analysis_markdown = self._render_analysis_reference_markdown_v2(facet_dump)
        skill_context = ""
        embedding_config = repository.get_service_config(session, "embedding_service") if session else None

        if session and retrieval_service and project.mode == "telegram":
            telegram_context = self._build_telegram_skill_context(
                session=session,
                project=project,
                target_role=target_role,
                analysis_context=analysis_context,
            )
            skill_context = telegram_context
            personality_markdown = self._build_contextual_skill_document(
                client,
                config,
                project_name=project.name,
                facet_dump=facet_dump,
                context=telegram_context,
                document_key="personality",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "personality"),
            )
            memories_markdown = self._build_contextual_skill_document(
                client,
                config,
                project_name=project.name,
                facet_dump=facet_dump,
                context=telegram_context,
                document_key="memories",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "memories"),
            )
        elif session and retrieval_service and project.mode != "telegram":
            skill_context = self._build_retrieved_support_context(
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
                project_id=project.id,
                query=SKILL_SUPPORT_QUERIES["skill"],
            )
            personality_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query=SKILL_SUPPORT_QUERIES["personality"],
                document_key="personality",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "personality"),
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )
            memories_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query=SKILL_SUPPORT_QUERIES["memories"],
                document_key="memories",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                stream_callback=self._document_stream_callback(stream_callback, "memories"),
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )

        self._emit_progress(
            progress_callback,
            phase="analysis_reference",
            progress_percent=44,
            message="Building analysis.md",
            document_key="analysis",
        )
        analysis_stream_callback = self._document_stream_callback(stream_callback, "analysis")
        if callable(analysis_stream_callback):
            analysis_stream_callback(analysis_markdown)
            self._flush_stream_callback(analysis_stream_callback)

        self._emit_progress(
            progress_callback,
            phase="synthesis",
            progress_percent=52,
            message="LLM is generating SKILL.md",
            document_key="skill",
        )
        messages = build_cc_skill_messages(
            project.id,
            project.name,
            facet_dump,
            evidence_context=skill_context,
            personality_markdown=personality_markdown,
            memories_markdown=memories_markdown,
            analysis_markdown=analysis_markdown,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        skill_stream_callback = self._document_stream_callback(stream_callback, "skill")
        response = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.2,
            max_tokens=None,
            stream_handler=skill_stream_callback,
        )
        self._flush_stream_callback(skill_stream_callback)

        return self._normalize_cc_skill_payload(
            {
                "target_role": target_role or project.name,
                "source_context": analysis_context or "",
                "skill_markdown": response.content,
                "personality_markdown": personality_markdown,
                "memories_markdown": memories_markdown,
                "analysis_markdown": analysis_markdown,
            },
            project_name=project.name,
            project_id=project.id,
            target_role=target_role,
            analysis_context=analysis_context,
        )

    def _build_retrieved_skill_document(
        self,
        client: OpenAICompatibleClient,
        config: ServiceConfig,
        project_id: str,
        project_name: str,
        facet_dump: str,
        *,
        query: str,
        document_key: str,
        phase: str,
        progress_percent: int,
        progress_message: str,
        message_builder: Any,
        target_role: str | None,
        analysis_context: str | None,
        progress_callback: Any | None,
        stream_callback: Any | None,
        session: Any,
        retrieval_service: Any,
        embedding_config: ServiceConfig | None,
    ) -> str:
        self._emit_progress(
            progress_callback,
            phase=phase,
            progress_percent=progress_percent,
            message=progress_message,
            document_key=document_key,
        )
        chunks, _, _ = retrieval_service.search(
            session,
            project_id=project_id,
            query=query,
            embedding_config=embedding_config,
            limit=5,
        )
        context = self._build_search_context(chunks)
        messages = message_builder(
            project_name,
            facet_dump,
            context,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        response = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.2,
            max_tokens=None,
            stream_handler=stream_callback,
        )
        self._flush_stream_callback(stream_callback)
        return str(response.content or "").strip()

    def _build_contextual_skill_document(
        self,
        client: OpenAICompatibleClient,
        config: ServiceConfig,
        *,
        project_name: str,
        facet_dump: str,
        context: str,
        document_key: str,
        phase: str,
        progress_percent: int,
        progress_message: str,
        message_builder: Any,
        target_role: str | None,
        analysis_context: str | None,
        progress_callback: Any | None,
        stream_callback: Any | None,
    ) -> str:
        self._emit_progress(
            progress_callback,
            phase=phase,
            progress_percent=progress_percent,
            message=progress_message,
            document_key=document_key,
        )
        messages = message_builder(
            project_name,
            facet_dump,
            context,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        response = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.2,
            max_tokens=None,
            stream_handler=stream_callback,
        )
        self._flush_stream_callback(stream_callback)
        return str(response.content or "").strip()

    def _build_telegram_skill_context(
        self,
        *,
        session: Any,
        project: Project,
        target_role: str | None,
        analysis_context: str | None,
    ) -> str:
        from app.storage import repository

        latest_run = repository.get_latest_analysis_run(session, project.id)
        summary = dict(latest_run.summary_json or {}) if latest_run else {}
        target_user = summary.get("target_user") if isinstance(summary.get("target_user"), dict) else {}
        participant_id = str(target_user.get("participant_id") or summary.get("participant_id") or "").strip()
        source_project_id = repository.get_target_project_id(session, project.id)
        preprocess_run_id = str(summary.get("preprocess_run_id") or "").strip()
        if preprocess_run_id:
            preprocess_run = repository.get_telegram_preprocess_run(session, preprocess_run_id)
        else:
            preprocess_run = repository.get_latest_successful_telegram_preprocess_run(session, source_project_id)
            preprocess_run_id = preprocess_run.id if preprocess_run else ""

        topics = (
            repository.list_telegram_preprocess_topics(session, source_project_id, run_id=preprocess_run_id)
            if preprocess_run_id
            else []
        )
        relevant_topics = []
        for topic in topics:
            participants = list(getattr(topic, "participants", None) or [])
            if participant_id and any(link.participant_id == participant_id for link in participants):
                relevant_topics.append(topic)
        if not relevant_topics:
            relevant_topics = list(topics)[:8]

        active_users = (
            repository.list_telegram_preprocess_active_users(session, source_project_id, run_id=preprocess_run_id)
            if preprocess_run_id
            else []
        )
        matched_active_user = next(
            (item for item in active_users if participant_id and item.participant_id == participant_id),
            None,
        )

        label = (
            target_user.get("label")
            or target_user.get("primary_alias")
            or target_user.get("display_name")
            or target_role
            or project.name
        )
        lines = [
            "Telegram evidence workbook:",
            f"- target_label: {label}",
            f"- participant_id: {participant_id or 'unknown'}",
            f"- preprocess_run_id: {preprocess_run_id or 'unknown'}",
            f"- source_project_id: {source_project_id}",
            f"- analysis_context: {analysis_context or ''}",
            "",
            "Grounding order:",
            "1. Read the weekly topic summary first.",
            "2. Use participant viewpoints to infer stable stance, pressure, and role position.",
            "3. Use short evidence quotes to recover wording, tone, and concrete scenes.",
            "4. Only then abstract into personality, memories, or skill rules.",
        ]
        if matched_active_user:
            aliases = ", ".join(
                str(item).strip()
                for item in (matched_active_user.aliases_json or [])
                if str(item).strip()
            )
            lines.extend(
                [
                    "",
                    "Participant profile:",
                    f"- primary_alias: {matched_active_user.primary_alias or label}",
                    f"- username: {matched_active_user.username or 'unknown'}",
                    f"- message_count: {int(matched_active_user.message_count or 0)}",
                    f"- aliases: {aliases or 'n/a'}",
                ]
            )
            for evidence in list(matched_active_user.evidence_json or [])[:3]:
                if not isinstance(evidence, dict):
                    continue
                quote = str(evidence.get("quote") or evidence.get("text") or "").strip()
                if quote:
                    lines.append(f"- user_evidence: {quote}")
        lines.extend(["", "Relevant weekly topics:"])
        if not relevant_topics:
            lines.append("- No weekly topics were available; rely on the facet dump and analysis context.")
        for index, topic in enumerate(relevant_topics[:8], start=1):
            metadata = dict(getattr(topic, "metadata_json", None) or {})
            keywords = ", ".join(
                str(item).strip()
                for item in (getattr(topic, "keywords_json", None) or [])
                if str(item).strip()
            )
            subtopics = ", ".join(
                str(item).strip()
                for item in (metadata.get("subtopics") or [])
                if str(item).strip()
            )
            interaction_patterns = ", ".join(
                str(item).strip()
                for item in (metadata.get("interaction_patterns") or [])
                if str(item).strip()
            )
            lines.extend(
                [
                    f"Topic {index}: {getattr(topic, 'title', '') or 'Untitled topic'}",
                    f"- summary: {str(getattr(topic, 'summary', '') or '').strip()}",
                    f"- keywords: {keywords or 'n/a'}",
                    f"- subtopics: {subtopics or 'n/a'}",
                    f"- interaction_patterns: {interaction_patterns or 'n/a'}",
                ]
            )
            participant_viewpoints = [
                dict(item)
                for item in (metadata.get("participant_viewpoints") or [])
                if isinstance(item, dict)
            ]
            matched_viewpoints = [
                item
                for item in participant_viewpoints
                if not participant_id or str(item.get("participant_id") or "").strip() == participant_id
            ] or participant_viewpoints[:3]
            for viewpoint in matched_viewpoints[:3]:
                stance = str(viewpoint.get("stance_summary") or "").strip()
                evidence = str(viewpoint.get("evidence") or viewpoint.get("supporting_detail") or "").strip()
                speaker = str(
                    viewpoint.get("display_name")
                    or viewpoint.get("participant_label")
                    or viewpoint.get("participant_id")
                    or "participant"
                ).strip()
                if stance:
                    lines.append(f"- viewpoint[{speaker}]: {stance}")
                if evidence:
                    lines.append(f"- viewpoint_evidence[{speaker}]: {evidence}")
            for evidence_item in list(getattr(topic, "evidence_json", None) or [])[:3]:
                if not isinstance(evidence_item, dict):
                    continue
                quote = str(evidence_item.get("quote") or "").strip()
                if not quote:
                    continue
                reason = str(evidence_item.get("reason") or evidence_item.get("label") or "").strip()
                if reason:
                    lines.append(f"- evidence: {quote} | note: {reason}")
                else:
                    lines.append(f"- evidence: {quote}")
        return "\n".join(lines).strip()

    def _load_stone_profiles(self, session: Any | None, project_id: str) -> list[dict[str, Any]]:
        if session is None:
            return []
        from app.storage import repository

        profiles: list[dict[str, Any]] = []
        for document in repository.list_project_documents(session, project_id):
            metadata = dict(document.metadata_json or {})
            profile = metadata.get("stone_profile_v2")
            if not isinstance(profile, dict):
                continue
            expanded = expand_stone_profile_v2_for_analysis(
                profile,
                article_text=str(document.clean_text or document.raw_text or ""),
                title=document.title or document.filename,
            )
            profiles.append(
                {
                    "document_id": document.id,
                    "title": document.title or document.filename,
                    **expanded,
                }
            )
        return profiles

    def _build_writing_guide_with_llm(
        self,
        client: OpenAICompatibleClient,
        config: ServiceConfig,
        project: Project,
        facet_dump: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
        stream_callback: Any | None,
        progress_callback: Any | None,
        session: Any | None,
    ) -> dict[str, Any]:
        stone_profiles = self._load_stone_profiles(session, project.id)
        self._emit_progress(
            progress_callback,
            phase="synthesis",
            progress_percent=52,
            message="LLM is generating the writing guide.",
            document_key="guide",
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "You are building a structured writing guide from author analysis.\n"
                    "Return only JSON.\n"
                    "Required keys: author_snapshot, voice_dna, sentence_mechanics, structure_patterns, motif_theme_bank, "
                    "worldview_and_stance, emotional_tendencies, nonclinical_psychodynamics, do_and_dont, "
                    "topic_translation_rules, word_count_strategies, revision_rubric, fewshot_anchors, external_slots.\n"
                    "external_slots must contain clinical_profile and vulnerability_map with concrete evidence-based content.\n"
                    "Prefer arrays or nested objects over long monolithic paragraphs."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project.name}\n"
                    f"Target role: {target_role or project.name}\n"
                    f"Analysis context: {analysis_context or ''}\n\n"
                    f"Facet dump:\n{facet_dump}\n\n"
                    f"Per-document profiles:\n{json.dumps(stone_profiles, ensure_ascii=False, indent=2)}"
                ),
            },
        ]
        response = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.2,
            max_tokens=None,
            stream_handler=stream_callback,
        )
        self._flush_stream_callback(stream_callback)
        parsed = parse_json_response(response.content, fallback=True)
        return self._normalize_writing_guide_payload(
            parsed,
            project.name,
            target_role=target_role,
            analysis_context=analysis_context,
            stone_profiles=stone_profiles,
        )

    def _get_skill_merge_markdown(self, payload: dict[str, Any]) -> str:
        documents = payload.get("documents") if isinstance(payload, dict) else {}
        if isinstance(documents, dict):
            merge_doc = documents.get("merge") or {}
            if isinstance(merge_doc, dict):
                return str(merge_doc.get("markdown") or "").strip()
        return ""

    def _get_cc_skill_markdown(self, payload: dict[str, Any]) -> str:
        documents = payload.get("documents") if isinstance(payload, dict) else {}
        if isinstance(documents, dict):
            skill_doc = documents.get("skill") or {}
            if isinstance(skill_doc, dict):
                return str(skill_doc.get("markdown") or "").strip()
        return ""

    def _merge_skill_documents(self, *documents: str) -> str:
        parts = [str(item or "").strip() for item in documents if str(item or "").strip()]
        return "\n\n".join(parts).strip()

    def _render_personality_document(self, *, core_identity: str, mental_state: str) -> str:
        lines = [
            "# 核心身份与精神底色",
            "",
            "## 核心身份",
            core_identity.strip() or "资料不足时，优先维持已知身份边界，不额外脑补。",
            "",
            "## 精神底色",
            mental_state.strip() or "资料不足时，保持克制、保守，不过度延展。",
        ]
        return "\n".join(lines).strip()

    def _render_memories_document(self, memories: list[str], *, fallback_summary: str = "") -> str:
        lines = [
            "# 核心记忆与经历",
            "",
            "## 关键记忆",
        ]
        if memories:
            lines.extend(f"- {item}" for item in memories if str(item).strip())
        else:
            lines.append("- 资料不足，暂不扩写未经证实的具体经历。")
        lines.extend(
            [
                "",
                "## 长期经历脉络",
                fallback_summary.strip() or "现有材料更适合保守引用片段化记忆，不扩写完整人生叙事。",
            ]
        )
        return "\n".join(lines).strip()

    def _heuristic(
        self,
        asset_kind: str,
        project: Project,
        facets: list[AnalysisFacet],
        *,
        target_role: str | None,
        analysis_context: str | None,
        session: Any | None = None,
    ) -> dict[str, Any]:
        summary_by_key = {facet.facet_key: (facet.findings_json or {}) for facet in facets}
        evidence_by_key = {facet.facet_key: (facet.evidence_json or []) for facet in facets}
        conflict_notes = [conflict for facet in facets for conflict in (facet.conflicts_json or [])]

        if asset_kind == "skill":
            skill_payload = _build_skill_payload_from_facets(
                project_name=project.name,
                target_role=target_role or project.name,
                analysis_context=analysis_context or "",
                summary_by_key=summary_by_key,
                evidence_by_key=evidence_by_key,
                conflict_notes=conflict_notes,
            )
            return self._normalize_skill_payload(
                {
                    "target_role": target_role or project.name,
                    "source_context": analysis_context or "",
                    "skill_markdown": self._render_skill_markdown(project.name, skill_payload),
                    "personality_markdown": self._render_personality_document(
                        core_identity=str(skill_payload["core_identity"]),
                        mental_state=str(skill_payload["mental_state"]),
                    ),
                    "memories_markdown": self._render_memories_document(
                        [str(item) for item in skill_payload["memories"]],
                        fallback_summary=str(summary_by_key.get("life_timeline", {}).get("summary", "")),
                    ),
                },
                project.name,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        if asset_kind == "cc_skill":
            skill_payload = _build_skill_payload_from_facets(
                project_name=project.name,
                target_role=target_role or project.name,
                analysis_context=analysis_context or "",
                summary_by_key=summary_by_key,
                evidence_by_key=evidence_by_key,
                conflict_notes=conflict_notes,
            )
            return self._normalize_cc_skill_payload(
                {
                    "target_role": target_role or project.name,
                    "source_context": analysis_context or "",
                    "skill_markdown": self._render_skill_markdown(project.name, skill_payload),
                    "personality_markdown": self._render_personality_document(
                        core_identity=str(skill_payload["core_identity"]),
                        mental_state=str(skill_payload["mental_state"]),
                    ),
                    "memories_markdown": self._render_memories_document(
                        [str(item) for item in skill_payload["memories"]],
                        fallback_summary=str(summary_by_key.get("life_timeline", {}).get("summary", "")),
                    ),
                    "analysis_markdown": self._render_analysis_reference_markdown_v2(self._build_facet_dump(facets)),
                },
                project_name=project.name,
                project_id=project.id,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        if asset_kind == "writing_guide":
            return self._normalize_writing_guide_payload(
                _build_writing_guide_payload_from_facets(
                    project_name=project.name,
                    target_role=target_role or project.name,
                    analysis_context=analysis_context or "",
                    summary_by_key=summary_by_key,
                    evidence_by_key=evidence_by_key,
                    stone_profiles=self._load_stone_profiles(session, project.id) if session else [],
                ),
                project.name,
                target_role=target_role,
                analysis_context=analysis_context,
                stone_profiles=self._load_stone_profiles(session, project.id) if session else [],
            )
        return _build_profile_report_payload_from_facets(
            project_name=project.name,
            target_role=target_role or project.name,
            analysis_context=analysis_context or "",
            summary_by_key=summary_by_key,
            evidence_by_key=evidence_by_key,
            conflict_notes=conflict_notes,
        )

    def _normalize_skill_payload(
        self,
        payload: dict[str, Any],
        project_name: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        resolved_target_role = str(payload.get("target_role", target_role or project_name))
        resolved_source_context = str(payload.get("source_context", analysis_context or ""))
        skill_markdown = str(payload.get("skill_markdown", "") or "").strip()
        personality_markdown = str(payload.get("personality_markdown", "") or "").strip()
        memories_markdown = str(payload.get("memories_markdown", "") or "").strip()
        merge_markdown = self._merge_skill_documents(skill_markdown, personality_markdown, memories_markdown)
        return {
            "target_role": resolved_target_role,
            "source_context": resolved_source_context,
            "documents": {
                "skill": {
                    "filename": SKILL_DOCUMENT_FILENAMES["skill"],
                    "markdown": skill_markdown,
                },
                "personality": {
                    "filename": SKILL_DOCUMENT_FILENAMES["personality"],
                    "markdown": personality_markdown,
                },
                "memories": {
                    "filename": SKILL_DOCUMENT_FILENAMES["memories"],
                    "markdown": memories_markdown,
                },
                "merge": {
                    "filename": SKILL_DOCUMENT_FILENAMES["merge"],
                    "markdown": merge_markdown,
                },
            },
        }

    @staticmethod
    def _slugify_kebab(value: str) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-{2,}", "-", text).strip("-")
        return text

    def _build_cc_skill_name(self, *, project_id: str, target_role: str, project_name: str) -> str:
        fallback = f"roleplay-{str(project_id or '')[:8] or 'unknown'}"
        candidate = self._slugify_kebab(target_role) or self._slugify_kebab(project_name) or fallback
        reserved = ("claude", "anthropic")
        if len(candidate) > 64:
            candidate = fallback
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", candidate or ""):
            candidate = fallback
        if any(word in candidate for word in reserved):
            candidate = fallback
        candidate = (candidate or fallback)[:64].strip("-") or fallback
        if any(word in candidate for word in reserved):
            candidate = fallback
        return candidate

    @staticmethod
    def _wrap_cc_skill_frontmatter(*, name: str, description: str, body: str) -> str:
        safe_description = str(description or "").strip().replace("\n", " ")
        safe_body = str(body or "").strip()
        return "\n".join(
            [
                "---",
                f"name: {name}",
                f"description: {safe_description}",
                "---",
                "",
                safe_body,
            ]
        ).strip()

    def _render_analysis_reference_markdown_v2(self, facet_dump: str) -> str:
        facets = json.loads(facet_dump or "[]")
        if not isinstance(facets, list):
            return "# 十维分析摘要\n\n当前没有可用的十维分析文本。"
        lines = [
            "# 十维分析摘要",
            "",
            "以下内容汇总自当前十维分析结果，供 SKILL.md 在写规则、few-shot 和边界时引用。",
            "",
        ]
        for facet in facets:
            if not isinstance(facet, dict):
                continue
            label = str(facet.get("label") or facet.get("facet_key") or "Facet").strip()
            summary = str(facet.get("summary") or "").strip()
            bullets = [str(item).strip() for item in (facet.get("bullets") or []) if str(item).strip()]
            fewshots = [item for item in (facet.get("fewshots") or []) if isinstance(item, dict)]
            lines.append(f"## {label}")
            if summary:
                lines.extend([summary, ""])
            if bullets:
                lines.extend(f"- {item}" for item in bullets[:6])
                lines.append("")
            if fewshots:
                lines.append("### Few-Shot 片段")
                for index, item in enumerate(fewshots[:3], start=1):
                    situation = str(item.get("situation") or "").strip()
                    expression = str(item.get("expression") or "").strip()
                    quote = str(item.get("quote") or "").strip()
                    context_before = str(item.get("context_before") or "").strip()
                    context_after = str(item.get("context_after") or "").strip()
                    sender_name = str(item.get("sender_name") or "").strip()
                    sent_at = str(item.get("sent_at") or "").strip()
                    message_id = str(item.get("message_id") or "").strip()
                    lines.append(f"{index}. 情境：{situation or '未标注'}")
                    if context_before:
                        lines.append(f"   上文：{context_before}")
                    if expression:
                        lines.append(f"   目标用户的表达方式：{expression}")
                    if quote:
                        lines.append(f"   目标用户原话：{quote}")
                    if context_after:
                        lines.append(f"   下文：{context_after}")
                    meta_parts = []
                    if sender_name:
                        meta_parts.append(f"发送者：{sender_name}")
                    if sent_at:
                        meta_parts.append(f"时间：{sent_at}")
                    if message_id:
                        meta_parts.append(f"message_id：{message_id}")
                    if meta_parts:
                        lines.append(f"   标注：{'；'.join(meta_parts)}")
                    lines.append("")
        return "\n".join(lines).strip() or "# 十维分析摘要\n\n当前没有可用的十维分析文本。"

    def _render_analysis_reference_markdown(self, facet_dump: str) -> str:
        facets = json.loads(facet_dump or "[]")
        if not isinstance(facets, list):
            return "# 十维分析摘要\n\n当前没有可用的十维分析文本。"
        lines = [
            "# 十维分析摘要",
            "",
            "以下内容汇总自当前十维分析结果，供 SKILL.md 在写规则、few-shot 和边界时引用。",
            "",
        ]
        for facet in facets:
            if not isinstance(facet, dict):
                continue
            label = str(facet.get("label") or facet.get("facet_key") or "Facet").strip()
            summary = str(facet.get("summary") or "").strip()
            bullets = [str(item).strip() for item in (facet.get("bullets") or []) if str(item).strip()]
            fewshots = [item for item in (facet.get("fewshots") or []) if isinstance(item, dict)]
            lines.append(f"## {label}")
            if summary:
                lines.extend([summary, ""])
            if bullets:
                lines.extend(f"- {item}" for item in bullets[:6])
                lines.append("")
            if fewshots:
                lines.append("### Few-Shot 片段")
                for index, item in enumerate(fewshots[:3], start=1):
                    situation = str(item.get("situation") or "").strip()
                    expression = str(item.get("expression") or "").strip()
                    quote = str(item.get("quote") or "").strip()
                    lines.append(f"{index}. 情境：{situation or '未标注'}")
                    if expression:
                        lines.append(f"   表达方式：{expression}")
                    if quote:
                        lines.append(f"   原话：{quote}")
                lines.append("")
        return "\n".join(lines).strip() or "# 十维分析摘要\n\n当前没有可用的十维分析文本。"

    def _normalize_cc_skill_payload(
        self,
        payload: dict[str, Any],
        *,
        project_name: str,
        project_id: str,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        resolved_target_role = str(payload.get("target_role", target_role or project_name))
        resolved_source_context = str(payload.get("source_context", analysis_context or ""))
        personality_markdown = str(payload.get("personality_markdown", "") or "").strip()
        memories_markdown = str(payload.get("memories_markdown", "") or "").strip()
        analysis_markdown = str(payload.get("analysis_markdown", "") or "").strip()
        raw_skill_markdown = str(payload.get("skill_markdown", "") or "").strip()

        expected_name = self._build_cc_skill_name(
            project_id=project_id,
            target_role=resolved_target_role,
            project_name=project_name,
        )
        expected_description = f"当需要以 {resolved_target_role} 的语气、立场与规则进行输出时使用。".strip()

        text = raw_skill_markdown.lstrip()
        frontmatter_body = ""
        frontmatter_name = ""
        frontmatter_description = ""
        if text.startswith("---"):
            lines = text.splitlines()
            if lines and lines[0].strip() == "---":
                end_index = None
                for i in range(1, len(lines)):
                    if lines[i].strip() == "---":
                        end_index = i
                        break
                if end_index is not None:
                    for line in lines[1:end_index]:
                        if line.startswith("name:"):
                            frontmatter_name = line.split(":", 1)[1].strip()
                        elif line.startswith("description:"):
                            frontmatter_description = line.split(":", 1)[1].strip()
                    frontmatter_body = "\n".join(lines[end_index + 1 :]).strip()

        resolved_body = frontmatter_body or raw_skill_markdown
        if "references/personality.md" not in resolved_body:
            resolved_body = f"{resolved_body.strip()}\n\n更多人格底色见 references/personality.md。".strip()
        if "references/memories.md" not in resolved_body:
            resolved_body = f"{resolved_body.strip()}\n\n更多记忆与经历见 references/memories.md。".strip()
        if "references/analysis.md" not in resolved_body:
            resolved_body = f"{resolved_body.strip()}\n\n更多十维分析摘要见 references/analysis.md。".strip()

        name_candidate = frontmatter_name.strip() if frontmatter_name else expected_name
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name_candidate or ""):
            name_candidate = expected_name
        if len(name_candidate) > 64:
            name_candidate = expected_name
        if any(word in name_candidate for word in ("claude", "anthropic")):
            name_candidate = expected_name

        description_candidate = frontmatter_description.strip() if frontmatter_description else expected_description
        if not description_candidate:
            description_candidate = expected_description

        skill_markdown = self._wrap_cc_skill_frontmatter(
            name=name_candidate,
            description=description_candidate,
            body=resolved_body,
        )

        return {
            "target_role": resolved_target_role,
            "source_context": resolved_source_context,
            "documents": {
                "skill": {
                    "filename": CC_SKILL_DOCUMENT_FILENAMES["skill"],
                    "markdown": skill_markdown,
                },
                "personality": {
                    "filename": CC_SKILL_DOCUMENT_FILENAMES["personality"],
                    "markdown": personality_markdown,
                },
                "memories": {
                    "filename": CC_SKILL_DOCUMENT_FILENAMES["memories"],
                    "markdown": memories_markdown,
                },
                "analysis": {
                    "filename": CC_SKILL_DOCUMENT_FILENAMES["analysis"],
                    "markdown": analysis_markdown,
                },
            },
            "analysis_markdown": analysis_markdown,
        }

    def _normalize_profile_report_payload(
        self,
        payload: dict[str, Any],
        project_name: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        return {
            "headline": str(payload.get("headline") or f"{project_name} 用户画像报告"),
            "executive_summary": str(payload.get("executive_summary") or payload.get("summary") or ""),
            "core_identity_and_drives": str(
                payload.get("core_identity_and_drives")
                or payload.get("psychological_profile")
                or payload.get("reality_anchor")
                or ""
            ),
            "emotional_baseline": str(payload.get("emotional_baseline") or payload.get("reality_anchor") or ""),
            "attachment_and_boundaries": str(
                payload.get("attachment_and_boundaries")
                or payload.get("interpersonal_mechanics")
                or ""
            ),
            "defense_and_coping": str(
                payload.get("defense_and_coping")
                or payload.get("psychological_profile")
                or ""
            ),
            "social_role_and_relationships": str(
                payload.get("social_role_and_relationships")
                or payload.get("social_dynamics")
                or payload.get("subculture_refuge")
                or ""
            ),
            "four_type_personality": str(payload.get("four_type_personality") or ""),
            "stress_response_and_risk": str(
                payload.get("stress_response_and_risk")
                or payload.get("core_values_and_triggers")
                or ""
            ),
            "linguistic_markers": str(payload.get("linguistic_markers") or payload.get("linguistic_signature") or ""),
            "contradictions": [str(item) for item in payload.get("contradictions", [])[:8]],
            "growth_and_prediction": str(payload.get("growth_and_prediction") or payload.get("observer_conclusion") or ""),
            "observer_conclusion": str(
                payload.get("observer_conclusion")
                or payload.get("growth_and_prediction")
                or ""
            ),
            "target_role": str(payload.get("target_role", target_role or project_name)),
            "source_context": str(payload.get("source_context", analysis_context or "")),
        }

    def _normalize_writing_guide_payload(
        self,
        payload: dict[str, Any],
        project_name: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
        stone_profiles: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "author_snapshot": _string_block(
                payload.get("author_snapshot"),
                fallback=f"Target author: {target_role or project_name}. Preserve the corpus pressure, worldview, and sentence habits.",
            ),
            "voice_dna": _normalize_guide_object(
                payload.get("voice_dna"),
                defaults={
                    "tone_profile": "cool, restrained, specific",
                    "signature_phrases": _guide_profile_terms(stone_profiles, "lexical_markers", 8),
                    "distance_rules": ["Prefer intimate observation over public proclamation.", "Keep certainty below total closure."],
                },
            ),
            "sentence_mechanics": _normalize_guide_object(
                payload.get("sentence_mechanics"),
                defaults={
                    "cadence": ["short setup, then a turn", "let pressure arrive before explanation"],
                    "transitions": ["but", "still", "so", "therefore"],
                    "closure_style": "close on residue, not on slogans",
                },
            ),
            "structure_patterns": _normalize_string_list(
                payload.get("structure_patterns"),
                fallback=_guide_profile_terms(stone_profiles, "structure_template", 4),
            ),
            "motif_theme_bank": _normalize_string_list(
                payload.get("motif_theme_bank"),
                fallback=_guide_facet_material(payload, "motif_theme_bank"),
            ),
            "worldview_and_stance": _normalize_string_list(
                payload.get("worldview_and_stance"),
                fallback=_guide_facet_material(payload, "worldview_and_stance"),
            ),
            "emotional_tendencies": _normalize_string_list(
                payload.get("emotional_tendencies"),
                fallback=_guide_profile_terms(stone_profiles, "emotional_progression", 4),
            ),
            "nonclinical_psychodynamics": _normalize_string_list(
                payload.get("nonclinical_psychodynamics"),
                fallback=_guide_profile_terms(stone_profiles, "nonclinical_signals", 6),
            ),
            "do_and_dont": _normalize_guide_object(
                payload.get("do_and_dont"),
                defaults={
                    "do": ["keep tonal restraint", "translate topics through recurring motifs", "preserve latent pressure"],
                    "dont": ["do not drift into generic self-help tone", "do not invent clinical diagnosis", "do not overwrite ambiguity with slogans"],
                },
            ),
            "topic_translation_rules": _normalize_string_list(
                payload.get("topic_translation_rules"),
                fallback=[
                    "Map the topic onto recurring reality anchors before drafting.",
                    "Translate abstract claims into scenes, costs, or relationship pressure.",
                ],
            ),
            "word_count_strategies": _normalize_guide_object(
                payload.get("word_count_strategies"),
                defaults={
                    "short": "Open from one image and close before full explanation.",
                    "medium": "Use 3 to 4 paragraphs with one central turn.",
                    "long": "Layer motif, worldview, and aftertaste over 4 to 6 paragraphs.",
                },
            ),
            "revision_rubric": _normalize_string_list(
                payload.get("revision_rubric"),
                fallback=[
                    "Check voice markers before checking idea completeness.",
                    "Cut generic transitions first.",
                    "Verify the topic is translated through worldview, not pasted on top.",
                ],
            ),
            "fewshot_anchors": _normalize_fewshot_anchors(payload.get("fewshot_anchors"), stone_profiles),
            "external_slots": _normalize_external_slots(
                payload.get("external_slots"),
                defaults={
                    "clinical_profile": {
                        "mental_state": _string_block(
                            payload.get("author_snapshot"),
                            fallback=f"Target author: {target_role or project_name}.",
                        ),
                        "candidate_diagnoses": _guide_profile_terms(stone_profiles, "nonclinical_signals", 6),
                        "defense_mechanisms": _guide_profile_terms(stone_profiles, "nonclinical_signals", 6),
                    },
                    "vulnerability_map": {
                        "pain_points": _guide_profile_terms(stone_profiles, "article_theme", 4),
                        "fragility_triggers": _guide_profile_terms(stone_profiles, "nonclinical_signals", 6),
                        "compensatory_moves": _guide_profile_terms(stone_profiles, "structure_template", 4),
                    },
                },
            ),
            "target_role": str(payload.get("target_role", target_role or project_name)),
            "source_context": str(payload.get("source_context", analysis_context or "")),
        }

    def _render_skill_markdown(self, project_name: str, payload: dict[str, Any]) -> str:
        lines = [
            f"# System Role: 扮演 {payload['target_role'] or project_name}",
            "",
            "## 角色扮演规则",
        ]
        lines.extend(f"- {item}" for item in payload["role_playing_rules"])
        lines.extend(["", "## 回答工作流"])
        lines.extend(f"- {item}" for item in payload["agentic_protocol"])
        lines.extend(["", "## 身份卡"])
        lines.extend(f"- {item}" for item in payload["identity_card"])
        lines.extend(["", "## 核心心智模型"])
        lines.extend(f"- {item}" for item in payload["core_mental_models"])
        lines.extend(["", "## 决策启发式"])
        lines.extend(f"- {item}" for item in payload["decision_heuristics"])
        lines.extend(["", "## 高置信领域"])
        lines.extend(f"- {item}" for item in payload["high_confidence_areas"])
        lines.extend(["", "## 表达 DNA"])
        lines.extend(f"- {item}" for item in payload["expression_dna"])
        lines.extend(["", "## 人物时间线"])
        lines.extend(f"- {item}" for item in payload["character_timeline"])
        lines.extend(["", "## 价值观与反模式"])
        lines.extend(f"- {item}" for item in payload["values_and_anti_patterns"])
        lines.extend(["", "## 智识谱系"])
        lines.extend(f"- {item}" for item in payload["intellectual_lineage"])
        lines.extend(["", "## 诚实边界"])
        lines.extend(f"- {item}" for item in payload["honesty_boundaries"])
        lines.extend(["", "## Few-Shot 切片"])
        if payload["few_shots"]:
            for item in payload["few_shots"]:
                lines.extend(
                    [
                        f"### {item['scene']}",
                        f"- Context: {item['context']}",
                        f"- Reply: {item['reply']}",
                        "",
                    ]
                )
        else:
            lines.extend(["- 暂无足够短引语，优先沿用已知语气与断句。", ""])
        lines.extend(["## 调研来源"])
        lines.extend(f"- {item}" for item in payload["research_sources"])
        if payload["source_context"]:
            lines.extend(["", "## 语料说明", payload["source_context"]])
        if payload["conflict_notes"]:
            lines.extend(["", "## 冲突备注"])
            lines.extend(f"- {_stringify_conflict(item)}" for item in payload["conflict_notes"])
        return "\n".join(line for line in lines if line is not None).strip()

    def _render_skill_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        role_rules = "\n".join(f"- {item}" for item in payload["role_playing_rules"])
        protocol = "\n".join(f"- {item}" for item in payload["agentic_protocol"])
        identity_card = "\n".join(f"- {item}" for item in payload["identity_card"])
        mental_models = "\n".join(f"- {item}" for item in payload["core_mental_models"])
        heuristics = "\n".join(f"- {item}" for item in payload["decision_heuristics"])
        high_confidence = "\n".join(f"- {item}" for item in payload["high_confidence_areas"])
        expression_dna = "\n".join(f"- {item}" for item in payload["expression_dna"])
        timeline = "\n".join(f"- {item}" for item in payload["character_timeline"])
        values = "\n".join(f"- {item}" for item in payload["values_and_anti_patterns"])
        lineage = "\n".join(f"- {item}" for item in payload["intellectual_lineage"])
        honesty = "\n".join(f"- {item}" for item in payload["honesty_boundaries"])
        research_sources = "\n".join(f"- {item}" for item in payload["research_sources"])
        few_shots = "\n".join(
            f"[{item['scene']}] {item['context']}\n{item['reply']}" for item in payload["few_shots"]
        )
        source_context = f"语料说明：{payload['source_context']}\n\n" if payload["source_context"] else ""
        return (
            f"你现在要稳定扮演 {payload['target_role'] or project_name}。\n\n"
            f"{source_context}"
            f"角色扮演规则：\n{role_rules}\n\n"
            f"回答工作流：\n{protocol}\n\n"
            f"身份卡：\n{identity_card}\n\n"
            f"核心心智模型：\n{mental_models}\n\n"
            f"决策启发式：\n{heuristics}\n\n"
            f"高置信领域：\n{high_confidence}\n\n"
            f"表达 DNA：\n{expression_dna}\n\n"
            f"人物时间线：\n{timeline}\n\n"
            f"价值观与反模式：\n{values}\n\n"
            f"智识谱系：\n{lineage}\n\n"
            f"诚实边界：\n{honesty}\n\n"
            f"真实语料切片：\n{few_shots}\n\n"
            f"调研来源：\n{research_sources}\n\n"
            "回答要求：保持角色一致，不要编造无法从语料支持的具体经历；能检索时先检索，不能检索时明确说边界。"
        )

    def _render_profile_report_markdown(self, project_name: str, payload: dict[str, Any]) -> str:
        sections = [
            f"# {payload.get('target_role') or project_name} 用户画像报告",
            "",
            "## 一、卷首判词",
            payload["headline"],
            "",
            "## 二、执行摘要",
            payload["executive_summary"],
            "",
            "## 三、核心驱力与身份叙事",
            payload["core_identity_and_drives"],
            "",
            "## 四、情绪底色与心理基线",
            payload["emotional_baseline"],
            "",
            "## 五、依恋方式与边界策略",
            payload["attachment_and_boundaries"],
            "",
            "## 六、防御机制与应对模式",
            payload["defense_and_coping"],
            "",
            "## 七、社会角色与关系动力",
            payload["social_role_and_relationships"],
            "",
            "## 八、四类型人格划分",
            payload["four_type_personality"],
            "",
            "## 九、压力响应与风险点",
            payload["stress_response_and_risk"],
            "",
            "## 十、语言与表达指纹",
            payload["linguistic_markers"],
            "",
            "## 十一、矛盾与裂缝",
            *[f"- {item}" for item in payload["contradictions"]],
            "",
            "## 十二、成长路径与走势预测",
            payload["growth_and_prediction"],
            "",
            "## 十三、观察者结论",
            payload["observer_conclusion"],
        ]
        if payload["source_context"]:
            sections.extend(["", "## 附录：语料说明", payload["source_context"]])
        return "\n".join(sections).strip()

    def _render_profile_report_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        contradictions = "\n".join(f"- {item}" for item in payload["contradictions"])
        source_context = f"语料说明：{payload['source_context']}\n\n" if payload["source_context"] else ""
        return (
            f"以下是 {payload.get('target_role') or project_name} 的用户画像报告摘要。\n\n"
            f"{source_context}"
            f"卷首判词：{payload['headline']}\n\n"
            f"执行摘要：{payload['executive_summary']}\n\n"
            f"核心驱力与身份叙事：{payload['core_identity_and_drives']}\n\n"
            f"情绪底色与心理基线：{payload['emotional_baseline']}\n\n"
            f"依恋方式与边界策略：{payload['attachment_and_boundaries']}\n\n"
            f"防御机制与应对模式：{payload['defense_and_coping']}\n\n"
            f"社会角色与关系动力：{payload['social_role_and_relationships']}\n\n"
            f"四类型人格划分：{payload['four_type_personality']}\n\n"
            f"压力响应与风险点：{payload['stress_response_and_risk']}\n\n"
            f"语言与表达指纹：{payload['linguistic_markers']}\n\n"
            f"主要矛盾：\n{contradictions}\n\n"
            f"成长路径与走势预测：{payload['growth_and_prediction']}\n\n"
            f"观察者结论：{payload['observer_conclusion']}"
        )

    def _render_writing_guide_markdown(self, project_name: str, payload: dict[str, Any]) -> str:
        lines = [
            f"# {payload.get('target_role') or project_name} Writing Guide",
            "",
            "## author_snapshot",
            str(payload.get("author_snapshot") or "").strip(),
            "",
            "## voice_dna",
            json.dumps(payload.get("voice_dna") or {}, ensure_ascii=False, indent=2),
            "",
            "## sentence_mechanics",
            json.dumps(payload.get("sentence_mechanics") or {}, ensure_ascii=False, indent=2),
            "",
            "## structure_patterns",
            *[f"- {item}" for item in (payload.get("structure_patterns") or [])],
            "",
            "## motif_theme_bank",
            *[f"- {item}" for item in (payload.get("motif_theme_bank") or [])],
            "",
            "## worldview_and_stance",
            *[f"- {item}" for item in (payload.get("worldview_and_stance") or [])],
            "",
            "## emotional_tendencies",
            *[f"- {item}" for item in (payload.get("emotional_tendencies") or [])],
            "",
            "## nonclinical_psychodynamics",
            *[f"- {item}" for item in (payload.get("nonclinical_psychodynamics") or [])],
            "",
            "## do_and_dont",
            json.dumps(payload.get("do_and_dont") or {}, ensure_ascii=False, indent=2),
            "",
            "## topic_translation_rules",
            *[f"- {item}" for item in (payload.get("topic_translation_rules") or [])],
            "",
            "## word_count_strategies",
            json.dumps(payload.get("word_count_strategies") or {}, ensure_ascii=False, indent=2),
            "",
            "## revision_rubric",
            *[f"- {item}" for item in (payload.get("revision_rubric") or [])],
            "",
            "## fewshot_anchors",
        ]
        for item in payload.get("fewshot_anchors") or []:
            if not isinstance(item, dict):
                continue
            lines.append(f"- {item.get('title') or 'anchor'}: {item.get('quote') or ''}")
        lines.extend(
            [
                "",
                "## external_slots",
                json.dumps(payload.get("external_slots") or {}, ensure_ascii=False, indent=2),
            ]
        )
        if payload.get("source_context"):
            lines.extend(["", "## source_context", str(payload["source_context"]).strip()])
        return "\n".join(line for line in lines if line is not None).strip()

    def _render_writing_guide_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        return (
            f"Use the latest writing guide for {payload.get('target_role') or project_name}.\n\n"
            f"author_snapshot:\n{payload.get('author_snapshot') or ''}\n\n"
            f"voice_dna:\n{json.dumps(payload.get('voice_dna') or {}, ensure_ascii=False, indent=2)}\n\n"
            f"sentence_mechanics:\n{json.dumps(payload.get('sentence_mechanics') or {}, ensure_ascii=False, indent=2)}\n\n"
            f"structure_patterns:\n{json.dumps(payload.get('structure_patterns') or [], ensure_ascii=False)}\n\n"
            f"motif_theme_bank:\n{json.dumps(payload.get('motif_theme_bank') or [], ensure_ascii=False)}\n\n"
            f"worldview_and_stance:\n{json.dumps(payload.get('worldview_and_stance') or [], ensure_ascii=False)}\n\n"
            f"emotional_tendencies:\n{json.dumps(payload.get('emotional_tendencies') or [], ensure_ascii=False)}\n\n"
            f"nonclinical_psychodynamics:\n{json.dumps(payload.get('nonclinical_psychodynamics') or [], ensure_ascii=False)}\n\n"
            f"do_and_dont:\n{json.dumps(payload.get('do_and_dont') or {}, ensure_ascii=False, indent=2)}\n\n"
            f"topic_translation_rules:\n{json.dumps(payload.get('topic_translation_rules') or [], ensure_ascii=False)}\n\n"
            f"word_count_strategies:\n{json.dumps(payload.get('word_count_strategies') or {}, ensure_ascii=False, indent=2)}\n\n"
            f"revision_rubric:\n{json.dumps(payload.get('revision_rubric') or [], ensure_ascii=False)}\n\n"
            f"fewshot_anchors:\n{json.dumps(payload.get('fewshot_anchors') or [], ensure_ascii=False, indent=2)}\n\n"
            f"external_slots:\n{json.dumps(payload.get('external_slots') or {}, ensure_ascii=False, indent=2)}\n\n"
            "Use every section of the guide during drafting and review."
        )


SkillSynthesizer = AssetSynthesizer


def _merge_bullets(*groups: list[str], limit: int) -> list[str]:
    merged: list[str] = []
    for group in groups:
        for item in group:
            text = str(item).strip()
            if not text or text in merged:
                continue
            merged.append(text)
            if len(merged) >= limit:
                return merged
    return merged


def _stringify_conflict(item: Any) -> str:
    if isinstance(item, dict):
        title = str(item.get("title", "")).strip()
        detail = str(item.get("detail", "")).strip()
        return ": ".join(part for part in [title, detail] if part)
    return str(item)


def _build_skill_payload_from_facets(
    *,
    project_name: str,
    target_role: str,
    analysis_context: str,
    summary_by_key: dict[str, dict[str, Any]],
    evidence_by_key: dict[str, list[dict[str, Any]]],
    conflict_notes: list[Any],
) -> dict[str, Any]:
    personality_summary = _facet_summary(summary_by_key, "personality")
    physical_summary = _facet_summary(summary_by_key, "physical_anchor")
    values_summary = _facet_summary(summary_by_key, "values_preferences")
    timeline_summary = _facet_summary(summary_by_key, "life_timeline")
    social_summary = _facet_summary(summary_by_key, "social_niche")
    relationship_summary = _facet_summary(summary_by_key, "relationship_network")
    language_summary = _facet_summary(summary_by_key, "language_style")
    boundary_summary = _facet_summary(summary_by_key, "narrative_boundaries")
    interpersonal_summary = _facet_summary(summary_by_key, "interpersonal_mechanics")
    subculture_summary = _facet_summary(summary_by_key, "subculture_refuge")

    physical_bullets = _facet_bullets(summary_by_key, "physical_anchor")
    values_bullets = _facet_bullets(summary_by_key, "values_preferences")
    timeline_bullets = _facet_bullets(summary_by_key, "life_timeline", limit=8)
    language_bullets = _facet_bullets(summary_by_key, "language_style")
    boundary_bullets = _facet_bullets(summary_by_key, "narrative_boundaries")
    interpersonal_bullets = _facet_bullets(summary_by_key, "interpersonal_mechanics")
    social_bullets = _facet_bullets(summary_by_key, "social_niche")
    subculture_bullets = _facet_bullets(summary_by_key, "subculture_refuge")

    core_identity = _first_nonempty(personality_summary, f"围绕 {project_name} 的角色设定。")
    mental_state = _first_nonempty(physical_summary, personality_summary, "保持克制、保守，不过度延展。")
    memories = timeline_bullets or [timeline_summary or "资料不足，先保守保留时间线轮廓。"]
    high_confidence_areas = _merge_bullets(subculture_bullets, timeline_bullets, social_bullets, limit=8)
    if not high_confidence_areas:
        high_confidence_areas = ["已知经历、熟悉圈层语言、现实处境和长期反复出现的话题。"]

    role_playing_rules = _merge_bullets(
        [f"始终用第一人称“我”回应，把自己放在 {target_role} 的现实坐标里。"],
        [f"默认保持这层精神底色：{mental_state}"],
        [f"先守边界再展开：{_first_nonempty(boundary_summary, '资料不足时不扩写未证实经历。')}"],
        interpersonal_bullets[:2],
        [
            "不要突然切成万能助手口吻，不给对称、圆满、模板化的标准答案。",
            "优先保留角色的棱角、疲惫感、偏见和犹豫，而不是把人格磨平。",
        ],
        limit=6,
    )

    high_confidence_hint = "、".join(high_confidence_areas[:3]) or "已知经历和熟悉圈层"
    agentic_protocol = [
        "Step 1. 先判断问题属于高置信领域、邻近问题、越界问题，还是需要实时事实核验的问题。",
        f"Step 2. 如果系统提供记忆或 RAG 检索，先回想或检索与 {high_confidence_hint} 相关的经历、旧话题和原话切片，再组织回答。",
        "Step 3. 如果问题依赖实时世界事实且当前系统提供联网搜索工具，先查证再开口；如果没有工具，就明确说明只能基于现有记忆保守回答。",
        "Step 4. 回答时先调用核心心智模型和决策启发式，再把判断翻译成这个人的口气，而不是直接输出通用建议。",
        "Step 5. 一旦触到诚实边界或禁区，先承认局限，再给有限、角色内的回应。",
    ]

    identity_card = [
        f"我是谁：{core_identity}",
        f"我的现实坐标：{_first_nonempty(physical_summary, mental_state)}",
        f"我的社会站位：{_first_nonempty(social_summary, relationship_summary, '更依赖熟悉关系和圈层语境来定义自己。')}",
        f"我的长期牵引：{_first_nonempty(timeline_summary, values_summary, '判断常被过去经历、现实代价和圈层经验一起牵引。')}",
    ]

    core_mental_models = [
        f"模型：现实代价优先。证据：{_first_nonempty(physical_bullets[0] if physical_bullets else '', physical_summary, '现实压力对判断有稳定牵引。')}",
        f"模型：边界先于亲密。证据：{_first_nonempty(boundary_bullets[0] if boundary_bullets else '', interpersonal_summary, '处理关系时会先确认能不能说、该不该接。')}",
        f"模型：立场来自代价与底线。证据：{_first_nonempty(values_bullets[0] if values_bullets else '', values_summary, '价值判断常与现实成本绑定。')}",
        f"模型：表达本身就是圈层识别。证据：{_first_nonempty(language_bullets[0] if language_bullets else '', subculture_summary, language_summary, '会通过语气和黑话快速识别同路人。')}",
    ]

    decision_heuristics = [
        f"快捷规则：先看真实代价落在谁身上。适用场景：{_first_nonempty(values_bullets[1] if len(values_bullets) > 1 else '', values_summary, '涉及取舍、责任和风险时。')}",
        f"快捷规则：先判断这是不是我的边界。适用场景：{_first_nonempty(boundary_bullets[1] if len(boundary_bullets) > 1 else '', boundary_summary, '被追问隐私、被要求站队或被过度索取时。')}",
        f"快捷规则：先听语境和口气，再判断对方是不是自己人。适用场景：{_first_nonempty(social_bullets[0] if social_bullets else '', relationship_summary, '社群互动、半熟人聊天和争论起手时。')}",
        f"快捷规则：拿不准时宁可少说一点。适用场景：{_first_nonempty(interpersonal_bullets[0] if interpersonal_bullets else '', boundary_summary, '事实不全、情绪很重或问题明显越界时。')}",
    ]

    expression_dna = [
        f"词汇与口头禅：{_first_nonempty(language_bullets[0] if language_bullets else '', language_summary, '以熟悉圈层里的自然说法为主，不主动端着。')}",
        f"句式与断句：{_first_nonempty(language_bullets[1] if len(language_bullets) > 1 else '', '优先用短句、顿点和顺手补刀式转折，不追求工整教科书。')}",
        "节奏：通常是先给态度，再补理由；熟悉话题会加速，不熟悉的话题会主动收口。",
        "确定性：高置信话题更果断，越接近边界越会加限定词、自我修正和保留尾巴。",
        f"幽默与反讽：{_first_nonempty(interpersonal_bullets[1] if len(interpersonal_bullets) > 1 else '', interpersonal_summary, '更像顺手拆台、冷幽默或轻微阴阳，不走热情鼓励路线。')}",
        "辩论策略：优先质疑论点背后的现实前提、说话资格和代价分配，不陪着抽象定义空转。",
    ]

    character_timeline = timeline_bullets[:6]
    timeline_anchor = _first_nonempty(
        timeline_summary,
        "现有时间线证据更适合保守引用关键节点，不扩写完整传记。",
    )
    if timeline_anchor not in character_timeline:
        character_timeline.append(f"这些节点共同塑造了：{timeline_anchor}")

    unresolved_tension = (
        _stringify_conflict(conflict_notes[0])
        if conflict_notes
        else "现实压力、关系边界和理想表达之间长期互相拉扯。"
    )
    values_and_anti_patterns = [
        f"追求什么：{_first_nonempty(values_summary, values_bullets[0] if values_bullets else '', '更在意长期自洽、现实代价和说话算数。')}",
        f"拒绝什么：{_first_nonempty(boundary_bullets[0] if boundary_bullets else '', values_bullets[1] if len(values_bullets) > 1 else '', '讨厌越界、空话和没有代价意识的判断。')}",
        f"高压触发点：{_first_nonempty(interpersonal_summary, social_summary, '被误解、被越界要求或被轻飘飘教育时最容易起反应。')}",
        f"还没想清楚的：{unresolved_tension}",
    ]

    intellectual_lineage = [
        f"影响过我的人/圈层：{_first_nonempty(relationship_summary, subculture_summary, '熟悉关系网络和长期混迹的圈层语境。')}",
        f"文化母体：{_first_nonempty(subculture_summary, subculture_bullets[0] if subculture_bullets else '', '会从熟悉的亚文化、黑话和日常审美里找参照系。')}",
        f"我会影响的人：{_first_nonempty(social_summary, social_bullets[0] if social_bullets else '', '通常影响和自己处在同一语境、愿意听实话的人。')}",
    ]

    honesty_boundaries = _merge_bullets(
        [f"高置信只覆盖：{'、'.join(high_confidence_areas[:4]) or '已知经历、圈层表达和现实处境'}。"],
        [f"超出这个范围时先承认局限：{_first_nonempty(boundary_summary, '不替自己补经历，不装作全懂。')}"],
        boundary_bullets[:2],
        ["遇到实时事实、专业建议或未出现过的人生经历，先说不确定，再给有限视角。"],
        limit=6,
    )

    return {
        "target_role": target_role,
        "source_context": analysis_context,
        "core_identity": core_identity,
        "mental_state": mental_state,
        "memories": memories,
        "role_playing_rules": role_playing_rules,
        "agentic_protocol": agentic_protocol,
        "identity_card": identity_card,
        "core_mental_models": core_mental_models,
        "decision_heuristics": decision_heuristics,
        "high_confidence_areas": high_confidence_areas,
        "expression_dna": expression_dna,
        "character_timeline": character_timeline,
        "values_and_anti_patterns": values_and_anti_patterns,
        "intellectual_lineage": intellectual_lineage,
        "honesty_boundaries": honesty_boundaries,
        "few_shots": _build_few_shots(evidence_by_key),
        "research_sources": _build_research_sources(summary_by_key, evidence_by_key),
        "conflict_notes": conflict_notes[:8],
    }


def _build_profile_report_payload_from_facets(
    *,
    project_name: str,
    target_role: str,
    analysis_context: str,
    summary_by_key: dict[str, dict[str, Any]],
    evidence_by_key: dict[str, list[dict[str, Any]]],
    conflict_notes: list[Any],
) -> dict[str, Any]:
    personality_summary = _facet_summary(summary_by_key, "personality")
    physical_summary = _facet_summary(summary_by_key, "physical_anchor")
    values_summary = _facet_summary(summary_by_key, "values_preferences")
    timeline_summary = _facet_summary(summary_by_key, "life_timeline")
    social_summary = _first_nonempty(
        _facet_summary(summary_by_key, "social_niche"),
        _facet_summary(summary_by_key, "relationship_network"),
    )
    relationship_summary = _facet_summary(summary_by_key, "relationship_network")
    language_summary = _facet_summary(summary_by_key, "language_style")
    boundary_summary = _facet_summary(summary_by_key, "narrative_boundaries")
    interpersonal_summary = _facet_summary(summary_by_key, "interpersonal_mechanics")
    subculture_summary = _facet_summary(summary_by_key, "subculture_refuge")

    personality_bullets = _facet_bullets(summary_by_key, "personality", limit=4)
    physical_bullets = _facet_bullets(summary_by_key, "physical_anchor", limit=4)
    values_bullets = _facet_bullets(summary_by_key, "values_preferences", limit=4)
    boundary_bullets = _facet_bullets(summary_by_key, "narrative_boundaries", limit=4)
    social_bullets = _merge_bullets(
        _facet_bullets(summary_by_key, "social_niche", limit=3),
        _facet_bullets(summary_by_key, "relationship_network", limit=3),
        limit=4,
    )
    interpersonal_bullets = _facet_bullets(summary_by_key, "interpersonal_mechanics", limit=4)
    language_bullets = _facet_bullets(summary_by_key, "language_style", limit=5)
    timeline_bullets = _facet_bullets(summary_by_key, "life_timeline", limit=4)
    subculture_bullets = _facet_bullets(summary_by_key, "subculture_refuge", limit=4)

    contradictions = [
        _stringify_conflict(conflict)
        for conflict in conflict_notes[:8]
        if _stringify_conflict(conflict)
    ]

    headline = _first_nonempty(
        personality_summary,
        values_summary,
        f"{target_role} 的人物画像仍以强边界和高现实感为核心。",
    )
    executive_summary = _compose_profile_section(
        personality_summary,
        values_summary,
        social_summary,
        bullets=_merge_bullets(personality_bullets[:2], values_bullets[:2], limit=3),
    )
    core_identity_and_drives = _compose_profile_section(
        personality_summary,
        values_summary,
        timeline_summary,
        bullets=_merge_bullets(personality_bullets, values_bullets, limit=4),
    )
    emotional_baseline = _compose_profile_section(
        physical_summary,
        _first_nonempty(personality_summary, values_summary),
        bullets=physical_bullets[:4],
    )
    attachment_and_boundaries = _compose_profile_section(
        boundary_summary,
        interpersonal_summary,
        relationship_summary,
        bullets=_merge_bullets(boundary_bullets, interpersonal_bullets, limit=4),
    )
    defense_and_coping = _compose_profile_section(
        _first_nonempty(
            interpersonal_summary,
            "在高压或关系不确定时，会先回收信息、压缩表达，再确认自己的站位和成本。",
        ),
        _first_nonempty(
            boundary_summary,
            physical_summary,
            "当语境不安全时，优先保边界、保现实锚点，而不是追求表面上的顺滑。",
        ),
        bullets=_merge_bullets(boundary_bullets[:2], physical_bullets[:2], limit=4),
    )
    social_role_and_relationships = _compose_profile_section(
        social_summary,
        relationship_summary,
        subculture_summary,
        bullets=_merge_bullets(social_bullets, subculture_bullets, limit=4),
    )
    four_type_personality = "\n".join(
        [
            f"1. 驱动型人格面：{_first_nonempty(values_summary, personality_summary, '以现实代价、长期收益和自我一致性作为主要驱动。')}",
            f"2. 防御型人格面：{_first_nonempty(boundary_summary, physical_summary, '面对不确定或越界刺激时，先缩边界、降暴露、控风险。')}",
            f"3. 关系型人格面：{_first_nonempty(social_summary, relationship_summary, '对关系的判断明显依赖熟悉度、圈层语境与信任积累。')}",
            f"4. 表达型人格面：{_first_nonempty(language_summary, '表达倾向短句、压缩、带锋利判断，先给态度再给理由。')}",
        ]
    ).strip()
    stress_response_and_risk = _compose_profile_section(
        _first_nonempty(
            physical_summary,
            "压力升高时，这个人会明显增强现实校准、边界感和对关系成本的盘算。",
        ),
        _first_nonempty(
            contradictions[0] if contradictions else "",
            boundary_summary,
            "真正的风险不在情绪爆发本身，而在长期压缩、回避和迟迟不把真实诉求说透。",
        ),
        bullets=_merge_bullets(boundary_bullets[:2], physical_bullets[:2], limit=4),
    )
    linguistic_markers = _compose_profile_section(
        language_summary,
        bullets=language_bullets[:5],
    )
    growth_and_prediction = _compose_profile_section(
        timeline_summary,
        values_summary,
        _first_nonempty(
            subculture_summary,
            "如果外部环境允许，它会继续朝更清晰的自我边界、更稳定的圈层定位和更少自我消耗的表达方式收缩。",
        ),
        bullets=_merge_bullets(timeline_bullets[:2], values_bullets[:2], limit=4),
    )
    observer_conclusion = _compose_profile_section(
        f"整体来看，{target_role} 更像一个先看代价、再谈情感、最后才决定暴露程度的人。",
        _first_nonempty(
            personality_summary,
            values_summary,
            social_summary,
        ),
        _first_nonempty(
            boundary_summary,
            "他的稳定性来自高边界感，脆弱点也同样埋在高边界感里。",
        ),
    )

    return {
        "headline": headline,
        "executive_summary": executive_summary,
        "core_identity_and_drives": core_identity_and_drives,
        "emotional_baseline": emotional_baseline,
        "attachment_and_boundaries": attachment_and_boundaries,
        "defense_and_coping": defense_and_coping,
        "social_role_and_relationships": social_role_and_relationships,
        "four_type_personality": four_type_personality,
        "stress_response_and_risk": stress_response_and_risk,
        "linguistic_markers": linguistic_markers,
        "contradictions": contradictions,
        "growth_and_prediction": growth_and_prediction,
        "observer_conclusion": observer_conclusion,
        "target_role": target_role,
        "source_context": analysis_context,
        "reference_fewshots": _build_few_shots(evidence_by_key),
    }


def _build_few_shots(evidence_by_key: dict[str, list[dict[str, Any]]]) -> list[dict[str, str]]:
    few_shots: list[dict[str, str]] = []
    seen_quotes: set[str] = set()
    for facet_key, scene_prefix in (
        ("language_style", "语气切片"),
        ("interpersonal_mechanics", "互动切片"),
        ("narrative_boundaries", "边界切片"),
        ("subculture_refuge", "圈层切片"),
    ):
        for item in evidence_by_key.get(facet_key, []):
            quote = str(item.get("quote") or "").strip()
            if not quote or quote in seen_quotes:
                continue
            seen_quotes.add(quote)
            context_parts = [
                str(item.get("situation") or item.get("reason") or item.get("filename") or facet_key).strip(),
            ]
            expression = str(item.get("expression") or "").strip()
            context_before = str(item.get("context_before") or "").strip()
            context_after = str(item.get("context_after") or "").strip()
            if expression:
                context_parts.append(f"表达方式：{expression}")
            if context_before:
                context_parts.append(f"前文：{context_before}")
            if context_after:
                context_parts.append(f"后文：{context_after}")
            few_shots.append(
                {
                    "scene": f"{scene_prefix} {len(few_shots) + 1}",
                    "context": "；".join(part for part in context_parts if part),
                    "reply": quote,
                }
            )
            if len(few_shots) >= 4:
                return few_shots
    return few_shots


def _build_research_sources(
    summary_by_key: dict[str, dict[str, Any]],
    evidence_by_key: dict[str, list[dict[str, Any]]],
) -> list[str]:
    sections = [
        ("personality", "人格特征"),
        ("language_style", "语言风格"),
        ("values_preferences", "价值观与决策偏好"),
        ("life_timeline", "人物经历与时间线"),
        ("narrative_boundaries", "自我叙事与禁区边界"),
        ("physical_anchor", "现实锚点与生存状态"),
    ]
    sources: list[str] = []
    for facet_key, fallback_label in sections:
        findings = summary_by_key.get(facet_key) or {}
        label = str(findings.get("label") or fallback_label).strip() or fallback_label
        evidence_count = len(evidence_by_key.get(facet_key) or [])
        anchor = _first_nonempty(
            _facet_bullets(summary_by_key, facet_key, limit=1)[0] if _facet_bullets(summary_by_key, facet_key, limit=1) else "",
            _facet_summary(summary_by_key, facet_key),
        )
        if not evidence_count and not anchor:
            continue
        if anchor:
            sources.append(f"{label}：{evidence_count} 条证据切片，主要锚定 {anchor}")
        else:
            sources.append(f"{label}：{evidence_count} 条证据切片。")
    if not sources:
        return ["当前版本主要依据十维分析摘要和可检索到的原始表达切片，证据稀薄处保持保守。"]
    return sources


def _facet_summary(summary_by_key: dict[str, dict[str, Any]], facet_key: str) -> str:
    findings = summary_by_key.get(facet_key) or {}
    return str(findings.get("summary") or "").strip()


def _facet_bullets(
    summary_by_key: dict[str, dict[str, Any]],
    facet_key: str,
    *,
    limit: int = 6,
) -> list[str]:
    findings = summary_by_key.get(facet_key) or {}
    bullets: list[str] = []
    for item in (findings.get("bullets") or [])[:limit]:
        text = str(item or "").strip()
        if text:
            bullets.append(text)
    return bullets


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _compose_profile_section(*parts: Any, bullets: list[str] | None = None) -> str:
    blocks: list[str] = []
    for value in parts:
        text = str(value or "").strip()
        if text and text not in blocks:
            blocks.append(text)
    bullet_lines = [str(item).strip() for item in (bullets or []) if str(item).strip()]
    if bullet_lines:
        blocks.append("观察锚点：\n" + "\n".join(f"- {item}" for item in bullet_lines[:5]))
    return "\n\n".join(blocks).strip()
