from __future__ import annotations

import json
import re
from typing import Any

from app.analysis.prompts import build_asset_messages, build_cc_skill_messages
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
        normalized_kind = asset_kind if asset_kind in ASSET_KINDS else "skill"
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
    ) -> None:
        if not callable(progress_callback):
            return
        progress_callback(
            {
                "phase": phase,
                "progress_percent": progress_percent,
                "message": message,
            }
        )

    def _build_facet_dump(self, facets: list[AnalysisFacet]) -> str:
        compact_facets = [self._compact_facet_for_prompt(facet) for facet in facets]
        return json.dumps(compact_facets, ensure_ascii=False, indent=2)

    def _compact_facet_for_prompt(self, facet: AnalysisFacet) -> dict[str, Any]:
        findings = dict(facet.findings_json or {})
        conflicts = list(facet.conflicts_json or [])
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
        }

    def _build_search_context(self, chunks: list[Any]) -> str:
        lines: list[str] = []
        for chunk in chunks[:SYNTHESIS_SEARCH_RESULT_LIMIT]:
            content = self._truncate_text(getattr(chunk, "content", ""), SYNTHESIS_SEARCH_CHUNK_LIMIT)
            if not content:
                continue
            source = getattr(chunk, "document_title", None) or getattr(chunk, "filename", None) or "source"
            lines.append(f"- [{source}] {content}")
        return "\n".join(lines)

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
        embedding_config = repository.get_service_config(session, "embedding_service") if session else None

        if session and retrieval_service and project.mode != "telegram":
            personality_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query="性格特质 精神状态 自我认知 核心身份",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
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
                query="核心记忆 经历 过往重要事件",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )

        self._emit_progress(
            progress_callback,
            phase="synthesis",
            progress_percent=52,
            message="LLM is generating Skill.md",
        )
        messages = build_asset_messages(
            "skill",
            project.name,
            facet_dump,
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
        flush_remaining = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_remaining):
            flush_remaining()

        self._emit_progress(
            progress_callback,
            phase="merge",
            progress_percent=78,
            message="Merging Skill_merge.md",
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
        embedding_config = repository.get_service_config(session, "embedding_service") if session else None

        if session and retrieval_service and project.mode != "telegram":
            personality_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query="性格特质 精神状态 自我认知 核心身份",
                phase="personality_context",
                progress_percent=24,
                progress_message="Building personality.md",
                message_builder=build_personality_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
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
                query="核心记忆 经历 过往重要事件",
                phase="memory_context",
                progress_percent=36,
                progress_message="Building memories.md",
                message_builder=build_memories_messages,
                target_role=target_role,
                analysis_context=analysis_context,
                progress_callback=progress_callback,
                session=session,
                retrieval_service=retrieval_service,
                embedding_config=embedding_config,
            )

        self._emit_progress(
            progress_callback,
            phase="synthesis",
            progress_percent=52,
            message="LLM is generating SKILL.md",
        )
        messages = build_cc_skill_messages(
            project.id,
            project.name,
            facet_dump,
            personality_markdown=personality_markdown,
            memories_markdown=memories_markdown,
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
        flush_remaining = getattr(stream_callback, "_flush_remaining", None)
        if callable(flush_remaining):
            flush_remaining()

        return self._normalize_cc_skill_payload(
            {
                "target_role": target_role or project.name,
                "source_context": analysis_context or "",
                "skill_markdown": response.content,
                "personality_markdown": personality_markdown,
                "memories_markdown": memories_markdown,
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
        phase: str,
        progress_percent: int,
        progress_message: str,
        message_builder: Any,
        target_role: str | None,
        analysis_context: str | None,
        progress_callback: Any | None,
        session: Any,
        retrieval_service: Any,
        embedding_config: ServiceConfig | None,
    ) -> str:
        self._emit_progress(
            progress_callback,
            phase=phase,
            progress_percent=progress_percent,
            message=progress_message,
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
        response = client.chat_completion_result(messages, model=config.model, temperature=0.2, max_tokens=None)
        return str(response.content or "").strip()

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
                },
                project_name=project.name,
                project_id=project.id,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        return {
            "headline": summary_by_key.get("personality", {}).get("summary", f"{project.name} 的人物剖析"),
            "executive_summary": summary_by_key.get("personality", {}).get("summary", ""),
            "reality_anchor": summary_by_key.get("physical_anchor", {}).get("summary", ""),
            "social_dynamics": summary_by_key.get("social_niche", {}).get("summary", "")
            or summary_by_key.get("relationship_network", {}).get("summary", ""),
            "interpersonal_mechanics": summary_by_key.get("interpersonal_mechanics", {}).get("summary", ""),
            "subculture_refuge": summary_by_key.get("subculture_refuge", {}).get("summary", ""),
            "core_values_and_triggers": summary_by_key.get("values_preferences", {}).get("summary", ""),
            "linguistic_signature": summary_by_key.get("language_style", {}).get("summary", ""),
            "psychological_profile": summary_by_key.get("personality", {}).get("summary", ""),
            "contradictions": [_stringify_conflict(conflict) for conflict in conflict_notes[:8]],
            "observer_conclusion": summary_by_key.get("life_timeline", {}).get("summary", "")
            or summary_by_key.get("narrative_boundaries", {}).get("summary", ""),
            "target_role": target_role or project.name,
            "source_context": analysis_context or "",
        }

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
            },
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
            "headline": str(payload.get("headline", f"{project_name} 的人物剖析")),
            "executive_summary": str(payload.get("executive_summary", "")),
            "reality_anchor": str(payload.get("reality_anchor", "")),
            "social_dynamics": str(payload.get("social_dynamics", "")),
            "interpersonal_mechanics": str(payload.get("interpersonal_mechanics", "")),
            "subculture_refuge": str(payload.get("subculture_refuge", "")),
            "core_values_and_triggers": str(payload.get("core_values_and_triggers", "")),
            "linguistic_signature": str(payload.get("linguistic_signature", "")),
            "psychological_profile": str(payload.get("psychological_profile", "")),
            "contradictions": [str(item) for item in payload.get("contradictions", [])[:8]],
            "observer_conclusion": str(payload.get("observer_conclusion", "")),
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
            f"# 档案编号：{payload.get('target_role') or project_name} 全景侧写",
            "",
            "## 卷首语：人物判词",
            payload["headline"],
            "",
            "## 第一章：现实映射与生存锚点",
            payload["reality_anchor"],
            "",
            "## 第二章：群体生态位与社会动力",
            payload["social_dynamics"],
            "",
            "## 第三章：待人接物与人际力学",
            payload["interpersonal_mechanics"],
            "",
            "## 第四章：亚文化偏好与精神避难所",
            payload["subculture_refuge"],
            "",
            "## 第五章：核心价值与触发点",
            payload["core_values_and_triggers"],
            "",
            "## 第六章：语言指纹",
            payload["linguistic_signature"],
            "",
            "## 第七章：心理剖面",
            payload["psychological_profile"],
            "",
            "## 第八章：矛盾与裂缝",
            *[f"- {item}" for item in payload["contradictions"]],
            "",
            "## 第九章：观察者结论",
            payload["observer_conclusion"],
        ]
        if payload["source_context"]:
            sections.extend(["", "## 附录：语料说明", payload["source_context"]])
        return "\n".join(sections).strip()

    def _render_profile_report_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        contradictions = "\n".join(f"- {item}" for item in payload["contradictions"])
        source_context = f"语料说明：{payload['source_context']}\n\n" if payload["source_context"] else ""
        return (
            f"以下是 {payload.get('target_role') or project_name} 的用户剖析报告摘要。\n\n"
            f"{source_context}"
            f"人物判词：{payload['headline']}\n\n"
            f"执行摘要：{payload['executive_summary']}\n\n"
            f"现实锚点：{payload['reality_anchor']}\n\n"
            f"社会动力：{payload['social_dynamics']}\n\n"
            f"人际机制：{payload['interpersonal_mechanics']}\n\n"
            f"精神避难所：{payload['subculture_refuge']}\n\n"
            f"核心价值与触发点：{payload['core_values_and_triggers']}\n\n"
            f"语言指纹：{payload['linguistic_signature']}\n\n"
            f"心理剖面：{payload['psychological_profile']}\n\n"
            f"主要矛盾：\n{contradictions}\n\n"
            f"观察者结论：{payload['observer_conclusion']}"
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
            few_shots.append(
                {
                    "scene": f"{scene_prefix} {len(few_shots) + 1}",
                    "context": str(item.get("reason") or item.get("filename") or facet_key),
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
