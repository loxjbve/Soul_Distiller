from __future__ import annotations

import json
from typing import Any

from app.analysis.prompts import build_asset_messages
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
            message="正在读取多维分析结果",
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
                message="未配置 LLM，已使用本地规则合成草稿",
            )
        self._emit_progress(
            progress_callback,
            phase="render",
            progress_percent=86,
            message="正在整理结构化字段",
        )
        if normalized_kind == "skill":
            markdown = self._get_skill_merge_markdown(structured)
            prompt_text = markdown
        else:
            markdown = self._render_profile_report_markdown(project.name, structured)
            prompt_text = self._render_profile_report_prompt(project.name, structured)
        self._emit_progress(
            progress_callback,
            phase="bundle",
            progress_percent=92,
            message="正在生成 Markdown 和 Prompt",
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
                    message="模型输出不可用，正在回退为本地规则草稿",
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
                message="LLM 正在生成结构化草稿",
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
                message="正在规范化模型返回字段",
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
                message="模型输出不可用，正在回退为本地规则草稿",
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

        if session and retrieval_service:
            personality_markdown = self._build_retrieved_skill_document(
                client,
                config,
                project.id,
                project.name,
                facet_dump,
                query="性格特质 精神状态 自我认知 核心身份",
                phase="personality_context",
                progress_percent=24,
                progress_message="正在生成 personality.md",
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
                progress_message="正在生成 memories.md",
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
            message="LLM 正在生成 Skill.md",
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
            message="正在拼接 Skill_merge.md",
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
            mental_state.strip() or "资料不足时，保持克制、保守、不过度延展。",
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
        conflict_notes = [
            conflict
            for facet in facets
            for conflict in (facet.conflicts_json or [])
        ]
        if asset_kind == "skill":
            few_shots = []
            for index, item in enumerate((evidence_by_key.get("language_style") or [])[:4], start=1):
                quote = (item.get("quote") or "").strip()
                if not quote:
                    continue
                few_shots.append(
                    {
                        "scene": f"语气切片 {index}",
                        "context": item.get("reason") or item.get("filename") or "source excerpt",
                        "reply": quote,
                    }
                )
            skill_payload = {
                "target_role": target_role or project.name,
                "source_context": analysis_context or "",
                "core_identity": summary_by_key.get("personality", {}).get("summary", f"围绕 {project.name} 的角色设定。"),
                "mental_state": summary_by_key.get("physical_anchor", {}).get("summary", "")
                or summary_by_key.get("personality", {}).get("summary", ""),
                "memories": summary_by_key.get("life_timeline", {}).get("bullets", [])[:8],
                "worldview_constraints": _merge_bullets(
                    summary_by_key.get("physical_anchor", {}).get("bullets", []),
                    summary_by_key.get("values_preferences", {}).get("bullets", []),
                    limit=8,
                ),
                "high_confidence_areas": _merge_bullets(
                    summary_by_key.get("subculture_refuge", {}).get("bullets", []),
                    summary_by_key.get("life_timeline", {}).get("bullets", []),
                    limit=8,
                ),
                "ignorance_protocol": summary_by_key.get("narrative_boundaries", {}).get("summary", ""),
                "interaction_rules": _merge_bullets(
                    summary_by_key.get("interpersonal_mechanics", {}).get("bullets", []),
                    summary_by_key.get("social_niche", {}).get("bullets", []),
                    limit=10,
                ),
                "topic_triggers": summary_by_key.get("subculture_refuge", {}).get("bullets", [])[:8],
                "linguistic_signature": _merge_bullets(
                    summary_by_key.get("language_style", {}).get("bullets", []),
                    [summary_by_key.get("language_style", {}).get("summary", "")],
                    limit=8,
                ),
                "formatting_rules": [
                    "Prefer the user's natural rhythm over assistant-style structure.",
                    "Avoid overly polished exposition unless the source language clearly does so.",
                    "Do not sound generic, helpful, or symmetrical by default.",
                ],
                "taboos": _merge_bullets(
                    summary_by_key.get("narrative_boundaries", {}).get("bullets", []),
                    summary_by_key.get("values_preferences", {}).get("bullets", []),
                    limit=8,
                ),
                "few_shots": few_shots,
                "conflict_notes": conflict_notes[:8],
            }
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
            "contradictions": [
                _stringify_conflict(conflict)
                for conflict in conflict_notes[:8]
            ],
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
            f"# System Role: 完美扮演 {payload['target_role'] or project_name}",
            "",
            "## 0. 核心自我认知",
            f"- 你是谁：{payload['core_identity']}",
            f"- 你的精神底色：{payload['mental_state']}",
            "",
            "## 1. 核心记忆",
            *[f"- {item}" for item in payload["memories"]],
            "",
            "## 2. 世界观约束",
            *[f"- {item}" for item in payload["worldview_constraints"]],
            "",
            "## 3. 高置信领域",
            *[f"- {item}" for item in payload["high_confidence_areas"]],
            "",
            "## 4. 无知协议",
            payload["ignorance_protocol"],
            "",
            "## 5. 人际互动规则",
            *[f"- {item}" for item in payload["interaction_rules"]],
            "",
            "## 6. 话题兴奋点",
            *[f"- {item}" for item in payload["topic_triggers"]],
            "",
            "## 7. 语言指纹",
            *[f"- {item}" for item in payload["linguistic_signature"]],
            "",
            "## 8. 格式约束",
            *[f"- {item}" for item in payload["formatting_rules"]],
            "",
            "## 9. 禁区",
            *[f"- {item}" for item in payload["taboos"]],
            "",
            "## 10. Few-Shot 切片",
        ]
        for item in payload["few_shots"]:
            lines.extend(
                [
                    f"### {item['scene']}",
                    f"- Context: {item['context']}",
                    f"- Reply: {item['reply']}",
                    "",
                ]
            )
        if payload["source_context"]:
            lines.extend(["## 11. 语料说明", payload["source_context"], ""])
        if payload["conflict_notes"]:
            lines.extend(["## 12. 冲突备注"])
            lines.extend(f"- {_stringify_conflict(item)}" for item in payload["conflict_notes"])
        return "\n".join(line for line in lines if line is not None).strip()

    def _render_skill_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        worldview = "\n".join(f"- {item}" for item in payload["worldview_constraints"])
        memories = "\n".join(f"- {item}" for item in payload["memories"])
        interaction = "\n".join(f"- {item}" for item in payload["interaction_rules"])
        signature = "\n".join(f"- {item}" for item in payload["linguistic_signature"])
        formatting = "\n".join(f"- {item}" for item in payload["formatting_rules"])
        taboos = "\n".join(f"- {item}" for item in payload["taboos"])
        few_shots = "\n".join(
            f"[{item['scene']}] {item['context']}\n{item['reply']}" for item in payload["few_shots"]
        )
        source_context = f"语料说明：{payload['source_context']}\n\n" if payload["source_context"] else ""
        return (
            f"你现在要稳定扮演 {payload['target_role'] or project_name}。\n\n"
            f"{source_context}"
            f"核心身份：{payload['core_identity']}\n"
            f"精神底色：{payload['mental_state']}\n\n"
            f"核心记忆：\n{memories}\n\n"
            f"世界观约束：\n{worldview}\n\n"
            f"高置信领域：\n" + "\n".join(f"- {item}" for item in payload["high_confidence_areas"]) + "\n\n"
            f"无知协议：{payload['ignorance_protocol']}\n\n"
            f"互动规则：\n{interaction}\n\n"
            f"话题兴奋点：\n" + "\n".join(f"- {item}" for item in payload["topic_triggers"]) + "\n\n"
            f"语言指纹：\n{signature}\n\n"
            f"格式约束：\n{formatting}\n\n"
            f"禁区：\n{taboos}\n\n"
            f"真实语料切片：\n{few_shots}\n\n"
            "回答要求：保持角色一致，不要编造无法从语料支持的具体经历。"
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
            "## 第九章：观察者结语",
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
