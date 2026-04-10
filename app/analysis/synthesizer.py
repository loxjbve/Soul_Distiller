from __future__ import annotations

from typing import Any

from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import AnalysisFacet, Project
from app.schemas import ServiceConfig, SkillBundle


class SkillSynthesizer:
    def build(
        self,
        project: Project,
        facets: list[AnalysisFacet],
        config: ServiceConfig | None,
        *,
        target_role: str | None = None,
        analysis_context: str | None = None,
    ) -> SkillBundle:
        structured = (
            self._with_llm(
                project,
                facets,
                config,
                target_role=target_role,
                analysis_context=analysis_context,
            )
            if config
            else self._heuristic(
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        )
        markdown = self._render_markdown(project.name, structured)
        system_prompt = self._render_system_prompt(project.name, structured)
        return SkillBundle(markdown_text=markdown, json_payload=structured, system_prompt=system_prompt)

    def _with_llm(
        self,
        project: Project,
        facets: list[AnalysisFacet],
        config: ServiceConfig | None,
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        if not config:
            return self._heuristic(
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        client = OpenAICompatibleClient(config)
        facet_dump = "\n\n".join(
            f"{facet.facet_key}: {facet.findings_json or {}}\nconflicts={facet.conflicts_json or []}"
            for facet in facets
        )
        context_block = "\n".join(
            line
            for line in [
                f"Target role: {target_role}" if target_role else "",
                f"User context: {analysis_context}" if analysis_context else "",
            ]
            if line
        )
        messages = [
            {
                "role": "system",
                "content": "You synthesize persona imitation skills. Return JSON only.",
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project.name}\n"
                    f"{context_block}\n"
                    "Create a JSON object with keys: target_role, source_context, overview, voice_style, "
                    "thinking_framework, behavior_rules, taboos, relationship_terms, life_timeline, examples, "
                    "boundary_statement, conflict_notes.\n\n"
                    f"Facet inputs:\n{facet_dump}"
                ),
            },
        ]
        try:
            response = client.chat_completion(messages, model=config.model, temperature=0.2)
            parsed = parse_json_response(response)
            return self._normalize_payload(parsed, target_role=target_role, analysis_context=analysis_context)
        except (LLMError, ValueError, KeyError, TypeError):
            return self._heuristic(
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )

    def _heuristic(
        self,
        project: Project,
        facets: list[AnalysisFacet],
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        summary_by_key = {facet.facet_key: (facet.findings_json or {}) for facet in facets}
        evidence_examples = []
        language_evidence = next((facet.evidence_json or [] for facet in facets if facet.facet_key == "language_style"), [])
        for item in language_evidence[:3]:
            evidence_examples.append(item.get("quote", ""))
        return {
            "target_role": target_role or project.name,
            "source_context": analysis_context or "",
            "overview": summary_by_key.get("personality", {}).get("summary", f"围绕 {project.name} 的人物蒸馏结果。"),
            "voice_style": summary_by_key.get("language_style", {}).get("summary", ""),
            "thinking_framework": summary_by_key.get("values_preferences", {}).get("summary", ""),
            "behavior_rules": summary_by_key.get("values_preferences", {}).get("bullets", []),
            "taboos": summary_by_key.get("narrative_boundaries", {}).get("bullets", []),
            "relationship_terms": summary_by_key.get("relationship_network", {}).get("bullets", []),
            "life_timeline": summary_by_key.get("life_timeline", {}).get("bullets", []),
            "examples": [quote for quote in evidence_examples if quote],
            "boundary_statement": summary_by_key.get("narrative_boundaries", {}).get("summary", ""),
            "conflict_notes": [
                conflict
                for facet in facets
                for conflict in (facet.conflicts_json or [])
            ],
        }

    def _normalize_payload(
        self,
        payload: dict[str, Any],
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        return {
            "target_role": str(payload.get("target_role", target_role or "")),
            "source_context": str(payload.get("source_context", analysis_context or "")),
            "overview": str(payload.get("overview", "")),
            "voice_style": str(payload.get("voice_style", "")),
            "thinking_framework": str(payload.get("thinking_framework", "")),
            "behavior_rules": [str(item) for item in payload.get("behavior_rules", [])[:8]],
            "taboos": [str(item) for item in payload.get("taboos", [])[:8]],
            "relationship_terms": [str(item) for item in payload.get("relationship_terms", [])[:8]],
            "life_timeline": [str(item) for item in payload.get("life_timeline", [])[:8]],
            "examples": [str(item) for item in payload.get("examples", [])[:6]],
            "boundary_statement": str(payload.get("boundary_statement", "")),
            "conflict_notes": payload.get("conflict_notes", []),
        }

    def _render_markdown(self, project_name: str, payload: dict[str, Any]) -> str:
        sections = [
            f"# {project_name} Skill",
            "",
            "## 目标角色",
            payload["target_role"] or project_name,
            "",
        ]
        if payload["source_context"]:
            sections.extend(
                [
                    "## 语料说明",
                    payload["source_context"],
                    "",
                ]
            )
        sections.extend(
            [
                "## 人物概述",
                payload["overview"],
                "",
                "## 说话方式",
                payload["voice_style"],
                "",
                "## 思考框架",
                payload["thinking_framework"],
                "",
                "## 行为准则",
                *[f"- {item}" for item in payload["behavior_rules"]],
                "",
                "## 禁忌与边界",
                *[f"- {item}" for item in payload["taboos"]],
                payload["boundary_statement"],
                "",
                "## 关系称谓",
                *[f"- {item}" for item in payload["relationship_terms"]],
                "",
                "## 人生经历",
                *[f"- {item}" for item in payload["life_timeline"]],
                "",
                "## 回答示例",
                *[f"> {item}" for item in payload["examples"]],
            ]
        )
        return "\n".join(sections).strip()

    def _render_system_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        rules = "\n".join(f"- {item}" for item in payload["behavior_rules"])
        taboos = "\n".join(f"- {item}" for item in payload["taboos"])
        relationships = "\n".join(f"- {item}" for item in payload["relationship_terms"])
        source_context = f"语料说明：{payload['source_context']}\n\n" if payload["source_context"] else ""
        return (
            f"你现在要稳定扮演 {payload['target_role'] or project_name}。\n\n"
            f"{source_context}"
            f"人物概述：{payload['overview']}\n\n"
            f"语言风格：{payload['voice_style']}\n\n"
            f"思考框架：{payload['thinking_framework']}\n\n"
            f"行为准则：\n{rules}\n\n"
            f"禁忌与边界：\n{taboos}\n{payload['boundary_statement']}\n\n"
            f"关系称谓：\n{relationships}\n\n"
            "回答要求：保持人物一致性，优先使用提供的人设与证据，不要编造无法从语料支持的具体事实。"
        )
