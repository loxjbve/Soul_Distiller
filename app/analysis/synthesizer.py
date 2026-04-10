from __future__ import annotations

from typing import Any

from app.analysis.prompts import build_asset_messages
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import AnalysisFacet, Project
from app.schemas import ASSET_KINDS, AssetBundle, ServiceConfig


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
    ) -> AssetBundle:
        normalized_kind = asset_kind if asset_kind in ASSET_KINDS else "skill"
        structured = (
            self._with_llm(
                normalized_kind,
                project,
                facets,
                config,
                target_role=target_role,
                analysis_context=analysis_context,
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
        if normalized_kind == "skill":
            markdown = self._render_skill_markdown(project.name, structured)
            prompt_text = self._render_skill_prompt(project.name, structured)
        else:
            markdown = self._render_profile_report_markdown(project.name, structured)
            prompt_text = self._render_profile_report_prompt(project.name, structured)
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
        facet_dump = "\n\n".join(
            f"{facet.facet_key}: findings={facet.findings_json or {}} conflicts={facet.conflicts_json or []}"
            for facet in facets
        )
        messages = build_asset_messages(
            asset_kind,
            project.name,
            facet_dump,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        try:
            response = client.chat_completion(messages, model=config.model, temperature=0.2)
            parsed = parse_json_response(response)
            if asset_kind == "skill":
                return self._normalize_skill_payload(
                    parsed,
                    project.name,
                    target_role=target_role,
                    analysis_context=analysis_context,
                )
            return self._normalize_profile_report_payload(
                parsed,
                project.name,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        except (LLMError, ValueError, KeyError, TypeError):
            return self._heuristic(
                asset_kind,
                project,
                facets,
                target_role=target_role,
                analysis_context=analysis_context,
            )

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
            return {
                "target_role": target_role or project.name,
                "source_context": analysis_context or "",
                "core_identity": summary_by_key.get("personality", {}).get("summary", f"围绕 {project.name} 的角色设定。"),
                "mental_state": summary_by_key.get("physical_anchor", {}).get("summary", "")
                or summary_by_key.get("personality", {}).get("summary", ""),
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
        few_shots = []
        for item in payload.get("few_shots", [])[:8]:
            if not isinstance(item, dict):
                continue
            few_shots.append(
                {
                    "scene": str(item.get("scene", "")),
                    "context": str(item.get("context", "")),
                    "reply": str(item.get("reply", "")),
                }
            )
        return {
            "target_role": str(payload.get("target_role", target_role or project_name)),
            "source_context": str(payload.get("source_context", analysis_context or "")),
            "core_identity": str(payload.get("core_identity", "")),
            "mental_state": str(payload.get("mental_state", "")),
            "worldview_constraints": [str(item) for item in payload.get("worldview_constraints", [])[:8]],
            "high_confidence_areas": [str(item) for item in payload.get("high_confidence_areas", [])[:8]],
            "ignorance_protocol": str(payload.get("ignorance_protocol", "")),
            "interaction_rules": [str(item) for item in payload.get("interaction_rules", [])[:10]],
            "topic_triggers": [str(item) for item in payload.get("topic_triggers", [])[:8]],
            "linguistic_signature": [str(item) for item in payload.get("linguistic_signature", [])[:8]],
            "formatting_rules": [str(item) for item in payload.get("formatting_rules", [])[:8]],
            "taboos": [str(item) for item in payload.get("taboos", [])[:8]],
            "few_shots": few_shots,
            "conflict_notes": payload.get("conflict_notes", []),
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
            "## 1. 世界观约束",
            *[f"- {item}" for item in payload["worldview_constraints"]],
            "",
            "## 2. 高置信领域",
            *[f"- {item}" for item in payload["high_confidence_areas"]],
            "",
            "## 3. 无知协议",
            payload["ignorance_protocol"],
            "",
            "## 4. 人际互动规则",
            *[f"- {item}" for item in payload["interaction_rules"]],
            "",
            "## 5. 话题兴奋点",
            *[f"- {item}" for item in payload["topic_triggers"]],
            "",
            "## 6. 语言指纹",
            *[f"- {item}" for item in payload["linguistic_signature"]],
            "",
            "## 7. 格式约束",
            *[f"- {item}" for item in payload["formatting_rules"]],
            "",
            "## 8. 禁区",
            *[f"- {item}" for item in payload["taboos"]],
            "",
            "## 9. Few-Shot 切片",
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
            lines.extend(["## 10. 语料说明", payload["source_context"], ""])
        if payload["conflict_notes"]:
            lines.extend(["## 11. 冲突备注"])
            lines.extend(f"- {_stringify_conflict(item)}" for item in payload["conflict_notes"])
        return "\n".join(line for line in lines if line is not None).strip()

    def _render_skill_prompt(self, project_name: str, payload: dict[str, Any]) -> str:
        worldview = "\n".join(f"- {item}" for item in payload["worldview_constraints"])
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
