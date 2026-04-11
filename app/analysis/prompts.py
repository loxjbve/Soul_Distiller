from __future__ import annotations

from typing import Any

from app.analysis.facets import FacetDefinition

ASSET_KIND_LABELS = {
    "skill": "角色技能",
    "profile_report": "用户剖析报告",
}


def build_facet_analysis_messages(
    project_name: str,
    facet: FacetDefinition,
    excerpt_text: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    context_block = _context_block(target_role, analysis_context)
    return [
        {
            "role": "system",
            "content": (
                "你是一名赛博人类学分析师和行为画像专家。\n"
                "你需要从噪声较多的原始文本中提炼具体、可验证的行为模式。\n"
                "避免空泛夸奖、套话式总结或模糊的人格评语。\n"
                "每一条重要判断都必须严格锚定到提供的 chunk_id。\n"
                "只返回 JSON。\n"
                "必须包含这些键：summary, bullets, confidence, evidence, conflicts, notes。\n"
                "evidence 必须是对象列表，每个对象包含 chunk_id、reason、quote。\n"
                "conflicts 必须是对象列表，每个对象包含 title、detail。\n"
                "chunk_id 必须与输入完全一致。quote 优先使用简短直接引语或接近原话的转述。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目：{project_name}\n"
                f"分析维度：{facet.label} ({facet.key})\n"
                f"分析目标：{facet.purpose}\n"
                f"{context_block}\n"
                "重点分析具体行为、社交逻辑、语言特征、现实约束与内部矛盾。\n"
                "当证据支持时，要明确指出禁区、底线、现实压力或亚文化标记。\n"
                "bullets 必须写得足够具体，能够直接用于实现，不要写成抽象标签。\n\n"
                f"证据摘录：\n{excerpt_text}"
            ),
        },
    ]


def build_asset_messages(
    asset_kind: str,
    project_name: str,
    facet_dump: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    context_block = _context_block(target_role, analysis_context)
    if asset_kind == "skill":
        return [
            {
                "role": "system",
                "content": (
                    "你是一名擅长塑造 LLM 人设的沉浸式导演和心理侧写师。\n"
                    "请把分析结论转换成自然、可执行的角色扮演技能设定。\n"
                    "凡是能够转成操作规则的内容，就不要再用抽象分析语言复述。\n"
                    "**极其重要**：你必须深度挖掘并还原角色的「语气语调」、「常用词汇」、「口癖习惯」以及「长期/短期记忆和经历」。这些细节占据人格的主导地位，必须在生成的结果中被放大和强调！\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"项目：{project_name}\n"
                    f"{context_block}\n"
                    "请生成一个 JSON 对象，必须包含这些键：\n"
                    "target_role, source_context, core_identity, mental_state, worldview_constraints,\n"
                    "high_confidence_areas, ignorance_protocol, interaction_rules, topic_triggers,\n"
                    "linguistic_signature, formatting_rules, taboos, few_shots, conflict_notes, memory_and_background。\n"
                    "**强烈要求**：\n"
                    "1. `linguistic_signature` 必须极其详细地描述口癖、常用语气词、说话节奏（如标点习惯、断句）、典型词汇库。\n"
                    "2. `memory_and_background` 必须列出角色最深刻的记忆、过往经历以及支撑其当前人设的核心事件。\n"
                    "3. `few_shots` 必须是对象列表，每个对象包含 scene、context、reply，且 reply 必须完美还原上述口癖和语气。\n"
                    "4. `interaction_rules` 要写成可执行的 if/then 式角色扮演规则，指导 LLM 如何运用记忆和口癖。\n"
                    "5. `formatting_rules` 要写成简短直接的表达与排版约束。\n\n"
                    f"维度输入：\n{facet_dump}"
                ),
            },
        ]
    return [
        {
            "role": "system",
            "content": (
                "你是一名赛博人类学家和用户画像分析专家。\n"
                "请撰写一份鲜明、具体、但始终有证据支撑的用户剖析报告。\n"
                "语气要犀利、具体、有人味，避免空话。\n"
                "只返回 JSON。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目：{project_name}\n"
                f"{context_block}\n"
                "请生成一个 JSON 对象，必须包含这些键：\n"
                "headline, executive_summary, reality_anchor, social_dynamics, interpersonal_mechanics,\n"
                "subculture_refuge, core_values_and_triggers, linguistic_signature,\n"
                "psychological_profile, contradictions, observer_conclusion。\n"
                "contradictions 必须是简短字符串列表。\n"
                "每个字段都要把分析结论整理成可阅读、章节级的总结。\n\n"
                f"维度输入：\n{facet_dump}"
            ),
        },
    ]


def _context_block(target_role: str | None, analysis_context: str | None) -> str:
    lines = []
    if target_role:
        lines.append(f"目标角色：{target_role}")
    if analysis_context:
        lines.append(f"来源语境：{analysis_context}")
    return "\n".join(lines)
