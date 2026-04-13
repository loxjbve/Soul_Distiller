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


def build_personality_messages(
    project_name: str,
    facet_dump: str,
    search_context: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    context_block = _context_block(target_role, analysis_context)
    return [
        {
            "role": "system",
            "content": (
                "你是人物模仿 Skill 的资料整理员。\n"
                "请结合十维分析摘要与检索片段，输出一份可直接保存为 personality.md 的 Markdown 文档。\n"
                "只输出 Markdown 正文，不要 JSON，不要解释，不要使用代码块。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目：{project_name}\n"
                f"{context_block}\n"
                "请写一份聚焦“身份+精神底色”的人格文档，结构固定为：\n"
                "# 核心身份与精神底色\n"
                "## 核心身份\n"
                "## 精神底色\n\n"
                "要求：\n"
                "1. 核心身份只总结最稳定的自我定位、现实处境与角色站位，不写泛泛性格评价。\n"
                "2. 精神底色只总结长期情绪底盘、心理张力和扮演时应保持的心智气候。\n"
                "3. 每节 1 到 3 段，语言直接，可执行，可供下游 Skill 追加引用。\n"
                "4. 不要编造具体经历，不要重复输出完整十维报告。\n\n"
                f"检索片段：\n{search_context}\n\n"
                f"十维分析摘要：\n{facet_dump}"
            ),
        },
    ]


def build_memories_messages(
    project_name: str,
    facet_dump: str,
    search_context: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    context_block = _context_block(target_role, analysis_context)
    return [
        {
            "role": "system",
            "content": (
                "你是人物模仿 Skill 的资料整理员。\n"
                "请结合十维分析摘要与检索片段，输出一份可直接保存为 memories.md 的 Markdown 文档。\n"
                "只输出 Markdown 正文，不要 JSON，不要解释，不要使用代码块。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目：{project_name}\n"
                f"{context_block}\n"
                "请写一份核心记忆文档，结构固定为：\n"
                "# 核心记忆与经历\n"
                "## 关键记忆\n"
                "## 长期经历脉络\n\n"
                "要求：\n"
                "1. 优先写能支撑角色稳定人设的记忆、经历和长期背景。\n"
                "2. 尽量保留时间、场景、关系或现实锚点；证据不足时宁可保守，不要脑补。\n"
                "3. `## 关键记忆` 用项目符号列出 4 到 8 条。\n"
                "4. `## 长期经历脉络` 用 1 到 3 段总结这些记忆如何塑造角色。\n\n"
                f"检索片段：\n{search_context}\n\n"
                f"十维分析摘要：\n{facet_dump}"
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
                    "你是人物模仿 Skill 的总编写手。\n"
                    "你只能依据十维分析摘要，生成一份可直接保存为 Skill.md 的 Markdown 文档。\n"
                    "不要输出 JSON，不要解释，不要使用代码块。\n"
                    "这份 Skill 只负责总结十维分析中的可执行扮演规则，不要把独立的人格文档和记忆文档再重复附录进来。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"项目：{project_name}\n"
                    f"{context_block}\n"
                    "请写一份最终 Skill 文档，结构固定为：\n"
                    "# System Role: 扮演 <目标角色>\n"
                    "## 角色定位\n"
                    "## 高置信领域\n"
                    "## 世界观与现实约束\n"
                    "## 互动规则\n"
                    "## 语言指纹\n"
                    "## 格式约束\n"
                    "## 触发话题\n"
                    "## 禁区\n"
                    "## Few-Shot 切片\n"
                    "## 冲突备注\n\n"
                    "要求：\n"
                    "1. 全文必须可直接作为系统 Prompt 使用，强调可执行规则，不写分析腔总结。\n"
                    "2. 语言指纹必须具体，包含常用语气、断句、词汇偏好、节奏和回复习惯。\n"
                    "3. Few-Shot 切片必须贴近原始表达，突出真实语气。\n"
                    "4. 不要单独展开“核心身份与精神底色”或“核心记忆与经历”章节，它们会在本次生成后由系统追加到底部。\n\n"
                    f"十维分析摘要：\n{facet_dump}"
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
