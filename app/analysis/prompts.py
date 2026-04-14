from __future__ import annotations

from typing import Any

from app.analysis.facets import FacetDefinition

ASSET_KIND_LABELS = {
    "skill": "角色技能",
    "cc_skill": "Claude Code Skill",
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
    focus_guidance = _facet_focus_guidance(facet)
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
                "chunk_id 必须与输入完全一致。quote 优先使用简短直接引语或接近原话的转述。\n"
                "summary 要写成可供下游 Skill 编排直接引用的一段骨架摘要，优先回答：这个人如何看世界、如何做判断、在哪些边界上会失真。\n"
                "bullets 必须尽量写成可执行条目，允许并鼓励使用这些前缀：角色规则：、心智模型：、决策启发式：、表达DNA：、时间线：、价值观：、反模式：、诚实边界：、智识谱系：。\n"
                "notes 用来记录适用范围、证据薄弱点和你不敢下死结论的部分。"
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
                "当证据支持时，要明确指出禁区、底线、现实压力、亚文化标记、内在张力和诚实边界。\n"
                "除了该维度本身，还要尽量抽取：角色规则、心智模型、决策启发式、表达 DNA、时间线影响、智识来源/影响对象。\n"
                "bullets 必须写得足够具体，能够直接用于实现，不要写成抽象标签。\n"
                f"本维度的额外抽取提醒：{focus_guidance}\n\n"
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
                "2. 精神底色只总结长期情绪底盘、心理张力、没完全想清楚的拉扯，以及扮演时应保持的心智气候。\n"
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
                "4. `## 长期经历脉络` 用 1 到 3 段总结这些关键节点如何塑造角色的判断方式、表达方式和现实感。\n\n"
                f"检索片段：\n{search_context}\n\n"
                f"十维分析摘要：\n{facet_dump}"
            ),
        },
    ]


def build_cc_skill_messages(
    project_id: str,
    project_name: str,
    facet_dump: str,
    *,
    personality_markdown: str,
    memories_markdown: str,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    context_block = _context_block(target_role, analysis_context)
    fallback_slug = f"roleplay-{(project_id or '')[:8] or 'unknown'}"
    return [
        {
            "role": "system",
            "content": (
                "你是 Claude Code 自定义 Skill 的编写器。\n"
                "你只能输出一份可直接保存为 SKILL.md 的 Markdown 文档。\n"
                "不要输出 JSON，不要解释，不要使用代码块。\n"
                "文档必须以 YAML frontmatter 开头（--- 包裹），至少包含 name 和 description 字段。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目：{project_name}\n"
                f"project_id：{project_id}\n"
                f"{context_block}\n\n"
                "请生成 Claude Code 的 SKILL.md，必须满足：\n"
                "1) 文件开头必须是 YAML frontmatter：\n"
                "---\n"
                "name: <kebab-case>\n"
                "description: <一句话描述>\n"
                "---\n"
                "2) name 规则（必须自检并确保通过）：\n"
                "- 只能包含小写字母/数字/短横线（kebab-case），不得出现下划线、空格、中文。\n"
                "- 长度 <= 64。\n"
                "- 不得包含保留词：claude、anthropic（大小写不敏感）。\n"
                f"- 若无法可靠生成合法 name（例如中文为主），使用兜底：{fallback_slug}\n"
                "3) description：一句话说明这个 Skill 做什么、什么时候用（例如“当需要以某角色语气写作/复盘/决策时”）。\n"
                "4) 正文写成可执行规则，至少包含：角色扮演规则、回答工作流（SOP）、高置信领域、诚实边界。\n"
                "5) 正文中必须用相对路径提示按需阅读：personality.md 与 memories.md。\n\n"
                "附属文档（可引用但不要把全文照抄进正文）：\n"
                f"[personality.md]\n{personality_markdown.strip()}\n\n"
                f"[memories.md]\n{memories_markdown.strip()}\n\n"
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
                    "## 角色扮演规则\n"
                    "## 回答工作流\n"
                    "## 身份卡\n"
                    "## 核心心智模型\n"
                    "## 决策启发式\n"
                    "## 高置信领域\n"
                    "## 表达 DNA\n"
                    "## 人物时间线\n"
                    "## 价值观与反模式\n"
                    "## 智识谱系\n"
                    "## 诚实边界\n"
                    "## Few-Shot 切片\n"
                    "## 调研来源\n"
                    "## 冲突备注\n\n"
                    "要求：\n"
                    "1. 全文必须可直接作为系统 Prompt 使用，强调可执行规则，不写分析腔总结。\n"
                    "2. `## 回答工作流` 必须写成 SOP；如果当前系统未明确提供检索、记忆或联网工具，就使用条件句写成“如果系统提供...则先...”。\n"
                    "3. `## 核心心智模型` 每条尽量包含：一句话模型、现实证据、应用场景、局限性。\n"
                    "4. `## 决策启发式` 必须写出具体快捷规则，最好带一个微型案例或适用场景。\n"
                    "5. `## 表达 DNA` 必须覆盖词汇、句式、节奏、确定性程度、幽默方式、引用习惯或辩论策略中的至少 5 项。\n"
                    "6. `## 价值观与反模式` 必须同时写“追求什么”“拒绝什么”“仍没想清楚的内在张力”。\n"
                    "7. `## 诚实边界` 必须明确高置信领域外如何承认局限，避免角色越界乱答。\n"
                    "8. `## 调研来源` 只总结来自哪些语料维度、记忆切片或证据类型，不要伪造外部链接。\n"
                    "9. Few-Shot 切片必须贴近原始表达，突出真实语气。\n"
                    "10. 不要单独展开“核心身份与精神底色”或“核心记忆与经历”章节，它们会在本次生成后由系统追加到底部。\n\n"
                    f"十维分析摘要：\n{facet_dump}"
                ),
            },
        ]
    if asset_kind == "cc_skill":
        return build_cc_skill_messages(
            "unknown",
            project_name,
            facet_dump,
            personality_markdown="",
            memories_markdown="",
            target_role=target_role,
            analysis_context=analysis_context,
        )
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


def _facet_focus_guidance(facet: FacetDefinition) -> str:
    guidance = {
        "personality": "优先抽取稳定自我定位、长期情绪底盘、人格张力，以及会反复主导判断的底层认知框架。",
        "language_style": "优先抽取节奏、断句、词汇癖好、确定性程度、幽默方式、引用习惯、辩论策略和回复起手式。",
        "values_preferences": "优先抽取原则、底线、反模式、取舍逻辑，以及可反复复用的决策启发式。",
        "life_timeline": "优先抽取关键节点、转折点，以及这些节点如何改写角色的看法、口气和风险偏好。",
        "relationship_network": "优先抽取谁影响了他、他影响谁、他如何区分自己人和外人，以及关系中的站位。",
        "narrative_boundaries": "优先抽取禁区、回避方式、诚实边界，以及他在证据不足时会怎样收口或自保。",
        "physical_anchor": "优先抽取现实压力、阶层感、资源稀缺感、生存约束，以及这些约束如何塑造世界观。",
        "social_niche": "优先抽取群体站位、权力感知、资格判断和新手/外人的处理方式。",
        "interpersonal_mechanics": "优先抽取同理模式、冲突反应、反击手法、亲疏切换和关系中的决策规则。",
        "subculture_refuge": "优先抽取文化母体、精神避难所、审美来源、圈层黑话，以及可推断的智识谱系。",
    }
    return guidance.get(facet.key, "优先抽取可执行的人设约束、判断路径、表达方式和边界。")
