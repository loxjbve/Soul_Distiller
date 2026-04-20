from __future__ import annotations

from typing import Any

from app.analysis.facets import FacetDefinition, get_facet_prompt_profile

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
    output_contract = _facet_output_contract(facet)
    return [
        {
            "role": "system",
            "content": (
                "你是一名赛博人类学分析师和行为画像专家。\n"
                "你当前只负责分析一个维度，不要把人物十维总卡、全人格总结或其他维度的结论塞进当前输出。\n"
                "你需要从噪声较多的原始文本中提炼具体、可验证的行为模式。\n"
                "避免空泛夸奖、套话式总结或模糊的人格评语。\n"
                "每一条重要判断都必须严格锚定到提供的 chunk_id。\n"
                "只返回 JSON。\n"
                "必须包含这些键：summary, bullets, confidence, evidence, conflicts, notes。\n"
                "evidence 必须是对象列表，每个对象包含 chunk_id、reason、quote。\n"
                "conflicts 必须是对象列表，每个对象包含 title、detail。\n"
                "chunk_id 必须与输入完全一致。quote 优先使用简短直接引语或接近原话的转述。\n"
                "summary 必须只总结当前维度，写成一段详细、可引用的中文摘要，不要扩写成人物全貌。\n"
                "bullets 必须是当前维度下的细节观察，优先使用这个维度自己的小标签，不要机械复用通用人格卡栏目名。\n"
                "如果某条证据更适合别的维度，只能在 notes 里简短提示，不要写进 summary 或 bullets。\n"
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
                "只分析这个维度，不要顺手总结其他维度，不要补全整个人物画像。\n"
                "如果证据不足，就明确写不足，不要用其他维度内容来凑数。\n"
                "重点抓具体行为、触发条件、稳定模式、例外情况和内部张力，避免只有一句泛化判断。\n"
                "bullets 必须写得足够具体，能够直接用于实现，不要写成抽象标签或简洁总述。\n"
                f"本维度的额外抽取提醒：{focus_guidance}\n\n"
                f"本维度的输出结构要求：{output_contract}\n\n"
                f"证据摘录：\n{excerpt_text}"
            ),
        },
    ]


def build_personality_messages(
    project_name: str,
    facet_dump: str,
    evidence_context: str,
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
                "你要先从话题总结、证据引文、参与者观点和十维摘要里提炼可支撑判断的语料，再写出一份可直接保存为 personality.md 的 Markdown 文档。\n"
                "你的工作不是泛泛写人设，而是抽出最稳定的身份坐标、内部张力、长期心智气候和表达时必须保留的气压。\n"
                "所有关键判断都要尽量锚定在提供的证据语料上，证据不足时宁可保守，不要脑补经历。 \n"
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
                "## 精神底色\n"
                "## 证据锚点\n\n"
                "要求：\n"
                "1. 必须遵循先证据、后判断的顺序：先从话题总结和语料里找 recurring pattern，再抽象成人格结论。\n"
                "2. `## 核心身份` 只总结最稳定的自我定位、现实处境、角色站位和自我边界，不写空泛性格词堆砌。\n"
                "3. `## 精神底色` 要写出长期情绪底盘、典型心理张力、尚未解决的内在拉扯，以及扮演时应持续保持的心智气候。\n"
                "4. `## 证据锚点` 用 3 到 6 条项目符号列出支撑前两节的主题证据、话题摘要、原话切片或观点线索，每条都要指出它支撑了什么判断。\n"
                "5. 每节 1 到 3 段，语言直接、可执行、可供下游 Skill 追加引用，尽量写得比普通人格概述更深一层。\n"
                "6. 允许指出矛盾和灰区，但不要编造具体经历，不要重复输出完整十维报告。\n\n"
                f"证据语料包：\n{_evidence_block(evidence_context)}\n\n"
                f"十维分析摘要：\n{facet_dump}"
            ),
        },
    ]


def build_memories_messages(
    project_name: str,
    facet_dump: str,
    evidence_context: str,
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
                "你要先从话题总结、原话证据、长期脉络线索中抽取可站得住脚的经历锚点，再输出一份可直接保存为 memories.md 的 Markdown 文档。\n"
                "重点不是写流水账，而是找出哪些记忆和长期处境真的塑造了这个人的判断、表达和边界。\n"
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
                "## 长期经历脉络\n"
                "## 证据锚点\n\n"
                "要求：\n"
                "1. 必须优先从话题总结和语料证据中抽取高可信的记忆、经历和长期背景，先找证据再写总结。\n"
                "2. `## 关键记忆` 用项目符号列出 4 到 8 条，每条尽量保留时间、场景、关系、圈层或现实锚点，并写明它留下了什么影响。\n"
                "3. `## 长期经历脉络` 用 1 到 3 段总结这些关键节点如何塑造角色的判断方式、表达方式、风险感和现实感。\n"
                "4. `## 证据锚点` 用 3 到 6 条项目符号列出支撑这些记忆判断的话题摘要、原话切片或观点证据。\n"
                "5. 证据不足时宁可保守，不要脑补，不要写成虚构传记。\n\n"
                f"证据语料包：\n{_evidence_block(evidence_context)}\n\n"
                f"十维分析摘要：\n{facet_dump}"
            ),
        },
    ]


def build_cc_skill_messages(
    project_id: str,
    project_name: str,
    facet_dump: str,
    *,
    evidence_context: str,
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
                "写之前先从给定的话题总结、证据语料、人格文档和记忆文档里提炼高置信行为规则，不要只把十维摘要改写一遍。\n"
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
                "5) 正文中必须用相对路径提示按需阅读：references/personality.md 与 references/memories.md。\n\n"
                f"证据语料包：\n{_evidence_block(evidence_context)}\n\n"
                "附属文档（可引用但不要把全文照抄进正文）：\n"
                f"[references/personality.md]\n{personality_markdown.strip()}\n\n"
                f"[references/memories.md]\n{memories_markdown.strip()}\n\n"
                f"十维分析摘要：\n{facet_dump}"
            ),
        },
    ]


def build_asset_messages(
    asset_kind: str,
    project_name: str,
    facet_dump: str,
    *,
    evidence_context: str = "",
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
                    "你要先从话题总结、证据语料和十维分析里提炼稳定模式，再生成一份可直接保存为 Skill.md 的 Markdown 文档。\n"
                    "目标是把人物的高置信行为规则、判断路径、表达骨架和诚实边界压缩成能直接运行的系统 Prompt，而不是空泛的人设描述。\n"
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
                    "2. 写之前先从证据语料包里归纳 recurring topic、稳定表达、决策依据、冲突与边界；没有证据支撑的判断不要写得过满。\n"
                    "3. `## 回答工作流` 必须写成 SOP；如果当前系统未明确提供检索、记忆或联网工具，就使用条件句写成“如果系统提供...则先...”。\n"
                    "4. `## 核心心智模型` 每条尽量包含：一句话模型、现实证据、应用场景、局限性，尽量带出背后的内部张力。\n"
                    "5. `## 决策启发式` 必须写出具体快捷规则，最好带一个微型案例或适用场景。\n"
                    "6. `## 表达 DNA` 必须覆盖词汇、句式、节奏、确定性程度、幽默方式、引用习惯或辩论策略中的至少 5 项，并尽量贴近原始语料。\n"
                    "7. `## 价值观与反模式` 必须同时写“追求什么”“拒绝什么”“仍没想清楚的内在张力”。\n"
                    "8. `## 诚实边界` 必须明确高置信领域外如何承认局限，避免角色越界乱答。\n"
                    "9. `## 调研来源` 只总结来自哪些语料维度、记忆切片、话题总结或证据类型，不要伪造外部链接。\n"
                    "10. Few-Shot 切片必须贴近原始表达，突出真实语气。\n"
                    "11. 不要单独展开“核心身份与精神底色”或“核心记忆与经历”章节，它们会在本次生成后由系统追加到底部。\n\n"
                    f"证据语料包：\n{_evidence_block(evidence_context)}\n\n"
                    f"十维分析摘要：\n{facet_dump}"
                ),
            },
        ]
    if asset_kind == "cc_skill":
        return build_cc_skill_messages(
            "unknown",
            project_name,
            facet_dump,
            evidence_context=evidence_context,
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


def _evidence_block(evidence_context: str) -> str:
    text = str(evidence_context or "").strip()
    if text:
        return text
    return "暂无额外话题总结或检索证据，请保守依赖十维分析摘要。"


def _facet_focus_guidance(facet: FacetDefinition) -> str:
    return get_facet_prompt_profile(facet.key).focus


def _facet_output_contract(facet: FacetDefinition) -> str:
    profile = get_facet_prompt_profile(facet.key)
    labels = "、".join(profile.bullet_labels)
    return (
        "summary 必须围绕当前维度展开，至少交代稳定模式、触发条件和边界/例外之一；"
        f"bullets 建议写 4 到 8 条，并优先使用这些小标签：{labels}；"
        "若材料明显属于其他维度，只在 notes 里一句话标注，不要在正文展开。"
    )
