from __future__ import annotations

from typing import Any


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
        single_bullet = _facet_bullets(summary_by_key, facet_key, limit=1)
        anchor = _first_nonempty(
            single_bullet[0] if single_bullet else "",
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


__all__ = [
    "_build_few_shots",
    "_build_profile_report_payload_from_facets",
    "_build_research_sources",
    "_build_skill_payload_from_facets",
    "_compose_profile_section",
    "_facet_bullets",
    "_facet_summary",
    "_first_nonempty",
    "_merge_bullets",
    "_stringify_conflict",
]
