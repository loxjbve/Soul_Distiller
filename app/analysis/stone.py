from __future__ import annotations

import json
import re
from typing import Any

from app.models import DocumentRecord
from app.utils.text import normalize_whitespace, top_terms

STONE_PROFILE_KEYS = (
    "content_summary",
    "content_type",
    "length_label",
    "emotion_label",
    "selected_passages",
)

STONE_REVIEW_DIMENSIONS = (
    "style_consistency",
    "structure_and_pacing",
    "lexicon_and_rhythm",
    "theme_and_worldview",
    "originality_and_overlap",
)

STONE_LENGTH_LABELS = ("长文", "短文")

_STOPWORDS = {
    "的",
    "了",
    "是",
    "我",
    "你",
    "他",
    "她",
    "它",
    "我们",
    "他们",
    "自己",
    "一个",
    "一种",
    "因为",
    "所以",
    "如果",
    "但是",
    "然后",
    "而且",
    "这个",
    "那个",
    "一些",
    "没有",
    "不是",
    "只是",
    "about",
    "from",
    "that",
    "this",
    "with",
}

def normalize_stone_profile(
    payload: dict[str, Any] | None,
    *,
    article_text: str | None = None,
    fallback_title: str | None = None,
) -> dict[str, Any]:
    raw = dict(payload or {})
    source_text = normalize_whitespace(
        str(
            article_text
            or raw.get("article_text")
            or raw.get("source_text")
            or raw.get("raw_text")
            or ""
        )
    )
    content_summary = normalize_whitespace(
        str(
            raw.get("content_summary")
            or raw.get("summary")
            or raw.get("content")
            or raw.get("article_theme")
            or ""
        )
    )
    length_label = _resolve_length_label(raw.get("length_label") or raw.get("length"), source_text)
    if length_label == "短文":
        content_summary = source_text or content_summary or str(fallback_title or "").strip()
    elif not content_summary:
        content_summary = _build_content_summary(source_text, fallback_title)

    content_type = normalize_whitespace(str(raw.get("content_type") or raw.get("nature") or ""))
    emotion_label = normalize_whitespace(str(raw.get("emotion_label") or raw.get("emotion") or raw.get("tone") or ""))

    selected_passages_source = (
        raw.get("selected_passages")
        or raw.get("featured_passages")
        or raw.get("representative_lines")
        or []
    )
    if isinstance(selected_passages_source, str):
        selected_passages = [normalize_whitespace(selected_passages_source)] if selected_passages_source.strip() else []
    else:
        selected_passages = [
            normalize_whitespace(str(item or ""))
            for item in selected_passages_source
            if normalize_whitespace(str(item or ""))
        ][:3]
    if not selected_passages and source_text:
        selected_passages = _pick_selected_passages(source_text, length_label=length_label)

    return {
        "content_summary": content_summary,
        "content_type": content_type,
        "length_label": length_label,
        "emotion_label": emotion_label,
        "selected_passages": selected_passages[:3],
    }


def expand_stone_profile_for_analysis(
    profile: dict[str, Any] | None,
    *,
    title: str | None = None,
) -> dict[str, Any]:
    raw = dict(profile or {})
    normalized = normalize_stone_profile(raw)
    legacy_markers = [str(item).strip() for item in (raw.get("lexical_markers") or []) if str(item).strip()][:6]
    legacy_signals = [str(item).strip() for item in (raw.get("nonclinical_signals") or []) if str(item).strip()][:6]
    legacy_lines = [str(item).strip() for item in (raw.get("representative_lines") or []) if str(item).strip()][:3]
    legacy_tone = normalize_whitespace(str(raw.get("tone") or ""))
    legacy_structure = normalize_whitespace(str(raw.get("structure_template") or ""))
    legacy_emotional_progression = normalize_whitespace(str(raw.get("emotional_progression") or ""))
    legacy_pov = normalize_whitespace(str(raw.get("narrative_pov") or ""))
    representative_lines = list(normalized.get("selected_passages") or legacy_lines)
    merged_text = "\n".join(
        [
            normalized["content_summary"],
            *representative_lines,
        ]
    ).strip()
    lexical_markers = legacy_markers or [term for term in top_terms(merged_text, limit=12) if term not in _STOPWORDS][:6]
    derived_tone = " / ".join(
        item
        for item in (
            normalized.get("content_type"),
            normalized.get("emotion_label"),
        )
        if item and item not in {"其他", "不确定"}
    ) or legacy_tone or normalized.get("emotion_label") or normalized.get("content_type") or ""
    return {
        **normalized,
        "document_theme": normalized["content_summary"] or (title or ""),
        "article_theme": normalized["content_summary"] or (title or ""),
        "narrative_pov": legacy_pov or _detect_pov_label(merged_text),
        "tone": derived_tone,
        "structure_template": legacy_structure or normalized["length_label"],
        "lexical_markers": lexical_markers,
        "emotional_progression": (
            normalized["emotion_label"]
            if normalized["emotion_label"] != "不确定" or not legacy_emotional_progression
            else legacy_emotional_progression
        ),
        "nonclinical_signals": legacy_signals or ([normalized["content_type"]] if normalized["content_type"] != "其他" else []),
        "representative_lines": representative_lines,
    }


def build_stone_profile(document: DocumentRecord) -> dict[str, Any]:
    text = normalize_whitespace(document.clean_text or document.raw_text or "")
    return normalize_stone_profile(
        {
            "selected_passages": _pick_selected_passages(text),
        },
        article_text=text,
        fallback_title=document.title or document.filename,
    )


def estimate_word_count(text: str) -> int:
    content = str(text or "")
    cjk_units = sum(1 for char in content if "\u4e00" <= char <= "\u9fff")
    latin_units = len(re.findall(r"[A-Za-z0-9_]+", content))
    return cjk_units + latin_units


def collect_style_markers(guide_payload: dict[str, Any]) -> list[str]:
    markers: list[str] = []
    for key in (
        "voice_dna",
        "motif_theme_bank",
        "worldview_and_stance",
        "do_and_dont",
        "nonclinical_psychodynamics",
        "external_slots",
    ):
        value = guide_payload.get(key)
        if isinstance(value, dict):
            for item in value.values():
                markers.extend(_flatten_text_list(item))
        else:
            markers.extend(_flatten_text_list(value))
    cleaned: list[str] = []
    for item in markers:
        token = str(item or "").strip()
        if len(token) < 2 or token in cleaned:
            continue
        cleaned.append(token)
        if len(cleaned) >= 16:
            break
    return cleaned


def render_writing_request(topic: str, target_word_count: int, extra_requirements: str | None) -> str:
    lines = [
        f"Topic: {topic}",
        f"Target Word Count: {int(target_word_count or 0)}",
    ]
    note = str(extra_requirements or "").strip()
    if note:
        lines.append(f"Extra Requirements: {note}")
    return "\n".join(lines).strip()


def build_stone_profile_messages(
    project_name: str,
    document_title: str | None,
    article_text: str,
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "你正在为 Stone 模式生成单篇文章的极简预分析画像。\n"
                "把整篇文章当成一个整体处理，只返回 JSON，不要输出任何额外解释。\n"
                "必须且只能使用这 5 个 key：content_summary, content_type, length_label, emotion_label, selected_passages。\n"
                "规则如下：\n"
                "1. content_summary：如果文章是短文（300 字/词及以内），不要总结，直接保留原文；如果是长文，再写一句非常简明的内容总结。\n"
                "2. content_type：用很短的自然语言概括文章性质，例如“诉苦”“抽象”“玩笑”“分享”等，但这只是示例，你可以使用更准确的词语。\n"
                "3. length_label：超过 300 字/词必须写“长文”，否则写“短文”。\n"
                "4. emotion_label：用很短的自然语言概括情绪状态，例如“消极”“积极”“不确定”“无情绪表达”等，但这只是示例，你可以使用更准确的词语。\n"
                "5. selected_passages：长文返回最能表达主旨的 2-3 句原文或自然段；短文可以直接返回原文作为 1 条段落。\n"
                "所有 selected_passages 都必须直接摘自原文，不能改写。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目名：{project_name}\n"
                f"文章标题：{document_title or '(untitled)'}\n\n"
                "请基于下面这篇文章，生成极简 Stone 文章画像。\n"
                "再次强调：如果是短文，content_summary 必须直接保留原文，不要总结。\n\n"
                f"文章原文：\n{article_text}"
            ),
        },
    ]


def build_stone_facet_messages(
    project_name: str,
    facet_label: str,
    facet_key: str,
    facet_purpose: str,
    profile_dump: str,
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "你是一个作者风格分析 agent，这一轮只分析一个 facet。\n"
                "你的默认证据来源，是已经提供的逐篇 Stone 文章画像列表。\n"
                "每篇画像都很短，只包含内容、性质、长短、情绪和精选段落。\n"
                "只有在画像信息不足、彼此冲突，或你需要核对风格判断时，才去读取原文。\n"
                "分析范围必须严格限制在当前 facet 内。\n"
                "只返回 JSON，使用这些 key：summary, bullets, confidence, fewshots, conflicts, notes。\n"
                "fewshots 必须是对象数组，每项都包含：document_id, document_title, situation, expression, quote, reason。\n"
                "conflicts 必须是对象数组，每项都包含：title, detail。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目名：{project_name}\n"
                f"当前 facet：{facet_label} ({facet_key})\n"
                f"facet 目标：{facet_purpose}\n"
                f"目标角色：{target_role or project_name}\n"
                f"分析上下文：{analysis_context or ''}\n\n"
                "请先从下面这些文章画像出发。\n"
                "只有当画像不够用、彼此冲突，或者你需要核对某个风格判断时，才去读取原文。\n\n"
                f"文章画像列表：\n{profile_dump}"
            ),
        },
    ]


def summarize_stone_profiles(profiles: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for item in profiles:
        lines.append(
            "\n".join(
                [
                    f"- document_id: {item.get('document_id')}",
                    f"  title: {item.get('title') or '(untitled)'}",
                    f"  content_summary: {item.get('content_summary') or ''}",
                    f"  content_type: {item.get('content_type') or ''}",
                    f"  length_label: {item.get('length_label') or ''}",
                    f"  emotion_label: {item.get('emotion_label') or ''}",
                    f"  selected_passages: {json.dumps(item.get('selected_passages') or [], ensure_ascii=False)}",
                ]
            )
        )
    return "\n\n".join(lines).strip()


def _build_content_summary(text: str, fallback_title: str | None) -> str:
    sentences = _split_sentences(text)
    if not sentences:
        return str(fallback_title or "").strip()
    if len(sentences[0]) >= 54 or len(sentences) == 1:
        return _truncate_text(sentences[0], 88)
    return _truncate_text("；".join(sentences[:2]), 96)


def _resolve_length_label(value: Any, article_text: str | None) -> str:
    if article_text:
        return "长文" if estimate_word_count(article_text) > 300 else "短文"
    normalized = normalize_whitespace(str(value or ""))
    if normalized in STONE_LENGTH_LABELS:
        return normalized
    return "短文"


def _pick_selected_passages(text: str, *, length_label: str | None = None) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []
    resolved_length = length_label or _resolve_length_label(None, normalized)
    if resolved_length == "短文":
        return [normalized]

    paragraphs = [
        normalize_whitespace(item)
        for item in re.split(r"\n\s*\n+", text or "")
        if normalize_whitespace(item)
    ]
    keywords = [term for term in top_terms(normalized, limit=10) if term not in _STOPWORDS][:5]
    target_count = 3 if estimate_word_count(normalized) > 300 else 2

    candidates: list[tuple[int, int, str]] = []
    if len(paragraphs) >= 2:
        for index, paragraph in enumerate(paragraphs):
            unit = paragraph if len(paragraph) <= 220 else _truncate_text(paragraph, 220)
            score = len(unit)
            score += sum(6 for token in keywords if token and token in unit)
            if index == 0:
                score += 10
            candidates.append((score, index, unit))
    else:
        sentences = _split_sentences(normalized)
        for index, sentence in enumerate(sentences):
            if len(sentence) < 14:
                continue
            score = len(sentence)
            score += sum(6 for token in keywords if token and token in sentence)
            if index == 0:
                score += 8
            candidates.append((score, index, sentence))

    selected = sorted(candidates, key=lambda item: item[0], reverse=True)[:target_count]
    selected.sort(key=lambda item: item[1])
    deduped: list[str] = []
    for _, _, passage in selected:
        if passage not in deduped:
            deduped.append(passage)
    return deduped[:target_count]


def _split_sentences(text: str) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[。！？!?])\s+|(?<=[。！？!?])", normalized)
    sentences = [part.strip() for part in parts if part and part.strip()]
    if len(sentences) <= 1:
        sentences = [item.strip() for item in re.split(r"[；;]", normalized) if item.strip()]
    return sentences[:12]


def _detect_pov_label(text: str) -> str:
    if not text:
        return ""
    first_person = sum(text.count(token) for token in ("我", "我们", "自己", "my ", " i "))
    second_person = sum(text.count(token) for token in ("你", "你们", "your ", " you "))
    third_person = sum(text.count(token) for token in ("他", "她", "他们", "她们", "he ", "she "))
    ranking = sorted(
        (
            ("first_person", first_person),
            ("second_person", second_person),
            ("third_person", third_person),
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    if ranking[0][1] <= 0:
        return ""
    return ranking[0][0]


def _truncate_text(text: str, limit: int) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    if limit <= 1:
        return value[:limit]
    return f"{value[: limit - 1]}…"


def _flatten_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        pieces = re.split(r"[，、；;\n]+", value)
        return [piece.strip() for piece in pieces if piece.strip()]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_flatten_text_list(item))
        return flattened
    if isinstance(value, (list, tuple)):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_text_list(item))
        return flattened
    return [str(value).strip()]
