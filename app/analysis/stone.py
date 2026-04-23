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

STONE_CONTENT_TYPES = ("诉苦", "抽象", "玩笑", "分享", "其他")
STONE_EMOTION_LABELS = ("消极", "积极", "不确定", "无情绪表达")
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

_POSITIVE_MARKERS = ("开心", "高兴", "喜欢", "温柔", "轻松", "希望", "庆幸", "幸福", "舒服", "满意")
_NEGATIVE_MARKERS = ("难受", "痛苦", "失望", "崩溃", "烦", "累", "压抑", "糟糕", "委屈", "无语", "绝望")
_JOKE_MARKERS = ("哈哈", "笑死", "玩笑", "调侃", "梗", "离谱", "乐子", "搞笑", "抽象到好笑")
_SHARE_MARKERS = ("分享", "推荐", "记录", "今天", "刚刚", "看到", "经历", "遇到", "想说", "转给")
_ABSTRACT_MARKERS = ("抽象", "意义", "世界", "时间", "关系", "人性", "逻辑", "命运", "现实", "存在")
_COMPLAINT_MARKERS = ("为什么", "受不了", "真的烦", "太累", "委屈", "气死", "糟心", "崩溃", "无奈", "压抑")


def normalize_stone_profile(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(payload or {})
    content_summary = normalize_whitespace(
        str(
            raw.get("content_summary")
            or raw.get("summary")
            or raw.get("content")
            or raw.get("article_theme")
            or ""
        )
    )
    content_type = str(raw.get("content_type") or raw.get("nature") or "").strip()
    if content_type not in STONE_CONTENT_TYPES:
        content_type = "其他"

    length_label = str(raw.get("length_label") or raw.get("length") or "").strip()
    if length_label not in STONE_LENGTH_LABELS:
        length_label = "短文"

    emotion_label = str(raw.get("emotion_label") or raw.get("emotion") or raw.get("tone") or "").strip()
    if emotion_label not in STONE_EMOTION_LABELS:
        emotion_label = "不确定"

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
            "content_summary": _build_content_summary(text, document.title or document.filename),
            "content_type": _detect_content_type(text),
            "length_label": "长文" if estimate_word_count(text) > 300 else "短文",
            "emotion_label": _detect_emotion_label(text),
            "selected_passages": _pick_selected_passages(text),
        }
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
                "You are analyzing one article for a compact per-article Stone preprocess profile.\n"
                "Treat the article as one whole piece and return only JSON.\n"
                "Use exactly these keys: content_summary, content_type, length_label, emotion_label, selected_passages.\n"
                "content_summary: one concise summary, ideally 1-2 sentences.\n"
                "content_type: choose the single best label from 诉苦, 抽象, 玩笑, 分享, 其他.\n"
                "length_label: 长文 when the article exceeds about 300 Chinese characters/words, otherwise 短文.\n"
                "emotion_label: choose one of 消极, 积极, 不确定, 无情绪表达.\n"
                "selected_passages: 2-3 original sentences or natural paragraphs copied from the article that best express the main point."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Project: {project_name}\n"
                f"Article title: {document_title or '(untitled)'}\n\n"
                "Please create a very compact Stone article profile using only the five required fields.\n\n"
                f"Article:\n{article_text}"
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
                "You are an author-style analysis agent working on one facet only.\n"
                "Your default evidence source is the per-article Stone profile list already provided.\n"
                "Each profile is intentionally compact and includes only summary, nature, length, emotion, and selected passages.\n"
                "You may call tools when you need to inspect original article text or verify a pattern.\n"
                "Keep the analysis scoped strictly to the requested facet.\n"
                "Return only JSON with keys: summary, bullets, confidence, fewshots, conflicts, notes.\n"
                "fewshots must be an array of objects containing document_id, document_title, situation, expression, quote, reason.\n"
                "conflicts must be an array of objects containing title and detail."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Project: {project_name}\n"
                f"Facet: {facet_label} ({facet_key})\n"
                f"Facet purpose: {facet_purpose}\n"
                f"Target role: {target_role or project_name}\n"
                f"Analysis context: {analysis_context or ''}\n\n"
                "Start from the article profiles below.\n"
                "Only read original article text when the profile is insufficient, conflicting, or you need to verify a style claim.\n\n"
                f"Article profiles:\n{profile_dump}"
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


def _detect_content_type(text: str) -> str:
    lowered = str(text or "").lower()
    scores = {
        "诉苦": sum(lowered.count(token) for token in _COMPLAINT_MARKERS + _NEGATIVE_MARKERS),
        "抽象": sum(lowered.count(token) for token in _ABSTRACT_MARKERS),
        "玩笑": sum(lowered.count(token) for token in _JOKE_MARKERS),
        "分享": sum(lowered.count(token) for token in _SHARE_MARKERS),
    }
    winner = max(scores.items(), key=lambda item: item[1])
    if winner[1] <= 0:
        return "其他"
    return winner[0]


def _detect_emotion_label(text: str) -> str:
    lowered = str(text or "").lower()
    positive = sum(lowered.count(token) for token in _POSITIVE_MARKERS)
    negative = sum(lowered.count(token) for token in _NEGATIVE_MARKERS)
    if positive == 0 and negative == 0:
        return "无情绪表达"
    if abs(positive - negative) <= 1 and positive + negative >= 2:
        return "不确定"
    return "积极" if positive > negative else "消极"


def _pick_selected_passages(text: str) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []

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
