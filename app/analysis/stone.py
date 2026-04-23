from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

from app.models import DocumentRecord
from app.utils.text import normalize_whitespace, top_terms

STONE_PROFILE_KEYS = (
    "article_theme",
    "narrative_pov",
    "tone",
    "structure_template",
    "lexical_markers",
    "emotional_progression",
    "nonclinical_signals",
    "representative_lines",
)

STONE_REVIEW_DIMENSIONS = (
    "style_consistency",
    "structure_and_pacing",
    "lexicon_and_rhythm",
    "theme_and_worldview",
    "originality_and_overlap",
)

_STOPWORDS = {
    "the",
    "and",
    "that",
    "with",
    "this",
    "from",
    "have",
    "were",
    "they",
    "their",
    "there",
    "about",
    "因为",
    "所以",
    "但是",
    "然后",
    "就是",
    "一个",
    "一种",
    "我们",
    "你们",
    "他们",
    "她们",
    "自己",
    "已经",
    "还是",
    "如果",
    "没有",
    "不是",
    "只是",
    "这样",
    "那些",
    "这些",
}

_POSITIVE_MARKERS = ("爱", "喜欢", "温柔", "轻", "亮", "希望", "热", "信任", "靠近", "安静")
_NEGATIVE_MARKERS = ("怕", "疼", "冷", "空", "失去", "疲惫", "压", "羞", "躲", "沉默", "怒")
_BOUNDARY_MARKERS = ("边界", "防备", "回避", "忍住", "克制", "撑住", "警惕", "躲开", "不说", "不想")
_WORLDVIEW_MARKERS = ("世界", "现实", "人", "关系", "价值", "命运", "时间", "生活", "欲望", "秩序")


def normalize_stone_profile(payload: dict[str, Any] | None) -> dict[str, Any]:
    profile = dict(payload or {})
    profile["article_theme"] = str(profile.get("article_theme") or "").strip()
    profile["narrative_pov"] = str(profile.get("narrative_pov") or "").strip()
    profile["tone"] = str(profile.get("tone") or "").strip()
    profile["structure_template"] = str(profile.get("structure_template") or "").strip()
    profile["lexical_markers"] = [str(item).strip() for item in (profile.get("lexical_markers") or []) if str(item).strip()][:8]
    profile["emotional_progression"] = str(profile.get("emotional_progression") or "").strip()
    profile["nonclinical_signals"] = [str(item).strip() for item in (profile.get("nonclinical_signals") or []) if str(item).strip()][:6]
    profile["representative_lines"] = [str(item).strip() for item in (profile.get("representative_lines") or []) if str(item).strip()][:5]
    return profile


def build_stone_profile(document: DocumentRecord) -> dict[str, Any]:
    text = normalize_whitespace(document.clean_text or document.raw_text or "")
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n+", text) if item.strip()]
    lines = [item.strip() for item in re.split(r"[\n。！？!?]", text) if item.strip()]
    keywords = [term for term in top_terms(text, limit=16) if term not in _STOPWORDS][:8]
    theme = "、".join(keywords[:4]) or (document.title or document.filename)

    pov = _detect_pov(text)
    tone = _detect_tone(text)
    structure = _detect_structure(paragraphs)
    emotional_progression = _detect_emotional_progression(paragraphs or [text])
    nonclinical = _detect_nonclinical_signals(text, keywords)
    representative_lines = _pick_representative_lines(lines, keywords)

    return normalize_stone_profile(
        {
            "article_theme": theme,
            "narrative_pov": pov,
            "tone": tone,
            "structure_template": structure,
            "lexical_markers": keywords,
            "emotional_progression": emotional_progression,
            "nonclinical_signals": nonclinical,
            "representative_lines": representative_lines,
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
                "You are analyzing a single article for author-style modeling.\n"
                "Treat the article as one whole piece. Do not split it into chunks.\n"
                "Return only JSON with these keys: article_theme, narrative_pov, tone, structure_template, "
                "lexical_markers, emotional_progression, nonclinical_signals, representative_lines.\n"
                "lexical_markers must be an array of short phrases.\n"
                "nonclinical_signals may include defense patterns, stress reactions, pain points, "
                "candidate pathology cues, and vulnerability markers when they are grounded in the text.\n"
                "representative_lines must be short direct lines copied from the article when possible."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Project: {project_name}\n"
                f"Article title: {document_title or '(untitled)'}\n\n"
                "Analyze this article as a single complete text.\n"
                "Focus on theme, point of view, tone, structure, lexical habits, emotional movement, "
                "and psychological signals including defense patterns, mental-state cues, and pain points.\n\n"
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
                "Your default evidence source is the per-article profile list already provided.\n"
                "You may call tools when you need to inspect original article text or verify a pattern.\n"
                "Keep the analysis scoped strictly to the requested facet.\n"
                "When the facet calls for it, you may infer defense mechanisms, mental-state patterns, "
                "candidate disorders, pain points, and vulnerability maps, but tie every claim back to textual evidence.\n"
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
                    f"  article_theme: {item.get('article_theme') or ''}",
                    f"  narrative_pov: {item.get('narrative_pov') or ''}",
                    f"  tone: {item.get('tone') or ''}",
                    f"  structure_template: {item.get('structure_template') or ''}",
                    f"  lexical_markers: {json.dumps(item.get('lexical_markers') or [], ensure_ascii=False)}",
                    f"  emotional_progression: {item.get('emotional_progression') or ''}",
                    f"  nonclinical_signals: {json.dumps(item.get('nonclinical_signals') or [], ensure_ascii=False)}",
                    f"  representative_lines: {json.dumps(item.get('representative_lines') or [], ensure_ascii=False)}",
                ]
            )
        )
    return "\n\n".join(lines).strip()


def _detect_pov(text: str) -> str:
    scores = {
        "first_person": sum(text.count(token) for token in ("我", "我们", "自己", "my ", " i ")),
        "second_person": sum(text.count(token) for token in ("你", "你们", "your ", " you ")),
        "third_person": sum(text.count(token) for token in ("他", "她", "他们", "她们", "其", "he ", "she ")),
    }
    ranking = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    if ranking[0][1] == 0:
        return "mixed_or_oblique"
    if len(ranking) > 1 and ranking[0][1] == ranking[1][1]:
        return "mixed_or_shifting"
    return ranking[0][0]


def _detect_tone(text: str) -> str:
    positive = sum(text.count(token) for token in _POSITIVE_MARKERS)
    negative = sum(text.count(token) for token in _NEGATIVE_MARKERS)
    question = text.count("?") + text.count("？")
    exclaim = text.count("!") + text.count("！")
    if negative >= positive + 2:
        base = "restrained_and_heavy"
    elif positive >= negative + 2:
        base = "warm_with_brightness"
    else:
        base = "cool_and_observational"
    if question >= 3:
        return f"{base}; interrogative"
    if exclaim >= 3:
        return f"{base}; emphatic"
    return base


def _detect_structure(paragraphs: list[str]) -> str:
    paragraph_count = len([item for item in paragraphs if item.strip()])
    if paragraph_count <= 1:
        return "single_breath_reflection"
    if paragraph_count == 2:
        return "setup_then_turn"
    if paragraph_count >= 6:
        return "multi_section_progression_with_delayed_closure"
    return "scene_or_claim_then_expansion_then_close"


def _detect_emotional_progression(paragraphs: list[str]) -> str:
    if not paragraphs:
        return "flat_or_implicit"
    left = paragraphs[0]
    right = paragraphs[-1]
    left_score = _emotion_score(left)
    right_score = _emotion_score(right)
    if left_score < right_score - 1:
        return "starts_restrained_then_rises"
    if left_score > right_score + 1:
        return "starts_heavy_then_releases"
    return "steady_pressure_with_small_turns"


def _emotion_score(text: str) -> int:
    return sum(text.count(token) for token in _POSITIVE_MARKERS) - sum(text.count(token) for token in _NEGATIVE_MARKERS)


def _detect_nonclinical_signals(text: str, keywords: list[str]) -> list[str]:
    signals: list[str] = []
    boundary_hits = [token for token in _BOUNDARY_MARKERS if token in text]
    if boundary_hits:
        signals.append(f"边界/防御线索偏强：{ '、'.join(boundary_hits[:4]) }")
    worldview_hits = [token for token in _WORLDVIEW_MARKERS if token in text]
    if worldview_hits:
        signals.append(f"常把情绪落回现实或价值判断：{ '、'.join(worldview_hits[:4]) }")
    if not signals and keywords:
        signals.append(f"主要通过母题和词汇惯性暴露压力源：{ '、'.join(keywords[:3]) }")
    return signals[:4]


def _pick_representative_lines(lines: list[str], keywords: list[str]) -> list[str]:
    if not lines:
        return []
    scored: list[tuple[int, str]] = []
    for line in lines:
        text = line.strip()
        if len(text) < 8:
            continue
        score = len(text)
        score += sum(4 for token in keywords[:4] if token and token in text)
        scored.append((score, text))
    scored.sort(key=lambda item: item[0], reverse=True)
    picked: list[str] = []
    for _, item in scored:
        if item in picked:
            continue
        picked.append(item)
        if len(picked) >= 3:
            break
    return picked


def _flatten_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        pieces = re.split(r"[，,、；;。\n]+", value)
        return [piece.strip() for piece in pieces if piece.strip()]
    if isinstance(value, dict):
        flattened: list[str] = []
        for item in value.values():
            flattened.extend(_flatten_text_list(item))
        return flattened
    if isinstance(value, (list, tuple)):
        flattened = []
        for item in value:
            flattened.extend(_flatten_text_list(item))
        return flattened
    return [str(value).strip()]
