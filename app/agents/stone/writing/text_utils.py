from __future__ import annotations

import re
from typing import Any

from app.analysis.stone import estimate_word_count


def _fit_word_count(
    text: str,
    target_word_count: int,
    analysis_bundle: Any,
    topic: str,
    extra_requirements: str | None,
) -> str:
    del target_word_count, analysis_bundle, topic, extra_requirements
    return str(text or "").strip()
    target = max(100, int(target_word_count or 0))
    lower = int(target * 0.9)
    upper = int(target * 1.05)
    current = estimate_word_count(text)

    while current < lower:
        text = f"{text}\n\n{_expansion_paragraph(analysis_bundle, topic, extra_requirements)}".strip()
        next_count = estimate_word_count(text)
        if next_count <= current:
            break
        current = next_count

    if current <= upper:
        return text.strip()

    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]
    while paragraphs and estimate_word_count("\n\n".join(paragraphs)) > upper:
        if len(paragraphs[-1]) > 120:
            paragraphs[-1] = paragraphs[-1][:-40].rstrip("，。；：!?")
            if paragraphs[-1] and paragraphs[-1][-1] not in "。！？":
                paragraphs[-1] = f"{paragraphs[-1]}。"
        else:
            paragraphs.pop()
    trimmed = "\n\n".join(paragraphs).strip()
    return trimmed or text.strip()


def _expansion_paragraph(
    analysis_bundle: Any,
    topic: str,
    extra_requirements: str | None,
) -> str:
    imagery_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "imagery_theme"))[:2], fallback="旧物和夜色")
    stance_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "stance_values"))[:2], fallback="代价与边界")
    emotion_hint = _join_terms(_facet_terms(_facet_lookup(analysis_bundle, "emotional_arc"))[:2], fallback="克制和回落")
    note = f" 同时继续守住“{extra_requirements}”这个要求。" if extra_requirements else ""
    return (
        f"{topic}真正难写的地方，不在于事件本身，而在于它总会重新碰到{imagery_hint}，"
        f"再把{stance_hint}慢慢照亮。视角只要再往里收一层，情绪就会回到{emotion_hint}这条线上。{note}"
    ).strip()


def _facet_lookup(bundle: Any, key: str) -> Any | None:
    facets = list(getattr(bundle, "facets", []) or [])
    return next((item for item in facets if getattr(item, "key", None) == key), None)


def _facet_terms(facet: Any | None) -> list[str]:
    if not facet:
        return []
    candidates: list[str] = []
    for text in [getattr(facet, "summary", ""), *(getattr(facet, "bullets", []) or [])]:
        candidates.extend(_extract_terms(text))
    for item in getattr(facet, "fewshots", []) or []:
        if not isinstance(item, dict):
            continue
        candidates.extend(_extract_terms(item.get("expression")))
        candidates.extend(_extract_terms(item.get("quote")))
    return _unique_preserve_order(candidates)[:10]


def _extract_terms(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return re.findall(r"[\u4e00-\u9fffA-Za-z0-9]{2,}", text)[:12]


def _light_trim_to_word_count(text: str, target_word_count: int) -> str:
    text = str(text or "").strip()
    target = max(100, int(target_word_count or 0))
    upper = int(target * 1.08)
    if estimate_word_count(text) <= upper:
        return text.strip()
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]
    while len(paragraphs) > 1 and estimate_word_count("\n\n".join(paragraphs)) > upper:
        last = paragraphs[-1]
        if len(last) <= 80:
            paragraphs.pop()
            continue
        paragraphs[-1] = last[:-40].rstrip("，。；：!?")
        if paragraphs[-1] and paragraphs[-1][-1] not in "。！？":
            paragraphs[-1] = f"{paragraphs[-1]}。"
        break
    return "\n\n".join(paragraphs).strip() or text.strip()


def _join_terms(terms: list[str], *, fallback: str) -> str:
    cleaned = [item for item in terms if item]
    if not cleaned:
        return fallback
    return "、".join(cleaned[:2])


def _unique_preserve_order(values) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


fit_word_count = _fit_word_count
light_trim_to_word_count = _light_trim_to_word_count

__all__ = [
    "_fit_word_count",
    "_light_trim_to_word_count",
    "fit_word_count",
    "light_trim_to_word_count",
]
