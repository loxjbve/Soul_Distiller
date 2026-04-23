from __future__ import annotations

import re
from typing import Any


def build_writing_guide_payload_from_facets(
    *,
    project_name: str,
    target_role: str,
    analysis_context: str,
    summary_by_key: dict[str, dict[str, Any]],
    evidence_by_key: dict[str, list[dict[str, Any]]],
    stone_profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    voice_summary = facet_summary(summary_by_key, "voice_signature")
    structure_summary = facet_summary(summary_by_key, "structure_composition")
    imagery_summary = facet_summary(summary_by_key, "imagery_theme")
    stance_summary = facet_summary(summary_by_key, "stance_values")
    emotional_summary = facet_summary(summary_by_key, "emotional_arc")
    return {
        "author_snapshot": compose_profile_section(voice_summary, stance_summary, emotional_summary),
        "voice_dna": {
            "tone_profile": first_nonempty(voice_summary, "restrained, image-led, pressure under the sentence"),
            "signature_phrases": merge_unique(
                facet_bullets(summary_by_key, "lexicon_idiolect", limit=6),
                guide_profile_terms(stone_profiles, "lexical_markers", 6),
                limit=8,
            ),
            "distance_rules": facet_bullets(summary_by_key, "voice_signature", limit=4),
        },
        "sentence_mechanics": {
            "cadence": merge_unique(
                facet_bullets(summary_by_key, "lexicon_idiolect", limit=4),
                facet_bullets(summary_by_key, "structure_composition", limit=4),
                limit=6,
            ),
            "closure_style": first_nonempty(structure_summary, "end on residue rather than neat closure"),
        },
        "structure_patterns": merge_unique(
            facet_bullets(summary_by_key, "structure_composition", limit=6),
            guide_profile_terms(stone_profiles, "structure_template", 4),
            limit=6,
        ),
        "motif_theme_bank": merge_unique(
            facet_bullets(summary_by_key, "imagery_theme", limit=6),
            guide_profile_terms(stone_profiles, "article_theme", 4),
            limit=8,
        ),
        "worldview_and_stance": merge_unique(
            facet_bullets(summary_by_key, "stance_values", limit=6),
            [first_nonempty(stance_summary, imagery_summary)],
            limit=6,
        ),
        "emotional_tendencies": merge_unique(
            facet_bullets(summary_by_key, "emotional_arc", limit=6),
            guide_profile_terms(stone_profiles, "emotional_progression", 4),
            limit=6,
        ),
        "nonclinical_psychodynamics": merge_unique(
            facet_bullets(summary_by_key, "nonclinical_psychodynamics", limit=6),
            guide_profile_terms(stone_profiles, "nonclinical_signals", 6),
            limit=6,
        ),
        "do_and_dont": {
            "do": merge_unique(
                facet_bullets(summary_by_key, "voice_signature", limit=3),
                facet_bullets(summary_by_key, "imagery_theme", limit=3),
                limit=5,
            ),
            "dont": merge_unique(
                facet_bullets(summary_by_key, "creative_constraints", limit=6),
                ["Do not generate clinical diagnosis.", "Do not flatten ambiguity into slogans."],
                limit=6,
            ),
        },
        "topic_translation_rules": merge_unique(
            facet_bullets(summary_by_key, "stance_values", limit=3),
            facet_bullets(summary_by_key, "creative_constraints", limit=3),
            ["Translate each topic through motif, worldview, and felt cost."],
            limit=6,
        ),
        "word_count_strategies": {
            "short": "Open from one motif and close before full explanation.",
            "medium": "Use 3 to 4 paragraphs with one central turn.",
            "long": "Use 4 to 6 paragraphs and preserve tonal consistency over plot novelty.",
        },
        "revision_rubric": [
            "Check recurring diction before checking idea completeness.",
            "Cut generic transitions first.",
            "Verify the topic is translated through worldview, not pasted on top.",
            "Check endings for over-closure.",
        ],
        "fewshot_anchors": normalize_fewshot_anchors(None, stone_profiles),
        "external_slots": {
            "clinical_profile": {},
            "vulnerability_map": {},
            "reserved_external": True,
        },
        "target_role": target_role,
        "source_context": analysis_context,
        "analysis_evidence_count": sum(len(items or []) for items in evidence_by_key.values()),
        "project_name": project_name,
    }


def string_block(value: Any, *, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def normalize_string_list(value: Any, *, fallback: list[str] | None = None, limit: int = 8) -> list[str]:
    items: list[str] = []
    if isinstance(value, str):
        candidates = re.split(r"[，,、；;\n]+", value)
    elif isinstance(value, dict):
        candidates = []
        for item in value.values():
            candidates.extend(normalize_string_list(item, limit=limit))
    elif isinstance(value, (list, tuple)):
        candidates = [str(item).strip() for item in value]
    else:
        candidates = []
    for item in candidates:
        text = str(item or "").strip()
        if text and text not in items:
            items.append(text)
        if len(items) >= limit:
            break
    if items:
        return items
    return [str(item).strip() for item in (fallback or []) if str(item).strip()][:limit]


def normalize_guide_object(value: Any, *, defaults: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        payload = dict(defaults)
        payload.update(
            {
                key: item
                for key, item in value.items()
                if item is not None and item != "" and item != [] and item != {}
            }
        )
        return payload
    return dict(defaults)


def guide_profile_terms(stone_profiles: list[dict[str, Any]], key: str, limit: int) -> list[str]:
    terms: list[str] = []
    for profile in stone_profiles:
        value = profile.get(key)
        candidates = value if isinstance(value, list) else [value]
        for item in candidates:
            text = str(item or "").strip()
            if text and text not in terms:
                terms.append(text)
            if len(terms) >= limit:
                return terms
    return terms


def guide_facet_material(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()][:8]
    if isinstance(value, dict):
        return normalize_string_list(value, limit=8)
    return []


def normalize_fewshot_anchors(value: Any, stone_profiles: list[dict[str, Any]]) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                quote = str(item.get("quote") or item.get("line") or "").strip()
                if not quote:
                    continue
                anchors.append(
                    {
                        "title": str(item.get("title") or item.get("theme") or "anchor").strip() or "anchor",
                        "quote": quote,
                    }
                )
            else:
                quote = str(item or "").strip()
                if quote:
                    anchors.append({"title": "anchor", "quote": quote})
            if len(anchors) >= 6:
                return anchors
    for profile in stone_profiles:
        for quote in profile.get("representative_lines") or []:
            text = str(quote or "").strip()
            if not text:
                continue
            anchors.append({"title": str(profile.get("title") or "article").strip() or "article", "quote": text})
            if len(anchors) >= 6:
                return anchors
    return anchors


def merge_unique(*groups: list[str], limit: int) -> list[str]:
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


def facet_summary(summary_by_key: dict[str, dict[str, Any]], facet_key: str) -> str:
    findings = summary_by_key.get(facet_key) or {}
    return str(findings.get("summary") or "").strip()


def facet_bullets(summary_by_key: dict[str, dict[str, Any]], facet_key: str, *, limit: int = 6) -> list[str]:
    findings = summary_by_key.get(facet_key) or {}
    bullets: list[str] = []
    for item in (findings.get("bullets") or [])[:limit]:
        text = str(item or "").strip()
        if text:
            bullets.append(text)
    return bullets


def first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def compose_profile_section(*parts: Any) -> str:
    blocks: list[str] = []
    for value in parts:
        text = str(value or "").strip()
        if text and text not in blocks:
            blocks.append(text)
    return "\n\n".join(blocks).strip()
