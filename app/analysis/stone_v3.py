from __future__ import annotations

import json
import hashlib
import logging
import math
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable

from app.llm import OpenAICompatibleClient, parse_json_response
from app.schemas import ServiceConfig
from app.utils.text import normalize_whitespace, token_count

STONE_V3_PROFILE_KEY = "stone_profile_v3"
STONE_V3_ASSET_KINDS = ("stone_author_model_v3", "stone_prototype_index_v3")
STONE_V3_LENGTH_BANDS = ("micro", "short", "medium", "long")
STONE_V3_SURFACE_FORMS = (
    "scene_vignette",
    "rant",
    "confession",
    "anecdote",
    "aphorism",
    "dialogue_bit",
    "manifesto",
    "list_bit",
)
STONE_V3_FAMILY_BATCH_SIZE = 20
STONE_V3_PROTOTYPE_BATCH_SIZE = 20
STONE_V3_BATCH_CONCURRENCY = 4
STONE_V3_MAX_RETRIES = 3
STONE_V3_STAGE_TIMEOUT_SECONDS = 110.0
STONE_V3_PROMPT_TOKEN_BUDGET = 128000
STONE_V3_PROFILE_CHUNK_TOKEN_BUDGET = 24000
STONE_V3_PROFILE_CHUNK_OVERLAP_CHARS = 280
STONE_V3_CRITIC_SHARD_SIZE = 48
STONE_V3_CHECKPOINT_VERSION = "stone_v3_baseline_checkpoint_v1"

logger = logging.getLogger(__name__)

StoneV3ProgressCallback = Callable[[dict[str, Any]], None]
StoneV3CancelRequested = Callable[[], bool]
StoneV3CheckpointCallback = Callable[[dict[str, Any]], None]

_STOPWORDS = {
    "",
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "have",
    "from",
    "into",
    "about",
    "thing",
    "people",
    "自己",
    "我们",
    "他们",
    "你们",
    "这个",
    "那个",
    "一种",
    "已经",
    "还是",
    "因为",
    "所以",
    "就是",
    "不是",
    "没有",
    "不会",
    "然后",
    "时候",
    "一下",
    "东西",
}


def estimate_word_count(text: str) -> int:
    content = str(text or "")
    cjk_units = sum(1 for char in content if "\u4e00" <= char <= "\u9fff")
    latin_units = len(re.findall(r"[A-Za-z0-9_]+", content))
    return cjk_units + latin_units


def estimate_stone_prompt_tokens(value: Any) -> int:
    content = str(value or "")
    if not content:
        return 0
    cjk_units = sum(1 for char in content if "\u4e00" <= char <= "\u9fff")
    ascii_alnum_units = sum(1 for char in content if char.isascii() and char.isalnum())
    whitespace_units = sum(1 for char in content if char.isspace())
    punctuation_units = max(0, len(content) - cjk_units - ascii_alnum_units - whitespace_units)
    rough_units = cjk_units + math.ceil(ascii_alnum_units / 3.5) + math.ceil(punctuation_units / 2.5)
    return max(token_count(content), rough_units)


def _normalize_short_text(value: Any, *, limit: int = 240) -> str:
    text = normalize_whitespace(str(value or ""))
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _trim_text(value: Any, limit: int) -> str:
    return _normalize_short_text(value, limit=limit)


def _normalize_string_list(value: Any, *, limit: int = 8, item_limit: int = 40) -> list[str]:
    if isinstance(value, str):
        items = re.split(r"[\n,，;；、|]+", value)
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_short_text(item, limit=item_limit)
        if not text:
            continue
        if text in seen:
            continue
        normalized.append(text)
        seen.add(text)
        if len(normalized) >= limit:
            break
    return normalized


def _unique_preserve_order(values: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    items: list[Any] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        items.append(value)
    return items


def _split_sentences(text: str) -> list[str]:
    source = normalize_whitespace(text)
    if not source:
        return []
    parts = re.split(r"(?<=[。！？!?\.])\s+|\n+", source)
    sentences = [_normalize_short_text(part, limit=480) for part in parts]
    return [item for item in sentences if item]


def _find_text_boundary(text: str, start: int, end: int) -> int:
    if end >= len(text):
        return len(text)
    min_index = start + max(240, (end - start) // 2)
    for marker in ("\n\n", "\n", "。", "！", "？", ".", "!", "?", ";", "；", "，", ",", " "):
        marker_index = text.rfind(marker, min_index, end)
        if marker_index != -1:
            return marker_index + len(marker)
    return end


def split_text_for_stone_budget(
    text: str,
    *,
    token_budget: int = STONE_V3_PROFILE_CHUNK_TOKEN_BUDGET,
    overlap_chars: int = STONE_V3_PROFILE_CHUNK_OVERLAP_CHARS,
) -> list[str]:
    source = normalize_whitespace(text)
    if not source:
        return []
    if estimate_stone_prompt_tokens(source) <= token_budget:
        return [source]

    chunks: list[str] = []
    start = 0
    max_chars = max(2400, token_budget * 2)
    min_chars = 600
    while start < len(source):
        end = min(len(source), start + max_chars)
        while end > start + min_chars and estimate_stone_prompt_tokens(source[start:end]) > token_budget:
            end = start + max(min_chars, int((end - start) * 0.82))
        boundary = _find_text_boundary(source, start, end)
        if boundary <= start:
            boundary = end
        chunk = source[start:boundary].strip()
        if not chunk:
            break
        chunks.append(chunk)
        if boundary >= len(source):
            break
        start = max(boundary - overlap_chars, start + 1)
    return chunks or [source]


def _extract_keyword_candidates(*values: Any, limit: int = 12) -> list[str]:
    tokens: Counter[str] = Counter()
    for value in values:
        for match in re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,24}|[\u4e00-\u9fff]{1,4}", str(value or "")):
            token = normalize_whitespace(match).lower()
            if not token or token in _STOPWORDS or token.isdigit():
                continue
            tokens[token] += 1
    return [token for token, _ in tokens.most_common(limit)]


def _derive_anchor_windows(text: str) -> dict[str, Any]:
    sentences = _split_sentences(text)
    if not sentences:
        return {
            "opening": "",
            "pivot": "",
            "closing": "",
            "signature_lines": [],
        }
    opening = sentences[0]
    closing = sentences[-1]
    pivot = sentences[len(sentences) // 2] if len(sentences) >= 3 else ""
    signature_lines = _unique_preserve_order([opening, pivot, closing])[:3]
    return {
        "opening": opening,
        "pivot": pivot if pivot not in {opening, closing} else "",
        "closing": closing,
        "signature_lines": signature_lines,
    }


def _resolve_length_band(text: str, raw_value: Any = None) -> str:
    normalized = _normalize_short_text(raw_value, limit=24).lower()
    if normalized in STONE_V3_LENGTH_BANDS:
        return normalized
    word_count = estimate_word_count(text)
    if word_count <= 120:
        return "micro"
    if word_count <= 320:
        return "short"
    if word_count <= 900:
        return "medium"
    return "long"


def _resolve_surface_form(text: str, raw_value: Any = None) -> str:
    normalized = _normalize_short_text(raw_value, limit=32).lower()
    if normalized in STONE_V3_SURFACE_FORMS:
        return normalized
    source = str(text or "")
    if "\n1." in source or "\n- " in source or "\n•" in source:
        return "list_bit"
    if "“" in source or "”" in source or ":" in source:
        return "dialogue_bit"
    if estimate_word_count(source) <= 140:
        return "aphorism"
    return "scene_vignette"


def normalize_stone_profile_v3(
    payload: dict[str, Any] | None,
    *,
    article_text: str | None = None,
    fallback_title: str | None = None,
    document_id: str | None = None,
    source_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = dict(payload or {})
    text = normalize_whitespace(
        str(
            article_text
            or raw.get("article_text")
            or raw.get("source_text")
            or raw.get("raw_text")
            or ""
        )
    )
    anchors = _derive_anchor_windows(text)
    title = _normalize_short_text(raw.get("title") or fallback_title, limit=120)
    word_count = estimate_word_count(text)
    length_band = _resolve_length_band(
        text,
        ((raw.get("document_core") or {}) if isinstance(raw.get("document_core"), dict) else {}).get("length_band")
        or raw.get("length_band"),
    )
    surface_form = _resolve_surface_form(
        text,
        ((raw.get("document_core") or {}) if isinstance(raw.get("document_core"), dict) else {}).get("surface_form")
        or raw.get("surface_form"),
    )
    summary = _normalize_short_text(
        ((raw.get("document_core") or {}) if isinstance(raw.get("document_core"), dict) else {}).get("summary")
        or raw.get("content_kernel")
        or anchors["opening"]
        or title
        or text,
        limit=220,
    )
    motif_tags = _normalize_string_list(
        ((raw.get("motif_and_scene_bank") or {}) if isinstance(raw.get("motif_and_scene_bank"), dict) else {}).get("motif_tags")
        or raw.get("motif_tags")
        or _extract_keyword_candidates(title, text, limit=8),
        limit=6,
        item_limit=24,
    )
    scene_terms = _normalize_string_list(
        ((raw.get("motif_and_scene_bank") or {}) if isinstance(raw.get("motif_and_scene_bank"), dict) else {}).get("scene_terms")
        or _extract_keyword_candidates(text, limit=10),
        limit=6,
        item_limit=24,
    )
    lexicon_markers = _normalize_string_list(
        ((raw.get("motif_and_scene_bank") or {}) if isinstance(raw.get("motif_and_scene_bank"), dict) else {}).get("lexicon_markers")
        or raw.get("lexicon_markers")
        or _extract_keyword_candidates(text, limit=12),
        limit=8,
        item_limit=30,
    )
    retrieval_keywords = _normalize_string_list(
        ((raw.get("retrieval_handles") or {}) if isinstance(raw.get("retrieval_handles"), dict) else {}).get("keywords")
        or _extract_keyword_candidates(title, text, limit=16),
        limit=12,
        item_limit=24,
    )
    value_and_judgment = (raw.get("value_and_judgment") if isinstance(raw.get("value_and_judgment"), dict) else {})
    voice_contract = (raw.get("voice_contract") if isinstance(raw.get("voice_contract"), dict) else {})
    structure_moves = (raw.get("structure_moves") if isinstance(raw.get("structure_moves"), dict) else {})
    prototype_affordances = (raw.get("prototype_affordances") if isinstance(raw.get("prototype_affordances"), dict) else {})
    retrieval_handles = (raw.get("retrieval_handles") if isinstance(raw.get("retrieval_handles"), dict) else {})
    normalized = {
        "document_id": _normalize_short_text(document_id or raw.get("document_id"), limit=80),
        "title": title,
        "document_core": {
            "title": title,
            "summary": summary,
            "length_band": length_band,
            "surface_form": surface_form,
            "dominant_theme": _normalize_short_text(
                ((raw.get("document_core") or {}) if isinstance(raw.get("document_core"), dict) else {}).get("dominant_theme")
                or summary,
                limit=120,
            ),
            "word_count": word_count,
        },
        "voice_contract": {
            "person": _normalize_short_text(voice_contract.get("person") or raw.get("person"), limit=24) or "first",
            "address_target": _normalize_short_text(voice_contract.get("address_target"), limit=24) or "self",
            "distance": _normalize_short_text(voice_contract.get("distance"), limit=24) or "回收",
            "self_position": _normalize_short_text(voice_contract.get("self_position"), limit=24) or "none",
            "cadence": _normalize_short_text(voice_contract.get("cadence") or raw.get("cadence"), limit=32) or "restrained",
            "sentence_shape": _normalize_short_text(voice_contract.get("sentence_shape"), limit=32) or "mixed",
            "tone_words": _normalize_string_list(voice_contract.get("tone_words"), limit=6, item_limit=20)
            or _normalize_string_list([surface_form, "restrained", "residue"], limit=6, item_limit=20),
        },
        "structure_moves": {
            "opening_move": _normalize_short_text(structure_moves.get("opening_move") or raw.get("opening_move"), limit=80)
            or "Start from a concrete gesture or object.",
            "development_move": _normalize_short_text(structure_moves.get("development_move"), limit=80)
            or "Let pressure rise through concrete detail rather than explanation.",
            "turning_move": _normalize_short_text(structure_moves.get("turning_move") or raw.get("turning_move"), limit=80)
            or "none",
            "closure_move": _normalize_short_text(structure_moves.get("closure_move") or raw.get("closure_move"), limit=80)
            or "Leave residue instead of summary.",
            "paragraph_strategy": _normalize_short_text(structure_moves.get("paragraph_strategy"), limit=80)
            or ("1-2 paragraphs" if length_band in {"micro", "short"} else "2-4 paragraphs"),
        },
        "motif_and_scene_bank": {
            "motif_tags": motif_tags[:6],
            "scene_terms": scene_terms[:6],
            "sensory_terms": _normalize_string_list(
                ((raw.get("motif_and_scene_bank") or {}) if isinstance(raw.get("motif_and_scene_bank"), dict) else {}).get("sensory_terms"),
                limit=6,
                item_limit=24,
            )
            or _normalize_string_list(retrieval_keywords[:6], limit=6, item_limit=24),
            "lexicon_markers": lexicon_markers[:8],
        },
        "value_and_judgment": {
            "judgment_target": _normalize_short_text(value_and_judgment.get("judgment_target"), limit=80) or "local relation",
            "judgment_mode": _normalize_short_text(value_and_judgment.get("judgment_mode"), limit=40)
            or _normalize_short_text(raw.get("judgment"), limit=40)
            or "悬置",
            "value_lens": _normalize_short_text(value_and_judgment.get("value_lens"), limit=40)
            or _normalize_short_text(raw.get("value_lens"), limit=40)
            or "代价",
            "felt_cost": _normalize_short_text(value_and_judgment.get("felt_cost"), limit=120)
            or "Translate pressure into felt cost before explicit explanation.",
        },
        "prototype_affordances": {
            "prototype_family": _normalize_short_text(
                prototype_affordances.get("prototype_family") or raw.get("prototype_family"),
                limit=120,
            )
            or f"{surface_form}|{length_band}|{(motif_tags or ['none'])[0]}",
            "cluster_hint": _normalize_short_text(prototype_affordances.get("cluster_hint"), limit=120)
            or f"{surface_form}|{(motif_tags or ['none'])[0]}|{normalized_voice_distance(voice_contract) or '回收'}",
            "suitable_for": _normalize_string_list(prototype_affordances.get("suitable_for"), limit=6, item_limit=28)
            or _normalize_string_list([surface_form, length_band, *motif_tags[:2]], limit=6, item_limit=28),
            "anti_drift_focus": _normalize_string_list(prototype_affordances.get("anti_drift_focus"), limit=6, item_limit=36)
            or ["Do not turn the piece into explanation.", "Keep the closure unresolved."],
        },
        "anchor_windows": {
            "opening": _normalize_short_text(((raw.get("anchor_windows") or {}) if isinstance(raw.get("anchor_windows"), dict) else {}).get("opening") or anchors["opening"], limit=260),
            "pivot": _normalize_short_text(((raw.get("anchor_windows") or {}) if isinstance(raw.get("anchor_windows"), dict) else {}).get("pivot") or anchors["pivot"], limit=260),
            "closing": _normalize_short_text(((raw.get("anchor_windows") or {}) if isinstance(raw.get("anchor_windows"), dict) else {}).get("closing") or anchors["closing"], limit=260),
            "signature_lines": _normalize_string_list(
                ((raw.get("anchor_windows") or {}) if isinstance(raw.get("anchor_windows"), dict) else {}).get("signature_lines")
                or anchors["signature_lines"],
                limit=3,
                item_limit=260,
            ),
        },
        "retrieval_handles": {
            "keywords": retrieval_keywords[:12],
            "routing_text": _normalize_short_text(
                retrieval_handles.get("routing_text")
                or raw.get("routing_text")
                or " ".join(_unique_preserve_order([summary, *motif_tags[:4], *retrieval_keywords[:4]])),
                limit=260,
            ),
            "routing_facets": {
                "surface_form": surface_form,
                "length_band": length_band,
                "value_lens": _normalize_short_text(value_and_judgment.get("value_lens"), limit=40)
                or _normalize_short_text(raw.get("value_lens"), limit=40)
                or "代价",
                "judgment_mode": _normalize_short_text(value_and_judgment.get("judgment_mode"), limit=40)
                or _normalize_short_text(raw.get("judgment"), limit=40)
                or "悬置",
                "distance": normalized_voice_distance(voice_contract) or "回收",
                "motif_tags": motif_tags[:4],
            },
        },
        "anti_patterns": _normalize_string_list(raw.get("anti_patterns"), limit=8, item_limit=48)
        or ["Do not explain the writing process.", "Do not flatten the piece into summary prose."],
        "evidence_trace": {
            "source_meta": dict(source_meta or raw.get("source_meta") or {}),
            "extracted_from_model": bool(payload),
            "normalization_version": "stone_profile_v3",
        },
    }
    return normalized


def normalized_voice_distance(voice_contract: dict[str, Any]) -> str:
    return _normalize_short_text(voice_contract.get("distance"), limit=24)


def compact_stone_profile_v3(profile: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_stone_profile_v3(profile)
    return {
        "document_id": normalized.get("document_id"),
        "title": normalized.get("title"),
        "summary": (((normalized.get("document_core") or {}).get("summary")) or ""),
        "length_band": (((normalized.get("document_core") or {}).get("length_band")) or ""),
        "surface_form": (((normalized.get("document_core") or {}).get("surface_form")) or ""),
        "prototype_family": (((normalized.get("prototype_affordances") or {}).get("prototype_family")) or ""),
        "value_lens": (((normalized.get("value_and_judgment") or {}).get("value_lens")) or ""),
        "judgment_mode": (((normalized.get("value_and_judgment") or {}).get("judgment_mode")) or ""),
        "distance": (((normalized.get("voice_contract") or {}).get("distance")) or ""),
        "motif_tags": list(((normalized.get("motif_and_scene_bank") or {}).get("motif_tags")) or [])[:4],
        "keywords": list(((normalized.get("retrieval_handles") or {}).get("keywords")) or [])[:8],
        "opening": _trim_text(((normalized.get("anchor_windows") or {}).get("opening")), 180),
        "closing": _trim_text(((normalized.get("anchor_windows") or {}).get("closing")), 180),
    }


def build_stone_profile_v3_messages(
    project_name: str,
    document_title: str | None,
    article_text: str,
) -> list[dict[str, str]]:
    schema = {
        "document_core": {
            "summary": "One concise kernel grounded in the actual text",
            "length_band": "micro|short|medium|long",
            "surface_form": "|".join(STONE_V3_SURFACE_FORMS),
            "dominant_theme": "Main lived theme",
        },
        "voice_contract": {
            "person": "first|second|third|mixed",
            "address_target": "self|you|crowd|specific_other|none",
            "distance": "贴脸|回收|旁观|宣判",
            "self_position": "自损|自嘲|冷眼|求稳|none",
            "cadence": "short label",
            "sentence_shape": "short label",
            "tone_words": ["up to 6"],
        },
        "structure_moves": {
            "opening_move": "How it enters",
            "development_move": "How pressure builds",
            "turning_move": "How it turns or none",
            "closure_move": "How it closes",
            "paragraph_strategy": "Short note",
        },
        "motif_and_scene_bank": {
            "motif_tags": ["up to 6"],
            "scene_terms": ["up to 6"],
            "sensory_terms": ["up to 6"],
            "lexicon_markers": ["up to 8"],
        },
        "value_and_judgment": {
            "judgment_target": "who/what is judged",
            "judgment_mode": "short label",
            "value_lens": "short label",
            "felt_cost": "what the lived cost is",
        },
        "prototype_affordances": {
            "prototype_family": "stable family label",
            "cluster_hint": "short retrieval hint",
            "suitable_for": ["up to 6"],
            "anti_drift_focus": ["up to 6"],
        },
        "anchor_windows": {
            "opening": "verbatim quote",
            "pivot": "verbatim quote or empty string",
            "closing": "verbatim quote",
            "signature_lines": ["1-3 verbatim quotes"],
        },
        "retrieval_handles": {
            "keywords": ["up to 12"],
            "routing_text": "compact routing text",
        },
        "anti_patterns": ["up to 8"],
    }
    return [
        {
            "role": "system",
            "content": (
                "You are generating a Stone v3 document profile.\n"
                "Return JSON only.\n"
                "Keep every anchor window verbatim from the source text.\n"
                "Do not output diagnosis, pathology labels, or writing advice.\n"
                f"Use this schema exactly: {json.dumps(schema, ensure_ascii=False)}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Project: {project_name}\n"
                f"Document title: {document_title or '(untitled)'}\n\n"
                "Build a Stone v3 profile for the following article.\n\n"
                f"Article text:\n{article_text}"
            ),
        },
    ]


def build_stone_profile_v3_merge_messages(
    project_name: str,
    document_title: str | None,
    chunk_profiles: list[dict[str, Any]],
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You are merging chunk-level Stone v3 profiles for one long document.\n"
                "Return JSON only.\n"
                "Synthesize one final Stone v3 document profile with keys document_core, voice_contract, "
                "structure_moves, motif_and_scene_bank, value_and_judgment, prototype_affordances, "
                "anchor_windows, retrieval_handles, anti_patterns, and evidence_trace.\n"
                "Do not mention chunking or summarize the writing process."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Project: {project_name}\n"
                f"Document title: {document_title or '(untitled)'}\n\n"
                "These are chunk-level Stone v3 profiles for one long document.\n"
                "Merge them into one corpus-grounded final profile.\n\n"
                f"Chunk profiles JSON:\n{json.dumps(chunk_profiles, ensure_ascii=False, indent=2)}"
            ),
        },
    ]


def normalize_stone_author_model_v3(
    payload: dict[str, Any] | None,
    *,
    project_name: str,
    profiles: list[dict[str, Any]],
    families: list[dict[str, Any]],
) -> dict[str, Any]:
    raw = dict(payload or {})
    family_map = [
        {
            "family_id": _normalize_short_text(item.get("family_id"), limit=40) or f"family-{index}",
            "label": _normalize_short_text(item.get("label"), limit=80) or _normalize_short_text(item.get("family_key"), limit=80) or f"Family {index}",
            "description": _normalize_short_text(item.get("description"), limit=180),
            "selection_cues": _normalize_string_list(item.get("selection_cues"), limit=6, item_limit=40),
            "motif_tags": _normalize_string_list(item.get("motif_tags"), limit=4, item_limit=24),
            "member_count": int(item.get("member_count") or 0),
        }
        for index, item in enumerate(families, start=1)
    ]
    top_motifs = Counter(
        motif
        for profile in profiles
        for motif in (((profile.get("motif_and_scene_bank") or {}).get("motif_tags")) or [])
        if motif
    )
    surface_counter = Counter(
        (((profile.get("document_core") or {}).get("surface_form")) or "")
        for profile in profiles
        if (((profile.get("document_core") or {}).get("surface_form")) or "")
    )
    value_counter = Counter(
        (((profile.get("value_and_judgment") or {}).get("value_lens")) or "")
        for profile in profiles
        if (((profile.get("value_and_judgment") or {}).get("value_lens")) or "")
    )
    evidence = [
        {
            "document_id": profile.get("document_id"),
            "title": profile.get("title"),
            "summary": _trim_text(((profile.get("document_core") or {}).get("summary")), 160),
            "opening": _trim_text(((profile.get("anchor_windows") or {}).get("opening")), 160),
            "closing": _trim_text(((profile.get("anchor_windows") or {}).get("closing")), 160),
        }
        for profile in profiles[:10]
    ]
    translation_rules = []
    for value_lens, _ in value_counter.most_common(6):
        matching = [
            profile
            for profile in profiles
            if (((profile.get("value_and_judgment") or {}).get("value_lens")) or "") == value_lens
        ]
        translation_rules.append(
            {
                "value_lens": value_lens,
                "preferred_motifs": _unique_preserve_order(
                    [motif for profile in matching for motif in (((profile.get("motif_and_scene_bank") or {}).get("motif_tags")) or [])]
                )[:4],
                "opening_moves": _unique_preserve_order(
                    [((profile.get("structure_moves") or {}).get("opening_move")) for profile in matching if ((profile.get("structure_moves") or {}).get("opening_move"))]
                )[:4],
                "closure_moves": _unique_preserve_order(
                    [((profile.get("structure_moves") or {}).get("closure_move")) for profile in matching if ((profile.get("structure_moves") or {}).get("closure_move"))]
                )[:4],
            }
        )
    author_core = raw.get("author_core") if isinstance(raw.get("author_core"), dict) else {}
    critic_rubrics = raw.get("critic_rubrics") if isinstance(raw.get("critic_rubrics"), dict) else {}
    normalized = {
        "asset_kind": "stone_author_model_v3",
        "version": "v3",
        "project_name": project_name,
        "profile_count": len(profiles),
        "family_count": len(family_map),
        "author_core": {
            "voice_summary": _normalize_short_text(author_core.get("voice_summary"), limit=180)
            or f"Primary forms: {', '.join(label for label, _ in surface_counter.most_common(3)) or 'scene_vignette'}",
            "worldview_summary": _normalize_short_text(author_core.get("worldview_summary"), limit=180)
            or f"Dominant lenses: {', '.join(label for label, _ in value_counter.most_common(3)) or '代价'}",
            "tone_summary": _normalize_short_text(author_core.get("tone_summary"), limit=180)
            or "Prefers concrete pressure, restrained tone, and residue over summary.",
            "signature_motifs": _normalize_string_list(author_core.get("signature_motifs"), limit=6, item_limit=24)
            or [motif for motif, _ in top_motifs.most_common(6)],
        },
        "translation_rules": list(raw.get("translation_rules") or [])[:8] or translation_rules,
        "stable_moves": _normalize_string_list(raw.get("stable_moves"), limit=8, item_limit=72)
        or [
            "Open from a concrete action, object, or scene.",
            "Let pressure rise from visible detail.",
            "Keep closure unresolved when possible.",
        ],
        "forbidden_moves": _normalize_string_list(raw.get("forbidden_moves"), limit=8, item_limit=72)
        or [
            "Do not turn the piece into explanation.",
            "Do not flatten the ending into summary or thesis.",
            "Do not leak backstage prompt language.",
        ],
        "family_map": family_map,
        "critic_rubrics": {
            "formal_fidelity": _normalize_string_list(critic_rubrics.get("formal_fidelity"), limit=6, item_limit=72)
            or ["Match the author's concrete entry move and residue-heavy closure."],
            "worldview_translation": _normalize_string_list(critic_rubrics.get("worldview_translation"), limit=6, item_limit=72)
            or ["Translate the request into the author's value lens instead of pasting topic words on top."],
            "syntheticness": _normalize_string_list(critic_rubrics.get("syntheticness"), limit=6, item_limit=72)
            or ["Reject checklist prose, abstract labels, and explanation-heavy sentences."],
        },
        "global_evidence": list(raw.get("global_evidence") or [])[:12] or evidence,
    }
    return normalized


def normalize_stone_prototype_index_v3(
    payload: dict[str, Any] | None,
    *,
    project_name: str,
    profiles: list[dict[str, Any]],
    documents: list[dict[str, Any]],
    families: list[dict[str, Any]],
) -> dict[str, Any]:
    raw = dict(payload or {})
    profile_lookup = {str(item.get("document_id") or ""): item for item in profiles}
    doc_lookup = {str(item.get("document_id") or item.get("id") or ""): item for item in documents}
    family_lookup: dict[str, dict[str, Any]] = {}
    for item in families:
        family_id = _normalize_short_text(item.get("family_id"), limit=40) or _normalize_short_text(item.get("family_key"), limit=40)
        if family_id:
            family_lookup[family_id] = dict(item)

    raw_documents = raw.get("documents") if isinstance(raw.get("documents"), list) else []
    documents_by_id = {
        str(item.get("document_id") or ""): item
        for item in raw_documents
        if isinstance(item, dict) and str(item.get("document_id") or "").strip()
    }
    normalized_documents: list[dict[str, Any]] = []
    global_anchors: list[dict[str, Any]] = []
    for profile in profiles:
        document_id = str(profile.get("document_id") or "").strip()
        if not document_id:
            continue
        raw_entry = dict(documents_by_id.get(document_id) or {})
        source_doc = dict(doc_lookup.get(document_id) or {})
        anchors = []
        raw_anchor_registry = raw_entry.get("anchor_registry") if isinstance(raw_entry.get("anchor_registry"), list) else []
        for index, anchor in enumerate(raw_anchor_registry, start=1):
            if not isinstance(anchor, dict):
                continue
            quote = _normalize_short_text(anchor.get("quote"), limit=260)
            if not quote:
                continue
            anchor_id = _normalize_short_text(anchor.get("id"), limit=80) or f"anchor:{document_id}:{index}"
            anchors.append(
                {
                    "id": anchor_id,
                    "document_id": document_id,
                    "title": _normalize_short_text(source_doc.get("title") or profile.get("title"), limit=120),
                    "role": _normalize_short_text(anchor.get("role"), limit=24) or "signature",
                    "quote": quote,
                    "reason": _normalize_short_text(anchor.get("reason"), limit=120),
                }
            )
        if not anchors:
            anchor_windows = dict(profile.get("anchor_windows") or {})
            generated = [
                ("opening", anchor_windows.get("opening")),
                ("pivot", anchor_windows.get("pivot")),
                ("closing", anchor_windows.get("closing")),
            ]
            generated.extend(
                [("signature", value) for value in (anchor_windows.get("signature_lines") or [])[:2]]
            )
            for index, (role, quote) in enumerate(generated, start=1):
                text = _normalize_short_text(quote, limit=260)
                if not text:
                    continue
                anchors.append(
                    {
                        "id": f"anchor:{document_id}:{role}:{index}",
                        "document_id": document_id,
                        "title": _normalize_short_text(source_doc.get("title") or profile.get("title"), limit=120),
                        "role": role,
                        "quote": text,
                        "reason": "Recovered from document profile anchors.",
                    }
                )
        family_id = _normalize_short_text(raw_entry.get("family_id"), limit=40)
        profile_family = _normalize_short_text(((profile.get("prototype_affordances") or {}).get("prototype_family")), limit=120)
        family_label = _normalize_short_text(raw_entry.get("family_label"), limit=120) or profile_family or family_id
        retrieval_handles = raw_entry.get("retrieval_handles") if isinstance(raw_entry.get("retrieval_handles"), dict) else {}
        selection_guides = raw_entry.get("selection_guides") if isinstance(raw_entry.get("selection_guides"), dict) else {}
        entry = {
            "document_id": document_id,
            "title": _normalize_short_text(source_doc.get("title") or profile.get("title"), limit=120),
            "family_id": family_id or family_label.lower().replace(" ", "-")[:40] or f"family-{len(normalized_documents)+1}",
            "family_label": family_label or f"Family {len(normalized_documents)+1}",
            "document_summary": _normalize_short_text(raw_entry.get("document_summary") or ((profile.get("document_core") or {}).get("summary")), limit=180),
            "length_band": _normalize_short_text(raw_entry.get("length_band") or ((profile.get("document_core") or {}).get("length_band")), limit=24),
            "surface_form": _normalize_short_text(raw_entry.get("surface_form") or ((profile.get("document_core") or {}).get("surface_form")), limit=32),
            "retrieval_handles": {
                "keywords": _normalize_string_list(retrieval_handles.get("keywords"), limit=12, item_limit=24)
                or _normalize_string_list((((profile.get("retrieval_handles") or {}).get("keywords")) or []), limit=12, item_limit=24),
                "routing_text": _normalize_short_text(
                    retrieval_handles.get("routing_text") or (((profile.get("retrieval_handles") or {}).get("routing_text")) or ""),
                    limit=260,
                ),
                "routing_facets": dict(retrieval_handles.get("routing_facets") or (((profile.get("retrieval_handles") or {}).get("routing_facets")) or {})),
            },
            "selection_guides": {
                "best_for": _normalize_string_list(selection_guides.get("best_for"), limit=6, item_limit=40)
                or _normalize_string_list((((profile.get("prototype_affordances") or {}).get("suitable_for")) or []), limit=6, item_limit=40),
                "avoid_when": _normalize_string_list(selection_guides.get("avoid_when"), limit=6, item_limit=40)
                or _normalize_string_list((((profile.get("prototype_affordances") or {}).get("anti_drift_focus")) or []), limit=6, item_limit=40),
                "lift_signals": _normalize_string_list(selection_guides.get("lift_signals"), limit=6, item_limit=48)
                or _normalize_string_list((((profile.get("motif_and_scene_bank") or {}).get("motif_tags")) or []), limit=6, item_limit=24),
            },
            "anchor_registry": anchors[:8],
        }
        normalized_documents.append(entry)
        global_anchors.extend(anchors[:8])

    families_payload = raw.get("families") if isinstance(raw.get("families"), list) else []
    normalized_families = []
    seen_family_ids: set[str] = set()
    for item in families_payload or families:
        if not isinstance(item, dict):
            continue
        family_id = _normalize_short_text(item.get("family_id") or item.get("family_key"), limit=40)
        if not family_id:
            continue
        if family_id in seen_family_ids:
            continue
        seen_family_ids.add(family_id)
        normalized_families.append(
            {
                "family_id": family_id,
                "label": _normalize_short_text(item.get("label"), limit=120) or family_id,
                "description": _normalize_short_text(item.get("description"), limit=180),
                "selection_cues": _normalize_string_list(item.get("selection_cues"), limit=6, item_limit=40),
                "motif_tags": _normalize_string_list(item.get("motif_tags"), limit=4, item_limit=24),
                "member_count": int(item.get("member_count") or 0),
            }
        )
    retrieval_policy = raw.get("retrieval_policy") if isinstance(raw.get("retrieval_policy"), dict) else {}
    selection_guides = raw.get("selection_guides") if isinstance(raw.get("selection_guides"), dict) else {}
    return {
        "asset_kind": "stone_prototype_index_v3",
        "version": "v3",
        "project_name": project_name,
        "document_count": len(normalized_documents),
        "family_count": len(normalized_families),
        "documents": normalized_documents,
        "families": normalized_families,
        "retrieval_policy": {
            "shortlist_formula": _normalize_short_text(retrieval_policy.get("shortlist_formula"), limit=180)
            or "keyword overlap + routing facets + family cues; LLM rerank decides final selection.",
            "target_shortlist_size": int(retrieval_policy.get("target_shortlist_size") or 12),
            "target_anchor_budget": int(retrieval_policy.get("target_anchor_budget") or 8),
            "notes": _normalize_string_list(retrieval_policy.get("notes"), limit=6, item_limit=64)
            or ["Shortlist is lightweight and only controls prompt size.", "Final exemplar choice belongs to the reranker."],
        },
        "selection_guides": {
            "when_to_expand": _normalize_string_list(selection_guides.get("when_to_expand"), limit=6, item_limit=64)
            or ["Expand when top candidates are too homogeneous.", "Expand when anchor coverage is too thin."],
            "when_to_prune": _normalize_string_list(selection_guides.get("when_to_prune"), limit=6, item_limit=64)
            or ["Prune when multiple candidates duplicate the same family move.", "Prune when anchors repeat the same sentence."],
            "quality_checks": _normalize_string_list(selection_guides.get("quality_checks"), limit=6, item_limit=64)
            or ["Keep at least one strong opening anchor.", "Keep at least one closure anchor with residue."],
        },
        "anchor_registry": global_anchors[:80],
    }


def validate_stone_v3_asset_payload(asset_kind: str, payload: dict[str, Any] | None) -> None:
    if asset_kind not in STONE_V3_ASSET_KINDS:
        raise ValueError(f"Unsupported Stone v3 asset kind: {asset_kind}")
    if not isinstance(payload, dict):
        raise ValueError("Stone v3 asset payload must be a JSON object.")
    if payload.get("asset_kind") != asset_kind:
        raise ValueError(f"Stone v3 payload asset_kind must be {asset_kind}.")
    if asset_kind == "stone_author_model_v3":
        for key in ("author_core", "translation_rules", "stable_moves", "forbidden_moves", "family_map", "critic_rubrics", "global_evidence"):
            if key not in payload:
                raise ValueError(f"Stone Author Model V3 must include {key}.")
        if not isinstance(payload.get("translation_rules"), list):
            raise ValueError("Stone Author Model V3 translation_rules must be a list.")
        if not isinstance(payload.get("family_map"), list):
            raise ValueError("Stone Author Model V3 family_map must be a list.")
        return
    for key in ("documents", "families", "retrieval_policy", "selection_guides", "anchor_registry"):
        if key not in payload:
            raise ValueError(f"Stone Prototype Index V3 must include {key}.")
    documents = payload.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ValueError("Stone Prototype Index V3 must include prototype documents.")
    if not any(isinstance(item, dict) and list(item.get("anchor_registry") or []) for item in documents):
        raise ValueError("Stone Prototype Index V3 must include per-document anchor_registry entries.")


def is_valid_stone_v3_asset_payload(asset_kind: str, payload: dict[str, Any] | None) -> bool:
    try:
        validate_stone_v3_asset_payload(asset_kind, payload)
    except ValueError:
        return False
    return True


def render_stone_author_model_v3_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Stone Author Model V3",
        "",
        f"- project: {payload.get('project_name') or ''}",
        f"- profile_count: {payload.get('profile_count') or 0}",
        f"- family_count: {payload.get('family_count') or 0}",
        "",
        "## Author Core",
    ]
    author_core = dict(payload.get("author_core") or {})
    for key in ("voice_summary", "worldview_summary", "tone_summary"):
        value = author_core.get(key)
        if value:
            lines.append(f"- {key}: {value}")
    signature_motifs = list(author_core.get("signature_motifs") or [])
    if signature_motifs:
        lines.append(f"- signature_motifs: {', '.join(signature_motifs)}")
    lines.extend(["", "## Translation Rules"])
    for item in (payload.get("translation_rules") or [])[:8]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- {item.get('value_lens') or ''}: motifs={', '.join(item.get('preferred_motifs') or [])}; "
            f"openings={', '.join(item.get('opening_moves') or [])}; "
            f"closures={', '.join(item.get('closure_moves') or [])}"
        )
    lines.extend(["", "## Stable Moves"])
    lines.extend(f"- {item}" for item in (payload.get("stable_moves") or []))
    lines.extend(["", "## Forbidden Moves"])
    lines.extend(f"- {item}" for item in (payload.get("forbidden_moves") or []))
    lines.extend(["", "## Family Map"])
    for item in (payload.get("family_map") or [])[:10]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- {item.get('label') or item.get('family_id') or ''}: "
            f"{item.get('member_count') or 0}; cues={', '.join(item.get('selection_cues') or [])}"
        )
    lines.extend(["", "## Critic Rubrics"])
    critic_rubrics = dict(payload.get("critic_rubrics") or {})
    for key in ("formal_fidelity", "worldview_translation", "syntheticness"):
        items = list(critic_rubrics.get(key) or [])
        if items:
            lines.append(f"- {key}: {' | '.join(items)}")
    lines.extend(["", "## Global Evidence"])
    for item in (payload.get("global_evidence") or [])[:8]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- {item.get('title') or item.get('document_id') or ''}: "
            f"{item.get('summary') or item.get('opening') or ''}"
        )
    return "\n".join(lines).strip()


def render_stone_prototype_index_v3_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Stone Prototype Index V3",
        "",
        f"- project: {payload.get('project_name') or ''}",
        f"- document_count: {payload.get('document_count') or 0}",
        f"- family_count: {payload.get('family_count') or 0}",
        "",
        "## Retrieval Policy",
    ]
    retrieval_policy = dict(payload.get("retrieval_policy") or {})
    for key in ("shortlist_formula", "target_shortlist_size", "target_anchor_budget"):
        value = retrieval_policy.get(key)
        if value not in {None, ""}:
            lines.append(f"- {key}: {value}")
    notes = list(retrieval_policy.get("notes") or [])
    lines.extend(f"- note: {item}" for item in notes)
    lines.extend(["", "## Families"])
    for item in (payload.get("families") or [])[:10]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- {item.get('label') or item.get('family_id') or ''}: "
            f"{item.get('member_count') or 0}; motifs={', '.join(item.get('motif_tags') or [])}"
        )
    lines.extend(["", "## Documents"])
    for item in (payload.get("documents") or [])[:12]:
        if not isinstance(item, dict):
            continue
        anchor_registry = list(item.get("anchor_registry") or [])
        first_anchor = anchor_registry[0]["quote"] if anchor_registry and isinstance(anchor_registry[0], dict) else ""
        lines.append(
            f"- {item.get('title') or item.get('document_id') or ''}: "
            f"{item.get('family_label') or item.get('family_id') or ''}; "
            f"keywords={', '.join((item.get('retrieval_handles') or {}).get('keywords') or [])}; "
            f"anchor={first_anchor}"
        )
    return "\n".join(lines).strip()


class StoneV3BaselineSynthesizer:
    def __init__(self, *, log_path: str | None = None) -> None:
        self.log_path = log_path

    def build(
        self,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
        config: ServiceConfig | None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
        checkpoint_callback: StoneV3CheckpointCallback | None = None,
        resume_from: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not config:
            raise ValueError("Stone v3 baseline synthesis requires a configured chat model.")
        client = OpenAICompatibleClient(config, log_path=self.log_path)
        compact_profiles = [compact_stone_profile_v3(profile) for profile in profiles]
        checkpoint_state = self._coerce_resume_checkpoint(
            resume_from,
            project_name=project_name,
            compact_profiles=compact_profiles,
            documents=documents,
        )
        stage_trace: list[dict[str, Any]] = list(checkpoint_state.get("stage_trace") or [])
        stage_trace_lock = Lock()
        if checkpoint_state.get("resume_available"):
            self._emit_progress(
                progress_callback,
                phase="resume_checkpoint_v3",
                progress_percent=8,
                message=(
                    "Resuming from saved Stone v3 checkpoint: "
                    f"{self._resume_summary(checkpoint_state)}."
                ),
                stage="resume_checkpoint_v3",
                status="running",
            )
        self._emit_progress(
            progress_callback,
            phase="family_induction_v3",
            progress_percent=16,
            message=f"Starting family induction across {len(compact_profiles)} compact profiles.",
            stage="family_induction_v3",
        )
        families = list(checkpoint_state.get("families") or [])
        if families:
            self._emit_progress(
                progress_callback,
                phase="family_induction_v3",
                progress_percent=48,
                message="Loaded saved family induction checkpoint.",
                stage="family_induction_v3_resume",
            )
        else:
            batch_family_outputs = self._run_family_batches(
                client,
                project_name,
                compact_profiles,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            self._ensure_not_cancelled(cancel_requested, stage="family_induction_v3_finalize")
            self._emit_progress(
                progress_callback,
                phase="family_induction_v3",
                progress_percent=44,
                message="Finalizing Stone v3 prototype families from batch outputs.",
                stage="family_induction_v3_finalize",
            )
            families = self._run_family_finalize(
                client,
                project_name,
                compact_profiles,
                batch_family_outputs,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["families"] = families
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._ensure_not_cancelled(cancel_requested, stage="author_model_v3")
        author_model = dict(checkpoint_state.get("author_model") or {})
        if author_model:
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=60,
                message="Loaded saved author-model checkpoint.",
                stage="author_model_v3_resume",
            )
        else:
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=52,
                message="Synthesizing the Stone v3 author model.",
                stage="author_model_v3",
            )
            author_model = self._run_author_model(
                client,
                project_name=project_name,
                profiles=profiles,
                compact_profiles=compact_profiles,
                families=families,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["author_model"] = author_model
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._emit_progress(
            progress_callback,
            phase="author_model_v3",
            progress_percent=60,
            message="Author model synthesis completed. Starting prototype card batches.",
            stage="author_model_v3",
        )
        prototype_index = dict(checkpoint_state.get("prototype_index") or {})
        if prototype_index:
            self._emit_progress(
                progress_callback,
                phase="prototype_index_v3",
                progress_percent=92,
                message="Loaded saved prototype-index checkpoint.",
                stage="prototype_index_v3_resume",
            )
        else:
            prototype_batch_outputs = self._run_prototype_batches(
                client,
                project_name,
                compact_profiles,
                documents,
                families,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            self._ensure_not_cancelled(cancel_requested, stage="prototype_index_v3_finalize")
            self._emit_progress(
                progress_callback,
                phase="prototype_index_v3",
                progress_percent=88,
                message="Finalizing the Stone v3 prototype index.",
                stage="prototype_index_v3_finalize",
            )
            prototype_index = self._run_prototype_finalize(
                client,
                project_name=project_name,
                profiles=profiles,
                compact_profiles=compact_profiles,
                families=families,
                batch_outputs=prototype_batch_outputs,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["prototype_index"] = prototype_index
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._ensure_not_cancelled(cancel_requested, stage="baseline_critic_v3")
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=94,
            message="Running the Stone v3 baseline critic.",
            stage="baseline_critic_v3",
        )
        critic_review = dict(checkpoint_state.get("critic_review") or {})
        if critic_review:
            self._emit_progress(
                progress_callback,
                phase="baseline_critic_v3",
                progress_percent=97,
                message="Loaded saved baseline-critic checkpoint.",
                stage="baseline_critic_v3_resume",
            )
        else:
            critic_review = self._run_baseline_critic(
                client,
                project_name=project_name,
                author_model=author_model,
                prototype_index=prototype_index,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["critic_review"] = critic_review
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        validate_stone_v3_asset_payload("stone_author_model_v3", author_model)
        validate_stone_v3_asset_payload("stone_prototype_index_v3", prototype_index)
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=97,
            message="Stone v3 baseline synthesis completed. Persisting generated assets next.",
            stage="baseline_ready_v3",
            status="running",
        )
        return {
            "author_model": author_model,
            "prototype_index": prototype_index,
            "families": families,
            "critic_review": critic_review,
            "stage_trace": stage_trace,
        }

    def _call_json_stage(
        self,
        client: OpenAICompatibleClient,
        *,
        stage: str,
        phase: str | None = None,
        messages: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        progress_percent: int | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = 2200,
        timeout: float = STONE_V3_STAGE_TIMEOUT_SECONDS,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        phase_label = phase or stage
        for attempt in range(1, STONE_V3_MAX_RETRIES + 1):
            self._ensure_not_cancelled(cancel_requested, stage=stage)
            self._emit_progress(
                progress_callback,
                phase=phase_label,
                stage=stage,
                progress_percent=progress_percent or 0,
                message=f"{stage} attempt {attempt}/{STONE_V3_MAX_RETRIES} started.",
                status="running",
                attempt=attempt,
            )
            logger.info("Stone v3 stage %s attempt %s started.", stage, attempt)
            try:
                response = client.chat_completion_result(
                    messages,
                    model=client.config.model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=timeout,
                )
                parsed = parse_json_response(response.content, fallback=True)
                if not isinstance(parsed, dict):
                    raise ValueError(f"{stage} did not return a JSON object.")
                self._append_stage_trace(
                    stage_trace,
                    {
                        "stage": stage,
                        "attempt": attempt,
                        "status": "completed",
                        "model": response.model,
                        "usage": dict(response.usage or {}),
                        "output_preview": _trim_text(response.content, 320),
                        "failure_reason": "",
                    },
                    stage_trace_lock=stage_trace_lock,
                )
                self._emit_progress(
                    progress_callback,
                    phase=phase_label,
                    stage=stage,
                    progress_percent=progress_percent or 0,
                    message=f"{stage} completed on attempt {attempt}.",
                    status="running",
                    attempt=attempt,
                    output_preview=_trim_text(response.content, 160),
                )
                logger.info("Stone v3 stage %s attempt %s completed.", stage, attempt)
                return parsed
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                failure_reason = _trim_text(exc, 240)
                self._append_stage_trace(
                    stage_trace,
                    {
                        "stage": stage,
                        "attempt": attempt,
                        "status": "failed",
                        "model": client.config.model,
                        "usage": {},
                        "output_preview": "",
                        "failure_reason": failure_reason,
                    },
                    stage_trace_lock=stage_trace_lock,
                )
                self._emit_progress(
                    progress_callback,
                    phase=phase_label,
                    stage=stage,
                    progress_percent=progress_percent or 0,
                    message=f"{stage} attempt {attempt} failed: {failure_reason}",
                    status="running",
                    attempt=attempt,
                    failure_reason=failure_reason,
                )
                logger.warning("Stone v3 stage %s attempt %s failed: %s", stage, attempt, failure_reason)
        raise RuntimeError(f"{stage} failed after {STONE_V3_MAX_RETRIES} attempts: {last_error}")

    def _run_family_batches(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        batches = self._chunk_items_to_fit_budget(
            compact_profiles,
            build_messages=lambda batch: self._family_batch_messages(project_name, batch),
            max_items=STONE_V3_FAMILY_BATCH_SIZE,
        )
        batch_specs = [(index, batch) for index, batch in enumerate(batches, start=1)]
        if not batch_specs:
            return []
        completed = 0
        outputs: dict[int, dict[str, Any]] = {}
        max_workers = min(STONE_V3_BATCH_CONCURRENCY, len(batch_specs))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-v3-family") as executor:
            future_map = {
                executor.submit(
                    self._call_json_stage,
                    client,
                    stage=f"family_induction_v3_batch_{position}",
                    phase="family_induction_v3",
                    messages=self._family_batch_messages(project_name, batch),
                    stage_trace=stage_trace,
                    stage_trace_lock=stage_trace_lock,
                    progress_callback=progress_callback,
                    progress_percent=16,
                    timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                    cancel_requested=cancel_requested,
                ): position
                for position, batch in batch_specs
            }
            total_batches = len(future_map)
            for future in as_completed(future_map):
                position = future_map[future]
                self._ensure_not_cancelled(cancel_requested, stage=f"family_induction_v3_batch_{position}")
                outputs[position] = future.result()
                completed += 1
                self._emit_progress(
                    progress_callback,
                    phase="family_induction_v3",
                    stage=f"family_induction_v3_batch_{position}",
                    progress_percent=self._interpolate_progress(16, 40, completed, total_batches),
                    message=f"Completed family batch {position}/{total_batches}.",
                    status="running",
                    batch_index=position,
                    batch_total=total_batches,
                )
        return [outputs[position] for position, _batch in batch_specs if position in outputs]

    def _run_family_finalize(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        messages = self._family_finalize_messages(project_name, compact_profiles, batch_outputs)
        if self._message_token_count(messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            payload = self._call_json_stage(
                client,
                stage="family_induction_v3_finalize",
                phase="family_induction_v3",
                messages=messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=46,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
        else:
            batch_shards = self._chunk_items_to_fit_budget(
                batch_outputs,
                build_messages=lambda shard: self._family_finalize_messages(project_name, compact_profiles[:24], shard),
                max_items=8,
            )
            shard_outputs: list[dict[str, Any]] = []
            total_shards = len(batch_shards)
            for index, shard in enumerate(batch_shards, start=1):
                self._emit_progress(
                    progress_callback,
                    phase="family_induction_v3",
                    progress_percent=self._interpolate_progress(44, 46, index - 1, max(total_shards, 1)),
                    message=f"Family-finalize input exceeded budget; merging shard {index}/{total_shards}.",
                    stage=f"family_induction_v3_finalize_shard_{index}",
                    batch_index=index,
                    batch_total=total_shards,
                )
                shard_outputs.append(
                    self._call_json_stage(
                        client,
                        stage=f"family_induction_v3_finalize_shard_{index}",
                        phase="family_induction_v3",
                        messages=self._family_finalize_messages(project_name, compact_profiles[:24], shard),
                        stage_trace=stage_trace,
                        stage_trace_lock=stage_trace_lock,
                        progress_callback=progress_callback,
                        progress_percent=self._interpolate_progress(45, 47, index, max(total_shards, 1)),
                        timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                        cancel_requested=cancel_requested,
                    )
                )
            payload = self._call_json_stage(
                client,
                stage="family_induction_v3_finalize",
                phase="family_induction_v3",
                messages=self._family_finalize_messages(project_name, compact_profiles[:36], shard_outputs),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=47,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
        families = payload.get("families") if isinstance(payload.get("families"), list) else []
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(families, start=1):
            if not isinstance(item, dict):
                continue
            label = _normalize_short_text(item.get("label"), limit=120)
            family_id = _normalize_short_text(item.get("family_id"), limit=40) or f"family-{index}"
            normalized.append(
                {
                    "family_id": family_id,
                    "label": label or family_id,
                    "description": _normalize_short_text(item.get("description"), limit=180),
                    "selection_cues": _normalize_string_list(item.get("selection_cues"), limit=6, item_limit=40),
                    "motif_tags": _normalize_string_list(item.get("motif_tags"), limit=4, item_limit=24),
                    "member_count": int(item.get("member_count") or 0),
                }
            )
        if normalized:
            return normalized
        raise RuntimeError("family_induction_v3_finalize returned no valid families.")

    def _run_author_model(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        direct_messages = self._author_model_messages(project_name, compact_profiles, families)
        if self._message_token_count(direct_messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            author_raw = self._call_json_stage(
                client,
                stage="author_model_v3",
                phase="author_model_v3",
                messages=direct_messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=56,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
            return normalize_stone_author_model_v3(
                author_raw,
                project_name=project_name,
                profiles=profiles,
                families=families,
            )

        profile_shards = self._chunk_items_to_fit_budget(
            compact_profiles,
            build_messages=lambda shard: self._author_model_messages(project_name, shard, families),
            max_items=32,
        )
        shard_models: list[dict[str, Any]] = []
        total_shards = len(profile_shards)
        for index, shard in enumerate(profile_shards, start=1):
            shard_document_ids = {str(item.get("document_id") or "") for item in shard}
            shard_profiles = [
                item
                for item in profiles
                if str(item.get("document_id") or "") in shard_document_ids
            ]
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=self._interpolate_progress(52, 56, index - 1, max(total_shards, 1)),
                message=f"Author-model input exceeded budget; synthesizing shard {index}/{total_shards}.",
                stage=f"author_model_v3_shard_{index}",
                batch_index=index,
                batch_total=total_shards,
            )
            shard_raw = self._call_json_stage(
                client,
                stage=f"author_model_v3_shard_{index}",
                phase="author_model_v3",
                messages=self._author_model_messages(project_name, shard, families),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=self._interpolate_progress(53, 57, index, max(total_shards, 1)),
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
            shard_models.append(
                self._compact_author_model_for_critic(
                    normalize_stone_author_model_v3(
                        shard_raw,
                        project_name=project_name,
                        profiles=shard_profiles,
                        families=families,
                    )
                )
            )
        author_raw = self._call_json_stage(
            client,
            stage="author_model_v3",
            phase="author_model_v3",
            messages=self._author_model_finalize_messages(project_name, families, shard_models),
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=58,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
        )
        return normalize_stone_author_model_v3(
            author_raw,
            project_name=project_name,
            profiles=profiles,
            families=families,
        )

    def _run_prototype_batches(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
        families: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        by_document = {str(item.get("document_id") or item.get("id") or ""): item for item in documents}
        seed_entries = []
        for item in compact_profiles:
            seed_entries.append(
                {
                    "profile": item,
                    "document": {
                        "document_id": item.get("document_id"),
                        "title": item.get("title"),
                        "text": _trim_text((by_document.get(str(item.get("document_id") or "")) or {}).get("text"), 1500),
                        "opening": item.get("opening"),
                        "closing": item.get("closing"),
                    },
                }
            )
        entry_groups = self._chunk_items_to_fit_budget(
            seed_entries,
            build_messages=lambda entries: self._prototype_batch_messages(
                project_name,
                [entry["profile"] for entry in entries],
                [entry["document"] for entry in entries],
                families,
            ),
            max_items=STONE_V3_PROTOTYPE_BATCH_SIZE,
        )
        batch_specs = [
            (
                position,
                [entry["profile"] for entry in group],
                [entry["document"] for entry in group],
            )
            for position, group in enumerate(entry_groups, start=1)
        ]
        if not batch_specs:
            return []
        completed = 0
        outputs: dict[int, dict[str, Any]] = {}
        max_workers = min(STONE_V3_BATCH_CONCURRENCY, len(batch_specs))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-v3-prototype") as executor:
            future_map = {
                executor.submit(
                    self._call_json_stage,
                    client,
                    stage=f"prototype_index_v3_batch_{position}",
                    phase="prototype_index_v3",
                    messages=self._prototype_batch_messages(project_name, batch, batch_docs, families),
                    stage_trace=stage_trace,
                    stage_trace_lock=stage_trace_lock,
                    progress_callback=progress_callback,
                    progress_percent=62,
                    max_tokens=2800,
                    timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                    cancel_requested=cancel_requested,
                ): position
                for position, batch, batch_docs in batch_specs
            }
            total_batches = len(future_map)
            for future in as_completed(future_map):
                position = future_map[future]
                self._ensure_not_cancelled(cancel_requested, stage=f"prototype_index_v3_batch_{position}")
                outputs[position] = future.result()
                completed += 1
                self._emit_progress(
                    progress_callback,
                    phase="prototype_index_v3",
                    stage=f"prototype_index_v3_batch_{position}",
                    progress_percent=self._interpolate_progress(62, 86, completed, total_batches),
                    message=f"Completed prototype batch {position}/{total_batches}.",
                    status="running",
                    batch_index=position,
                    batch_total=total_batches,
                )
        return [outputs[position] for position, _batch, _batch_docs in batch_specs if position in outputs]

    def _run_prototype_finalize(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        merged = self._merge_prototype_batch_outputs(batch_outputs)
        corpus_summary = self._compact_prototype_index_for_critic(
            {
                "documents": merged["documents"],
                "families": families,
                "anchor_registry": merged["anchor_registry"],
                "document_count": len(merged["documents"]),
                "family_count": len(families),
            }
        )
        guidance_messages = self._prototype_finalize_guidance_messages(
            project_name,
            compact_profiles,
            families,
            corpus_summary,
        )
        if self._message_token_count(guidance_messages) > STONE_V3_PROMPT_TOKEN_BUDGET:
            corpus_summary["sample_documents"] = list(corpus_summary.get("sample_documents") or [])[:16]
            corpus_summary["anchor_samples"] = list(corpus_summary.get("anchor_samples") or [])[:8]
            guidance_messages = self._prototype_finalize_guidance_messages(
                project_name,
                compact_profiles[:24],
                families[:12],
                corpus_summary,
            )
        if self._message_token_count(guidance_messages) > STONE_V3_PROMPT_TOKEN_BUDGET:
            raise RuntimeError("prototype_index_v3_finalize still exceeded the 128k prompt budget after compaction.")
        prototype_guidance = self._call_json_stage(
            client,
            stage="prototype_index_v3_finalize",
            phase="prototype_index_v3",
            messages=guidance_messages,
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=90,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
        )
        return normalize_stone_prototype_index_v3(
            {
                "documents": merged["documents"],
                "families": prototype_guidance.get("families") or families,
                "retrieval_policy": prototype_guidance.get("retrieval_policy") or {},
                "selection_guides": prototype_guidance.get("selection_guides") or {},
                "anchor_registry": merged["anchor_registry"],
            },
            project_name=project_name,
            profiles=profiles,
            documents=[
                {
                    "document_id": item.get("document_id"),
                    "title": item.get("title"),
                }
                for item in merged["documents"]
            ],
            families=families,
        )

    def _run_baseline_critic(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        author_model: dict[str, Any],
        prototype_index: dict[str, Any],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        author_summary = self._compact_author_model_for_critic(author_model)
        prototype_summary = self._compact_prototype_index_for_critic(prototype_index)
        direct_messages = self._baseline_critic_messages(project_name, author_summary, prototype_summary)
        if self._message_token_count(direct_messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            return self._call_json_stage(
                client,
                stage="baseline_critic_v3",
                phase="baseline_critic_v3",
                messages=direct_messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=96,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )

        shards = self._build_prototype_critic_shards(prototype_index)
        shard_reviews: list[dict[str, Any]] = []
        total_shards = len(shards)
        for index, shard in enumerate(shards, start=1):
            self._emit_progress(
                progress_callback,
                phase="baseline_critic_v3",
                progress_percent=self._interpolate_progress(94, 96, index - 1, max(total_shards, 1)),
                message=f"Critic input exceeded budget; reviewing shard {index}/{total_shards}.",
                stage=f"baseline_critic_v3_shard_{index}",
                batch_index=index,
                batch_total=total_shards,
            )
            shard_review = self._call_json_stage(
                client,
                stage=f"baseline_critic_v3_shard_{index}",
                phase="baseline_critic_v3",
                messages=self._baseline_critic_shard_messages(project_name, author_summary, prototype_summary, shard, index, total_shards),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=self._interpolate_progress(94, 96, index, max(total_shards, 1)),
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
                max_tokens=1400,
            )
            shard_reviews.append(
                {
                    "shard_index": index,
                    "document_count": len(list(shard.get("documents") or [])),
                    "review": shard_review,
                }
            )
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=96,
            message=f"Summarizing {total_shards} critic shard reviews into a final verdict.",
            stage="baseline_critic_v3_finalize",
        )
        return self._call_json_stage(
            client,
            stage="baseline_critic_v3",
            phase="baseline_critic_v3",
            messages=self._baseline_critic_finalize_messages(project_name, author_summary, prototype_summary, shard_reviews),
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=96,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
            max_tokens=1600,
        )

    @staticmethod
    def _persist_checkpoint(
        checkpoint_callback: StoneV3CheckpointCallback | None,
        checkpoint_state: dict[str, Any],
    ) -> None:
        if checkpoint_callback:
            checkpoint_callback(dict(checkpoint_state))

    @staticmethod
    def _coerce_resume_checkpoint(
        payload: dict[str, Any] | None,
        *,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
    ) -> dict[str, Any]:
        fingerprint = StoneV3BaselineSynthesizer._build_corpus_fingerprint(compact_profiles, documents)
        checkpoint = dict(payload or {})
        valid = (
            checkpoint.get("version") == STONE_V3_CHECKPOINT_VERSION
            and checkpoint.get("project_name") == project_name
            and checkpoint.get("corpus_fingerprint") == fingerprint
        )
        if not valid:
            checkpoint = {}
        families = list(checkpoint.get("families") or []) if isinstance(checkpoint.get("families"), list) else []
        author_model = dict(checkpoint.get("author_model") or {}) if isinstance(checkpoint.get("author_model"), dict) else {}
        prototype_index = dict(checkpoint.get("prototype_index") or {}) if isinstance(checkpoint.get("prototype_index"), dict) else {}
        critic_review = dict(checkpoint.get("critic_review") or {}) if isinstance(checkpoint.get("critic_review"), dict) else {}
        if author_model:
            try:
                validate_stone_v3_asset_payload("stone_author_model_v3", author_model)
            except ValueError:
                author_model = {}
        if prototype_index:
            try:
                validate_stone_v3_asset_payload("stone_prototype_index_v3", prototype_index)
            except ValueError:
                prototype_index = {}
        return {
            "version": STONE_V3_CHECKPOINT_VERSION,
            "project_name": project_name,
            "profile_count": len(compact_profiles),
            "document_count": len(documents),
            "corpus_fingerprint": fingerprint,
            "resume_count": int(checkpoint.get("resume_count") or 0) + (1 if valid else 0),
            "resume_available": valid,
            "families": families,
            "author_model": author_model,
            "prototype_index": prototype_index,
            "critic_review": critic_review,
            "stage_trace": list(checkpoint.get("stage_trace") or []),
        }

    @staticmethod
    def _resume_summary(checkpoint_state: dict[str, Any]) -> str:
        recovered: list[str] = []
        if checkpoint_state.get("families"):
            recovered.append("families")
        if checkpoint_state.get("author_model"):
            recovered.append("author model")
        if checkpoint_state.get("prototype_index"):
            recovered.append("prototype index")
        if checkpoint_state.get("critic_review"):
            recovered.append("baseline critic")
        return ", ".join(recovered) if recovered else "no reusable stage"

    @staticmethod
    def _build_corpus_fingerprint(
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
    ) -> str:
        profile_parts = [
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "summary": item.get("summary"),
                "opening": item.get("opening"),
                "closing": item.get("closing"),
            }
            for item in compact_profiles
        ]
        document_parts = [
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "text": _trim_text(item.get("text"), 240),
            }
            for item in documents
        ]
        digest = hashlib.sha256(
            json.dumps(
                {"profiles": profile_parts, "documents": document_parts},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return digest

    @staticmethod
    def _message_token_count(messages: list[dict[str, Any]]) -> int:
        return sum(estimate_stone_prompt_tokens(item.get("content")) for item in messages)

    def _chunk_items_to_fit_budget(
        self,
        items: list[Any],
        *,
        build_messages: Callable[[list[Any]], list[dict[str, Any]]],
        max_items: int,
    ) -> list[list[Any]]:
        if not items:
            return []
        chunks: list[list[Any]] = []
        current: list[Any] = []
        for item in items:
            candidate = [*current, item]
            if current and (
                len(candidate) > max_items
                or self._message_token_count(build_messages(candidate)) > STONE_V3_PROMPT_TOKEN_BUDGET
            ):
                chunks.append(current)
                current = [item]
            else:
                current = candidate
            if self._message_token_count(build_messages(current)) > STONE_V3_PROMPT_TOKEN_BUDGET:
                raise RuntimeError("A single Stone v3 batch item exceeded the 128k prompt budget.")
        if current:
            chunks.append(current)
        return chunks

    @staticmethod
    def _merge_prototype_batch_outputs(batch_outputs: list[dict[str, Any]]) -> dict[str, Any]:
        documents_by_id: dict[str, dict[str, Any]] = {}
        anchor_seen: set[str] = set()
        anchor_registry: list[dict[str, Any]] = []
        for batch in batch_outputs:
            for document in list((batch or {}).get("documents") or []):
                if not isinstance(document, dict):
                    continue
                document_id = str(document.get("document_id") or "").strip()
                if not document_id:
                    continue
                existing = dict(documents_by_id.get(document_id) or {})
                merged = dict(existing)
                for key, value in document.items():
                    if value in (None, "", [], {}):
                        continue
                    if key == "anchor_registry":
                        continue
                    merged[key] = value
                merged_anchors = []
                merged_anchor_seen: set[str] = set()
                for anchor in list(existing.get("anchor_registry") or []) + list(document.get("anchor_registry") or []):
                    if not isinstance(anchor, dict):
                        continue
                    anchor_id = _normalize_short_text(anchor.get("id"), limit=96)
                    anchor_key = anchor_id or json.dumps(anchor, ensure_ascii=False, sort_keys=True)
                    if anchor_key in merged_anchor_seen:
                        continue
                    merged_anchors.append(dict(anchor))
                    merged_anchor_seen.add(anchor_key)
                    if anchor_key not in anchor_seen:
                        anchor_registry.append(dict(anchor))
                        anchor_seen.add(anchor_key)
                merged["anchor_registry"] = merged_anchors
                documents_by_id[document_id] = merged
        return {
            "documents": list(documents_by_id.values()),
            "anchor_registry": anchor_registry,
        }

    @staticmethod
    def _compact_author_model_for_critic(author_model: dict[str, Any]) -> dict[str, Any]:
        author_core = dict(author_model.get("author_core") or {})
        critic_rubrics = dict(author_model.get("critic_rubrics") or {})
        translation_rules = []
        for item in list(author_model.get("translation_rules") or [])[:8]:
            if not isinstance(item, dict):
                continue
            translation_rules.append(
                {
                    "value_lens": _normalize_short_text(item.get("value_lens"), limit=48),
                    "preferred_motifs": _normalize_string_list(item.get("preferred_motifs"), limit=4, item_limit=24),
                    "opening_moves": _normalize_string_list(item.get("opening_moves"), limit=3, item_limit=60),
                    "closure_moves": _normalize_string_list(item.get("closure_moves"), limit=3, item_limit=60),
                }
            )
        evidence = []
        for item in list(author_model.get("global_evidence") or [])[:12]:
            if not isinstance(item, dict):
                continue
            evidence.append(
                {
                    "document_id": _normalize_short_text(item.get("document_id"), limit=80),
                    "title": _normalize_short_text(item.get("title"), limit=120),
                    "summary": _normalize_short_text(item.get("summary"), limit=180),
                    "opening": _normalize_short_text(item.get("opening"), limit=180),
                    "closing": _normalize_short_text(item.get("closing"), limit=180),
                }
            )
        return {
            "author_core": {
                "voice_summary": _normalize_short_text(author_core.get("voice_summary"), limit=180),
                "worldview_summary": _normalize_short_text(author_core.get("worldview_summary"), limit=180),
                "tone_summary": _normalize_short_text(author_core.get("tone_summary"), limit=180),
                "signature_motifs": _normalize_string_list(author_core.get("signature_motifs"), limit=6, item_limit=24),
            },
            "translation_rules": translation_rules,
            "stable_moves": _normalize_string_list(author_model.get("stable_moves"), limit=8, item_limit=72),
            "forbidden_moves": _normalize_string_list(author_model.get("forbidden_moves"), limit=8, item_limit=72),
            "family_map": list(author_model.get("family_map") or [])[:12],
            "critic_rubrics": {
                key: _normalize_string_list(critic_rubrics.get(key), limit=6, item_limit=72)
                for key in ("formal_fidelity", "worldview_translation", "syntheticness")
            },
            "global_evidence": evidence,
        }

    @staticmethod
    def _compact_prototype_document_for_critic(document: dict[str, Any]) -> dict[str, Any]:
        retrieval_handles = dict(document.get("retrieval_handles") or {})
        anchors = list(document.get("anchor_registry") or [])
        compact_anchors = []
        for item in anchors[:2]:
            if not isinstance(item, dict):
                continue
            compact_anchors.append(
                {
                    "role": _normalize_short_text(item.get("role"), limit=24),
                    "quote": _normalize_short_text(item.get("quote"), limit=180),
                }
            )
        return {
            "document_id": _normalize_short_text(document.get("document_id"), limit=80),
            "title": _normalize_short_text(document.get("title"), limit=120),
            "family_label": _normalize_short_text(document.get("family_label") or document.get("family_id"), limit=120),
            "length_band": _normalize_short_text(document.get("length_band"), limit=24),
            "surface_form": _normalize_short_text(document.get("surface_form"), limit=32),
            "document_summary": _normalize_short_text(document.get("document_summary"), limit=180),
            "keywords": _normalize_string_list(retrieval_handles.get("keywords"), limit=6, item_limit=24),
            "anchors": compact_anchors,
        }

    @staticmethod
    def _compact_prototype_index_for_critic(prototype_index: dict[str, Any]) -> dict[str, Any]:
        documents = list(prototype_index.get("documents") or [])
        families = list(prototype_index.get("families") or [])
        family_distribution = Counter(
            _normalize_short_text(item.get("family_label") or item.get("family_id"), limit=120)
            for item in documents
            if isinstance(item, dict)
        )
        sampled_documents: list[dict[str, Any]] = []
        seen_families: set[str] = set()
        for item in documents:
            if not isinstance(item, dict):
                continue
            family_label = _normalize_short_text(item.get("family_label") or item.get("family_id"), limit=120)
            if family_label and family_label in seen_families and len(sampled_documents) >= 24:
                continue
            if family_label:
                seen_families.add(family_label)
            sampled_documents.append(StoneV3BaselineSynthesizer._compact_prototype_document_for_critic(item))
            if len(sampled_documents) >= 32:
                break
        return {
            "document_count": int(prototype_index.get("document_count") or len(documents)),
            "family_count": int(prototype_index.get("family_count") or len(families)),
            "retrieval_policy": dict(prototype_index.get("retrieval_policy") or {}),
            "selection_guides": dict(prototype_index.get("selection_guides") or {}),
            "families": list(families)[:16],
            "family_distribution": [
                {"family": family, "count": count}
                for family, count in family_distribution.most_common(16)
                if family
            ],
            "sample_documents": sampled_documents,
            "anchor_samples": [
                {
                    "document_id": _normalize_short_text(item.get("document_id"), limit=80),
                    "role": _normalize_short_text(item.get("role"), limit=24),
                    "quote": _normalize_short_text(item.get("quote"), limit=180),
                }
                for item in list(prototype_index.get("anchor_registry") or [])[:16]
                if isinstance(item, dict)
            ],
        }

    @staticmethod
    def _build_prototype_critic_shards(prototype_index: dict[str, Any]) -> list[dict[str, Any]]:
        documents = [
            StoneV3BaselineSynthesizer._compact_prototype_document_for_critic(item)
            for item in list(prototype_index.get("documents") or [])
            if isinstance(item, dict)
        ]
        shards: list[dict[str, Any]] = []
        for index in range(0, len(documents), STONE_V3_CRITIC_SHARD_SIZE):
            shards.append(
                {
                    "documents": documents[index : index + STONE_V3_CRITIC_SHARD_SIZE],
                }
            )
        return shards or [{"documents": []}]

    @staticmethod
    def _append_stage_trace(
        stage_trace: list[dict[str, Any]],
        item: dict[str, Any],
        *,
        stage_trace_lock: Lock | None = None,
    ) -> None:
        if stage_trace_lock is None:
            stage_trace.append(item)
            return
        with stage_trace_lock:
            stage_trace.append(item)

    @staticmethod
    def _emit_progress(
        progress_callback: StoneV3ProgressCallback | None,
        *,
        phase: str,
        progress_percent: int,
        message: str,
        stage: str | None = None,
        status: str = "running",
        attempt: int | None = None,
        batch_index: int | None = None,
        batch_total: int | None = None,
        output_preview: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        if not progress_callback:
            return
        payload = {
            "phase": phase,
            "stage": stage or phase,
            "status": status,
            "progress_percent": int(progress_percent),
            "message": message,
        }
        if attempt is not None:
            payload["attempt"] = attempt
        if batch_index is not None:
            payload["batch_index"] = batch_index
        if batch_total is not None:
            payload["batch_total"] = batch_total
        if output_preview:
            payload["output_preview"] = output_preview
        if failure_reason:
            payload["failure_reason"] = failure_reason
        progress_callback(payload)

    @staticmethod
    def _ensure_not_cancelled(
        cancel_requested: StoneV3CancelRequested | None,
        *,
        stage: str,
    ) -> None:
        if cancel_requested and cancel_requested():
            raise TimeoutError(f"{stage} cancelled after 120 seconds without stream activity.")

    @staticmethod
    def _interpolate_progress(start: int, end: int, completed: int, total: int) -> int:
        if total <= 0:
            return end
        span = max(0, end - start)
        return min(end, start + round(span * max(0, completed) / total))

    @staticmethod
    def _family_batch_messages(project_name: str, batch: list[dict[str, Any]]) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 family induction stage.\n"
                    "Read a batch of compact document profiles and propose draft prototype families.\n"
                    "Return JSON only with {\"families\": [...]}.\n"
                    "Each family needs family_id, label, description, selection_cues, motif_tags, member_count."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(batch, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _family_finalize_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 family synthesis stage.\n"
                    "Merge batch-level draft families into a canonical family map for the whole corpus.\n"
                    "Return JSON only with {\"families\": [...]}.\n"
                    "Keep labels concrete and retrieval-friendly."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Corpus compact profiles JSON:\n{json.dumps(compact_profiles[:48], ensure_ascii=False, indent=2)}\n\n"
                    f"Batch family drafts JSON:\n{json.dumps(batch_outputs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _author_model_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 author-model synthesizer.\n"
                    "Return JSON only.\n"
                    "Build a corpus-level author model with keys author_core, translation_rules, stable_moves, "
                    "forbidden_moves, family_map, critic_rubrics, global_evidence."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles[:60], ensure_ascii=False, indent=2)}\n\n"
                    f"Canonical family map JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _author_model_finalize_messages(
        project_name: str,
        families: list[dict[str, Any]],
        shard_models: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 author-model finalizer.\n"
                    "Merge shard-level author-model syntheses into one canonical author model.\n"
                    "Return JSON only.\n"
                    "Build keys author_core, translation_rules, stable_moves, forbidden_moves, family_map, "
                    "critic_rubrics, and global_evidence."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Canonical family map JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Shard author models JSON:\n{json.dumps(shard_models, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_batch_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_docs: list[dict[str, Any]],
        families: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-card synthesis stage.\n"
                    "Return JSON only with {\"documents\": [...]}.\n"
                    "Each document needs document_id, family_id, family_label, document_summary, retrieval_handles, "
                    "selection_guides, and anchor_registry."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles, ensure_ascii=False, indent=2)}\n\n"
                    f"Document excerpts JSON:\n{json.dumps(batch_docs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_finalize_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-index finalizer.\n"
                    "Return JSON only with documents, families, retrieval_policy, selection_guides, and anchor_registry.\n"
                    "Preserve batch-level document cards and synthesize corpus-level retrieval guidance."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles[:60], ensure_ascii=False, indent=2)}\n\n"
                    f"Families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype batch outputs JSON:\n{json.dumps(batch_outputs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_finalize_guidance_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        corpus_summary: dict[str, Any],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-index finalizer.\n"
                    "The per-document prototype cards already exist.\n"
                    "Review the corpus summary and return JSON only with families, retrieval_policy, and selection_guides.\n"
                    "Do not rewrite the full document list."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles sample JSON:\n{json.dumps(compact_profiles[:48], ensure_ascii=False, indent=2)}\n\n"
                    f"Canonical families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(corpus_summary, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_messages(
        project_name: str,
        author_model: dict[str, Any],
        prototype_index: dict[str, Any],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic.\n"
                    "Review only the baseline quality and corpus grounding.\n"
                    "Return JSON only with verdict, score, strengths, risks, and repair_notes."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_model, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype index summary JSON:\n{json.dumps(prototype_index, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_shard_messages(
        project_name: str,
        author_summary: dict[str, Any],
        prototype_summary: dict[str, Any],
        shard: dict[str, Any],
        shard_index: int,
        shard_total: int,
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic working on one shard of prototype documents.\n"
                    "Review only this shard for corpus grounding, retrieval usefulness, and synthetic drift risk.\n"
                    "Return JSON only with strengths, risks, repair_notes, and shard_focus."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n"
                    f"Shard: {shard_index}/{shard_total}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(prototype_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype shard JSON:\n{json.dumps(shard, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_finalize_messages(
        project_name: str,
        author_summary: dict[str, Any],
        prototype_summary: dict[str, Any],
        shard_reviews: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic finalizer.\n"
                    "Merge shard reviews into one baseline verdict.\n"
                    "Return JSON only with verdict, score, strengths, risks, and repair_notes."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(prototype_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Critic shard reviews JSON:\n{json.dumps(shard_reviews, ensure_ascii=False, indent=2)}"
                ),
            },
        ]
