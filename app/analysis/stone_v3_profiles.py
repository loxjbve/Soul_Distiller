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


_STYLE_PRONOUN_TERMS = (
    "窝",
    "窝们",
    "咱",
    "咱们",
    "我",
    "我们",
    "自己",
    "泥",
    "泥们",
    "你",
    "你们",
    "他",
    "她",
    "他们",
    "她们",
)
_STYLE_CONNECTIVE_TERMS = ("但是", "不过", "其实", "只是", "所以", "因为", "如果", "可是", "然后", "后来")
_STYLE_PUNCTUATION_TERMS = ("，", "。", "！", "？", "；", "：", "、", "…")


def _derive_style_stats(text: str) -> dict[str, Any]:
    source = normalize_whitespace(str(text or ""))
    sentences = _split_sentences(source)
    sentence_lengths = [estimate_word_count(item) for item in sentences if item]
    sentence_length_buckets = {
        "short": sum(1 for size in sentence_lengths if size <= 18),
        "medium": sum(1 for size in sentence_lengths if 18 < size <= 36),
        "long": sum(1 for size in sentence_lengths if size > 36),
    }
    punctuation_counts = {
        mark: source.count(mark)
        for mark in _STYLE_PUNCTUATION_TERMS
        if source.count(mark) > 0
    }
    pronoun_counts = {
        term: source.count(term)
        for term in _STYLE_PRONOUN_TERMS
        if source.count(term) > 0
    }
    connective_counts = {
        term: source.count(term)
        for term in _STYLE_CONNECTIVE_TERMS
        if source.count(term) > 0
    }
    word_count = max(1, estimate_word_count(source))
    self_reference_total = sum(pronoun_counts.get(term, 0) for term in ("窝", "窝们", "咱", "咱们", "我", "我们", "自己"))
    return {
        "pronoun_counts": pronoun_counts,
        "connective_counts": connective_counts,
        "sentence_length_buckets": sentence_length_buckets,
        "avg_sentence_length": round(sum(sentence_lengths) / max(1, len(sentence_lengths)), 1),
        "punctuation_counts": punctuation_counts,
        "self_reference_ratio": round(self_reference_total / word_count, 4),
        "sentence_count": len(sentence_lengths),
    }


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
    style_stats = _derive_style_stats(text)
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
        "style_stats": style_stats,
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


def _aggregate_style_counter(profiles: list[dict[str, Any]], key: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        stats = dict(profile.get("style_stats") or {})
        source = dict(stats.get(key) or {})
        for token, count in source.items():
            normalized = _normalize_short_text(token, limit=40)
            if not normalized:
                continue
            try:
                counter[normalized] += int(count or 0)
            except (TypeError, ValueError):
                continue
    return counter


def _aggregate_style_fingerprint(
    profiles: list[dict[str, Any]],
    *,
    stable_moves: list[str],
    forbidden_moves: list[str],
) -> dict[str, Any]:
    person_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("person")), limit=24)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("person")), limit=24)
    )
    address_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("address_target")), limit=24)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("address_target")), limit=24)
    )
    self_position_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("self_position")), limit=24)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("self_position")), limit=24)
    )
    distance_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("distance")), limit=24)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("distance")), limit=24)
    )
    cadence_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("cadence")), limit=32)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("cadence")), limit=32)
    )
    sentence_shape_counter = Counter(
        _normalize_short_text(((profile.get("voice_contract") or {}).get("sentence_shape")), limit=32)
        for profile in profiles
        if _normalize_short_text(((profile.get("voice_contract") or {}).get("sentence_shape")), limit=32)
    )
    lexicon_counter = Counter(
        token
        for profile in profiles
        for token in _normalize_string_list(((profile.get("motif_and_scene_bank") or {}).get("lexicon_markers")), limit=8, item_limit=24)
    )
    connective_counter = _aggregate_style_counter(profiles, "connective_counts")
    pronoun_counter = _aggregate_style_counter(profiles, "pronoun_counts")
    punctuation_counter = _aggregate_style_counter(profiles, "punctuation_counts")
    sentence_bucket_counter = _aggregate_style_counter(profiles, "sentence_length_buckets")
    opening_moves = _unique_preserve_order(
        [
            _normalize_short_text(((profile.get("structure_moves") or {}).get("opening_move")), limit=80)
            for profile in profiles
            if _normalize_short_text(((profile.get("structure_moves") or {}).get("opening_move")), limit=80)
        ]
    )[:4]
    turning_moves = _unique_preserve_order(
        [
            _normalize_short_text(((profile.get("structure_moves") or {}).get("turning_move")), limit=80)
            for profile in profiles
            if _normalize_short_text(((profile.get("structure_moves") or {}).get("turning_move")), limit=80)
        ]
    )[:4]
    closure_moves = _unique_preserve_order(
        [
            _normalize_short_text(((profile.get("structure_moves") or {}).get("closure_move")), limit=80)
            for profile in profiles
            if _normalize_short_text(((profile.get("structure_moves") or {}).get("closure_move")), limit=80)
        ]
    )[:4]
    closing_lines = _unique_preserve_order(
        [
            _trim_text(((profile.get("anchor_windows") or {}).get("closing")), 160)
            for profile in profiles
            if _trim_text(((profile.get("anchor_windows") or {}).get("closing")), 160)
        ]
    )[:4]
    felt_costs = _unique_preserve_order(
        [
            _normalize_short_text(((profile.get("value_and_judgment") or {}).get("felt_cost")), limit=120)
            for profile in profiles
            if _normalize_short_text(((profile.get("value_and_judgment") or {}).get("felt_cost")), limit=120)
        ]
    )[:4]
    judgment_modes = _unique_preserve_order(
        [
            _normalize_short_text(((profile.get("value_and_judgment") or {}).get("judgment_mode")), limit=40)
            for profile in profiles
            if _normalize_short_text(((profile.get("value_and_judgment") or {}).get("judgment_mode")), limit=40)
        ]
    )[:4]
    self_reference_ratios = [
        float((profile.get("style_stats") or {}).get("self_reference_ratio") or 0.0)
        for profile in profiles
    ]
    avg_sentence_lengths = [
        float((profile.get("style_stats") or {}).get("avg_sentence_length") or 0.0)
        for profile in profiles
        if float((profile.get("style_stats") or {}).get("avg_sentence_length") or 0.0) > 0
    ]
    self_reference_terms = [
        term
        for term, _count in pronoun_counter.most_common(6)
        if term in {"我", "我们", "自己", "你", "你们"}
    ]
    overfit_terms = _unique_preserve_order(
        [term for term, _count in lexicon_counter.most_common(6)] + self_reference_terms
    )[:6]
    return {
        "narrator_profile": {
            "person": person_counter.most_common(1)[0][0] if person_counter else "first",
            "address_target": address_counter.most_common(1)[0][0] if address_counter else "self",
            "self_position": self_position_counter.most_common(1)[0][0] if self_position_counter else "none",
            "narrative_distance": distance_counter.most_common(1)[0][0] if distance_counter else "回收",
            "self_reference_terms": self_reference_terms,
            "self_reference_ratio": round(sum(self_reference_ratios) / max(1, len(self_reference_ratios)), 4),
        },
        "lexicon_profile": {
            "high_frequency_terms": [term for term, _count in lexicon_counter.most_common(8)],
            "connective_keep": [term for term, _count in connective_counter.most_common(6)],
            "self_reference_terms": self_reference_terms,
            "overfit_risk_terms": overfit_terms,
        },
        "rhythm_profile": {
            "cadence": cadence_counter.most_common(1)[0][0] if cadence_counter else "restrained",
            "sentence_shape": sentence_shape_counter.most_common(1)[0][0] if sentence_shape_counter else "mixed",
            "sentence_length_buckets": {
                "short": int(sentence_bucket_counter.get("short", 0)),
                "medium": int(sentence_bucket_counter.get("medium", 0)),
                "long": int(sentence_bucket_counter.get("long", 0)),
            },
            "avg_sentence_length": round(sum(avg_sentence_lengths) / max(1, len(avg_sentence_lengths)), 1),
            "punctuation_habits": [token for token, _count in punctuation_counter.most_common(6)],
        },
        "closure_profile": {
            "opening_moves": opening_moves,
            "turning_devices": turning_moves,
            "closure_moves": closure_moves,
            "signature_closures": closing_lines,
        },
        "extreme_state_profile": {
            "pressure_translation": felt_costs,
            "judgment_modes": judgment_modes,
            "defense_moves": _unique_preserve_order([*stable_moves[:4], *forbidden_moves[:4]])[:6],
        },
    }


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
    stable_moves = _normalize_string_list(raw.get("stable_moves"), limit=8, item_limit=72) or [
        "Open from a concrete action, object, or scene.",
        "Let pressure rise from visible detail.",
        "Keep closure unresolved when possible.",
    ]
    forbidden_moves = _normalize_string_list(raw.get("forbidden_moves"), limit=8, item_limit=72) or [
        "Do not turn the piece into explanation.",
        "Do not flatten the ending into summary or thesis.",
        "Do not leak backstage prompt language.",
    ]
    style_fingerprint = _aggregate_style_fingerprint(
        profiles,
        stable_moves=stable_moves,
        forbidden_moves=forbidden_moves,
    )
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
        "style_fingerprint": style_fingerprint,
        "translation_rules": list(raw.get("translation_rules") or [])[:8] or translation_rules,
        "stable_moves": stable_moves,
        "forbidden_moves": forbidden_moves,
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
    style_fingerprint = dict(payload.get("style_fingerprint") or {})
    if style_fingerprint:
        narrator_profile = dict(style_fingerprint.get("narrator_profile") or {})
        lexicon_profile = dict(style_fingerprint.get("lexicon_profile") or {})
        rhythm_profile = dict(style_fingerprint.get("rhythm_profile") or {})
        closure_profile = dict(style_fingerprint.get("closure_profile") or {})
        extreme_state_profile = dict(style_fingerprint.get("extreme_state_profile") or {})
        lines.extend(["", "## Style Fingerprint"])
        lines.append(
            f"- narrator: {narrator_profile.get('person') or ''} | "
            f"distance={narrator_profile.get('narrative_distance') or ''} | "
            f"self_position={narrator_profile.get('self_position') or ''}"
        )
        lines.append(f"- self_reference_terms: {', '.join(narrator_profile.get('self_reference_terms') or [])}")
        lines.append(f"- connective_keep: {', '.join(lexicon_profile.get('connective_keep') or [])}")
        lines.append(f"- high_frequency_terms: {', '.join(lexicon_profile.get('high_frequency_terms') or [])}")
        lines.append(f"- punctuation_habits: {', '.join(rhythm_profile.get('punctuation_habits') or [])}")
        lines.append(f"- closure_moves: {', '.join(closure_profile.get('closure_moves') or [])}")
        lines.append(f"- defense_moves: {', '.join(extreme_state_profile.get('defense_moves') or [])}")
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


