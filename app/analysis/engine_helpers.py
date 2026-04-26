from __future__ import annotations

from time import perf_counter
from typing import Any

from app.analysis.facets import ALL_FACETS, FacetDefinition, get_facet_definition, get_facet_prompt_profile, get_facets_for_mode
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY, MIN_ANALYSIS_CONCURRENCY
from app.utils.text import top_terms

FACET_EVIDENCE_LIMIT = 20
FACET_BULLET_LIMIT = 8
RAW_TEXT_PREVIEW_LIMIT = 20000
GLOBAL_PERSONA_CARD_LABELS = (
    "角色规则",
    "心智模型",
    "决策启发式",
    "表达DNA",
    "表达 DNA",
    "时间线",
    "价值观",
    "反模式",
    "诚实边界",
    "智识谱系",
)


class AnalysisCancelledError(RuntimeError):
    pass


def _parse_confidence(val: Any, default: float) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        if isinstance(val, str):
            s = val.strip().lower()
            if "high" in s:
                return 0.8
            if "medium" in s:
                return 0.5
            if "low" in s:
                return 0.2
        return default


def _normalize_concurrency(value: Any) -> int:
    if value is None:
        return DEFAULT_ANALYSIS_CONCURRENCY
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        candidate = DEFAULT_ANALYSIS_CONCURRENCY
    return max(MIN_ANALYSIS_CONCURRENCY, candidate)


def _facet_catalog_from_summary(summary: dict[str, Any] | None) -> tuple[FacetDefinition, ...]:
    payload = dict(summary or {})
    mode = payload.get("project_mode")
    fallback_catalog = get_facets_for_mode(mode)
    requested_keys = [str(item or "").strip() for item in (payload.get("facet_keys") or []) if str(item or "").strip()]
    if not requested_keys:
        return fallback_catalog
    resolved: list[FacetDefinition] = []
    fallback_lookup = {facet.key: facet for facet in fallback_catalog}
    for key in requested_keys:
        facet = fallback_lookup.get(key) or get_facet_definition(key)
        if facet:
            resolved.append(facet)
    return tuple(resolved) or fallback_catalog


def _facet_order_keys(summary: dict[str, Any] | None) -> list[str]:
    return [facet.key for facet in _facet_catalog_from_summary(summary)]


def _collapse_whitespace(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _strip_persona_card_label(text: str) -> tuple[str | None, str]:
    for label in GLOBAL_PERSONA_CARD_LABELS:
        for separator in ("：", ":", "-", " - "):
            prefix = f"{label}{separator}"
            if text.startswith(prefix):
                return label, text[len(prefix):].strip()
    return None, text


def _facet_keyword_score(text: str, facet_key: str) -> int:
    profile = get_facet_prompt_profile(facet_key)
    return sum(1 for term in profile.relevance_terms if term and term in text)


def _best_foreign_facet_match(text: str, facet_key: str) -> tuple[str | None, int]:
    best_key: str | None = None
    best_score = 0
    for other in ALL_FACETS:
        if other.key == facet_key:
            continue
        score = _facet_keyword_score(text, other.key)
        if score > best_score:
            best_key = other.key
            best_score = score
    return best_key, best_score


def _normalize_facet_bullets(items: list[Any], facet: FacetDefinition) -> tuple[list[str], int]:
    normalized: list[str] = []
    seen: set[str] = set()
    removed = 0
    for item in list(items or [])[: FACET_BULLET_LIMIT * 3]:
        raw_text = _collapse_whitespace(item)
        if not raw_text:
            continue
        _, stripped_text = _strip_persona_card_label(raw_text)
        text = _collapse_whitespace(stripped_text)
        if not text:
            removed += 1
            continue
        current_score = _facet_keyword_score(text, facet.key)
        _, foreign_score = _best_foreign_facet_match(text, facet.key)
        if current_score == 0 and foreign_score > 0:
            removed += 1
            continue
        if current_score == 0 and stripped_text != raw_text:
            removed += 1
            continue
        if text in seen:
            continue
        normalized.append(text)
        seen.add(text)
        if len(normalized) >= FACET_BULLET_LIMIT:
            break
    return normalized, removed


def _build_facet_summary_from_bullets(facet: FacetDefinition, bullets: list[str]) -> str:
    cleaned_bullets = [item.rstrip("。；;，, ") for item in bullets if item]
    if not cleaned_bullets:
        return f"现有证据主要支持从 {facet.label} 继续观察，但可直接复用的细节仍然有限。"
    if len(cleaned_bullets) == 1:
        return f"围绕 {facet.label}，材料最稳定地显示：{cleaned_bullets[0]}。"
    return f"围绕 {facet.label}，材料最稳定地显示：{cleaned_bullets[0]}；{cleaned_bullets[1]}。"


def _normalize_facet_summary(summary: Any, bullets: list[str], facet: FacetDefinition) -> tuple[str, bool]:
    text = _collapse_whitespace(summary)
    if not text:
        return _build_facet_summary_from_bullets(facet, bullets), True
    current_score = _facet_keyword_score(text, facet.key)
    _, foreign_score = _best_foreign_facet_match(text, facet.key)
    global_label_hits = sum(1 for label in GLOBAL_PERSONA_CARD_LABELS if label in text)
    if current_score == 0 and (foreign_score > 0 or global_label_hits >= 2):
        return _build_facet_summary_from_bullets(facet, bullets), True
    return text, False


def _analyze_heuristically(
    facet: FacetDefinition,
    chunks: list[dict[str, Any]],
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> dict[str, Any]:
    started = perf_counter()
    joined = "\n".join(chunk["content"] for chunk in chunks)
    terms = top_terms(joined, limit=10)
    profile = get_facet_prompt_profile(facet.key)
    label_cycle = iter(profile.bullet_labels)
    bullets: list[str] = []
    if target_role:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：分析对象为 {target_role}，当前只归纳 {facet.label}。")
    if analysis_context:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：语境约束为 {analysis_context[:100]}。")
    if terms:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：高频词包括 {', '.join(terms[:5])}。")
    for chunk in chunks[:4]:
        preview = chunk["content"][:100].replace("\n", " ")
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：{preview}")
    bullets, _ = _normalize_facet_bullets(bullets, facet)
    summary_focus = "、".join(terms[:4]) or profile.focus.split("、", 1)[0]
    fewshots = [
        {
            "chunk_id": chunk["chunk_id"],
            "situation": f"{facet.label} 相关片段",
            "expression": "原始文本直述",
            "quote": chunk["content"][:160],
            "reason": f"{facet.label} 的代表片段",
            "document_title": chunk["document_title"],
            "filename": chunk["filename"],
            "page_number": chunk["page_number"],
        }
        for chunk in chunks[:FACET_EVIDENCE_LIMIT]
    ]
    return {
        "summary": (
            f"围绕 {facet.label}，现有 {len(chunks)} 个高相关片段主要指向 {summary_focus}；"
            "由于未调用 LLM，当前结果以证据驱动的保守归纳为主。"
        ),
        "bullets": bullets[:FACET_BULLET_LIMIT],
        "confidence": min(0.45 + (len(chunks) * 0.07), 0.78),
        "fewshots": fewshots,
        "evidence": fewshots,
        "conflicts": [],
        "notes": "LLM 未配置，结果来自启发式降级分析。",
        "_meta": {
            "llm_called": False,
            "llm_success": False,
            "llm_attempts": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "duration_ms": int((perf_counter() - started) * 1000),
        },
    }


def _normalize_facet_payload(
    payload: dict[str, Any],
    chunks: list[dict[str, Any]],
    facet: FacetDefinition,
) -> dict[str, Any]:
    chunk_map = {chunk["chunk_id"]: chunk for chunk in chunks}
    evidence: list[dict[str, Any]] = []
    raw_fewshots = payload.get("fewshots") or payload.get("evidence") or []
    for item in raw_fewshots[:FACET_EVIDENCE_LIMIT]:
        chunk_id = item.get("chunk_id")
        if not chunk_id or chunk_id not in chunk_map:
            continue
        source = chunk_map[chunk_id]
        quote = _collapse_whitespace(item.get("quote")) or source["content"][:160]
        situation = _collapse_whitespace(item.get("situation") or item.get("reason")) or f"{facet.label} 相关片段"
        expression = _collapse_whitespace(item.get("expression")) or "原始文本直述"
        evidence.append(
            {
                "chunk_id": chunk_id,
                "situation": situation,
                "expression": expression,
                "reason": _collapse_whitespace(item.get("reason")) or situation,
                "quote": quote,
                "document_title": source["document_title"],
                "filename": source["filename"],
                "page_number": source["page_number"],
            }
        )
    seen = {item["chunk_id"] for item in evidence}
    for chunk in chunks:
        if len(evidence) >= FACET_EVIDENCE_LIMIT:
            break
        if chunk["chunk_id"] in seen:
            continue
        evidence.append(
            {
                "chunk_id": chunk["chunk_id"],
                "situation": f"{facet.label} 相关片段",
                "expression": "原始文本直述",
                "reason": "Retrieved few-shot candidate",
                "quote": chunk["content"][:160],
                "document_title": chunk["document_title"],
                "filename": chunk["filename"],
                "page_number": chunk["page_number"],
            }
        )
        seen.add(chunk["chunk_id"])
    bullets, removed_bullets = _normalize_facet_bullets(payload.get("bullets", []), facet)
    summary, summary_rebuilt = _normalize_facet_summary(payload.get("summary", ""), bullets, facet)
    notes_parts = []
    raw_notes = _collapse_whitespace(payload.get("notes"))
    if raw_notes:
        notes_parts.append(raw_notes)
    if removed_bullets:
        notes_parts.append(
            f"Normalization removed {removed_bullets} off-facet bullet(s) so {facet.label} stays scoped to the current dimension."
        )
    if summary_rebuilt:
        notes_parts.append("Summary was rebuilt during normalization to keep the result focused on the current facet.")
    return {
        "summary": summary,
        "bullets": bullets,
        "confidence": _parse_confidence(payload.get("confidence"), 0.65),
        "fewshots": evidence,
        "evidence": evidence,
        "conflicts": [
            {
                "title": _collapse_whitespace(item.get("title")),
                "detail": _collapse_whitespace(item.get("detail")),
            }
            for item in payload.get("conflicts", [])[:5]
            if _collapse_whitespace(item.get("title")) or _collapse_whitespace(item.get("detail"))
        ],
        "notes": "\n".join(notes_parts) or None,
    }


__all__ = [
    "AnalysisCancelledError",
    "FACET_BULLET_LIMIT",
    "FACET_EVIDENCE_LIMIT",
    "GLOBAL_PERSONA_CARD_LABELS",
    "RAW_TEXT_PREVIEW_LIMIT",
    "_analyze_heuristically",
    "_collapse_whitespace",
    "_facet_catalog_from_summary",
    "_facet_order_keys",
    "_normalize_concurrency",
    "_normalize_facet_payload",
    "_parse_confidence",
]
