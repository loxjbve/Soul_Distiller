from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from statistics import mean
from typing import Any, Iterable

from app.utils.text import normalize_whitespace

STONE_PROFILE_V2_KEYS = (
    "length_band",
    "content_kernel",
    "surface_form",
    "voice_mask",
    "lexicon_markers",
    "syntax_signature",
    "segment_map",
    "opening_move",
    "turning_move",
    "closure_move",
    "motif_tags",
    "stance_vector",
    "emotion_curve",
    "rhetorical_devices",
    "prototype_family",
    "anchor_spans",
    "anti_patterns",
)

STONE_PROFILE_V2_LENGTH_BANDS = ("micro", "short", "medium", "long")
STONE_PROFILE_V2_SURFACE_FORMS = (
    "scene_vignette",
    "rant",
    "confession",
    "anecdote",
    "aphorism",
    "dialogue_bit",
    "manifesto",
    "list_bit",
)

STONE_V2_ASSET_KINDS = ("stone_author_model_v2", "stone_prototype_index_v2")

_COMMON_CJK_NGRAMS = {
    "这个",
    "那个",
    "不是",
    "只是",
    "什么",
    "我们",
    "他们",
    "自己",
    "然后",
    "如果",
    "但是",
    "因为",
    "所以",
    "时候",
    "已经",
    "还有",
    "没有",
    "一个",
    "一种",
    "一下",
    "这样",
    "那些",
    "东西",
}

_SCENE_TERMS = (
    "夜",
    "雨",
    "风",
    "街",
    "站台",
    "车站",
    "灯",
    "玻璃",
    "门",
    "楼道",
    "窗",
    "屋",
    "房间",
    "路",
    "地铁",
    "公交",
    "商场",
    "店里",
    "桌面",
    "床",
    "天空",
    "电梯",
    "走廊",
    "巷子",
)

_VALUE_LENS_TERMS = {
    "代价": ("代价", ("代价", "亏", "损失", "赔", "花钱", "成本")),
    "资格": ("资格", ("资格", "配", "体面", "门槛", "站位")),
    "体面": ("体面", ("体面", "难看", "丢脸", "面子", "尊严")),
    "生存": ("生存", ("生存", "活着", "工作", "房租", "吃饭", "钱")),
    "虚假": ("虚假", ("虚假", "骗人", "假装", "宣传", "包装", "套路")),
}

_SELF_DEPRECATING_TERMS = ("废物", "窝囊", "丢人", "穷", "蠢", "鼠", "蛆", "烂", "废", "惨")
_JUDGMENT_TERMS = {
    "厌恶": ("恶心", "讨厌", "烦", "脏", "假", "屎", "骗", "活该"),
    "怜悯": ("可怜", "心软", "心疼", "难受", "怜悯"),
    "自损": ("我这种", "我就", "我这种人", "活该", "窝囊", "废物"),
    "讥讽": ("笑死", "装", "高贵", "贵族", "资本", "表演"),
}
_OPENING_MARKERS = {
    "scene_entry": ("夜", "雨", "风", "站台", "门", "窗", "灯", "街", "店", "楼道"),
    "judgment_first": ("就是", "根本", "从来", "本来", "结果", "其实"),
    "self_declare": ("我", "窝", "自己"),
    "question": ("为什么", "凭什么", "怎么", "难道", "?"),
    "memory_drop": ("那天", "后来", "以前", "有次", "当时"),
}
_TURNING_MARKERS = ("但", "但是", "可是", "却", "然而", "不过", "忽然", "结果", "直到", "后来")
_CLOSING_MARKERS = {
    "residue_image": ("灯", "风", "夜", "余温", "影子", "玻璃", "雨"),
    "open_question": ("吗", "呢", "?"),
    "self_judgment": ("活该", "算了", "认了", "也就这样"),
    "gesture_stop": ("放回去", "停住", "转身", "低头", "站着", "走开"),
}


def normalize_stone_profile_v2(
    payload: dict[str, Any] | None,
    *,
    article_text: str | None = None,
    fallback_title: str | None = None,
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
    length_band = _resolve_length_band(raw.get("length_band"), text)
    opening, pivot, closing, signature = _derive_anchor_windows(text)
    voice_mask = _normalize_voice_mask(raw.get("voice_mask"), text)
    syntax_signature = _normalize_syntax_signature(raw.get("syntax_signature"), text)
    stance_vector = _normalize_stance_vector(raw.get("stance_vector"), text, voice_mask)
    motif_tags = _normalize_string_list(raw.get("motif_tags"), limit=4) or _derive_motif_tags(text)
    segment_map = _normalize_string_list(raw.get("segment_map"), limit=4) or _derive_segment_map(
        text,
        has_pivot=bool(pivot),
        length_band=length_band,
    )
    content_kernel = _normalize_content_kernel(
        raw.get("content_kernel") or raw.get("content_summary"),
        text=text,
        length_band=length_band,
        fallback_title=fallback_title,
    )
    opening_move = _normalize_short_text(raw.get("opening_move")) or _derive_opening_move(opening)
    turning_move = _normalize_short_text(raw.get("turning_move")) or _derive_turning_move(pivot)
    closure_move = _normalize_short_text(raw.get("closure_move")) or _derive_closure_move(closing)
    lexicon_markers = _normalize_string_list(raw.get("lexicon_markers"), limit=8) or _derive_lexicon_markers(text)
    emotion_curve = _normalize_string_list(raw.get("emotion_curve"), limit=3) or _derive_emotion_curve(
        opening,
        pivot,
        closing,
        voice_mask=voice_mask,
        stance_vector=stance_vector,
    )
    rhetorical_devices = _normalize_string_list(raw.get("rhetorical_devices"), limit=6) or _derive_rhetorical_devices(
        text,
        opening=opening,
        pivot=pivot,
        closing=closing,
        voice_mask=voice_mask,
    )
    anchor_spans = _normalize_anchor_spans(
        raw.get("anchor_spans"),
        opening,
        pivot,
        closing,
        signature,
        selected_passages=raw.get("selected_passages"),
    )
    surface_form = _resolve_surface_form(
        raw.get("surface_form") or _legacy_surface_form(raw.get("content_type")),
        text,
        length_band=length_band,
        voice_mask=voice_mask,
    )
    anti_patterns = _normalize_string_list(raw.get("anti_patterns"), limit=6) or _derive_anti_patterns(
        surface_form=surface_form,
        voice_mask=voice_mask,
        closure_move=closure_move,
        stance_vector=stance_vector,
    )

    normalized = {
        "length_band": length_band,
        "content_kernel": content_kernel,
        "surface_form": surface_form,
        "voice_mask": voice_mask,
        "lexicon_markers": lexicon_markers[:8],
        "syntax_signature": syntax_signature,
        "segment_map": segment_map[:4],
        "opening_move": opening_move,
        "turning_move": turning_move or "none",
        "closure_move": closure_move,
        "motif_tags": motif_tags[:4],
        "stance_vector": stance_vector,
        "emotion_curve": emotion_curve[:3],
        "rhetorical_devices": rhetorical_devices[:6],
        "prototype_family": _normalize_short_text(raw.get("prototype_family")),
        "anchor_spans": anchor_spans,
        "anti_patterns": anti_patterns[:6],
    }
    normalized["prototype_family"] = normalized["prototype_family"] or build_short_text_cluster_key(normalized)
    for passthrough_key in ("document_id", "title", "article_text", "source_text", "raw_text"):
        if raw.get(passthrough_key) is not None:
            normalized[passthrough_key] = raw.get(passthrough_key)
    return normalized


def build_stone_profile_v2(document: Any) -> dict[str, Any]:
    text = normalize_whitespace(str(getattr(document, "clean_text", None) or getattr(document, "raw_text", None) or ""))
    return normalize_stone_profile_v2(
        {},
        article_text=text,
        fallback_title=getattr(document, "title", None) or getattr(document, "filename", None),
    )


def build_stone_profile_v2_messages(
    project_name: str,
    document_title: str | None,
    article_text: str,
) -> list[dict[str, str]]:
    schema = {
        "length_band": "micro|short|medium|long",
        "content_kernel": "一句语义核；短文可返回精确字面量 raw",
        "surface_form": "scene_vignette|rant|confession|anecdote|aphorism|dialogue_bit|manifesto|list_bit",
        "voice_mask": {
            "person": "first|second|third|mixed",
            "address_target": "self|you|crowd|specific_other|none",
            "distance": "贴脸|回收|旁观|宣判",
            "self_position": "自损|自嘲|冷眼|求援|none",
        },
        "lexicon_markers": ["高辨识词或固定搭配"],
        "syntax_signature": {
            "cadence": "短促|堆叠|回环|顿挫|长压短收",
            "sentence_shape": "短句群|长句拖行|混合",
            "punctuation_habits": ["……", "？", "、"],
        },
        "segment_map": ["opening", "pressure", "pivot", "residue"],
        "opening_move": "如何起笔",
        "turning_move": "如何转折，可为 none",
        "closure_move": "如何收口",
        "motif_tags": ["意象/场景/母题"],
        "stance_vector": {
            "target": "在判断谁/什么",
            "judgment": "厌恶|怜悯|自损|讥讽|悬置",
            "value_lens": "代价|资格|体面|生存|虚假",
        },
        "emotion_curve": ["起点", "峰值/转折", "回落"],
        "rhetorical_devices": ["反衬", "荒诞比喻", "自贬黑话", "重复", "反问"],
        "prototype_family": "按稳定写法概括的套路族名",
        "anchor_spans": {
            "opening": "原文",
            "pivot": "原文或空字符串",
            "closing": "原文",
            "signature": ["1-3条原文"],
        },
        "anti_patterns": ["这篇最怕被写成什么"],
    }
    return [
        {
            "role": "system",
            "content": (
                "你正在为 Stone v2 生成单篇文章画像。\n"
                "只返回 JSON，不要输出解释。\n"
                "所有 anchor_spans 必须逐字摘自原文，不能改写。\n"
                "如果文章很短且 content_kernel 没必要总结，请把 content_kernel 写成 raw。\n"
                "不要使用 DSM、诊断、病理标签。\n"
                f"必须且只能使用这些字段：{json.dumps(schema, ensure_ascii=False)}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"项目名：{project_name}\n"
                f"文章标题：{document_title or '(untitled)'}\n\n"
                "请基于下面这篇文章输出 Stone v2 画像。\n"
                f"文章原文：\n{article_text}"
            ),
        },
    ]


def expand_stone_profile_v2_for_analysis(
    profile: dict[str, Any] | None,
    *,
    article_text: str = "",
    title: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_stone_profile_v2(
        profile,
        article_text=article_text,
        fallback_title=title,
    )
    anchor_spans = dict(normalized.get("anchor_spans") or {})
    selected_passages = [
        item
        for item in (
            anchor_spans.get("opening"),
            *(anchor_spans.get("signature") or []),
            anchor_spans.get("closing"),
        )
        if normalize_whitespace(str(item or ""))
    ][:3]
    emotion_curve = [
        str(item or "").strip()
        for item in (normalized.get("emotion_curve") or [])
        if str(item or "").strip()
    ]
    voice_mask = dict(normalized.get("voice_mask") or {})
    stance_vector = dict(normalized.get("stance_vector") or {})
    return {
        "content_summary": normalized.get("content_kernel") or "",
        "content_type": normalized.get("surface_form") or "",
        "length_label": normalized.get("length_band") or "",
        "emotion_label": " / ".join(emotion_curve[:2]),
        "selected_passages": selected_passages,
        "article_theme": normalized.get("content_kernel") or (title or ""),
        "document_theme": normalized.get("content_kernel") or (title or ""),
        "narrative_pov": voice_mask.get("person") or "",
        "tone": " / ".join(
            item
            for item in (
                normalized.get("surface_form"),
                stance_vector.get("judgment"),
                voice_mask.get("distance"),
            )
            if str(item or "").strip()
        ),
        "structure_template": " -> ".join(
            str(item or "").strip()
            for item in (normalized.get("segment_map") or [])
            if str(item or "").strip()
        ),
        "lexical_markers": list(normalized.get("lexicon_markers") or [])[:6],
        "emotional_progression": " -> ".join(emotion_curve[:3]),
        "nonclinical_signals": list(normalized.get("anti_patterns") or [])[:4],
        "representative_lines": list(anchor_spans.get("signature") or [])[:3],
    }


def build_short_text_cluster_key(profile: dict[str, Any]) -> str:
    voice_mask = dict(profile.get("voice_mask") or {})
    stance_vector = dict(profile.get("stance_vector") or {})
    motif_tags = _normalize_string_list(profile.get("motif_tags"), limit=2)
    parts = [
        profile.get("surface_form") or "unknown",
        profile.get("opening_move") or "unknown_opening",
        profile.get("closure_move") or "unknown_closing",
        (voice_mask.get("distance") or "unknown_distance"),
        (stance_vector.get("judgment") or "悬置"),
        *(motif_tags or ["none"]),
    ]
    return "|".join(_slug_piece(item) for item in parts if _slug_piece(item))


def build_short_text_clusters(
    profiles: list[dict[str, Any]],
    *,
    chat_config: Any | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in profiles:
        normalized = normalize_stone_profile_v2(profile)
        if normalized["length_band"] not in {"micro", "short"}:
            continue
        grouped[build_short_text_cluster_key(normalized)].append(normalized)

    clusters: list[dict[str, Any]] = []
    for index, (cluster_key, items) in enumerate(sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])), start=1):
        prototype_family = _cluster_label(items)
        representative = items[0]
        exemplar_windows = _collect_cluster_windows(items)
        clusters.append(
            {
                "cluster_key": cluster_key,
                "prototype_family": prototype_family,
                "cluster_label": prototype_family,
                "member_count": len(items),
                "member_document_ids": [str(item.get("document_id") or "") for item in items if item.get("document_id")],
                "surface_form": representative.get("surface_form"),
                "opening_move": representative.get("opening_move"),
                "closure_move": representative.get("closure_move"),
                "distance": ((representative.get("voice_mask") or {}).get("distance") or ""),
                "judgment": ((representative.get("stance_vector") or {}).get("judgment") or ""),
                "motif_tags": _shared_terms(items, "motif_tags", limit=4),
                "summary": _cluster_summary(items),
                "exemplar_windows": exemplar_windows,
                "index": index,
            }
        )
    return clusters


def build_stone_author_model_v2(
    *,
    project_name: str,
    profiles: list[dict[str, Any]],
    short_text_clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_profiles = [normalize_stone_profile_v2(profile) for profile in profiles]
    voice_form = _build_voice_form_view(normalized_profiles)
    motif_worldview = _build_motif_worldview_view(normalized_profiles)
    style_invariants = {
        "voice_form": voice_form,
        "motif_worldview": motif_worldview,
        "lexicon_tics": _build_lexicon_tics(normalized_profiles),
        "rhetoric_preferences": _build_rhetoric_preferences(normalized_profiles),
        "opening_signatures": _build_opening_signatures(normalized_profiles),
        "closure_signatures": _build_closure_signatures(normalized_profiles),
    }
    blueprint_rules = _build_blueprint_rules(normalized_profiles, short_text_clusters)
    prototype_families = [
        {
            "family_key": cluster["cluster_key"],
            "label": cluster["prototype_family"],
            "member_count": cluster["member_count"],
            "shared_traits": [
                f"起笔：{cluster.get('opening_move') or ''}",
                f"收口：{cluster.get('closure_move') or ''}",
                f"距离：{cluster.get('distance') or ''}",
                f"判断：{cluster.get('judgment') or ''}",
            ],
            "motif_tags": list(cluster.get("motif_tags") or [])[:4],
        }
        for cluster in short_text_clusters
    ]
    anti_patterns = _derive_author_anti_patterns(normalized_profiles)
    length_behaviors = _build_length_behaviors(normalized_profiles)
    topic_translation_map = _build_topic_translation_map(normalized_profiles)
    return {
        "asset_kind": "stone_author_model_v2",
        "version": "v2",
        "project_name": project_name,
        "profile_count": len(normalized_profiles),
        "family_count": len(prototype_families),
        "evidence_window_count": len(_collect_author_evidence_windows(normalized_profiles)),
        "style_invariants": style_invariants,
        "blueprint_rules": blueprint_rules,
        "prototype_families": prototype_families,
        "topic_translation_map": topic_translation_map,
        "anti_patterns": anti_patterns,
        "length_behaviors": length_behaviors,
        "views": {
            "voice_form": voice_form,
            "motif_worldview": motif_worldview,
            "prototype_families": [
                f"{item['label']} ({item['member_count']}篇)"
                for item in prototype_families[:8]
            ],
            "anti_patterns": anti_patterns,
        },
        "evidence_windows": _collect_author_evidence_windows(normalized_profiles),
    }


def is_valid_stone_v2_asset_payload(asset_kind: str, payload: dict[str, Any] | None) -> bool:
    try:
        validate_stone_v2_asset_payload(asset_kind, payload)
    except ValueError:
        return False
    return True


def validate_stone_v2_asset_payload(asset_kind: str, payload: dict[str, Any] | None) -> None:
    if asset_kind not in STONE_V2_ASSET_KINDS:
        raise ValueError(f"Unsupported Stone v2 asset kind: {asset_kind}")
    if not isinstance(payload, dict):
        raise ValueError("Stone v2 asset payload must be a JSON object.")
    if payload.get("asset_kind") != asset_kind:
        raise ValueError(f"Stone v2 payload asset_kind must be {asset_kind}.")
    if asset_kind == "stone_author_model_v2":
        _validate_stone_author_model_payload(payload)
        return
    _validate_stone_prototype_index_payload(payload)


def _validate_stone_author_model_payload(payload: dict[str, Any]) -> None:
    views = payload.get("views")
    style_invariants = payload.get("style_invariants")
    if not isinstance(views, dict) or not isinstance(style_invariants, dict):
        raise ValueError("Stone Author Model V2 must include views and style_invariants.")
    if not isinstance(payload.get("topic_translation_map"), list):
        raise ValueError("Stone Author Model V2 must include topic_translation_map.")
    if not isinstance(payload.get("anti_patterns"), list):
        raise ValueError("Stone Author Model V2 must include anti_patterns.")
    has_author_signal = any(
        isinstance(views.get(key), list)
        for key in ("voice_form", "motif_worldview", "prototype_families", "anti_patterns")
    )
    has_style_signal = any(
        isinstance(style_invariants.get(key), list)
        for key in (
            "lexicon_tics",
            "rhetoric_preferences",
            "opening_signatures",
            "closure_signatures",
            "voice_form",
            "motif_worldview",
        )
    )
    if not has_author_signal or not has_style_signal:
        raise ValueError("Stone Author Model V2 lacks author-style signals.")


def _validate_stone_prototype_index_payload(payload: dict[str, Any]) -> None:
    documents = payload.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ValueError("Stone Prototype Index V2 must include prototype documents.")
    if not any(_prototype_document_has_anchor_window(item) for item in documents if isinstance(item, dict)):
        raise ValueError("Stone Prototype Index V2 must include document windows that can form source anchors.")


def _prototype_document_has_anchor_window(item: dict[str, Any]) -> bool:
    document_id = str(item.get("document_id") or item.get("id") or "").strip()
    if not document_id:
        return False
    windows = item.get("windows")
    if not isinstance(windows, dict):
        return False
    for key in ("opening", "pivot", "closing"):
        if normalize_whitespace(str(windows.get(key) or "")):
            return True
    signatures = windows.get("signature_line")
    return isinstance(signatures, list) and any(normalize_whitespace(str(value or "")) for value in signatures)


def build_stone_prototype_index_v2(
    *,
    project_name: str,
    profiles: list[dict[str, Any]],
    documents: list[dict[str, Any]],
) -> dict[str, Any]:
    document_lookup = {str(item.get("document_id") or item.get("id") or ""): item for item in documents}
    entries: list[dict[str, Any]] = []
    for profile in profiles:
        normalized = normalize_stone_profile_v2(profile)
        document_id = str(normalized.get("document_id") or "")
        document = document_lookup.get(document_id) or {}
        raw_text = normalize_whitespace(str(document.get("text") or document.get("clean_text") or document.get("raw_text") or ""))
        windows = {
            "opening": normalized["anchor_spans"].get("opening") or "",
            "pivot": normalized["anchor_spans"].get("pivot") or "",
            "closing": normalized["anchor_spans"].get("closing") or "",
            "signature_line": list(normalized["anchor_spans"].get("signature") or [])[:3],
        }
        entries.append(
            {
                "document_id": document_id,
                "title": str(document.get("title") or normalized.get("title") or "（未命名）").strip() or "（未命名）",
                "length_band": normalized["length_band"],
                "prototype_family": normalized["prototype_family"],
                "surface_form": normalized["surface_form"],
                "retrieval_facets": {
                    "prototype_family": normalized["prototype_family"],
                    "length_band": normalized["length_band"],
                    "judgment": ((normalized.get("stance_vector") or {}).get("judgment") or ""),
                    "value_lens": ((normalized.get("stance_vector") or {}).get("value_lens") or ""),
                    "distance": ((normalized.get("voice_mask") or {}).get("distance") or ""),
                    "motif_tags": list(normalized.get("motif_tags") or [])[:4],
                },
                "motif_tags": list(normalized.get("motif_tags") or [])[:4],
                "voice_mask": dict(normalized.get("voice_mask") or {}),
                "stance_vector": dict(normalized.get("stance_vector") or {}),
                "exemplar_text": _truncate_text(raw_text or normalized["content_kernel"], 900),
                "windows": windows,
                "anchor_spans": dict(normalized.get("anchor_spans") or {}),
                "retrieval_terms": _unique_preserve_order(
                    [
                        normalized["prototype_family"],
                        normalized["surface_form"],
                        normalized["opening_move"],
                        normalized["closure_move"],
                        *((normalized.get("motif_tags") or [])[:4]),
                        *((normalized.get("lexicon_markers") or [])[:4]),
                    ]
                )[:12],
            }
        )
    family_summary = _summarize_prototype_families(entries)
    return {
        "asset_kind": "stone_prototype_index_v2",
        "version": "v2",
        "project_name": project_name,
        "document_count": len(entries),
        "family_count": len(family_summary),
        "retrieval_policy": {
            "ranking_formula": "prototype_family 35% + length_band 25% + stance_vector 20% + motif_tags 10% + voice_mask 10%",
            "weights": {
                "prototype_family": 35,
                "length_band": 25,
                "stance_vector": 20,
                "motif_tags": 10,
                "voice_mask": 10,
            },
            "ranking_fields": [
                "prototype_family",
                "length_band",
                "stance_vector.judgment",
                "stance_vector.value_lens",
                "motif_tags",
                "voice_mask.distance",
            ],
        },
        "prototype_families": family_summary,
        "retrieval_term_index": _build_retrieval_term_index(entries),
        "documents": entries,
    }


def render_stone_author_model_markdown(payload: dict[str, Any]) -> str:
    views = dict(payload.get("views") or {})
    style_invariants = dict(payload.get("style_invariants") or {})
    blueprint_rules = dict(payload.get("blueprint_rules") or {})
    length_behaviors = list(payload.get("length_behaviors") or [])[:6]
    topic_translation_map = list(payload.get("topic_translation_map") or [])[:6]
    prototype_families = list(payload.get("prototype_families") or [])[:6]
    evidence_windows = list(payload.get("evidence_windows") or [])[:4]
    lines = [
        "# Stone Author Model V2",
        "",
        f"- project: {payload.get('project_name') or ''}",
        f"- profile_count: {payload.get('profile_count') or 0}",
        f"- family_count: {payload.get('family_count') or len(prototype_families)}",
        "",
        "## Voice / Form",
    ]
    lines.extend(f"- {item}" for item in views.get("voice_form") or [])
    lines.extend(["", "## Motif / Worldview"])
    lines.extend(f"- {item}" for item in views.get("motif_worldview") or [])
    lines.extend(["", "## Lexicon / Rhetoric"])
    lines.extend(f"- {item}" for item in style_invariants.get("lexicon_tics") or [])
    lines.extend(f"- {item}" for item in style_invariants.get("rhetoric_preferences") or [])
    lines.extend(["", "## Opening / Closure Signatures"])
    for item in style_invariants.get("opening_signatures") or []:
        lines.append(f"- opening: {item.get('move') or ''} ({item.get('count') or 0}) | {item.get('anchor') or ''}")
    for item in style_invariants.get("closure_signatures") or []:
        lines.append(f"- closing: {item.get('move') or ''} ({item.get('count') or 0}) | {item.get('anchor') or ''}")
    lines.extend(["", "## Blueprint Rules"])
    for key in ("entry_rules", "development_rules", "closure_rules"):
        values = list(blueprint_rules.get(key) or [])
        if values:
            lines.append(f"- {key}: {'；'.join(values)}")
    lines.extend(["", "## Length Behaviors"])
    for item in length_behaviors:
        lines.append(
            f"- {item.get('length_band') or ''}: {item.get('surface_form') or ''}; "
            f"opening={item.get('opening_move') or ''}; closing={item.get('closure_move') or ''}"
        )
    lines.extend(["", "## Topic Translation Map"])
    for item in topic_translation_map:
        lines.append(
            f"- {item.get('value_lens') or ''}: motifs={', '.join(item.get('motif_tags') or [])}; "
            f"opening={', '.join(item.get('opening_moves') or [])}; closing={', '.join(item.get('closure_moves') or [])}"
        )
    lines.extend(["", "## Prototype Families"])
    for item in prototype_families:
        lines.append(f"- {item.get('label') or item.get('family_key') or ''} ({item.get('member_count') or 0})")
    lines.extend(["", "## Anti-Patterns"])
    lines.extend(f"- {item}" for item in views.get("anti_patterns") or [])
    lines.extend(["", "## Evidence Windows"])
    for item in evidence_windows:
        lines.extend(
            [
                f"- family: {item.get('prototype_family') or ''}",
                f"  opening: {item.get('opening') or ''}",
                f"  closing: {item.get('closing') or ''}",
            ]
        )
    return "\n".join(lines).strip()


def render_stone_prototype_index_markdown(payload: dict[str, Any]) -> str:
    family_summary = list(payload.get("prototype_families") or [])[:8]
    retrieval_policy = dict(payload.get("retrieval_policy") or {})
    term_index = list(payload.get("retrieval_term_index") or [])[:12]
    lines = [
        "# Stone Prototype Index V2",
        "",
        f"- project: {payload.get('project_name') or ''}",
        f"- document_count: {payload.get('document_count') or 0}",
        f"- family_count: {payload.get('family_count') or len(family_summary)}",
        "",
        "## Retrieval Policy",
        f"- formula: {retrieval_policy.get('ranking_formula') or ''}",
        f"- fields: {', '.join(retrieval_policy.get('ranking_fields') or [])}",
        "",
        "## Family Summary",
    ]
    for item in family_summary:
        lines.append(
            f"- {item.get('label') or item.get('family_key') or ''}: "
            f"{item.get('member_count') or 0} docs; motifs={', '.join(item.get('motif_tags') or [])}"
        )
    lines.extend(["", "## Retrieval Term Index"])
    for item in term_index:
        lines.append(
            f"- {item.get('term') or ''}: {item.get('count') or 0}; "
            f"families={', '.join(item.get('families') or [])}"
        )
    lines.append("")
    for item in (payload.get("documents") or [])[:12]:
        lines.extend(
            [
                f"## {item.get('title') or '（未命名）'}",
                f"- family: {item.get('prototype_family') or ''}",
                f"- length_band: {item.get('length_band') or ''}",
                f"- motifs: {', '.join(item.get('motif_tags') or [])}",
                f"- retrieval_terms: {', '.join(item.get('retrieval_terms') or [])}",
                f"- opening: {(item.get('windows') or {}).get('opening') or ''}",
                f"- closing: {(item.get('windows') or {}).get('closing') or ''}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _summarize_prototype_families(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in entries:
        family = _normalize_short_text(item.get("prototype_family"))
        if family:
            grouped[family].append(item)
    rows: list[dict[str, Any]] = []
    for family, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        motif_counter: Counter[str] = Counter()
        for item in items:
            motif_counter.update(item.get("motif_tags") or [])
        rows.append(
            {
                "family_key": family,
                "label": family,
                "member_count": len(items),
                "motif_tags": [value for value, _ in motif_counter.most_common(4)],
                "sample_titles": [str(item.get("title") or "").strip() for item in items[:3] if str(item.get("title") or "").strip()],
            }
        )
    return rows


def _resolve_length_band(value: Any, text: str) -> str:
    normalized = _normalize_short_text(value).lower()
    if normalized in STONE_PROFILE_V2_LENGTH_BANDS:
        return normalized
    legacy = _normalize_short_text(value)
    legacy_map = {
        "微型": "micro",
        "超短": "micro",
        "短文": "short",
        "中篇": "medium",
        "长文": "long",
    }
    if legacy in legacy_map:
        return legacy_map[legacy]
    word_count = estimate_word_count(text)
    if word_count <= 120:
        return "micro"
    if word_count <= 300:
        return "short"
    if word_count <= 900:
        return "medium"
    return "long"


def estimate_word_count(text: str) -> int:
    content = str(text or "")
    cjk_units = sum(1 for char in content if "\u4e00" <= char <= "\u9fff")
    latin_units = len(re.findall(r"[A-Za-z0-9_]+", content))
    return cjk_units + latin_units


def _normalize_content_kernel(value: Any, *, text: str, length_band: str, fallback_title: str | None) -> str:
    raw_value = _normalize_short_text(value)
    if raw_value.lower() == "raw":
        return text or _normalize_short_text(fallback_title)
    if raw_value:
        return raw_value
    if length_band in {"micro", "short"}:
        return text or _normalize_short_text(fallback_title)
    sentences = _split_sentences(text)
    if not sentences:
        return _normalize_short_text(fallback_title)
    if len(sentences) == 1:
        return _truncate_text(sentences[0], 96)
    return _truncate_text("；".join(sentences[:2]), 110)


def _legacy_surface_form(value: Any) -> str:
    normalized = _normalize_short_text(value)
    mapping = {
        "自嘲式抱怨": "confession",
        "抱怨": "rant",
        "宣言": "manifesto",
        "场景": "scene_vignette",
        "片段": "scene_vignette",
        "对话": "dialogue_bit",
        "短句感想": "aphorism",
        "叙事": "anecdote",
        "自白": "confession",
    }
    return mapping.get(normalized, "")


def _resolve_surface_form(value: Any, text: str, *, length_band: str, voice_mask: dict[str, str]) -> str:
    normalized = _normalize_short_text(value)
    if normalized in STONE_PROFILE_V2_SURFACE_FORMS:
        return normalized
    paragraphs = _split_paragraphs(text)
    first = paragraphs[0] if paragraphs else text
    lower = text.lower()
    if re.search(r"^\s*[-*•]\s+", text, flags=re.MULTILINE) or re.search(r"^\s*\d+[.)、]\s+", text, flags=re.MULTILINE):
        return "list_bit"
    if first.count("“") + first.count("\"") >= 2 or re.search(r"^[-—]{0,1}\s*[^，。！？!?]{1,20}[：:]", first):
        return "dialogue_bit"
    if any(marker in first for marker in ("必须", "应该", "别再", "不要", "滚", "你们")) and voice_mask.get("distance") in {"贴脸", "宣判"}:
        return "manifesto"
    if any(marker in text for marker in ("后来", "那天", "有次", "结果", "直到")):
        return "anecdote"
    if estimate_word_count(text) <= 180 and not any(term in text for term in _SCENE_TERMS):
        return "aphorism"
    if any(term in text for term in _SCENE_TERMS):
        return "scene_vignette"
    if voice_mask.get("self_position") in {"自损", "自嘲", "求援"}:
        return "confession"
    if any(term in lower for term in ("妈的", "恶心", "垃圾", "活该", "骗")):
        return "rant"
    if length_band in {"micro", "short"}:
        return "confession"
    return "scene_vignette"


def _normalize_voice_mask(value: Any, text: str) -> dict[str, str]:
    raw = value if isinstance(value, dict) else {}
    person = _normalize_short_text(raw.get("person")) or _detect_person(text)
    address_target = _normalize_short_text(raw.get("address_target")) or _detect_address_target(text)
    distance = _normalize_short_text(raw.get("distance")) or _detect_distance(text)
    self_position = _normalize_short_text(raw.get("self_position")) or _detect_self_position(text)
    return {
        "person": person if person in {"first", "second", "third", "mixed"} else "mixed",
        "address_target": (
            address_target if address_target in {"self", "you", "crowd", "specific_other", "none"} else "none"
        ),
        "distance": distance if distance in {"贴脸", "回收", "旁观", "宣判"} else "回收",
        "self_position": (
            self_position if self_position in {"自损", "自嘲", "冷眼", "求援", "none"} else "none"
        ),
    }


def _normalize_syntax_signature(value: Any, text: str) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    derived = _derive_syntax_signature(text)
    cadence = _normalize_short_text(raw.get("cadence")) or derived["cadence"]
    sentence_shape = _normalize_short_text(raw.get("sentence_shape")) or derived["sentence_shape"]
    punctuation_habits = _normalize_string_list(raw.get("punctuation_habits"), limit=6) or derived["punctuation_habits"]
    return {
        "cadence": cadence,
        "sentence_shape": sentence_shape,
        "punctuation_habits": punctuation_habits[:6],
    }


def _normalize_stance_vector(value: Any, text: str, voice_mask: dict[str, str]) -> dict[str, str]:
    raw = value if isinstance(value, dict) else {}
    derived = _derive_stance_vector(text, voice_mask=voice_mask)
    target = _normalize_short_text(raw.get("target")) or derived["target"]
    judgment = _normalize_short_text(raw.get("judgment")) or derived["judgment"]
    value_lens = _normalize_short_text(raw.get("value_lens")) or derived["value_lens"]
    return {
        "target": target,
        "judgment": judgment if judgment in {"厌恶", "怜悯", "自损", "讥讽", "悬置"} else "悬置",
        "value_lens": value_lens if value_lens in {"代价", "资格", "体面", "生存", "虚假"} else "代价",
    }


def _normalize_anchor_spans(
    value: Any,
    opening: str,
    pivot: str,
    closing: str,
    signature: list[str],
    *,
    selected_passages: Any = None,
) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    legacy_signature = _normalize_string_list(selected_passages, limit=3)
    signature_values = _normalize_string_list(raw.get("signature"), limit=3) or legacy_signature or signature
    return {
        "opening": _normalize_short_text(raw.get("opening")) or (legacy_signature[0] if legacy_signature else opening),
        "pivot": _normalize_short_text(raw.get("pivot")) or pivot,
        "closing": _normalize_short_text(raw.get("closing")) or (legacy_signature[-1] if legacy_signature else closing),
        "signature": signature_values[:3],
    }


def _derive_anchor_windows(text: str) -> tuple[str, str, str, list[str]]:
    paragraphs = _split_paragraphs(text)
    sentences = _split_sentences(text)
    opening = paragraphs[0] if paragraphs else (sentences[0] if sentences else text)
    pivot_sentence = _locate_pivot_sentence(sentences)
    closing = paragraphs[-1] if paragraphs else (sentences[-1] if sentences else text)
    signature = _unique_preserve_order(
        [
            _truncate_text(opening, 220),
            _truncate_text(pivot_sentence, 220),
            _truncate_text(closing, 220),
            _strongest_sentence(sentences),
        ]
    )
    return (
        _truncate_text(opening, 260),
        _truncate_text(pivot_sentence, 260),
        _truncate_text(closing, 260),
        signature[:3],
    )


def _derive_segment_map(text: str, *, has_pivot: bool, length_band: str) -> list[str]:
    if not text:
        return ["opening", "residue"]
    if length_band == "micro":
        return ["opening", "residue"]
    if length_band == "short":
        return ["opening", "pressure", "residue"] if not has_pivot else ["opening", "pivot", "residue"]
    if has_pivot:
        return ["opening", "pressure", "pivot", "residue"]
    return ["opening", "pressure", "residue"]


def _derive_opening_move(text: str) -> str:
    candidate = text or ""
    for label, markers in _OPENING_MARKERS.items():
        if any(marker in candidate for marker in markers):
            return {
                "scene_entry": "从场景或物件切入",
                "judgment_first": "先下判断再补情境",
                "self_declare": "先报出自我姿态",
                "question": "用问题起笔",
                "memory_drop": "从回忆节点切入",
            }[label]
    return "从一个轻动作起笔"


def _derive_turning_move(text: str) -> str:
    if not text:
        return "none"
    if "忽然" in text:
        return "突发转折"
    if "结果" in text or "直到" in text:
        return "延迟揭露"
    if any(marker in text for marker in ("但", "但是", "可是", "却", "然而", "不过")):
        return "用反差拧转句意"
    return "none"


def _derive_closure_move(text: str) -> str:
    candidate = text or ""
    if any(marker in candidate for marker in _CLOSING_MARKERS["open_question"]):
        return "留问号收口"
    if any(marker in candidate for marker in _CLOSING_MARKERS["self_judgment"]):
        return "落回自我判断"
    if any(marker in candidate for marker in _CLOSING_MARKERS["gesture_stop"]):
        return "用动作停住"
    if any(marker in candidate for marker in _CLOSING_MARKERS["residue_image"]):
        return "把情绪收进意象残响"
    return "留一层没说尽的余味"


def _derive_lexicon_markers(text: str) -> list[str]:
    counts: Counter[str] = Counter()
    for token in _cjk_ngrams(text, min_len=2, max_len=4):
        if token in _COMMON_CJK_NGRAMS:
            continue
        counts[token] += 1
    markers = [term for term, count in counts.most_common() if count > 1][:6]
    if markers:
        return markers
    fallback = []
    for marker in ("我", "你", "我们", "他们", "结果", "其实", "但是", "就是", "好像", "像"):
        if marker in text and marker not in fallback:
            fallback.append(marker)
    return fallback[:6]


def _derive_syntax_signature(text: str) -> dict[str, Any]:
    sentences = _split_sentences(text)
    lengths = [max(1, estimate_word_count(item)) for item in sentences] or [0]
    avg_length = mean(lengths)
    variance = max(lengths) - min(lengths)
    if avg_length <= 18:
        cadence = "短促"
    elif variance >= 20 and avg_length >= 18:
        cadence = "长压短收"
    elif text.count("，") >= 4 and avg_length >= 14:
        cadence = "堆叠"
    elif text.count("又") >= 2 or text.count("还是") >= 2:
        cadence = "回环"
    else:
        cadence = "顿挫"
    if variance <= 6:
        sentence_shape = "短句群" if avg_length <= 18 else "长句拖行"
    elif avg_length >= 22:
        sentence_shape = "混合"
    else:
        sentence_shape = "短句群"
    punctuation = []
    for token in ("……", "？", "！", "、", "——", "，", "。"):
        if token in text and token not in punctuation:
            punctuation.append(token)
    return {
        "cadence": cadence,
        "sentence_shape": sentence_shape,
        "punctuation_habits": punctuation[:6],
    }


def _derive_motif_tags(text: str) -> list[str]:
    tags: list[str] = []
    for term in _SCENE_TERMS:
        if term in text and term not in tags:
            tags.append(term)
        if len(tags) >= 4:
            return tags
    tags.extend(item for item in _derive_lexicon_markers(text) if item not in tags)
    return tags[:4] or ["日常残响"]


def _derive_stance_vector(text: str, *, voice_mask: dict[str, str]) -> dict[str, str]:
    if voice_mask.get("self_position") in {"自损", "自嘲"}:
        target = "自己"
        judgment = "自损"
    elif any(token in text for token in ("你们", "他们", "这些人", "资本", "父母", "店里")):
        target = "外部对象"
        judgment = "厌恶" if any(token in text for token in ("恶心", "骗", "垃圾", "假")) else "讥讽"
    else:
        target = "关系处境"
        judgment = "悬置"
    value_lens = "代价"
    for _, (label, terms) in _VALUE_LENS_TERMS.items():
        if any(term in text for term in terms):
            value_lens = label
            break
    for label, terms in _JUDGMENT_TERMS.items():
        if any(term in text for term in terms):
            judgment = label
            break
    return {
        "target": target,
        "judgment": judgment,
        "value_lens": value_lens,
    }


def _derive_emotion_curve(
    opening: str,
    pivot: str,
    closing: str,
    *,
    voice_mask: dict[str, str],
    stance_vector: dict[str, str],
) -> list[str]:
    start = "压低" if voice_mask.get("distance") == "回收" else "逼近"
    if voice_mask.get("distance") == "旁观":
        start = "旁看"
    middle = "显影" if pivot else ("压着不说" if stance_vector.get("judgment") == "悬置" else "逼出判断")
    end = "回落" if "余" in closing or "没说尽" in closing or voice_mask.get("distance") == "回收" else "停住"
    return [start, middle, end]


def _derive_rhetorical_devices(
    text: str,
    *,
    opening: str,
    pivot: str,
    closing: str,
    voice_mask: dict[str, str],
) -> list[str]:
    devices: list[str] = []
    if "像" in text or "仿佛" in text:
        devices.append("荒诞比喻")
    if pivot:
        devices.append("反衬")
    if voice_mask.get("self_position") in {"自损", "自嘲"}:
        devices.append("自贬黑话")
    if text.count("？") or "难道" in text or "凭什么" in text:
        devices.append("反问")
    if _has_repetition(opening, closing, text):
        devices.append("重复")
    return devices[:6] or ["留白"]


def _derive_anti_patterns(
    *,
    surface_form: str,
    voice_mask: dict[str, str],
    closure_move: str,
    stance_vector: dict[str, str],
) -> list[str]:
    patterns = [
        "不要写成解释性分析",
        "不要写成诊断或病理报告",
        "不要把收口写成工整结论",
    ]
    if surface_form in {"scene_vignette", "aphorism"}:
        patterns.append("不要写成完整故事梗概")
    if voice_mask.get("distance") == "回收":
        patterns.append("不要突然拔高成喊话或鸡汤")
    if stance_vector.get("judgment") in {"厌恶", "讥讽"}:
        patterns.append("不要把火气抹平成温和感悟")
    if "动作" in closure_move or "意象" in closure_move:
        patterns.append("不要在最后一行补解释")
    return _unique_preserve_order(patterns)[:6]


def _detect_person(text: str) -> str:
    first = sum(text.count(token) for token in ("我", "我们", "窝", "咱"))
    second = sum(text.count(token) for token in ("你", "你们", "泥", "您"))
    third = sum(text.count(token) for token in ("他", "她", "他们", "她们"))
    scores = [("first", first), ("second", second), ("third", third)]
    scores.sort(key=lambda item: item[1], reverse=True)
    top, top_score = scores[0]
    if top_score <= 0:
        return "mixed"
    if len([item for item in scores if item[1] > 0]) >= 2:
        return "mixed"
    return top


def _detect_address_target(text: str) -> str:
    if any(token in text for token in ("你们", "泥们", "各位", "大家")):
        return "crowd"
    if any(token in text for token in ("你", "泥", "您")):
        return "you"
    if any(token in text for token in ("我", "窝", "自己")):
        return "self"
    if any(token in text for token in ("他", "她", "父母", "老板", "那个人")):
        return "specific_other"
    return "none"


def _detect_distance(text: str) -> str:
    if any(token in text for token in ("你们", "滚", "恶心", "活该", "骗")) or text.count("！") >= 2:
        return "贴脸"
    if any(token in text for token in ("沉默", "回去", "没说", "压低", "不肯")):
        return "回收"
    if any(token in text for token in ("有人", "看着", "他们", "她", "他")) and "我" not in text:
        return "旁观"
    if any(token in text for token in ("就是", "根本", "必须", "应该", "从来")):
        return "宣判"
    return "回收"


def _detect_self_position(text: str) -> str:
    if any(term in text for term in _SELF_DEPRECATING_TERMS):
        return "自损"
    if any(token in text for token in ("笑死", "活该", "窝囊", "就这")) and "我" in text:
        return "自嘲"
    if any(token in text for token in ("求你", "帮我", "能不能", "想要")):
        return "求援"
    if any(token in text for token in ("看着", "懒得", "不屑", "冷眼")):
        return "冷眼"
    return "none"


def _locate_pivot_sentence(sentences: list[str]) -> str:
    for sentence in sentences[1:-1]:
        if any(marker in sentence for marker in _TURNING_MARKERS):
            return sentence
    return ""


def _strongest_sentence(sentences: list[str]) -> str:
    best = ""
    best_score = -1
    for sentence in sentences:
        score = len(sentence)
        if "像" in sentence or "仿佛" in sentence:
            score += 16
        if any(marker in sentence for marker in _TURNING_MARKERS):
            score += 12
        if "？" in sentence or "!" in sentence or "！" in sentence:
            score += 6
        if score > best_score:
            best = sentence
            best_score = score
    return _truncate_text(best, 220)


def _cluster_label(items: list[dict[str, Any]]) -> str:
    representative = items[0]
    motifs = " / ".join((representative.get("motif_tags") or [])[:2]) or "无显著意象"
    return f"{representative.get('surface_form') or 'unknown'} · {representative.get('opening_move') or '起笔'} · {motifs}"


def _cluster_summary(items: list[dict[str, Any]]) -> str:
    representative = items[0]
    return (
        f"这一簇多以“{representative.get('opening_move') or '起笔'}”进入，"
        f"最后用“{representative.get('closure_move') or '收口'}”结束，"
        f"叙述距离偏“{((representative.get('voice_mask') or {}).get('distance') or '')}”。"
    )


def _collect_cluster_windows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for item in items[:4]:
        anchor_spans = dict(item.get("anchor_spans") or {})
        windows.append(
            {
                "document_id": str(item.get("document_id") or ""),
                "opening": anchor_spans.get("opening") or "",
                "pivot": anchor_spans.get("pivot") or "",
                "closing": anchor_spans.get("closing") or "",
                "signature": list(anchor_spans.get("signature") or [])[:2],
            }
        )
    return windows


def _shared_terms(items: list[dict[str, Any]], key: str, *, limit: int) -> list[str]:
    counts: Counter[str] = Counter()
    for item in items:
        for term in item.get(key) or []:
            text = _normalize_short_text(term)
            if text:
                counts[text] += 1
    return [term for term, _ in counts.most_common(limit)]


def _build_voice_form_view(profiles: list[dict[str, Any]]) -> list[str]:
    distance_counter = Counter(((profile.get("voice_mask") or {}).get("distance") or "") for profile in profiles)
    surface_counter = Counter(profile.get("surface_form") or "" for profile in profiles)
    opening_counter = Counter(profile.get("opening_move") or "" for profile in profiles)
    closure_counter = Counter(profile.get("closure_move") or "" for profile in profiles)
    sentence_counter = Counter(((profile.get("syntax_signature") or {}).get("cadence") or "") for profile in profiles)
    lines = []
    if distance_counter:
        lines.append(f"叙述距离常驻：{_top_counter_line(distance_counter)}")
    if surface_counter:
        lines.append(f"表层形态偏好：{_top_counter_line(surface_counter)}")
    if opening_counter:
        lines.append(f"常见起笔动作：{_top_counter_line(opening_counter)}")
    if closure_counter:
        lines.append(f"常见收口动作：{_top_counter_line(closure_counter)}")
    if sentence_counter:
        lines.append(f"句法节奏：{_top_counter_line(sentence_counter)}")
    return lines[:6]


def _build_motif_worldview_view(profiles: list[dict[str, Any]]) -> list[str]:
    motif_counter: Counter[str] = Counter()
    lens_counter: Counter[str] = Counter()
    judgment_counter: Counter[str] = Counter()
    for profile in profiles:
        motif_counter.update(profile.get("motif_tags") or [])
        lens_counter.update([((profile.get("stance_vector") or {}).get("value_lens") or "")])
        judgment_counter.update([((profile.get("stance_vector") or {}).get("judgment") or "")])
    lines = []
    if motif_counter:
        lines.append(f"高频意象：{_top_counter_line(motif_counter)}")
    if lens_counter:
        lines.append(f"判断镜头：{_top_counter_line(lens_counter)}")
    if judgment_counter:
        lines.append(f"常见判断姿态：{_top_counter_line(judgment_counter)}")
    return lines[:6]


def _build_lexicon_tics(profiles: list[dict[str, Any]]) -> list[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        counter.update(profile.get("lexicon_markers") or [])
    return [f"高辨识词：{_top_counter_line(counter)}"] if counter else []


def _build_rhetoric_preferences(profiles: list[dict[str, Any]]) -> list[str]:
    rhetoric_counter: Counter[str] = Counter()
    cadence_counter: Counter[str] = Counter()
    punctuation_counter: Counter[str] = Counter()
    for profile in profiles:
        rhetoric_counter.update(profile.get("rhetorical_devices") or [])
        cadence_counter.update([((profile.get("syntax_signature") or {}).get("cadence") or "")])
        punctuation_counter.update((profile.get("syntax_signature") or {}).get("punctuation_habits") or [])
    lines: list[str] = []
    if rhetoric_counter:
        lines.append(f"偏好修辞：{_top_counter_line(rhetoric_counter)}")
    if cadence_counter:
        lines.append(f"节拍重心：{_top_counter_line(cadence_counter)}")
    if punctuation_counter:
        lines.append(f"标点习惯：{_top_counter_line(punctuation_counter)}")
    return lines[:4]


def _build_opening_signatures(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in profiles:
        move = _normalize_short_text(profile.get("opening_move"))
        if move:
            grouped[move].append(profile)
    rows: list[dict[str, Any]] = []
    for move, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))[:4]:
        rows.append(
            {
                "move": move,
                "count": len(items),
                "anchor": str(((items[0].get("anchor_spans") or {}).get("opening") or "")).strip(),
            }
        )
    return rows


def _build_closure_signatures(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in profiles:
        move = _normalize_short_text(profile.get("closure_move"))
        if move:
            grouped[move].append(profile)
    rows: list[dict[str, Any]] = []
    for move, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))[:4]:
        rows.append(
            {
                "move": move,
                "count": len(items),
                "anchor": str(((items[0].get("anchor_spans") or {}).get("closing") or "")).strip(),
            }
        )
    return rows


def _derive_author_anti_patterns(profiles: list[dict[str, Any]]) -> list[str]:
    counts: Counter[str] = Counter()
    for profile in profiles:
        for item in profile.get("anti_patterns") or []:
            text = _normalize_short_text(item)
            if text:
                counts[text] += 1
    return [term for term, _ in counts.most_common(8)]


def _build_length_behaviors(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in profiles:
        grouped[str(profile.get("length_band") or "short")].append(profile)
    rows: list[dict[str, Any]] = []
    for band in STONE_PROFILE_V2_LENGTH_BANDS:
        items = grouped.get(band) or []
        if not items:
            continue
        rows.append(
            {
                "length_band": band,
                "count": len(items),
                "opening_move": _top_counter_value(Counter(item.get("opening_move") or "" for item in items)),
                "closure_move": _top_counter_value(Counter(item.get("closure_move") or "" for item in items)),
                "surface_form": _top_counter_value(Counter(item.get("surface_form") or "" for item in items)),
            }
        )
    return rows


def _build_topic_translation_map(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for profile in profiles:
        key = str((profile.get("stance_vector") or {}).get("value_lens") or "代价")
        grouped[key].append(profile)
    rows: list[dict[str, Any]] = []
    for lens, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        rows.append(
            {
                "value_lens": lens,
                "motif_tags": _shared_terms(items, "motif_tags", limit=4),
                "opening_moves": _top_counter_values(Counter(item.get("opening_move") or "" for item in items), limit=2),
                "closure_moves": _top_counter_values(Counter(item.get("closure_move") or "" for item in items), limit=2),
            }
        )
    return rows[:8]


def _build_blueprint_rules(
    profiles: list[dict[str, Any]],
    short_text_clusters: list[dict[str, Any]],
) -> dict[str, list[str]]:
    entry_rules = _top_counter_values(Counter(profile.get("opening_move") or "" for profile in profiles), limit=3)
    closure_rules = _top_counter_values(Counter(profile.get("closure_move") or "" for profile in profiles), limit=3)
    development_rules = _unique_preserve_order(
        [
            *[
                f"{cluster.get('prototype_family') or ''} 常走 {cluster.get('opening_move') or ''} -> {cluster.get('closure_move') or ''}"
                for cluster in short_text_clusters[:2]
                if cluster.get("prototype_family")
            ],
            *[
                f"{item.get('length_band') or ''} 常见 {item.get('surface_form') or ''}"
                for item in _build_length_behaviors(profiles)[:2]
            ],
        ]
    )
    return {
        "entry_rules": entry_rules[:3],
        "development_rules": development_rules[:4],
        "closure_rules": closure_rules[:3],
    }


def _collect_author_evidence_windows(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for profile in profiles[:8]:
        anchor_spans = dict(profile.get("anchor_spans") or {})
        windows.append(
            {
                "document_id": str(profile.get("document_id") or ""),
                "prototype_family": profile.get("prototype_family") or "",
                "opening": anchor_spans.get("opening") or "",
                "closing": anchor_spans.get("closing") or "",
                "signature": list(anchor_spans.get("signature") or [])[:2],
            }
        )
    return windows


def _build_retrieval_term_index(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    term_counts: Counter[str] = Counter()
    families_by_term: dict[str, set[str]] = defaultdict(set)
    for item in entries:
        family = _normalize_short_text(item.get("prototype_family"))
        for term in item.get("retrieval_terms") or []:
            text = _normalize_short_text(term)
            if not text:
                continue
            term_counts[text] += 1
            if family:
                families_by_term[text].add(family)
    rows: list[dict[str, Any]] = []
    for term, count in term_counts.most_common(16):
        rows.append(
            {
                "term": term,
                "count": count,
                "families": sorted(families_by_term.get(term) or [])[:4],
            }
        )
    return rows


def _top_counter_line(counter: Counter[str], *, limit: int = 3) -> str:
    parts = []
    for value, count in counter.most_common(limit):
        text = _normalize_short_text(value)
        if not text:
            continue
        parts.append(f"{text} {count}篇")
    return "，".join(parts)


def _top_counter_value(counter: Counter[str]) -> str:
    for value, _ in counter.most_common():
        text = _normalize_short_text(value)
        if text:
            return text
    return ""


def _top_counter_values(counter: Counter[str], *, limit: int) -> list[str]:
    values: list[str] = []
    for value, _ in counter.most_common(limit):
        text = _normalize_short_text(value)
        if text and text not in values:
            values.append(text)
    return values


def _split_sentences(text: str) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[。！？!?])\s+|(?<=[。！？!?])", normalized)
    sentences = [part.strip() for part in parts if part and part.strip()]
    if len(sentences) <= 1:
        sentences = [item.strip() for item in re.split(r"[；;]", normalized) if item.strip()]
    return sentences[:32]


def _split_paragraphs(text: str) -> list[str]:
    return [
        normalize_whitespace(item)
        for item in re.split(r"\n\s*\n+", str(text or ""))
        if normalize_whitespace(item)
    ][:12]


def _cjk_ngrams(text: str, *, min_len: int, max_len: int) -> Iterable[str]:
    for run in re.findall(r"[\u4e00-\u9fff]{2,}", text):
        limit = min(max_len, len(run))
        for size in range(min_len, limit + 1):
            for index in range(len(run) - size + 1):
                yield run[index:index + size]


def _normalize_short_text(value: Any) -> str:
    return normalize_whitespace(str(value or ""))


def _normalize_string_list(value: Any, *, limit: int) -> list[str]:
    items: list[str] = []
    if isinstance(value, str):
        candidates = re.split(r"[，,、；;\n]+", value)
    elif isinstance(value, dict):
        candidates = []
        for item in value.values():
            candidates.extend(_normalize_string_list(item, limit=limit))
    elif isinstance(value, (list, tuple)):
        candidates = [str(item).strip() for item in value]
    else:
        candidates = []
    for item in candidates:
        text = _normalize_short_text(item)
        if text and text not in items:
            items.append(text)
        if len(items) >= limit:
            break
    return items[:limit]


def _slug_piece(value: Any) -> str:
    text = _normalize_short_text(value).lower()
    if not text:
        return ""
    text = text.replace(" ", "_")
    text = re.sub(r"[^a-z0-9_\u4e00-\u9fff-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _truncate_text(text: str, limit: int) -> str:
    value = _normalize_short_text(text)
    if len(value) <= limit:
        return value
    if limit <= 1:
        return value[:limit]
    return f"{value[: limit - 1]}…"


def _unique_preserve_order(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = _normalize_short_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _has_repetition(opening: str, closing: str, text: str) -> bool:
    if opening and closing and any(token and token in closing for token in opening[:8].split()):
        return True
    repeated = [token for token, count in Counter(_derive_lexicon_markers(text)).items() if count > 1]
    return bool(repeated)
