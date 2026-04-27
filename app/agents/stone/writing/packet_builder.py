from __future__ import annotations

from importlib import import_module

_service = import_module("app.agents.stone.writing.service")

globals().update(
    {
        name: value
        for name, value in vars(_service).items()
        if not (name.startswith("__") and name.endswith("__"))
    }
)

def _load_stone_profiles_v3(session, project_id: str) -> list[dict[str, Any]]:
    from app.analysis.stone_v3 import STONE_V3_PROFILE_KEY, normalize_stone_profile_v3

    profiles: list[dict[str, Any]] = []
    for document in repository.list_project_documents(session, project_id):
        metadata = dict(document.metadata_json or {})
        profile = metadata.get(STONE_V3_PROFILE_KEY)
        if not isinstance(profile, dict):
            continue
        normalized = normalize_stone_profile_v3(
            profile,
            article_text=str(document.clean_text or document.raw_text or ""),
            fallback_title=document.title or document.filename,
            document_id=document.id,
            source_meta={
                "created_at_guess": document.created_at_guess,
                "source_type": document.source_type,
            },
        )
        normalized["document_id"] = document.id
        normalized["title"] = document.title or document.filename
        profiles.append(normalized)
    return profiles




def _load_v3_asset_payload(session, project_id: str, *, asset_kind: str) -> dict[str, Any]:
    from app.analysis.stone_v3 import is_valid_stone_v3_asset_payload

    return load_latest_valid_asset_payload(
        session,
        project_id,
        asset_kind=asset_kind,
        validator=is_valid_stone_v3_asset_payload,
    )


def _build_source_anchors_v3(prototype_index: dict[str, Any]) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def add_anchor(item: dict[str, Any]) -> None:
        anchor_id = str(item.get("id") or "").strip()
        quote = _trim_text(item.get("quote"), 260)
        if not anchor_id or not quote or anchor_id in seen_ids:
            return
        seen_ids.add(anchor_id)
        anchors.append(
            {
                "id": anchor_id,
                "source": "stone_prototype_index_v3",
                "document_id": str(item.get("document_id") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "role": str(item.get("role") or "signature").strip() or "signature",
                "quote": quote,
                "note": _trim_text(item.get("reason"), 120),
            }
        )

    for item in prototype_index.get("anchor_registry") or []:
        if isinstance(item, dict):
            add_anchor(item)
    if anchors:
        return anchors[:80]

    for document in prototype_index.get("documents") or []:
        if not isinstance(document, dict):
            continue
        for anchor in document.get("anchor_registry") or []:
            if isinstance(anchor, dict):
                add_anchor(anchor)
    return anchors[:80]


def _build_analysis_prompt_text_v3(bundle: StoneWritingAnalysisBundle) -> str:
    author_core = dict(bundle.author_model.get("author_core") or {})
    retrieval_policy = dict(bundle.prototype_index.get("retrieval_policy") or {})
    profile_index = dict(bundle.profile_index or {})
    parts = [
        "Stone v3 writing baseline",
        f"Preprocess run: {bundle.run_id}",
        f"Target role: {bundle.target_role or ''}",
        f"Profile count: {len(bundle.stone_profiles)}",
        f"Sampled profiles: {len(bundle.profile_slices)}",
        f"Large corpus mode: {bool(profile_index.get('sparse_profile_mode'))}",
        f"Prototype documents: {len((bundle.prototype_index or {}).get('documents') or [])}",
        "",
        "Author core:",
        f"- voice: {author_core.get('voice_summary') or ''}",
        f"- worldview: {author_core.get('worldview_summary') or ''}",
        f"- tone: {author_core.get('tone_summary') or ''}",
        "",
        "Coverage warnings:",
        *[f"- {item}" for item in (bundle.coverage_warnings or [])[:6]],
        "",
        "Retrieval policy:",
        f"- shortlist_formula: {retrieval_policy.get('shortlist_formula') or ''}",
        f"- target_shortlist_size: {retrieval_policy.get('target_shortlist_size') or 12}",
        f"- target_anchor_budget: {retrieval_policy.get('target_anchor_budget') or 8}",
    ]
    return "\n".join(parts).strip()


def _build_generation_packet_v3(bundle: StoneWritingAnalysisBundle) -> dict[str, Any]:
    author_core = dict(bundle.author_model.get("author_core") or {})
    analysis_run_id = (bundle.analysis_summary or {}).get("run_id")
    baseline_status = "ready" if bundle.analysis_ready else "analysis_incomplete"
    return {
        "baseline": {
            "stone_v3": True,
            "preprocess_ready": True,
            "corpus_ready": bool(bundle.stone_profiles),
            "profile_count": len(bundle.stone_profiles),
            "sampled_profile_count": len(bundle.profile_slices),
            "analysis_ready": bool(bundle.analysis_ready),
            "writing_packet_ready": bool(bundle.analysis_ready and bundle.author_model and bundle.prototype_index),
            "status": baseline_status,
            "profile_version": "v3",
            "baseline_version": "v3",
            "author_model_ready": bool(bundle.author_model),
            "prototype_index_ready": bool(bundle.prototype_index),
            "author_model_v3_ready": bool(bundle.author_model),
            "prototype_index_v3_ready": bool(bundle.prototype_index),
            "source_anchor_count": len(bundle.source_anchors),
            "coverage_warnings": list(bundle.coverage_warnings or [])[:8],
            "source": "stone_profile_v3 + stone_analysis_run_v3 + stone_author_model_v3 + stone_prototype_index_v3",
        },
        "analysis_run": {
            "run_id": analysis_run_id,
            "preprocess_run_id": bundle.run_id,
            "version_label": bundle.version_label,
            "target_role": bundle.target_role,
            "analysis_context": bundle.analysis_context,
            "analysis_summary": _compact_analysis_summary_for_prompt_v3(bundle.analysis_summary),
        },
        "author_model": {
            "author_core": author_core,
            "translation_rules": list(bundle.author_model.get("translation_rules") or [])[:8],
            "stable_moves": list(bundle.author_model.get("stable_moves") or [])[:8],
            "forbidden_moves": list(bundle.author_model.get("forbidden_moves") or [])[:8],
        },
        "prototype_index": {
            "document_count": int(bundle.prototype_index.get("document_count") or 0),
            "family_count": int(bundle.prototype_index.get("family_count") or 0),
            "retrieval_policy": dict(bundle.prototype_index.get("retrieval_policy") or {}),
            "selection_guides": dict(bundle.prototype_index.get("selection_guides") or {}),
        },
        "source_anchors": bundle.source_anchors[:24],
    }


def _extract_analysis_evidence_ids_v3(evidence: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        for key in ("chunk_id", "anchor_id", "id", "document_id"):
            value = str(item.get(key) or "").strip()
            if value and value not in ids:
                ids.append(value)
                break
    return ids[:12]


def _compact_analysis_fewshots_v3(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fewshots: list[dict[str, Any]] = []
    for item in evidence[:3]:
        if not isinstance(item, dict):
            continue
        fewshots.append(
            {
                "situation": _trim_text(item.get("situation") or item.get("reason"), 120),
                "expression": _trim_text(item.get("expression"), 120),
                "quote": _trim_text(item.get("quote"), 180),
                "document_title": _trim_text(item.get("document_title"), 120),
            }
        )
    return fewshots


def _build_latest_analysis_v3(session, project_id: str) -> dict[str, Any]:
    analysis_run = repository.get_latest_analysis_run(session, project_id, load_facets=True, load_events=True)
    if not analysis_run:
        return {
            "run_id": None,
            "status": None,
            "summary": {},
            "summary_by_key": {},
            "evidence_by_key": {},
            "facets": [],
            "facet_packets": [],
            "missing_facet_keys": [facet.key for facet in STONE_WRITING_FACETS],
            "analysis_ready": False,
            "warnings": ["No Stone analysis run was found."],
        }

    summary = dict(analysis_run.summary_json or {})
    facet_lookup = {
        str(facet.facet_key or "").strip(): facet
        for facet in list(analysis_run.facets or [])
        if str(facet.facet_key or "").strip()
    }
    summary_by_key: dict[str, dict[str, Any]] = {}
    evidence_by_key: dict[str, list[dict[str, Any]]] = {}
    facet_packets: list[dict[str, Any]] = []
    facets: list[StoneWritingFacetContext] = []
    missing_facet_keys: list[str] = []

    for facet_def in STONE_WRITING_FACETS:
        facet = facet_lookup.get(facet_def.key)
        if not facet:
            missing_facet_keys.append(facet_def.key)
            continue

        findings = dict(facet.findings_json or {})
        evidence = [dict(item) for item in list(facet.evidence_json or []) if isinstance(item, dict)]
        conflicts = [dict(item) for item in list(facet.conflicts_json or []) if isinstance(item, dict)]
        summary_text = _trim_text(findings.get("summary") or findings.get("notes") or "", 280)
        bullets = _normalize_string_list(findings.get("bullets"), limit=8, item_limit=180)
        evidence_ids = _extract_analysis_evidence_ids_v3(evidence)
        fewshots = _compact_analysis_fewshots_v3(evidence)
        confidence = _clamp_score(facet.confidence if facet.confidence is not None else findings.get("confidence"), default=0.0)

        facets.append(
            StoneWritingFacetContext(
                key=facet_def.key,
                label=facet_def.label,
                purpose=facet_def.purpose,
                confidence=confidence,
                summary=summary_text,
                bullets=bullets,
                fewshots=fewshots,
                conflicts=conflicts,
                evidence=evidence,
                evidence_ids=evidence_ids,
                anchor_ids=_normalize_string_list(findings.get("anchor_ids"), limit=8),
            )
        )
        summary_by_key[facet_def.key] = {
            "facet_key": facet_def.key,
            "label": facet_def.label,
            "purpose": facet_def.purpose,
            "summary": summary_text,
            "bullets": bullets,
            "confidence": confidence,
            "status": facet.status,
            "evidence_count": len(evidence),
            "evidence_ids": evidence_ids,
            "anchor_ids": _normalize_string_list(findings.get("anchor_ids"), limit=8),
        }
        evidence_by_key[facet_def.key] = evidence
        facet_packets.append(
            {
                "facet_key": facet_def.key,
                "label": facet_def.label,
                "purpose": facet_def.purpose,
                "summary": summary_text,
                "bullets": bullets,
                "confidence": confidence,
                "evidence_count": len(evidence),
                "evidence_ids": evidence_ids,
                "anchor_ids": _normalize_string_list(findings.get("anchor_ids"), limit=8),
                "fewshots": fewshots,
                "conflicts": conflicts,
                "source_map": {
                    "document_ids": _unique_preserve_order(
                        [str(item.get("document_id") or "").strip() for item in evidence if str(item.get("document_id") or "").strip()]
                    ),
                    "chunk_ids": evidence_ids,
                },
            }
        )

    analysis_ready = bool(summary.get("analysis_ready")) or (analysis_run.status == "completed" and not missing_facet_keys)
    warnings: list[str] = []
    if analysis_run.status != "completed":
        warnings.append(f"Latest analysis run status is {analysis_run.status}.")
    if missing_facet_keys:
        warnings.append(f"Missing analysis facets: {', '.join(missing_facet_keys)}")
    if not analysis_ready:
        warnings.append("Latest analysis run is not fully ready.")

    return {
        "run_id": analysis_run.id,
        "status": analysis_run.status,
        "summary": summary,
        "summary_by_key": summary_by_key,
        "evidence_by_key": evidence_by_key,
        "facets": facets,
        "facet_packets": facet_packets,
        "missing_facet_keys": missing_facet_keys,
        "analysis_ready": analysis_ready,
        "warnings": warnings,
    }


def _build_profile_index_v3(profiles: list[dict[str, Any]], *, sample_limit: int = 24) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    compact_profiles = [compact_stone_profile_v3(profile) for profile in profiles]
    profile_count = len(compact_profiles)
    if not compact_profiles:
        return (
            {
                "profile_count": 0,
                "sample_budget": sample_limit,
                "sampled_profile_count": 0,
                "sparse_profile_mode": False,
                "length_band_counts": {},
                "surface_form_counts": {},
                "top_motifs": [],
                "top_families": [],
                "top_value_lenses": [],
                "top_judgment_modes": [],
                "top_distances": [],
                "selected_profile_ids": [],
                "selection_policy": {
                    "method": "family-first greedy coverage",
                    "notes": ["No Stone profiles were available."],
                },
                "coverage_notes": ["No Stone profiles were available."],
            },
            [],
            [],
        )

    family_counts: Counter[str] = Counter()
    length_counts: Counter[str] = Counter()
    surface_counts: Counter[str] = Counter()
    motif_counts: Counter[str] = Counter()
    keyword_counts: Counter[str] = Counter()
    value_lens_counts: Counter[str] = Counter()
    judgment_mode_counts: Counter[str] = Counter()
    distance_counts: Counter[str] = Counter()
    family_members: dict[str, list[dict[str, Any]]] = {}
    for profile in compact_profiles:
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        length_band = str(profile.get("length_band") or "").strip() or "unknown"
        surface_form = str(profile.get("surface_form") or "").strip() or "unknown"
        family_counts[family] += 1
        length_counts[length_band] += 1
        surface_counts[surface_form] += 1
        family_members.setdefault(family, []).append(profile)
        motif_counts.update(str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip())
        keyword_counts.update(str(item).strip() for item in list(profile.get("keywords") or []) if str(item).strip())
        value_lens = _repair_stone_signal_text(profile.get("value_lens"))
        judgment_mode = _repair_stone_signal_text(profile.get("judgment_mode"))
        distance = _repair_stone_signal_text(profile.get("distance"))
        if value_lens:
            value_lens_counts[value_lens] += 1
        if judgment_mode:
            judgment_mode_counts[judgment_mode] += 1
        if distance:
            distance_counts[distance] += 1

    scored_profiles: list[tuple[float, str, dict[str, Any]]] = []
    for profile in compact_profiles:
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        length_band = str(profile.get("length_band") or "").strip() or "unknown"
        surface_form = str(profile.get("surface_form") or "").strip() or "unknown"
        motifs = [str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip()]
        keywords = [str(item).strip() for item in list(profile.get("keywords") or []) if str(item).strip()]
        score = 0.0
        score += len(motifs) * 2.5
        score += len(keywords[:6]) * 0.45
        score += 1.0 if profile.get("opening") else 0.0
        score += 1.0 if profile.get("closing") else 0.0
        score += 0.5 if profile.get("summary") else 0.0
        score += 0.3 if profile.get("value_lens") else 0.0
        score += 0.3 if profile.get("judgment_mode") else 0.0
        score += 0.25 if profile.get("distance") else 0.0
        score -= family_counts[family] * 0.02
        score += 0.1 if length_band in {"micro", "short"} else 0.0
        score += 0.1 if surface_form in {"scene_vignette", "confession", "aphorism"} else 0.0
        scored_profiles.append((score, str(profile.get("document_id") or ""), profile))

    scored_profiles.sort(key=lambda item: (item[0], item[1]), reverse=True)
    selected_profiles: list[dict[str, Any]] = []
    selected_ids: list[str] = []
    seen_families: set[str] = set()
    seen_length_bands: set[str] = set()
    seen_surface_forms: set[str] = set()
    seen_motifs: set[str] = set()

    def _add_profile(profile: dict[str, Any]) -> bool:
        document_id = str(profile.get("document_id") or "").strip()
        if not document_id or document_id in selected_ids:
            return False
        selected_profiles.append(profile)
        selected_ids.append(document_id)
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        length_band = str(profile.get("length_band") or "").strip() or "unknown"
        surface_form = str(profile.get("surface_form") or "").strip() or "unknown"
        seen_families.add(family)
        seen_length_bands.add(length_band)
        seen_surface_forms.add(surface_form)
        seen_motifs.update(str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip())
        return True

    diversity_floor = min(max(4, sample_limit // 3), sample_limit)
    for score, _, profile in scored_profiles:
        if len(selected_profiles) >= sample_limit:
            break
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        length_band = str(profile.get("length_band") or "").strip() or "unknown"
        surface_form = str(profile.get("surface_form") or "").strip() or "unknown"
        motifs = [str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip()]
        adds_diversity = (
            family not in seen_families
            or length_band not in seen_length_bands
            or surface_form not in seen_surface_forms
            or any(motif not in seen_motifs for motif in motifs[:2])
        )
        if adds_diversity or len(selected_profiles) < diversity_floor:
            _add_profile(profile)

    if len(selected_profiles) < sample_limit:
        for _, _, profile in scored_profiles:
            if len(selected_profiles) >= sample_limit:
                break
            _add_profile(profile)

    top_families: list[dict[str, Any]] = []
    for family, count in family_counts.most_common(8):
        family_sample_ids = [item.get("document_id") for item in selected_profiles if str(item.get("prototype_family") or "").strip() == family][:3]
        if not family_sample_ids:
            family_sample_ids = [item.get("document_id") for item in family_members.get(family, [])[:3]]
        top_families.append(
            {
                "family": family,
                "count": count,
                "sample_document_ids": [str(item or "").strip() for item in family_sample_ids if str(item or "").strip()],
            }
        )

    coverage_notes = [
        "Large corpora are reduced to representative profile slices instead of full profile dumps."
        if profile_count > sample_limit
        else "Profile slices cover the corpus directly.",
    ]
    if profile_count > sample_limit * 4:
        coverage_notes.append("Sparse profile mode is active because the corpus is large.")
    if len(family_counts) <= 1:
        coverage_notes.append("Profile family diversity is very narrow.")
    if len(selected_profiles) < min(sample_limit, 6):
        coverage_notes.append("Representative profile coverage is thin.")

    profile_index = {
        "profile_count": profile_count,
        "sample_budget": sample_limit,
        "sampled_profile_count": len(selected_profiles),
        "sparse_profile_mode": profile_count > sample_limit * 4 or profile_count > 120,
        "length_band_counts": dict(length_counts.most_common()),
        "surface_form_counts": dict(surface_counts.most_common()),
        "top_motifs": [item for item, _count in motif_counts.most_common(12)],
        "top_keywords": [item for item, _count in keyword_counts.most_common(12)],
        "top_families": top_families,
        "top_value_lenses": [item for item, _count in value_lens_counts.most_common(8)],
        "top_judgment_modes": [item for item, _count in judgment_mode_counts.most_common(8)],
        "top_distances": [item for item, _count in distance_counts.most_common(8)],
        "selected_profile_ids": selected_ids,
        "selection_policy": {
            "method": "family-first greedy coverage",
            "notes": [
                "Select one or more representatives from the strongest families first.",
                "Keep length bands, surface forms, and motif signals diverse.",
                "Use compact slices instead of shipping the full profile corpus.",
            ],
        },
        "coverage_notes": coverage_notes,
    }
    return profile_index, selected_profiles, selected_ids


def _build_coverage_warnings_v3(
    *,
    analysis_summary: dict[str, Any],
    profile_index: dict[str, Any],
    author_model: dict[str, Any],
    prototype_index: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    warnings.extend(str(item).strip() for item in (analysis_summary.get("warnings") or []) if str(item).strip())
    warnings.extend(str(item).strip() for item in (profile_index.get("coverage_notes") or []) if str(item).strip())
    if profile_index.get("sparse_profile_mode"):
        warnings.append("Sparse profile mode is active; only representative slices are used in the writing packet.")
    if int(profile_index.get("profile_count") or 0) > 120:
        warnings.append("The profile corpus is large enough that full-profile prompts are intentionally avoided.")
    missing_facet_keys = [str(item).strip() for item in (analysis_summary.get("missing_facet_keys") or []) if str(item).strip()]
    if missing_facet_keys:
        warnings.append(f"Analysis coverage is missing facets: {', '.join(missing_facet_keys)}")
    if not author_model:
        warnings.append("Stone Author Model V3 is missing.")
    if not prototype_index:
        warnings.append("Stone Prototype Index V3 is missing.")
    return _unique_preserve_order(warnings)[:10]


def _score_profile_slice_for_request_v3(
    profile: dict[str, Any],
    request_adapter: dict[str, Any],
) -> tuple[float, list[str]]:
    keywords = [str(item).strip().lower() for item in list(profile.get("keywords") or []) if str(item).strip()]
    motifs = [str(item).strip().lower() for item in list(profile.get("motif_tags") or []) if str(item).strip()]
    haystack = " ".join(
        [
            str(profile.get("title") or ""),
            str(profile.get("summary") or ""),
            str(profile.get("prototype_family") or ""),
            str(profile.get("value_lens") or ""),
            str(profile.get("judgment_mode") or ""),
            str(profile.get("distance") or ""),
            str(profile.get("opening") or ""),
            str(profile.get("closing") or ""),
            " ".join(keywords),
            " ".join(motifs),
        ]
    ).lower()
    score = 0.0
    reasons: list[str] = []
    for term in request_adapter.get("query_terms") or []:
        token = str(term).strip().lower()
        if not token:
            continue
        if token in keywords:
            score += 2.8
            reasons.append(f"keyword:{token}")
        else:
            match = _text_match_score_v3(token, haystack)
            if match:
                score += match
                reasons.append(f"text:{token}")
    for term in request_adapter.get("motif_terms") or []:
        token = str(term).strip().lower()
        if not token:
            continue
        if token in motifs:
            score += 2.2
            reasons.append(f"motif:{token}")
        else:
            match = _text_match_score_v3(token, haystack)
            if match:
                score += match
                reasons.append(f"motif-text:{token}")
    if str(profile.get("surface_form") or "").strip().lower() == str(request_adapter.get("surface_form") or "").strip().lower():
        score += 1.9
        reasons.append("surface_match")
    if str(profile.get("length_band") or "").strip().lower() == str(request_adapter.get("desired_length_band") or "").strip().lower():
        score += 1.5
        reasons.append("length_match")
    if _value_overlap_v3(profile.get("value_lens"), request_adapter.get("value_lens")):
        score += 1.4
        reasons.append("value_lens_match")
    if _value_overlap_v3(profile.get("judgment_mode"), request_adapter.get("judgment_mode")):
        score += 1.0
        reasons.append("judgment_match")
    if _value_overlap_v3(profile.get("distance"), request_adapter.get("distance")):
        score += 1.0
        reasons.append("distance_match")
    if profile.get("opening"):
        score += 0.25
    if profile.get("closing"):
        score += 0.25
    return score, _unique_preserve_order(reasons)[:10]


def _summarize_profile_selection_v3(profile_slices: list[dict[str, Any]]) -> dict[str, Any]:
    family_counts: Counter[str] = Counter()
    motif_counts: Counter[str] = Counter()
    keyword_counts: Counter[str] = Counter()
    value_lens_counts: Counter[str] = Counter()
    judgment_mode_counts: Counter[str] = Counter()
    distance_counts: Counter[str] = Counter()
    for profile in profile_slices:
        family = str(profile.get("prototype_family") or "").strip()
        if family:
            family_counts[family] += 1
        motif_counts.update(str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip())
        keyword_counts.update(str(item).strip() for item in list(profile.get("keywords") or []) if str(item).strip())
        value_lens = _repair_stone_signal_text(profile.get("value_lens"))
        judgment_mode = _repair_stone_signal_text(profile.get("judgment_mode"))
        distance = _repair_stone_signal_text(profile.get("distance"))
        if value_lens:
            value_lens_counts[value_lens] += 1
        if judgment_mode:
            judgment_mode_counts[judgment_mode] += 1
        if distance:
            distance_counts[distance] += 1
    return {
        "selected_profile_ids": [str(profile.get("document_id") or "").strip() for profile in profile_slices if str(profile.get("document_id") or "").strip()][:24],
        "selected_profile_count": len(profile_slices),
        "top_families": [item for item, _count in family_counts.most_common(8)],
        "top_motifs": [item for item, _count in motif_counts.most_common(10)],
        "top_keywords": [item for item, _count in keyword_counts.most_common(10)],
        "top_value_lenses": [item for item, _count in value_lens_counts.most_common(6)],
        "top_judgment_modes": [item for item, _count in judgment_mode_counts.most_common(6)],
        "top_distances": [item for item, _count in distance_counts.most_common(6)],
    }


def _select_profile_slices_for_request_v3(
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    *,
    limit: int = 18,
) -> dict[str, Any]:
    compact_profiles = [compact_stone_profile_v3(profile) for profile in bundle.stone_profiles]
    if not compact_profiles:
        return {
            "profile_slices": [],
            "selected_profile_ids": [],
            "selection_notes": ["没有可用于本轮写作的逐篇画像。"],
            "summary": {
                "selected_profile_ids": [],
                "selected_profile_count": 0,
                "top_families": [],
                "top_motifs": [],
                "top_keywords": [],
                "top_value_lenses": [],
                "top_judgment_modes": [],
                "top_distances": [],
            },
        }

    scored: list[tuple[float, str, dict[str, Any], list[str]]] = []
    family_counts: Counter[str] = Counter(
        str(profile.get("prototype_family") or "").strip() or "unknown" for profile in compact_profiles
    )
    for profile in compact_profiles:
        score, reasons = _score_profile_slice_for_request_v3(profile, request_adapter)
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        score -= family_counts[family] * 0.03
        scored.append((score, str(profile.get("document_id") or ""), profile, reasons))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)

    selected_profiles: list[dict[str, Any]] = []
    selected_ids: list[str] = []
    seen_families: set[str] = set()
    seen_surface_forms: set[str] = set()
    seen_length_bands: set[str] = set()
    seen_motifs: set[str] = set()

    def _take(profile: dict[str, Any]) -> bool:
        document_id = str(profile.get("document_id") or "").strip()
        if not document_id or document_id in selected_ids:
            return False
        selected_profiles.append(profile)
        selected_ids.append(document_id)
        seen_families.add(str(profile.get("prototype_family") or "").strip() or "unknown")
        seen_surface_forms.add(str(profile.get("surface_form") or "").strip() or "unknown")
        seen_length_bands.add(str(profile.get("length_band") or "").strip() or "unknown")
        seen_motifs.update(str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip())
        return True

    diversity_floor = min(max(6, limit // 2), limit)
    for _score, _document_id, profile, _reasons in scored:
        if len(selected_profiles) >= limit:
            break
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        surface_form = str(profile.get("surface_form") or "").strip() or "unknown"
        length_band = str(profile.get("length_band") or "").strip() or "unknown"
        motifs = [str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip()]
        adds_diversity = (
            family not in seen_families
            or surface_form not in seen_surface_forms
            or length_band not in seen_length_bands
            or any(motif not in seen_motifs for motif in motifs[:2])
        )
        if adds_diversity or len(selected_profiles) < diversity_floor:
            _take(profile)

    if len(selected_profiles) < limit:
        for _score, _document_id, profile, _reasons in scored:
            if len(selected_profiles) >= limit:
                break
            _take(profile)

    summary = _summarize_profile_selection_v3(selected_profiles)
    selection_notes = [
        "画像切片已按当前题目重新排序，不再只复用全库代表性样本。",
    ]
    if not selected_profiles:
        selection_notes.append("动态画像检索没有选出有效结果，已退回代表性样本。")
        fallback_profiles = list(bundle.profile_slices or [])[:limit]
        summary = _summarize_profile_selection_v3(fallback_profiles)
        return {
            "profile_slices": fallback_profiles,
            "selected_profile_ids": list(summary.get("selected_profile_ids") or []),
            "selection_notes": selection_notes,
            "summary": summary,
        }
    if len(selected_profiles) < min(limit, 8):
        selection_notes.append("与题目强匹配的画像切片偏少，写作时要谨防过度依赖少数样本。")
    if len(summary.get("top_families") or []) <= 1:
        selection_notes.append("本轮画像家族过窄，容易过拟合单一种写法。")
    return {
        "profile_slices": selected_profiles,
        "selected_profile_ids": list(summary.get("selected_profile_ids") or []),
        "selection_notes": selection_notes,
        "summary": summary,
    }

def _build_source_map_v3(
    *,
    profile_index: dict[str, Any],
    analysis_summary: dict[str, Any],
    rerank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_map: dict[str, Any] = {
        "profiles": {
            "profile_count": int(profile_index.get("profile_count") or 0),
            "sampled_profile_count": int(profile_index.get("sampled_profile_count") or 0),
            "selected_profile_ids": list(profile_index.get("selected_profile_ids") or []),
            "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
        },
        "analysis_facets": {},
    }
    facet_packets = list(analysis_summary.get("facet_packets") or [])
    for packet in facet_packets:
        if not isinstance(packet, dict):
            continue
        facet_key = str(packet.get("facet_key") or "").strip()
        if not facet_key:
            continue
        source_map["analysis_facets"][facet_key] = {
            "summary": _trim_text(packet.get("summary"), 260),
            "confidence": _clamp_score(packet.get("confidence"), default=0.0),
            "evidence_ids": list(packet.get("evidence_ids") or [])[:8],
            "anchor_ids": list(packet.get("anchor_ids") or [])[:8],
            "document_ids": list((packet.get("source_map") or {}).get("document_ids") or [])[:8],
        }
    if rerank:
        source_map["prototype_anchors"] = list(rerank.get("anchor_ids") or [])[:8]
        source_map["selected_documents"] = [
            str(item.get("document_id") or "").strip()
            for item in list(rerank.get("selected_documents") or [])
            if isinstance(item, dict) and str(item.get("document_id") or "").strip()
        ][:8]
    return source_map


def _compact_analysis_summary_for_prompt_v3(analysis_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": analysis_summary.get("run_id"),
        "status": analysis_summary.get("status"),
        "analysis_ready": bool(analysis_summary.get("analysis_ready")),
        "missing_facet_keys": list(analysis_summary.get("missing_facet_keys") or []),
        "warnings": list(analysis_summary.get("warnings") or [])[:8],
        "summary_by_key": dict(analysis_summary.get("summary_by_key") or {}),
        "facet_packets": list(analysis_summary.get("facet_packets") or [])[:8],
    }


def _build_axis_source_map_v3(
    *,
    analysis_summary: dict[str, Any],
    source_map: dict[str, Any],
    writing_guide: dict[str, Any],
) -> dict[str, Any]:
    guide_slot_map = {
        "creative_constraints": ["do_and_dont", "topic_translation_rules"],
        "emotional_arc": ["emotional_tendencies", "external_slots"],
        "imagery_theme": ["motif_theme_bank", "fewshot_anchors"],
        "lexicon_idiolect": ["voice_dna", "sentence_mechanics"],
        "nonclinical_psychodynamics": ["nonclinical_psychodynamics", "external_slots"],
        "stance_values": ["worldview_and_stance", "topic_translation_rules"],
        "structure_composition": ["structure_patterns", "word_count_strategies", "revision_rubric"],
        "voice_signature": ["author_snapshot", "voice_dna", "sentence_mechanics"],
    }
    axis_source_map: dict[str, Any] = {}
    facet_sources = dict(source_map.get("analysis_facets") or {})
    for facet_def in STONE_WRITING_FACETS:
        facet_key = facet_def.key
        facet_summary = dict((analysis_summary.get("summary_by_key") or {}).get(facet_key) or {})
        axis_source_map[facet_key] = {
            "label": facet_def.label,
            "purpose": facet_def.purpose,
            "summary": _trim_text(facet_summary.get("summary"), 260),
            "bullets": list(facet_summary.get("bullets") or [])[:6],
            "confidence": _clamp_score(facet_summary.get("confidence"), default=0.0),
            "source": dict(facet_sources.get(facet_key) or {}),
            "guide_slots": [slot for slot in guide_slot_map.get(facet_key, []) if slot in writing_guide],
        }
    return axis_source_map


def _build_writing_packet_shell_v3(
    *,
    analysis_summary: dict[str, Any],
    profile_index: dict[str, Any],
    writing_guide: dict[str, Any],
    author_model: dict[str, Any],
    prototype_index: dict[str, Any],
    rerank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compact_analysis_summary = _compact_analysis_summary_for_prompt_v3(analysis_summary)
    source_map = _build_source_map_v3(
        profile_index=profile_index,
        analysis_summary=analysis_summary,
        rerank=rerank,
    )
    axis_source_map = _build_axis_source_map_v3(
        analysis_summary=analysis_summary,
        source_map=source_map,
        writing_guide=writing_guide,
    )
    return {
        "packet_version": "v3",
        "packet_kind": "writing_packet_v3",
        "analysis_run": {
            "run_id": compact_analysis_summary.get("run_id"),
            "status": compact_analysis_summary.get("status"),
            "analysis_ready": bool(compact_analysis_summary.get("analysis_ready")),
            "missing_facet_keys": list(compact_analysis_summary.get("missing_facet_keys") or []),
        },
        "corpus": {
            "profile_count": int(profile_index.get("profile_count") or 0),
            "sample_budget": int(profile_index.get("sample_budget") or 0),
            "sampled_profile_count": int(profile_index.get("sampled_profile_count") or 0),
            "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
            "length_band_counts": dict(profile_index.get("length_band_counts") or {}),
            "surface_form_counts": dict(profile_index.get("surface_form_counts") or {}),
            "top_motifs": list(profile_index.get("top_motifs") or [])[:12],
            "top_keywords": list(profile_index.get("top_keywords") or [])[:12],
            "top_families": list(profile_index.get("top_families") or [])[:8],
            "selected_profile_ids": list(profile_index.get("selected_profile_ids") or [])[:24],
        },
        "style_packet_v3": {
            "writing_guide": dict(writing_guide or {}),
            "author_model": {
                "author_core": dict(author_model.get("author_core") or {}),
                "translation_rules": list(author_model.get("translation_rules") or [])[:8],
                "stable_moves": list(author_model.get("stable_moves") or [])[:8],
                "forbidden_moves": list(author_model.get("forbidden_moves") or [])[:8],
            },
            "prototype_index": {
                "document_count": int(prototype_index.get("document_count") or 0),
                "family_count": int(prototype_index.get("family_count") or 0),
                "retrieval_policy": dict(prototype_index.get("retrieval_policy") or {}),
                "selection_guides": dict(prototype_index.get("selection_guides") or {}),
            },
        },
        "selection": {
            "selected_documents": list((rerank or {}).get("selected_documents") or [])[:8],
            "anchor_ids": list((rerank or {}).get("anchor_ids") or [])[:8],
        },
        "source_map": source_map,
        "axis_source_map": axis_source_map,
        "coverage_warnings": _build_coverage_warnings_v3(
            analysis_summary=analysis_summary,
            profile_index=profile_index,
            author_model=author_model,
            prototype_index=prototype_index,
        ),
    }


def _v3_keyword_units(*values: Any, limit: int = 16) -> list[str]:
    items: list[str] = []
    for value in values:
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,24}|[\u4e00-\u9fff]{1,4}", normalize_whitespace(str(value or "")).lower()):
            if token in items:
                continue
            items.append(token)
            if len(items) >= limit:
                return items
    return items


def _resolve_length_band_v3(target_word_count: int) -> str:
    if target_word_count <= 160:
        return "micro"
    if target_word_count <= 420:
        return "short"
    if target_word_count <= 1000:
        return "medium"
    return "long"


def _normalize_request_adapter_v3(
    payload: dict[str, Any],
    state: WritingStreamState,
    bundle: StoneWritingAnalysisBundle,
) -> dict[str, Any]:
    translation_rules = list(bundle.author_model.get("translation_rules") or [])
    first_rule = next((dict(item) for item in translation_rules if isinstance(item, dict)), {})
    corpus_motifs = list((bundle.profile_index or {}).get("top_motifs") or [])
    corpus_value_lenses = list((bundle.profile_index or {}).get("top_value_lenses") or [])
    corpus_judgment_modes = list((bundle.profile_index or {}).get("top_judgment_modes") or [])
    corpus_distances = list((bundle.profile_index or {}).get("top_distances") or [])
    desired_length_band = str(payload.get("desired_length_band") or _resolve_length_band_v3(state.target_word_count)).strip().lower()
    if desired_length_band not in {"micro", "short", "medium", "long"}:
        desired_length_band = _resolve_length_band_v3(state.target_word_count)
    surface_form = str(payload.get("surface_form") or "").strip().lower() or "scene_vignette"
    query_terms = _unique_preserve_order([
        *_normalize_string_list(payload.get("query_terms"), limit=10),
        *_v3_keyword_units(state.topic, state.extra_requirements, limit=10),
    ])[:10]
    motif_terms = _unique_preserve_order([
        *_normalize_string_list(payload.get("motif_terms"), limit=8),
        *_normalize_string_list(corpus_motifs, limit=6),
        *_normalize_string_list((bundle.author_model.get("author_core") or {}).get("signature_motifs"), limit=6),
    ])[:8]
    anchor_preferences = _unique_preserve_order([
        *_normalize_string_list(payload.get("anchor_preferences"), limit=6),
        "opening",
        "closing",
    ])[:6]
    hard_constraints = _unique_preserve_order([
        "全文必须使用简体中文，避免整句英文或整段英文表达。",
        *_normalize_string_list(payload.get("hard_constraints"), limit=6),
        *_normalize_string_list(state.extra_requirements, limit=4),
    ])[:6]
    value_lens, value_lens_source = _first_supported_signal_v3(
        ("llm", payload.get("value_lens")),
        ("corpus_prior", corpus_value_lenses[0] if corpus_value_lenses else ""),
        ("author_model", first_rule.get("value_lens")),
        ("generic", "代价"),
    )
    judgment_mode, judgment_mode_source = _first_supported_signal_v3(
        ("llm", payload.get("judgment_mode")),
        ("corpus_prior", corpus_judgment_modes[0] if corpus_judgment_modes else ""),
        ("generic", "通过贴身细节稳住判断"),
    )
    distance, distance_source = _first_supported_signal_v3(
        ("llm", payload.get("distance")),
        ("corpus_prior", corpus_distances[0] if corpus_distances else ""),
        ("generic", "回收式第一人称"),
    )
    entry_scene, entry_scene_source = _first_supported_signal_v3(
        ("llm", payload.get("entry_scene")),
        ("generic", "从一个具体动作或物件进入。"),
    )
    felt_cost, felt_cost_source = _first_supported_signal_v3(
        ("llm", payload.get("felt_cost")),
        ("generic", "先把压力落成身体能感到的代价，再进入解释。"),
    )
    source_support = {
        "value_lens": value_lens_source,
        "judgment_mode": judgment_mode_source,
        "distance": distance_source,
        "entry_scene": entry_scene_source,
        "felt_cost": felt_cost_source,
    }
    defaulted_fields = [field for field, source in source_support.items() if source == "generic"]
    return {
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "desired_length_band": desired_length_band,
        "surface_form": surface_form,
        "value_lens": value_lens,
        "judgment_mode": judgment_mode,
        "distance": distance,
        "entry_scene": entry_scene,
        "felt_cost": felt_cost,
        "query_terms": query_terms,
        "motif_terms": motif_terms,
        "anchor_preferences": anchor_preferences,
        "hard_constraints": hard_constraints,
        "reasoning": _trim_text(payload.get("reasoning"), 220),
        "source_support": source_support,
        "defaulted_fields": defaulted_fields,
        "support_score": round((5 - len(defaulted_fields)) / 5, 3),
    }

def _score_v3_shortlist_candidate(
    document: dict[str, Any],
    request_adapter: dict[str, Any],
    profile_selection_summary: dict[str, Any] | None = None,
) -> tuple[float, list[str]]:
    profile_selection_summary = profile_selection_summary or {}
    handles = dict(document.get("retrieval_handles") or {})
    routing_text = normalize_whitespace(handles.get("routing_text") or "")
    keywords = [str(item).lower() for item in (handles.get("keywords") or []) if str(item).strip()]
    routing_facets = dict(handles.get("routing_facets") or {})
    best_for = [str(item).lower() for item in ((document.get("selection_guides") or {}).get("best_for") or []) if str(item).strip()]
    lift_signals = [str(item).lower() for item in ((document.get("selection_guides") or {}).get("lift_signals") or []) if str(item).strip()]
    all_text = " ".join(
        [
            routing_text.lower(),
            " ".join(keywords),
            " ".join(best_for),
            " ".join(lift_signals),
            str(document.get("family_label") or "").lower(),
            str(document.get("document_summary") or "").lower(),
        ]
    )
    score = 0.0
    reasons: list[str] = []
    for term in request_adapter.get("query_terms") or []:
        token = str(term).lower()
        if not token:
            continue
        if token in keywords:
            score += 3.0
            reasons.append(f"keyword:{token}")
        elif token in all_text:
            score += 1.4
            reasons.append(f"text:{token}")
    for motif in request_adapter.get("motif_terms") or []:
        token = str(motif).lower()
        if token and token in all_text:
            score += 1.8
            reasons.append(f"motif:{token}")
    if str(document.get("length_band") or "").lower() == str(request_adapter.get("desired_length_band") or "").lower():
        score += 2.0
        reasons.append("length_match")
    if str(document.get("surface_form") or "").lower() == str(request_adapter.get("surface_form") or "").lower():
        score += 2.0
        reasons.append("surface_match")
    if str(routing_facets.get("value_lens") or "").strip() == str(request_adapter.get("value_lens") or "").strip():
        score += 1.5
        reasons.append("value_lens_match")
    if str(routing_facets.get("distance") or "").strip() == str(request_adapter.get("distance") or "").strip():
        score += 1.0
        reasons.append("distance_match")
    if str(routing_facets.get("judgment_mode") or "").strip() == str(request_adapter.get("judgment_mode") or "").strip():
        score += 1.0
        reasons.append("judgment_match")
    top_families = [str(item).strip().lower() for item in list(profile_selection_summary.get("top_families") or []) if str(item).strip()]
    top_motifs = [str(item).strip().lower() for item in list(profile_selection_summary.get("top_motifs") or []) if str(item).strip()]
    top_keywords = [str(item).strip().lower() for item in list(profile_selection_summary.get("top_keywords") or []) if str(item).strip()]
    family_label = str(document.get("family_label") or "").strip().lower()
    if family_label and family_label in top_families:
        score += 1.4
        reasons.append("profile_family_match")
    for token in top_motifs[:4]:
        if token and token in all_text:
            score += 0.6
            reasons.append(f"profile_motif:{token}")
    for token in top_keywords[:4]:
        if token and token in all_text:
            score += 0.4
            reasons.append(f"profile_keyword:{token}")
    return score, _unique_preserve_order(reasons)[:8]


def _build_candidate_shortlist_v3(
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    *,
    profile_selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    anchor_budget = int(((bundle.prototype_index.get("retrieval_policy") or {}).get("target_anchor_budget")) or 8)
    profile_selection_summary = dict((profile_selection or {}).get("summary") or {})
    for document in bundle.prototype_index.get("documents") or []:
        if not isinstance(document, dict):
            continue
        score, reasons = _score_v3_shortlist_candidate(
            document,
            request_adapter,
            profile_selection_summary=profile_selection_summary,
        )
        candidates.append(
            {
                "document_id": str(document.get("document_id") or "").strip(),
                "title": str(document.get("title") or "").strip(),
                "family_id": str(document.get("family_id") or "").strip(),
                "family_label": str(document.get("family_label") or "").strip(),
                "length_band": str(document.get("length_band") or "").strip(),
                "surface_form": str(document.get("surface_form") or "").strip(),
                "score": round(score, 4),
                "reasons": reasons,
                "summary": _trim_text(document.get("document_summary"), 180),
                "retrieval_handles": dict(document.get("retrieval_handles") or {}),
                "selection_guides": dict(document.get("selection_guides") or {}),
                "anchor_registry": list(document.get("anchor_registry") or [])[:anchor_budget],
            }
        )
    candidates.sort(key=lambda item: (float(item.get("score") or 0.0), item.get("document_id") or ""), reverse=True)
    shortlist_size = int(((bundle.prototype_index.get("retrieval_policy") or {}).get("target_shortlist_size")) or 12)
    shortlisted = [item for item in candidates if item.get("document_id")][: max(1, shortlist_size)]
    return {
        "desired_length_band": request_adapter.get("desired_length_band"),
        "surface_form": request_adapter.get("surface_form"),
        "query_terms": list(request_adapter.get("query_terms") or []),
        "motif_terms": list(request_adapter.get("motif_terms") or []),
        "profile_selection_summary": profile_selection_summary,
        "shortlist_size": len(shortlisted),
        "documents": shortlisted,
    }


def _compact_shortlist_for_prompt_v3(shortlist: dict[str, Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in shortlist.get("documents") or []:
        anchors = []
        for anchor in item.get("anchor_registry") or []:
            if not isinstance(anchor, dict):
                continue
            anchors.append(
                {
                    "id": anchor.get("id"),
                    "role": anchor.get("role"),
                    "quote": _trim_text(anchor.get("quote"), 180),
                }
            )
        compact.append(
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "family_id": item.get("family_id"),
                "family_label": item.get("family_label"),
                "length_band": item.get("length_band"),
                "surface_form": item.get("surface_form"),
                "score": item.get("score"),
                "summary": item.get("summary"),
                "reasons": item.get("reasons"),
                "selection_guides": item.get("selection_guides"),
                "anchors": anchors[:4],
            }
        )
    return compact


def _normalize_rerank_v3(
    payload: dict[str, Any],
    bundle: StoneWritingAnalysisBundle,
    shortlist: dict[str, Any],
) -> dict[str, Any]:
    shortlist_docs = {
        str(item.get("document_id") or "").strip(): item
        for item in shortlist.get("documents") or []
        if str(item.get("document_id") or "").strip()
    }
    shortlist_anchor_ids: set[str] = set()
    document_anchor_ids: dict[str, list[str]] = {}
    for document_id, item in shortlist_docs.items():
        ids = [
            str(anchor.get("id") or "").strip()
            for anchor in (item.get("anchor_registry") or [])
            if isinstance(anchor, dict) and str(anchor.get("id") or "").strip()
        ]
        document_anchor_ids[document_id] = ids
        shortlist_anchor_ids.update(ids)

    selected_documents: list[str] = []
    for item in payload.get("selected_documents") or []:
        if isinstance(item, dict):
            document_id = str(item.get("document_id") or item.get("id") or "").strip()
        else:
            document_id = str(item or "").strip()
        if document_id and document_id in shortlist_docs and document_id not in selected_documents:
            selected_documents.append(document_id)
    selected_documents = selected_documents[:6]

    anchor_ids: list[str] = []
    for item in payload.get("anchor_ids") or []:
        if isinstance(item, dict):
            anchor_id = str(item.get("anchor_id") or item.get("id") or "").strip()
        else:
            anchor_id = str(item or "").strip()
        if anchor_id and anchor_id in shortlist_anchor_ids and anchor_id not in anchor_ids:
            anchor_ids.append(anchor_id)

    if not selected_documents and anchor_ids:
        anchor_to_document = {
            str(anchor.get("id") or "").strip(): str(anchor.get("document_id") or "").strip()
            for anchor in bundle.source_anchors
            if str(anchor.get("id") or "").strip()
        }
        for anchor_id in anchor_ids:
            document_id = anchor_to_document.get(anchor_id)
            if document_id and document_id in shortlist_docs and document_id not in selected_documents:
                selected_documents.append(document_id)
        selected_documents = selected_documents[:6]

    if not selected_documents:
        raise ValueError("llm_rerank_v3 returned no valid shortlist documents.")

    for document_id in selected_documents:
        for anchor_id in document_anchor_ids.get(document_id, []):
            if anchor_id not in anchor_ids:
                anchor_ids.append(anchor_id)
            if len(anchor_ids) >= 8:
                break
        if len(anchor_ids) >= 8:
            break
    anchor_ids = anchor_ids[:8]
    if not anchor_ids:
        raise ValueError("llm_rerank_v3 returned no valid anchor ids.")

    selected_doc_payload = []
    for document_id in selected_documents:
        document = shortlist_docs[document_id]
        selected_doc_payload.append(
            {
                "document_id": document_id,
                "title": document.get("title"),
                "family_id": document.get("family_id"),
                "family_label": document.get("family_label"),
                "score": document.get("score"),
                "reasons": document.get("reasons"),
            }
        )
    return {
        "selected_documents": selected_doc_payload,
        "anchor_ids": anchor_ids,
        "selection_reason": _trim_text(payload.get("selection_reason"), 220),
        "rerank_notes": _normalize_string_list(payload.get("rerank_notes"), limit=6),
    }


def _selected_anchor_records_v3(bundle: StoneWritingAnalysisBundle, rerank: dict[str, Any]) -> list[dict[str, Any]]:
    selected_ids = set(rerank.get("anchor_ids") or [])
    anchors = [item for item in bundle.source_anchors if item.get("id") in selected_ids]
    if anchors:
        return anchors[:8]
    return bundle.source_anchors[:8]


_LOCAL_PERSONA_MARKERS_V3 = (
    "\u9119\u4eba",
    "\u5728\u4e0b",
    "\u672c\u4eba",
    "\u5c0f\u53ef",
)
_LOCAL_RHETORICAL_MARKERS_V3 = (
    "\u53e4\u8bed\u6709\u4e91",
    "\u65e0\u4ed6",
    "\u552f\u624b\u719f\u8033",
    "\u8a00\u91cd",
    "\u53cd\u89c2",
    "\u76f8\u8f83\u4e8e",
    "\u6bcf\u5f53",
    "\u65e0\u4e0d",
    "\u5b9e\u5728\u662f\u9ad8",
    "\u4e0d\u80fd\u5426\u8ba4",
)
_LOCAL_THESIS_MARKERS_V3 = (
    "\u7d20\u8d28",
    "\u54c1\u5473",
    "\u4fee\u517b",
    "\u827a\u672f",
    "\u6863\u6b21",
    "\u9ad8\u7aef\u4eba\u58eb",
    "\u826f\u5e08\u8bde\u53cb",
    "\u5272\u5e2d",
)


def _aggregate_profile_style_counter_v3(profiles: list[dict[str, Any]], key: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        source = dict((profile.get("style_stats") or {}).get(key) or {})
        for token, count in source.items():
            normalized = _trim_text(token, 40)
            if not normalized:
                continue
            try:
                counter[normalized] += int(count or 0)
            except (TypeError, ValueError):
                continue
    return counter


def _selected_full_profiles_v3(
    bundle: StoneWritingAnalysisBundle,
    selected_profile_ids: list[str] | None,
    rerank: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    preferred_ids = {
        str(item).strip()
        for item in list(selected_profile_ids or [])
        if str(item).strip()
    }
    if not preferred_ids:
        preferred_ids = {
            str((item or {}).get("document_id") or "").strip()
            for item in list((rerank or {}).get("selected_documents") or [])
            if isinstance(item, dict) and str((item or {}).get("document_id") or "").strip()
        }
    if not preferred_ids:
        return list(bundle.stone_profiles or [])[:6]
    return [
        profile
        for profile in list(bundle.stone_profiles or [])
        if str(profile.get("document_id") or "").strip() in preferred_ids
    ][:6]


def _collect_profile_signature_texts_v3(profile: dict[str, Any]) -> list[str]:
    anchor_windows = dict(profile.get("anchor_windows") or {})
    structure_moves = dict(profile.get("structure_moves") or {})
    document_core = dict(profile.get("document_core") or {})
    lines = [
        _trim_text(document_core.get("summary"), 180),
        _trim_text(anchor_windows.get("opening"), 180),
        _trim_text(anchor_windows.get("pivot"), 180),
        _trim_text(anchor_windows.get("closing"), 180),
        _trim_text(structure_moves.get("opening_move"), 120),
        _trim_text(structure_moves.get("development_move"), 120),
        _trim_text(structure_moves.get("turning_move"), 120),
        _trim_text(structure_moves.get("closure_move"), 120),
    ]
    lines.extend(_normalize_string_list(anchor_windows.get("signature_lines"), limit=4, item_limit=180))
    return [item for item in lines if item]


def _aggregate_marker_counts_v3(texts: list[str], markers: tuple[str, ...]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for text in texts:
        source = normalize_whitespace(str(text or ""))
        for marker in markers:
            hits = source.count(marker)
            if hits > 0:
                counter[marker] += hits
    return counter


def _build_selected_sample_style_context_v3(
    bundle: StoneWritingAnalysisBundle,
    selected_profile_ids: list[str] | None,
    rerank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profiles = _selected_full_profiles_v3(bundle, selected_profile_ids, rerank)
    if not profiles:
        return {}

    person_counter = Counter(
        _trim_text(((profile.get("voice_contract") or {}).get("person")), 24)
        for profile in profiles
        if _trim_text(((profile.get("voice_contract") or {}).get("person")), 24)
    )
    self_position_counter = Counter(
        _trim_text(((profile.get("voice_contract") or {}).get("self_position")), 24)
        for profile in profiles
        if _trim_text(((profile.get("voice_contract") or {}).get("self_position")), 24)
    )
    distance_counter = Counter(
        _trim_text(((profile.get("voice_contract") or {}).get("distance")), 24)
        for profile in profiles
        if _trim_text(((profile.get("voice_contract") or {}).get("distance")), 24)
    )
    cadence_counter = Counter(
        _trim_text(((profile.get("voice_contract") or {}).get("cadence")), 32)
        for profile in profiles
        if _trim_text(((profile.get("voice_contract") or {}).get("cadence")), 32)
    )
    sentence_shape_counter = Counter(
        _trim_text(((profile.get("voice_contract") or {}).get("sentence_shape")), 32)
        for profile in profiles
        if _trim_text(((profile.get("voice_contract") or {}).get("sentence_shape")), 32)
    )
    value_lens_counter = Counter(
        _trim_text(((profile.get("value_and_judgment") or {}).get("value_lens")), 40)
        for profile in profiles
        if _trim_text(((profile.get("value_and_judgment") or {}).get("value_lens")), 40)
    )
    judgment_mode_counter = Counter(
        _trim_text(((profile.get("value_and_judgment") or {}).get("judgment_mode")), 40)
        for profile in profiles
        if _trim_text(((profile.get("value_and_judgment") or {}).get("judgment_mode")), 40)
    )
    lexicon_counter = Counter(
        token
        for profile in profiles
        for token in _normalize_string_list(
            ((profile.get("motif_and_scene_bank") or {}).get("lexicon_markers")),
            limit=8,
            item_limit=24,
        )
    )
    connective_counter = _aggregate_profile_style_counter_v3(profiles, "connective_counts")
    pronoun_counter = _aggregate_profile_style_counter_v3(profiles, "pronoun_counts")
    punctuation_counter = _aggregate_profile_style_counter_v3(profiles, "punctuation_counts")
    sentence_bucket_counter = _aggregate_profile_style_counter_v3(profiles, "sentence_length_buckets")
    signature_texts = [text for profile in profiles for text in _collect_profile_signature_texts_v3(profile)]
    persona_counter = _aggregate_marker_counts_v3(signature_texts, _LOCAL_PERSONA_MARKERS_V3)
    rhetorical_counter = _aggregate_marker_counts_v3(signature_texts, _LOCAL_RHETORICAL_MARKERS_V3)
    thesis_counter = _aggregate_marker_counts_v3(signature_texts, _LOCAL_THESIS_MARKERS_V3)
    for marker in _LOCAL_THESIS_MARKERS_V3:
        if lexicon_counter.get(marker):
            thesis_counter[marker] += int(lexicon_counter.get(marker) or 0)
    persona_markers = [term for term, _count in persona_counter.most_common(4)]
    rhetorical_markers = [term for term, _count in rhetorical_counter.most_common(6)]
    thesis_markers = [term for term, _count in thesis_counter.most_common(6)]
    pronoun_terms = [
        term
        for term, _count in pronoun_counter.most_common(6)
        if term in {"\u6211", "\u6211\u4eec", "\u81ea\u5df1", "\u4f60", "\u4f60\u4eec"}
    ]
    self_reference_terms = _unique_preserve_order([*persona_markers, *pronoun_terms])[:6]
    opening_moves = _unique_preserve_order(
        [
            _trim_text(((profile.get("structure_moves") or {}).get("opening_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("opening_move")), 80)
        ]
    )[:4]
    turning_moves = _unique_preserve_order(
        [
            _trim_text(((profile.get("structure_moves") or {}).get("turning_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("turning_move")), 80)
        ]
    )[:4]
    closure_moves = _unique_preserve_order(
        [
            _trim_text(((profile.get("structure_moves") or {}).get("closure_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("closure_move")), 80)
        ]
    )[:4]
    signature_closures = _unique_preserve_order(
        [
            _trim_text(((profile.get("anchor_windows") or {}).get("closing")), 160)
            for profile in profiles
            if _trim_text(((profile.get("anchor_windows") or {}).get("closing")), 160)
        ]
    )[:4]
    pressure_translation = _unique_preserve_order(
        [
            _trim_text(((profile.get("value_and_judgment") or {}).get("felt_cost")), 120)
            for profile in profiles
            if _trim_text(((profile.get("value_and_judgment") or {}).get("felt_cost")), 120)
        ]
    )[:4]
    stable_moves = _unique_preserve_order(
        [
            _trim_text(((profile.get("structure_moves") or {}).get("opening_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("opening_move")), 80)
        ]
        + [
            _trim_text(((profile.get("structure_moves") or {}).get("development_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("development_move")), 80)
        ]
        + [
            _trim_text(((profile.get("structure_moves") or {}).get("closure_move")), 80)
            for profile in profiles
            if _trim_text(((profile.get("structure_moves") or {}).get("closure_move")), 80)
        ]
    )[:8]
    forbidden_moves = _unique_preserve_order(
        [
            item
            for profile in profiles
            for item in _normalize_string_list(profile.get("anti_patterns"), limit=8, item_limit=80)
        ]
    )[:8]
    style_fingerprint = {
        "narrator_profile": {
            "person": person_counter.most_common(1)[0][0] if person_counter else "first",
            "self_reference_terms": self_reference_terms,
            "self_position": self_position_counter.most_common(1)[0][0] if self_position_counter else "",
            "narrative_distance": distance_counter.most_common(1)[0][0] if distance_counter else "",
            "persona_markers": persona_markers,
        },
        "lexicon_profile": {
            "high_frequency_terms": [term for term, _count in lexicon_counter.most_common(8)],
            "connective_keep": [term for term, _count in connective_counter.most_common(6)],
            "overfit_risk_terms": _unique_preserve_order(
                [term for term, _count in lexicon_counter.most_common(6)] + thesis_markers + persona_markers
            )[:8],
            "rhetorical_markers": rhetorical_markers,
            "thesis_markers": thesis_markers,
        },
        "rhythm_profile": {
            "cadence": cadence_counter.most_common(1)[0][0] if cadence_counter else "mixed",
            "sentence_shape": sentence_shape_counter.most_common(1)[0][0] if sentence_shape_counter else "mixed",
            "sentence_length_buckets": {
                "short": int(sentence_bucket_counter.get("short", 0)),
                "medium": int(sentence_bucket_counter.get("medium", 0)),
                "long": int(sentence_bucket_counter.get("long", 0)),
            },
            "punctuation_habits": [term for term, _count in punctuation_counter.most_common(6)],
        },
        "closure_profile": {
            "opening_moves": opening_moves,
            "turning_devices": turning_moves,
            "closure_moves": closure_moves,
            "signature_closures": signature_closures,
        },
        "extreme_state_profile": {
            "pressure_translation": pressure_translation,
            "judgment_modes": [term for term, _count in judgment_mode_counter.most_common(4)],
            "defense_moves": stable_moves[:6],
        },
    }
    style_fingerprint_brief = _build_style_fingerprint_brief({"style_fingerprint": style_fingerprint})
    style_fingerprint_brief["persona_markers"] = persona_markers[:4]
    style_fingerprint_brief["rhetorical_devices"] = rhetorical_markers[:6]
    style_fingerprint_brief["thesis_refrains"] = thesis_markers[:6]
    style_fingerprint_brief["argument_rules"] = _unique_preserve_order(
        [
            "\u4f18\u5148\u4fdd\u4f4f\u4f5c\u8005\u7684\u81ea\u79f0\u9762\u5177\uff0c\u4e0d\u8981\u6539\u5199\u6210\u4e2d\u6027\u65c1\u89c2\u53e3\u6c14\u3002"
            if persona_markers
            else "",
            "\u5141\u8bb8\u4f2a\u5e84\u91cd\u3001\u5f15\u8bed\u3001\u5938\u5f20\u62ac\u4e3e\u540e\u518d\u5ba1\u5224\u56de\u843d\u3002"
            if rhetorical_markers
            else "",
            "\u8ba9\u6838\u5fc3\u5224\u65ad\u8bcd\u5728\u7bc7\u4e2d\u56de\u73af\uff0c\u4e0d\u8981\u53ea\u5728\u5f00\u5934\u51fa\u73b0\u4e00\u6b21\u3002"
            if thesis_markers
            else "",
        ]
    )[:4]
    style_fingerprint_brief["self_reference_rules"] = _unique_preserve_order(
        list(style_fingerprint_brief.get("self_reference_rules") or [])
        + [
            f"\u81ea\u79f0\u9762\u5177\u4f18\u5148\uff1a{', '.join(persona_markers[:3])}" if persona_markers else "",
        ]
    )[:4]
    style_fingerprint_brief["overfit_limits"] = _unique_preserve_order(
        list(style_fingerprint_brief.get("overfit_limits") or [])
        + [
            "\u4e0d\u8981\u628a\u4f2a\u5e84\u91cd\u53e3\u7656\u5806\u6210\u53f0\u8bcd\u5899\u3002" if rhetorical_markers else "",
        ]
    )[:6]
    sample_titles = [
        str(profile.get("title") or profile.get("document_id") or "").strip()
        for profile in profiles
        if str(profile.get("title") or profile.get("document_id") or "").strip()
    ][:6]
    return {
        "style_fingerprint": style_fingerprint,
        "style_fingerprint_brief": style_fingerprint_brief,
        "dominant_signals": {
            "value_lens": value_lens_counter.most_common(1)[0][0] if value_lens_counter else "",
            "judgment_mode": judgment_mode_counter.most_common(1)[0][0] if judgment_mode_counter else "",
            "distance": distance_counter.most_common(1)[0][0] if distance_counter else "",
            "entry_scene": opening_moves[0] if opening_moves else "",
            "felt_cost": pressure_translation[0] if pressure_translation else "",
        },
        "sample_local_floor": {
            "author_core": {
                "voice_summary": _trim_text(
                    " | ".join(
                        item
                        for item in [
                            person_counter.most_common(1)[0][0] if person_counter else "",
                            distance_counter.most_common(1)[0][0] if distance_counter else "",
                            self_position_counter.most_common(1)[0][0] if self_position_counter else "",
                        ]
                        if item
                    ),
                    180,
                ),
                "worldview_summary": _trim_text(
                    " | ".join(
                        item
                        for item in [
                            value_lens_counter.most_common(1)[0][0] if value_lens_counter else "",
                            judgment_mode_counter.most_common(1)[0][0] if judgment_mode_counter else "",
                        ]
                        if item
                    ),
                    180,
                ),
                "tone_summary": _trim_text(
                    " | ".join(
                        item
                        for item in [
                            cadence_counter.most_common(1)[0][0] if cadence_counter else "",
                            sentence_shape_counter.most_common(1)[0][0] if sentence_shape_counter else "",
                            ", ".join(rhetorical_markers[:3]) if rhetorical_markers else "",
                        ]
                        if item
                    ),
                    180,
                ),
                "signature_motifs": _unique_preserve_order(
                    thesis_markers + [term for term, _count in lexicon_counter.most_common(6)]
                )[:6],
            },
            "style_fingerprint": style_fingerprint,
            "stable_moves": stable_moves,
            "forbidden_moves": forbidden_moves,
            "signal_source": "selected_samples",
            "sample_titles": sample_titles,
        },
        "sample_titles": sample_titles,
    }


def _build_local_sample_packet_context_v3(
    bundle: StoneWritingAnalysisBundle,
    selected_profile_ids: list[str] | None,
    rerank: dict[str, Any],
) -> dict[str, Any]:
    profiles = _selected_full_profiles_v3(bundle, selected_profile_ids, rerank)
    style_context = _build_selected_sample_style_context_v3(bundle, selected_profile_ids, rerank)
    samples = []
    for profile in profiles[:6]:
        voice_contract = dict(profile.get("voice_contract") or {})
        value_and_judgment = dict(profile.get("value_and_judgment") or {})
        anchor_windows = dict(profile.get("anchor_windows") or {})
        samples.append(
            {
                "document_id": profile.get("document_id"),
                "title": profile.get("title"),
                "summary": _trim_text(((profile.get("document_core") or {}).get("summary")), 180),
                "voice_contract": {
                    "person": voice_contract.get("person"),
                    "distance": voice_contract.get("distance"),
                    "self_position": voice_contract.get("self_position"),
                },
                "value_and_judgment": {
                    "value_lens": value_and_judgment.get("value_lens"),
                    "judgment_mode": value_and_judgment.get("judgment_mode"),
                    "felt_cost": _trim_text(value_and_judgment.get("felt_cost"), 120),
                },
                "opening": _trim_text(anchor_windows.get("opening"), 180),
                "closing": _trim_text(anchor_windows.get("closing"), 180),
                "signature_lines": _normalize_string_list(anchor_windows.get("signature_lines"), limit=3, item_limit=180),
            }
        )
    return {
        "signal_source": "selected_samples",
        "sample_count": len(samples),
        "sample_titles": list((style_context.get("sample_titles") or []))[:6],
        "local_style_brief": dict(style_context.get("style_fingerprint_brief") or {}),
        "samples": samples,
        "selected_anchors": _selected_anchor_records_v3(bundle, rerank),
    }


def _build_style_fingerprint_brief(author_model: dict[str, Any]) -> dict[str, Any]:
    fingerprint = dict(author_model.get("style_fingerprint") or {})
    narrator_profile = dict(fingerprint.get("narrator_profile") or {})
    lexicon_profile = dict(fingerprint.get("lexicon_profile") or {})
    rhythm_profile = dict(fingerprint.get("rhythm_profile") or {})
    closure_profile = dict(fingerprint.get("closure_profile") or {})
    extreme_state_profile = dict(fingerprint.get("extreme_state_profile") or {})
    person = str(narrator_profile.get("person") or "first").strip() or "first"
    self_reference_terms = _normalize_string_list(narrator_profile.get("self_reference_terms"), limit=4, item_limit=12)
    persona_markers = _normalize_string_list(narrator_profile.get("persona_markers"), limit=4, item_limit=12)
    connective_keep = _normalize_string_list(lexicon_profile.get("connective_keep"), limit=6, item_limit=16)
    overfit_terms = _normalize_string_list(lexicon_profile.get("overfit_risk_terms"), limit=6, item_limit=16)
    rhetorical_markers = _normalize_string_list(lexicon_profile.get("rhetorical_markers"), limit=6, item_limit=16)
    thesis_markers = _normalize_string_list(lexicon_profile.get("thesis_markers"), limit=6, item_limit=16)
    punctuation_habits = _normalize_string_list(rhythm_profile.get("punctuation_habits"), limit=6, item_limit=8)
    closure_moves = _normalize_string_list(closure_profile.get("closure_moves"), limit=4, item_limit=36)
    signature_closures = _normalize_string_list(closure_profile.get("signature_closures"), limit=3, item_limit=80)
    argument_rules = _normalize_string_list(
        author_model.get("argument_rules") or fingerprint.get("argument_rules"),
        limit=4,
        item_limit=48,
    )
    return {
        "narrator_profile": {
            "person": person,
            "self_reference_terms": self_reference_terms,
            "self_position": _trim_text(narrator_profile.get("self_position"), 40),
            "narrative_distance": _trim_text(narrator_profile.get("narrative_distance"), 40),
            "persona_markers": persona_markers,
        },
        "lexicon_profile": {
            "high_frequency_terms": _normalize_string_list(lexicon_profile.get("high_frequency_terms"), limit=8, item_limit=16),
            "connective_keep": connective_keep,
            "overfit_risk_terms": overfit_terms,
            "rhetorical_markers": rhetorical_markers,
            "thesis_markers": thesis_markers,
        },
        "rhythm_profile": {
            "cadence": _trim_text(rhythm_profile.get("cadence"), 40),
            "sentence_shape": _trim_text(rhythm_profile.get("sentence_shape"), 40),
            "sentence_length_buckets": dict(rhythm_profile.get("sentence_length_buckets") or {}),
            "punctuation_habits": punctuation_habits,
        },
        "closure_profile": {
            "opening_moves": _normalize_string_list(closure_profile.get("opening_moves"), limit=4, item_limit=36),
            "turning_devices": _normalize_string_list(closure_profile.get("turning_devices"), limit=4, item_limit=36),
            "closure_moves": closure_moves,
            "signature_closures": signature_closures,
        },
        "extreme_state_profile": {
            "pressure_translation": _normalize_string_list(extreme_state_profile.get("pressure_translation"), limit=4, item_limit=48),
            "judgment_modes": _normalize_string_list(extreme_state_profile.get("judgment_modes"), limit=4, item_limit=36),
            "defense_moves": _normalize_string_list(extreme_state_profile.get("defense_moves"), limit=6, item_limit=48),
        },
        "self_reference_rules": _unique_preserve_order([
            f"人称保持：{person}",
            f"自称优先使用：{', '.join(self_reference_terms)}" if self_reference_terms else "",
            f"优先保留作者自称面具：{', '.join(persona_markers)}" if persona_markers else "",
            _trim_text(narrator_profile.get("self_position"), 40),
        ])[:4],
        "connective_keep": connective_keep,
        "connective_avoid": overfit_terms[:4],
        "cadence_rules": _unique_preserve_order([
            _trim_text(rhythm_profile.get("cadence"), 40),
            _trim_text(rhythm_profile.get("sentence_shape"), 40),
            f"标点倾向：{', '.join(punctuation_habits)}" if punctuation_habits else "",
        ])[:3],
        "closure_guardrails": _unique_preserve_order([
            *closure_moves[:3],
            "结尾不要改写成总结或立论。",
            f"可参考收口：{signature_closures[0]}" if signature_closures else "",
        ])[:4],
        "overfit_limits": _unique_preserve_order([
            *overfit_terms[:4],
            "不要把作者高频词堆成标签墙。",
        ])[:5],
        "persona_markers": persona_markers,
        "rhetorical_devices": rhetorical_markers,
        "thesis_refrains": thesis_markers,
        "argument_rules": argument_rules,
    }


def _split_text_sentences_v3(text: str) -> list[str]:
    parts = re.split(r"(?<=[。！？!?])\s+|\n+", normalize_whitespace(text))
    return [str(item).strip() for item in parts if str(item).strip()]


def _draft_pronoun_stats_v3(text: str) -> dict[str, int]:
    return {
        "first": sum(text.count(token) for token in ("我", "我们", "自己")),
        "second": sum(text.count(token) for token in ("你", "你们")),
        "third": sum(text.count(token) for token in ("他", "她", "它", "他们", "她们", "它们")),
    }


def _build_v3_draft_fingerprint_report(
    draft: str,
    writing_packet: dict[str, Any],
    blueprint: dict[str, Any],
) -> dict[str, Any]:
    text = normalize_whitespace(draft)
    brief = dict(writing_packet.get("style_fingerprint_brief") or {})
    narrator = dict(brief.get("narrator_profile") or {})
    lexicon_profile = dict(brief.get("lexicon_profile") or {})
    rhythm_profile = dict(brief.get("rhythm_profile") or {})
    closure_profile = dict(brief.get("closure_profile") or {})
    pronoun_stats = _draft_pronoun_stats_v3(text)
    expected_person = str(narrator.get("person") or "first").strip() or "first"
    dominant_pronoun = max(pronoun_stats, key=lambda key: pronoun_stats.get(key, 0)) if pronoun_stats else "first"
    pronoun_hard_fail = (
        expected_person in {"first", "second", "third"}
        and pronoun_stats.get(expected_person, 0) == 0
        and sum(pronoun_stats.values()) > 0
    )
    sentences = _split_text_sentences_v3(text)
    sentence_sizes = [estimate_word_count(item) for item in sentences]
    draft_buckets = {
        "short": sum(1 for size in sentence_sizes if size <= 18),
        "medium": sum(1 for size in sentence_sizes if 18 < size <= 36),
        "long": sum(1 for size in sentence_sizes if size > 36),
    }
    target_buckets = dict(rhythm_profile.get("sentence_length_buckets") or {})
    target_bucket = max(target_buckets, key=lambda key: int(target_buckets.get(key) or 0)) if target_buckets else "medium"
    draft_bucket = max(draft_buckets, key=lambda key: int(draft_buckets.get(key) or 0)) if draft_buckets else "medium"
    punctuation_targets = _normalize_string_list(rhythm_profile.get("punctuation_habits"), limit=6, item_limit=8)
    punctuation_hits = [token for token in punctuation_targets if token and token in text]
    connective_keep = _normalize_string_list(writing_packet.get("connective_keep"), limit=8, item_limit=16)
    lexicon_keep = _normalize_string_list(writing_packet.get("lexicon_keep"), limit=8, item_limit=16)
    persona_markers = _normalize_string_list(
        brief.get("persona_markers") or narrator.get("persona_markers"),
        limit=4,
        item_limit=12,
    )
    rhetorical_devices = _normalize_string_list(brief.get("rhetorical_devices"), limit=6, item_limit=16)
    thesis_refrains = _normalize_string_list(brief.get("thesis_refrains"), limit=6, item_limit=16)
    expected_lexicon = _unique_preserve_order([*connective_keep, *lexicon_keep, *thesis_refrains[:4]])[:12]
    lexicon_hits = [token for token in expected_lexicon if token and token in text]
    persona_hits = [token for token in persona_markers if token and token in text]
    rhetorical_hits = [token for token in rhetorical_devices if token and token in text]
    thesis_hits = [token for token in thesis_refrains if token and token in text]
    closing_sentence = sentences[-1] if sentences else text
    summary_markers = ("总之", "总而言之", "说到底", "归根结底", "最后", "于是")
    closure_hard_fail = any(marker in closing_sentence for marker in summary_markers)
    overfit_terms = _normalize_string_list(writing_packet.get("overfit_limits"), limit=8, item_limit=16)
    repeated_overfit_terms = [term for term in overfit_terms if term and text.count(term) >= 3]
    overfit_hard_fail = len(repeated_overfit_terms) >= 2
    persona_hard_fail = bool(persona_markers) and not persona_hits
    hard_failures: list[str] = []
    if pronoun_hard_fail:
        hard_failures.append("pronoun_drift")
    if closure_hard_fail:
        hard_failures.append("closure_summary")
    if overfit_hard_fail:
        hard_failures.append("overfit_stack")
    if persona_hard_fail:
        hard_failures.append("persona_drop")
    return {
        "pronoun_match": {
            "expected_person": expected_person,
            "dominant_pronoun": dominant_pronoun,
            "counts": pronoun_stats,
            "score": 1.0 if not pronoun_hard_fail else 0.25,
            "hard_fail": pronoun_hard_fail,
        },
        "persona_match": {
            "expected_markers": persona_markers,
            "matched_markers": persona_hits,
            "score": 1.0 if not persona_markers else round(len(persona_hits) / max(1, len(persona_markers)), 3),
            "hard_fail": persona_hard_fail,
        },
        "lexicon_match": {
            "expected_terms": expected_lexicon,
            "matched_terms": lexicon_hits,
            "score": round(len(lexicon_hits) / max(1, min(len(expected_lexicon), 6)), 3),
        },
        "rhetorical_match": {
            "expected_devices": rhetorical_devices,
            "matched_devices": rhetorical_hits,
            "score": round(len(rhetorical_hits) / max(1, len(rhetorical_devices)), 3) if rhetorical_devices else 1.0,
        },
        "thesis_match": {
            "expected_refrains": thesis_refrains,
            "matched_refrains": thesis_hits,
            "score": round(len(thesis_hits) / max(1, len(thesis_refrains)), 3) if thesis_refrains else 1.0,
        },
        "cadence_match": {
            "target_bucket": target_bucket,
            "draft_bucket": draft_bucket,
            "sentence_length_buckets": draft_buckets,
            "score": 1.0 if draft_bucket == target_bucket else 0.6,
        },
        "punctuation_match": {
            "target_punctuation": punctuation_targets,
            "matched_punctuation": punctuation_hits,
            "score": round(len(punctuation_hits) / max(1, len(punctuation_targets)), 3) if punctuation_targets else 1.0,
        },
        "closure_match": {
            "closure_move_targets": _normalize_string_list(closure_profile.get("closure_moves"), limit=4, item_limit=36),
            "closure_residue": _trim_text(blueprint.get("closure_residue"), 80),
            "closing_sentence": _trim_text(closing_sentence, 120),
            "hard_fail": closure_hard_fail,
            "score": 1.0 if not closure_hard_fail else 0.2,
        },
        "overfit_risk": {
            "risk_terms": overfit_terms,
            "repeated_terms": repeated_overfit_terms,
            "hard_fail": overfit_hard_fail,
            "score": 0.2 if overfit_hard_fail else 0.85,
        },
        "hard_failures": hard_failures,
        "style_fingerprint_brief": brief,
    }

def _normalize_style_packet_v3(
    payload: dict[str, Any],
    *,
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    selected_profile_ids: list[str] | None = None,
) -> dict[str, Any]:
    selected_docs = rerank.get("selected_documents") or []
    family_labels = _unique_preserve_order([item.get("family_label") for item in selected_docs if isinstance(item, dict)])
    request_support = dict(request_adapter.get("source_support") or {})
    local_style_context = _build_selected_sample_style_context_v3(bundle, selected_profile_ids, rerank)
    dominant_signals = dict(local_style_context.get("dominant_signals") or {})
    sample_local_floor = dict(local_style_context.get("sample_local_floor") or {})
    style_fingerprint_brief = (
        dict(local_style_context.get("style_fingerprint_brief") or {})
        or _build_style_fingerprint_brief(bundle.author_model)
    )
    global_translation_rules = list(bundle.author_model.get("translation_rules") or [])
    global_first_rule = next((dict(item) for item in global_translation_rules if isinstance(item, dict)), {})

    def _resolve_signal(*candidates: tuple[str, Any]) -> tuple[str, str]:
        value, source = _first_supported_signal_v3(*candidates)
        return _repair_stone_signal_text(value), source

    entry_scene, entry_scene_source = _resolve_signal(
        ("packet_llm", payload.get("entry_scene")),
        ("selected_samples", dominant_signals.get("entry_scene")),
        ("request_adapter", request_adapter.get("entry_scene")),
        ("author_model", (global_first_rule.get("opening_moves") or [None])[0]),
        ("generic", "从一个具体物件或动作进入。"),
    )
    felt_cost, felt_cost_source = _resolve_signal(
        ("packet_llm", payload.get("felt_cost")),
        ("selected_samples", dominant_signals.get("felt_cost")),
        ("request_adapter", request_adapter.get("felt_cost")),
        ("generic", "先把压力落成身体能感到的代价，再进入解释。"),
    )
    value_lens, value_lens_source = _resolve_signal(
        ("packet_llm", payload.get("value_lens")),
        ("selected_samples", dominant_signals.get("value_lens")),
        ("request_adapter", request_adapter.get("value_lens")),
        ("author_model", global_first_rule.get("value_lens")),
        ("generic", "代价"),
    )
    judgment_mode, judgment_mode_source = _resolve_signal(
        ("packet_llm", payload.get("judgment_mode")),
        ("selected_samples", dominant_signals.get("judgment_mode")),
        ("request_adapter", request_adapter.get("judgment_mode")),
        ("generic", "通过贴身细节稳住判断"),
    )
    distance, distance_source = _resolve_signal(
        ("packet_llm", payload.get("distance")),
        ("selected_samples", dominant_signals.get("distance")),
        ("request_adapter", request_adapter.get("distance")),
        ("generic", "回收式第一人称"),
    )
    source_support = {
        "entry_scene": entry_scene_source or str(request_support.get("entry_scene") or "generic"),
        "felt_cost": felt_cost_source or str(request_support.get("felt_cost") or "generic"),
        "value_lens": value_lens_source or str(request_support.get("value_lens") or "generic"),
        "judgment_mode": judgment_mode_source or str(request_support.get("judgment_mode") or "generic"),
        "distance": distance_source or str(request_support.get("distance") or "generic"),
    }
    defaulted_fields = [field for field, source in source_support.items() if source == "generic"]
    local_author_core = dict(sample_local_floor.get("author_core") or {})
    persona_markers = _normalize_string_list(style_fingerprint_brief.get("persona_markers"), limit=4, item_limit=12)
    rhetorical_devices = _normalize_string_list(style_fingerprint_brief.get("rhetorical_devices"), limit=6, item_limit=16)
    thesis_refrains = _normalize_string_list(style_fingerprint_brief.get("thesis_refrains"), limit=6, item_limit=16)
    argument_rules = _normalize_string_list(style_fingerprint_brief.get("argument_rules"), limit=4, item_limit=48)
    style_signal_source = "selected_samples" if local_style_context else "author_model_fallback"
    return {
        "entry_scene": entry_scene or "从一个具体物件或动作进入。",
        "felt_cost": felt_cost or "先把压力落成身体能感到的代价，再进入解释。",
        "value_lens": value_lens or "代价",
        "judgment_mode": judgment_mode or "通过贴身细节稳住判断",
        "distance": distance or "回收式第一人称",
        "family_labels": family_labels[:6],
        "lexicon_keep": _unique_preserve_order([
            *_normalize_string_list(payload.get("lexicon_keep"), limit=8),
            *_normalize_string_list((style_fingerprint_brief.get("lexicon_profile") or {}).get("high_frequency_terms"), limit=8),
            *_normalize_string_list(local_author_core.get("signature_motifs"), limit=6),
            *thesis_refrains[:4],
        ])[:10],
        "motif_obligations": _unique_preserve_order([
            *_normalize_string_list(payload.get("motif_obligations"), limit=6),
            *_normalize_string_list(request_adapter.get("motif_terms"), limit=6),
            *_normalize_string_list(local_author_core.get("signature_motifs"), limit=6),
        ])[:8],
        "syntax_rules": _normalize_string_list(payload.get("syntax_rules"), limit=6)
        or _normalize_string_list(sample_local_floor.get("stable_moves"), limit=6)
        or _normalize_string_list(bundle.author_model.get("stable_moves"), limit=6),
        "structure_recipe": _normalize_string_list(payload.get("structure_recipe"), limit=8)
        or [
            "从一个具体动作进入。",
            "通过可见细节把压力往前推。",
            "让结尾留下余韵。",
        ],
        "do_not_do": _unique_preserve_order([
            *_normalize_string_list(payload.get("do_not_do"), limit=8),
            *_normalize_string_list(sample_local_floor.get("forbidden_moves"), limit=8),
            *_normalize_string_list(bundle.author_model.get("forbidden_moves"), limit=8),
        ])[:8],
        "anchor_ids": list(rerank.get("anchor_ids") or [])[:8],
        "style_fingerprint_brief": style_fingerprint_brief,
        "self_reference_rules": list(style_fingerprint_brief.get("self_reference_rules") or [])[:4],
        "connective_keep": _unique_preserve_order([
            *_normalize_string_list(payload.get("connective_keep"), limit=6),
            *_normalize_string_list(style_fingerprint_brief.get("connective_keep"), limit=6),
        ])[:6],
        "connective_avoid": _unique_preserve_order([
            *_normalize_string_list(payload.get("connective_avoid"), limit=4),
            *_normalize_string_list(style_fingerprint_brief.get("connective_avoid"), limit=4),
        ])[:4],
        "cadence_rules": _unique_preserve_order([
            *_normalize_string_list(payload.get("cadence_rules"), limit=4),
            *_normalize_string_list(style_fingerprint_brief.get("cadence_rules"), limit=4),
        ])[:4],
        "closure_guardrails": _unique_preserve_order([
            *_normalize_string_list(payload.get("closure_guardrails"), limit=4),
            *_normalize_string_list(style_fingerprint_brief.get("closure_guardrails"), limit=4),
        ])[:4],
        "overfit_limits": _unique_preserve_order([
            *_normalize_string_list(payload.get("overfit_limits"), limit=6),
            *_normalize_string_list(style_fingerprint_brief.get("overfit_limits"), limit=6),
        ])[:6],
        "persona_markers": persona_markers,
        "rhetorical_devices": rhetorical_devices,
        "thesis_refrains": thesis_refrains,
        "argument_rules": argument_rules,
        "sample_titles": list(local_style_context.get("sample_titles") or [])[:6],
        "sample_local_floor": sample_local_floor,
        "style_signal_source": style_signal_source,
        "style_thesis": _trim_text(payload.get("style_thesis"), 220),
        "source_support": source_support,
        "defaulted_fields": defaulted_fields,
        "support_score": round((5 - len(defaulted_fields)) / 5, 3),
    }


def _normalize_blueprint_v3(
    payload: dict[str, Any],
    state: WritingStreamState,
    style_packet: dict[str, Any],
) -> dict[str, Any]:
    paragraph_count = _clamp_int(
        payload.get("paragraph_count"),
        default=_default_paragraph_count(state.target_word_count),
        minimum=2,
        maximum=6,
    )
    anchor_ids = _unique_preserve_order([
        *_normalize_string_list(payload.get("anchor_ids"), limit=8),
        *(style_packet.get("anchor_ids") or []),
    ])[:8]
    return {
        "paragraph_count": paragraph_count,
        "shape_note": str(payload.get("shape_note") or "").strip() or "通过选中的原型动作建立克制的压力。",
        "entry_move": str(payload.get("entry_move") or style_packet.get("entry_scene") or "").strip() or "从一个可见动作起笔。",
        "development_move": str(payload.get("development_move") or "").strip() or "让压力沿着反复出现的具体细节慢慢抬升。",
        "turning_device": str(payload.get("turning_device") or "").strip() or "轻微转向，不要上升成立论。",
        "closure_residue": str(payload.get("closure_residue") or "").strip() or "收在余味上，不要写成总结。",
        "keep_terms": _unique_preserve_order([
            *_normalize_string_list(payload.get("keep_terms"), limit=8),
            *(style_packet.get("lexicon_keep") or []),
        ])[:8],
        "motif_obligations": _unique_preserve_order([
            *_normalize_string_list(payload.get("motif_obligations"), limit=6),
            *(style_packet.get("motif_obligations") or []),
        ])[:6],
        "steps": _normalize_string_list(payload.get("steps"), limit=8) or list(style_packet.get("structure_recipe") or [])[:8],
        "do_not_do": _unique_preserve_order([
            *_normalize_string_list(payload.get("do_not_do"), limit=8),
            *(style_packet.get("do_not_do") or []),
        ])[:8],
        "anchor_ids": anchor_ids,
    }

def _normalize_blueprint_axis_map_v3(
    payload: Any,
    *,
    writing_packet: dict[str, Any],
    paragraph_count: int,
) -> dict[str, Any]:
    allowed_anchor_ids = set(str(item).strip() for item in list(writing_packet.get("anchor_ids") or []) if str(item).strip())
    axis_source_map = dict(writing_packet.get("axis_source_map") or {})
    axis_payload = payload if isinstance(payload, dict) else {}
    normalized: dict[str, Any] = {}
    for axis_key, source in axis_source_map.items():
        raw = axis_payload.get(axis_key)
        raw_dict = raw if isinstance(raw, dict) else {}
        anchor_ids = [
            anchor_id
            for anchor_id in _normalize_string_list(raw_dict.get("anchor_ids"), limit=4)
            if anchor_id in allowed_anchor_ids
        ]
        normalized[axis_key] = {
            "goal": str(raw_dict.get("goal") or raw or source.get("summary") or "").strip(),
            "paragraph_hint": _clamp_int(
                raw_dict.get("paragraph_hint"),
                default=min(paragraph_count, 3),
                minimum=1,
                maximum=paragraph_count,
            ),
            "anchor_ids": anchor_ids[:4],
            "guide_slots": list(source.get("guide_slots") or [])[:4],
            "confidence": _clamp_score(source.get("confidence"), default=0.0),
        }
    return normalized


def _default_paragraph_map_v3(
    *,
    paragraph_count: int,
    steps: list[str],
    axis_map: dict[str, Any],
    anchor_ids: list[str],
) -> list[dict[str, Any]]:
    axis_keys = list(axis_map.keys())
    paragraph_map: list[dict[str, Any]] = []
    for index in range(paragraph_count):
        role = "opening" if index == 0 else "closing" if index == paragraph_count - 1 else "development"
        step = steps[index] if index < len(steps) else (steps[-1] if steps else "")
        paragraph_axis_keys = axis_keys[index::paragraph_count] or axis_keys[: min(2, len(axis_keys))]
        paragraph_anchor_ids = anchor_ids[index::paragraph_count] or anchor_ids[: min(2, len(anchor_ids))]
        paragraph_map.append(
            {
                "paragraph_index": index + 1,
                "role": role,
                "objective": step,
                "axis_keys": paragraph_axis_keys[:3],
                "anchor_ids": paragraph_anchor_ids[:3],
            }
        )
    return paragraph_map


def _normalize_blueprint_packet_v3(
    payload: dict[str, Any],
    state: WritingStreamState,
    writing_packet: dict[str, Any],
) -> dict[str, Any]:
    paragraph_count = _clamp_int(
        payload.get("paragraph_count"),
        default=_default_paragraph_count(state.target_word_count),
        minimum=2,
        maximum=6,
    )
    anchor_ids = _unique_preserve_order([
        *_normalize_string_list(payload.get("anchor_ids"), limit=8),
        *(writing_packet.get("anchor_ids") or []),
    ])[:8]
    steps = _normalize_string_list(payload.get("steps"), limit=8) or list(writing_packet.get("structure_recipe") or [])[:8]
    axis_map = _normalize_blueprint_axis_map_v3(
        payload.get("axis_map"),
        writing_packet=writing_packet,
        paragraph_count=paragraph_count,
    )
    paragraph_map: list[dict[str, Any]] = []
    for index, item in enumerate(payload.get("paragraph_map") or [], start=1):
        if not isinstance(item, dict):
            continue
        paragraph_anchor_ids = [
            anchor_id
            for anchor_id in _normalize_string_list(item.get("anchor_ids"), limit=4)
            if anchor_id in anchor_ids
        ]
        paragraph_axis_keys = [
            axis_key
            for axis_key in _normalize_string_list(item.get("axis_keys"), limit=4)
            if axis_key in axis_map
        ]
        paragraph_map.append(
            {
                "paragraph_index": _clamp_int(item.get("paragraph_index"), default=index, minimum=1, maximum=paragraph_count),
                "role": str(item.get("role") or "").strip() or ("opening" if index == 1 else "closing" if index == paragraph_count else "development"),
                "objective": str(item.get("objective") or item.get("move") or "").strip(),
                "axis_keys": paragraph_axis_keys[:3],
                "anchor_ids": paragraph_anchor_ids[:3],
            }
        )
        if len(paragraph_map) >= paragraph_count:
            break
    if not paragraph_map:
        paragraph_map = _default_paragraph_map_v3(
            paragraph_count=paragraph_count,
            steps=steps,
            axis_map=axis_map,
            anchor_ids=anchor_ids,
        )
    return {
        "paragraph_count": paragraph_count,
        "shape_note": str(payload.get("shape_note") or "").strip() or "通过选中的原型动作建立克制的压力。",
        "entry_move": str(payload.get("entry_move") or writing_packet.get("entry_scene") or "").strip() or "从一个可见动作起笔。",
        "development_move": str(payload.get("development_move") or "").strip() or "让压力沿着反复出现的具体细节慢慢抬升。",
        "turning_device": str(payload.get("turning_device") or "").strip() or "轻微转向，不要上升成立论。",
        "closure_residue": str(payload.get("closure_residue") or "").strip() or "收在余味上，不要写成总结。",
        "keep_terms": _unique_preserve_order([
            *_normalize_string_list(payload.get("keep_terms"), limit=8),
            *(writing_packet.get("lexicon_keep") or []),
        ])[:8],
        "motif_obligations": _unique_preserve_order([
            *_normalize_string_list(payload.get("motif_obligations"), limit=6),
            *(writing_packet.get("motif_obligations") or []),
        ])[:6],
        "steps": steps,
        "do_not_do": _unique_preserve_order([
            *_normalize_string_list(payload.get("do_not_do"), limit=8),
            *(writing_packet.get("do_not_do") or []),
        ])[:8],
        "axis_map": axis_map,
        "paragraph_map": paragraph_map,
        "anchor_ids": anchor_ids,
    }

def _normalize_v3_critic_payload(
    payload: dict[str, Any],
    *,
    critic_key: str,
    selected_anchor_ids: list[str],
) -> dict[str, Any]:
    allowed_ids = set(selected_anchor_ids)
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=8)
        if anchor_id in allowed_ids
    ]
    if not anchor_ids:
        anchor_ids = list(selected_anchor_ids)[:3]
    verdict = str(payload.get("verdict") or "").strip()
    if verdict not in {"approve", "line_edit", "redraft"}:
        verdict = "approve" if payload.get("pass", True) else "line_edit"
    return {
        "critic_key": critic_key,
        "critic_label": _critic_spec_v3(critic_key)["label"],
        "pass": bool(payload.get("pass", verdict == "approve")),
        "score": _clamp_score(payload.get("score"), default=0.72 if verdict == "approve" else 0.58),
        "verdict": verdict,
        "anchor_ids": anchor_ids,
        "matched_signals": _normalize_string_list(payload.get("matched_signals"), limit=5),
        "must_keep_spans": _normalize_string_list(payload.get("must_keep_spans"), limit=4),
        "line_edits": _normalize_string_list(payload.get("line_edits"), limit=6),
        "redraft_reason": str(payload.get("redraft_reason") or "").strip(),
        "risks": _normalize_string_list(payload.get("risks"), limit=4),
    }


def _build_v3_author_floor(
    bundle: StoneWritingAnalysisBundle,
    writing_packet: dict[str, Any] | None = None,
) -> dict[str, Any]:
    writing_packet = writing_packet or {}
    sample_local_floor = dict(writing_packet.get("sample_local_floor") or {})
    if sample_local_floor:
        return {
            "author_core": dict(sample_local_floor.get("author_core") or {}),
            "style_fingerprint": dict(sample_local_floor.get("style_fingerprint") or {}),
            "stable_moves": list(sample_local_floor.get("stable_moves") or [])[:8],
            "forbidden_moves": list(sample_local_floor.get("forbidden_moves") or [])[:8],
            "critic_rubrics": dict(bundle.author_model.get("critic_rubrics") or {}),
            "signal_source": str(sample_local_floor.get("signal_source") or "selected_samples"),
            "sample_titles": list(sample_local_floor.get("sample_titles") or [])[:6],
        }
    return {
        "author_core": dict(bundle.author_model.get("author_core") or {}),
        "style_fingerprint": dict(bundle.author_model.get("style_fingerprint") or {}),
        "stable_moves": list(bundle.author_model.get("stable_moves") or [])[:8],
        "forbidden_moves": list(bundle.author_model.get("forbidden_moves") or [])[:8],
        "critic_rubrics": dict(bundle.author_model.get("critic_rubrics") or {}),
        "signal_source": "author_model",
        "sample_titles": [],
    }


def _build_v3_draft_guardrails(writing_packet: dict[str, Any], blueprint: dict[str, Any]) -> dict[str, Any]:
    return {
        "language_constraint": "全文必须使用自然、完整的简体中文，不要输出整句英文或提示词式表达。",
        "entry_scene": writing_packet.get("entry_scene"),
        "felt_cost": writing_packet.get("felt_cost"),
        "style_fingerprint_brief": dict(writing_packet.get("style_fingerprint_brief") or {}),
        "style_signal_source": writing_packet.get("style_signal_source"),
        "hard_constraints": [
            "人称和自称不能漂。",
            "结尾姿态不能跑成总结或立论。",
            "不要把作者高频词堆成标签墙。",
        ],
        "self_reference_rules": list(writing_packet.get("self_reference_rules") or [])[:4],
        "persona_markers": list(writing_packet.get("persona_markers") or [])[:4],
        "rhetorical_devices": list(writing_packet.get("rhetorical_devices") or [])[:6],
        "thesis_refrains": list(writing_packet.get("thesis_refrains") or [])[:6],
        "argument_rules": list(writing_packet.get("argument_rules") or [])[:4],
        "connective_keep": list(writing_packet.get("connective_keep") or [])[:6],
        "connective_avoid": list(writing_packet.get("connective_avoid") or [])[:4],
        "cadence_rules": list(writing_packet.get("cadence_rules") or [])[:4],
        "closure_guardrails": list(writing_packet.get("closure_guardrails") or [])[:4],
        "overfit_limits": list(writing_packet.get("overfit_limits") or [])[:6],
        "motif_obligations": list(blueprint.get("motif_obligations") or [])[:6],
        "movement_steps": list(blueprint.get("steps") or [])[:8],
        "negative_constraints": list(writing_packet.get("do_not_do") or [])[:8],
        "axis_map": dict(blueprint.get("axis_map") or {}),
        "paragraph_map": list(blueprint.get("paragraph_map") or [])[:6],
    }


def _build_v3_line_edit_brief(
    draft: str,
    critics: list[dict[str, Any]],
    blueprint: dict[str, Any],
    target_word_count: int,
    writing_packet: dict[str, Any] | None = None,
) -> dict[str, Any]:
    writing_packet = writing_packet or {}
    return {
        "target_word_count": target_word_count,
        "current_word_count": estimate_word_count(draft),
        "language_constraint": "保持全文为自然简体中文，不要混入整句英文或分析说明。",
        "style_fingerprint_brief": dict(writing_packet.get("style_fingerprint_brief") or {}),
        "style_signal_source": writing_packet.get("style_signal_source"),
        "self_reference_rules": list(writing_packet.get("self_reference_rules") or [])[:4],
        "persona_markers": list(writing_packet.get("persona_markers") or [])[:4],
        "rhetorical_devices": list(writing_packet.get("rhetorical_devices") or [])[:6],
        "thesis_refrains": list(writing_packet.get("thesis_refrains") or [])[:6],
        "argument_rules": list(writing_packet.get("argument_rules") or [])[:4],
        "cadence_rules": list(writing_packet.get("cadence_rules") or [])[:4],
        "closure_guardrails": list(writing_packet.get("closure_guardrails") or [])[:4],
        "overfit_limits": list(writing_packet.get("overfit_limits") or [])[:6],
        "must_keep_spans": _unique_preserve_order([span for critic in critics for span in (critic.get("must_keep_spans") or [])])[:6],
        "line_edits": _unique_preserve_order([edit for critic in critics for edit in (critic.get("line_edits") or [])])[:8],
        "shape_note": blueprint.get("shape_note"),
        "closure_residue": blueprint.get("closure_residue"),
        "axis_map": dict(blueprint.get("axis_map") or {}),
        "paragraph_map": list(blueprint.get("paragraph_map") or [])[:6],
    }

def _revision_action_v3(
    critics: list[dict[str, Any]],
    draft_fingerprint_report: dict[str, Any] | None = None,
) -> str:
    hard_failures = list((draft_fingerprint_report or {}).get("hard_failures") or [])
    if any(str(item.get("verdict") or "") == "redraft" for item in critics):
        return "redraft"
    if hard_failures:
        return "line_edit"
    if any((not item.get("pass")) or item.get("line_edits") for item in critics):
        return "line_edit"
    return "none"

def _stone_json_chinese_instruction(*, preserve_tokens: str | None = None) -> str:
    base = (
        "除固定枚举、ID 与必须原样复用的来源 token 外，"
        "JSON 里的所有自然语言字段都必须使用简体中文。"
    )
    if preserve_tokens:
        return f"{base}\n以下字段或 token 需要保留原格式：{preserve_tokens}。"
    return base


_STONE_BODY_CHINESE_ONLY = (
    "正文必须只使用自然、完整的简体中文。\n"
    "不要输出英文句子、双语复述、提示词字段名或分析术语。\n"
    "除非用户明确要求，只有无法翻译的专有名词或引文才保留原文。"
)

def _render_request_adapter_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"desired_length_band: {payload.get('desired_length_band') or ''}",
        f"surface_form: {payload.get('surface_form') or ''}",
        f"value_lens: {payload.get('value_lens') or ''}",
        f"judgment_mode: {payload.get('judgment_mode') or ''}",
        f"distance: {payload.get('distance') or ''}",
        f"support_score: {payload.get('support_score') or 0}",
        "",
        f"entry_scene: {payload.get('entry_scene') or ''}",
        f"felt_cost: {payload.get('felt_cost') or ''}",
        "",
        "query_terms:",
        *[f"- {item}" for item in (payload.get("query_terms") or [])[:8]],
    ]
    if payload.get("defaulted_fields"):
        lines.extend(["", "defaulted_fields:", *[f"- {item}" for item in (payload.get("defaulted_fields") or [])[:6]]])
    return "\n".join(lines).strip()


def _render_profile_selection_v3(payload: dict[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    lines = [
        f"selected_profile_count: {summary.get('selected_profile_count') or 0}",
        f"selected_profile_ids: {len(summary.get('selected_profile_ids') or [])}",
        "",
        "top_families:",
        *[f"- {item}" for item in (summary.get("top_families") or [])[:6]],
        "",
        "top_motifs:",
        *[f"- {item}" for item in (summary.get("top_motifs") or [])[:6]],
    ]
    if payload.get("selection_notes"):
        lines.extend(["", "selection_notes:", *[f"- {item}" for item in (payload.get("selection_notes") or [])[:6]]])
    return "\n".join(lines).strip()


def _render_candidate_shortlist_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"shortlist_size: {payload.get('shortlist_size') or 0}",
        f"desired_length_band: {payload.get('desired_length_band') or ''}",
        f"surface_form: {payload.get('surface_form') or ''}",
        "",
        "documents:",
    ]
    for item in (payload.get("documents") or [])[:12]:
        lines.extend(
            [
                f"- {item.get('title') or item.get('document_id') or 'document'}",
                f"  family: {item.get('family_label') or item.get('family_id') or ''}",
                f"  score: {round(float(item.get('score') or 0.0), 2)}",
                f"  reasons: {', '.join(item.get('reasons') or [])}",
            ]
        )
    return "\n".join(lines).strip()


def _render_rerank_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"selected_documents: {len(payload.get('selected_documents') or [])}",
        f"selected_anchors: {len(payload.get('anchor_ids') or [])}",
    ]
    if payload.get("selection_reason"):
        lines.extend(["", f"selection_reason: {payload.get('selection_reason')}"])
    lines.extend(["", "documents:"])
    for item in payload.get("selected_documents") or []:
        lines.append(
            f"- {item.get('title') or item.get('document_id') or 'document'} | "
            f"{item.get('family_label') or item.get('family_id') or ''}"
        )
    return "\n".join(lines).strip()


def _render_style_packet_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"style_signal_source: {payload.get('style_signal_source') or ''}",
        f"entry_scene: {payload.get('entry_scene') or ''}",
        f"felt_cost: {payload.get('felt_cost') or ''}",
        f"value_lens: {payload.get('value_lens') or ''}",
        f"distance: {payload.get('distance') or ''}",
        "",
        "family_labels:",
        *[f"- {item}" for item in (payload.get("family_labels") or [])[:6]],
        "",
        "motif_obligations:",
        *[f"- {item}" for item in (payload.get("motif_obligations") or [])[:6]],
        "",
        "lexicon_keep:",
        *[f"- {item}" for item in (payload.get("lexicon_keep") or [])[:8]],
        "",
        "self_reference_rules:",
        *[f"- {item}" for item in (payload.get("self_reference_rules") or [])[:4]],
        "",
        "persona_markers:",
        *[f"- {item}" for item in (payload.get("persona_markers") or [])[:4]],
        "",
        "rhetorical_devices:",
        *[f"- {item}" for item in (payload.get("rhetorical_devices") or [])[:6]],
        "",
        "thesis_refrains:",
        *[f"- {item}" for item in (payload.get("thesis_refrains") or [])[:6]],
        "",
        "connective_keep:",
        *[f"- {item}" for item in (payload.get("connective_keep") or [])[:6]],
        "",
        "closure_guardrails:",
        *[f"- {item}" for item in (payload.get("closure_guardrails") or [])[:4]],
    ]
    return "\n".join(lines).strip()


def _normalize_writing_packet_v3(
    payload: dict[str, Any],
    *,
    bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    profile_selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile_selection = profile_selection or {}
    style_selected_profile_ids = [
        str((item or {}).get("document_id") or "").strip()
        for item in list(rerank.get("selected_documents") or [])
        if isinstance(item, dict) and str((item or {}).get("document_id") or "").strip()
    ]
    if not style_selected_profile_ids:
        style_selected_profile_ids = list(profile_selection.get("selected_profile_ids") or bundle.selected_profile_ids or [])[:24]
    style_packet = _normalize_style_packet_v3(
        payload,
        bundle=bundle,
        request_adapter=request_adapter,
        rerank=rerank,
        selected_profile_ids=style_selected_profile_ids,
    )
    analysis_summary = dict(bundle.analysis_summary or {})
    compact_analysis_summary = _compact_analysis_summary_for_prompt_v3(analysis_summary)
    profile_index = dict(bundle.profile_index or {})
    selected_profile_slices = list(profile_selection.get("profile_slices") or bundle.profile_slices or [])[:24]
    selected_profile_ids = list(profile_selection.get("selected_profile_ids") or bundle.selected_profile_ids or [])[:24]
    profile_selection_summary = dict(profile_selection.get("summary") or {})
    selected_sample_context = _build_local_sample_packet_context_v3(bundle, style_selected_profile_ids, rerank)
    source_map = _build_source_map_v3(
        profile_index=profile_index,
        analysis_summary=analysis_summary,
        rerank=rerank,
    )
    axis_source_map = _build_axis_source_map_v3(
        analysis_summary=analysis_summary,
        source_map=source_map,
        writing_guide=dict(bundle.writing_guide or {}),
    )
    coverage_warnings = _unique_preserve_order(
        [
            *list(bundle.coverage_warnings or []),
            *_normalize_string_list(payload.get("coverage_warnings"), limit=8, item_limit=160),
            *_normalize_string_list(analysis_summary.get("warnings"), limit=8, item_limit=160),
            *_normalize_string_list(profile_index.get("coverage_notes"), limit=8, item_limit=160),
            *_normalize_string_list(profile_selection.get("selection_notes"), limit=8, item_limit=180),
        ]
    )[:10]
    defaulted_fields = list(style_packet.get("defaulted_fields") or [])
    source_support = dict(style_packet.get("source_support") or {})
    if defaulted_fields:
        coverage_warnings.append(f"写作包仍有通用兜底字段：{', '.join(defaulted_fields[:5])}")
    packet = dict(style_packet)
    packet.update(
        {
            "packet_version": "v3",
            "packet_kind": "writing_packet_v3",
            "analysis_run": {
                "run_id": compact_analysis_summary.get("run_id"),
                "status": compact_analysis_summary.get("status"),
                "analysis_ready": bool(compact_analysis_summary.get("analysis_ready")),
                "missing_facet_keys": list(compact_analysis_summary.get("missing_facet_keys") or []),
            },
            "analysis_facet_packets": list(compact_analysis_summary.get("facet_packets") or [])[:8],
            "profile_index": {
                "profile_count": int(profile_index.get("profile_count") or 0),
                "sample_budget": int(profile_index.get("sample_budget") or 0),
                "sampled_profile_count": int(profile_index.get("sampled_profile_count") or 0),
                "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
                "top_motifs": list(profile_index.get("top_motifs") or [])[:12],
                "top_keywords": list(profile_index.get("top_keywords") or [])[:12],
                "top_families": list(profile_index.get("top_families") or [])[:8],
                "selected_profile_ids": list(profile_index.get("selected_profile_ids") or [])[:24],
                "top_value_lenses": list(profile_index.get("top_value_lenses") or [])[:8],
                "top_judgment_modes": list(profile_index.get("top_judgment_modes") or [])[:8],
                "top_distances": list(profile_index.get("top_distances") or [])[:8],
                "selection_policy": dict(profile_index.get("selection_policy") or {}),
            },
            "profile_selection": {
                "selected_profile_count": len(selected_profile_slices),
                "selected_profile_ids": selected_profile_ids,
                "top_families": list(profile_selection_summary.get("top_families") or [])[:8],
                "top_motifs": list(profile_selection_summary.get("top_motifs") or [])[:10],
                "top_keywords": list(profile_selection_summary.get("top_keywords") or [])[:10],
                "top_value_lenses": list(profile_selection_summary.get("top_value_lenses") or [])[:6],
                "top_judgment_modes": list(profile_selection_summary.get("top_judgment_modes") or [])[:6],
                "top_distances": list(profile_selection_summary.get("top_distances") or [])[:6],
                "selection_notes": list(profile_selection.get("selection_notes") or [])[:6],
            },
            "profile_slices": selected_profile_slices,
            "selected_profile_ids": selected_profile_ids,
            "style_selected_profile_ids": style_selected_profile_ids[:24],
            "selected_sample_context": selected_sample_context,
            "writing_guide": dict(bundle.writing_guide or {}),
            "source_map": source_map,
            "axis_source_map": axis_source_map,
            "coverage_warnings": _unique_preserve_order(coverage_warnings)[:10],
            "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
            "source_support": source_support,
            "defaulted_fields": defaulted_fields,
            "support_score": style_packet.get("support_score"),
            "style_packet_v3": dict(style_packet),
        }
    )
    return packet


def _render_writing_packet_v3(payload: dict[str, Any]) -> str:
    style_body = _render_style_packet_v3(payload)
    lines = [
        f"packet_version: {payload.get('packet_version') or ''}",
        f"sparse_profile_mode: {bool(payload.get('sparse_profile_mode'))}",
        f"support_score: {payload.get('support_score') or 0}",
        f"coverage_warnings: {len(payload.get('coverage_warnings') or [])}",
        f"selected_profile_ids: {len(payload.get('selected_profile_ids') or [])}",
        f"axis_source_map: {len(payload.get('axis_source_map') or {})}",
        "",
        "analysis_run:",
        f"- run_id: {((payload.get('analysis_run') or {}).get('run_id') or '')}",
        f"- status: {((payload.get('analysis_run') or {}).get('status') or '')}",
        f"- analysis_ready: {bool((payload.get('analysis_run') or {}).get('analysis_ready'))}",
        "",
        "profile_index:",
        f"- profile_count: {((payload.get('profile_index') or {}).get('profile_count') or 0)}",
        f"- sampled_profile_count: {((payload.get('profile_index') or {}).get('sampled_profile_count') or 0)}",
        f"- sparse_profile_mode: {bool((payload.get('profile_index') or {}).get('sparse_profile_mode'))}",
        "",
        "coverage_warnings:",
        *[f"- {item}" for item in (payload.get("coverage_warnings") or [])[:8]],
        "",
        "selected_profile_ids:",
        *[f"- {item}" for item in (payload.get("selected_profile_ids") or [])[:8]],
        "",
        "defaulted_fields:",
        *[f"- {item}" for item in (payload.get("defaulted_fields") or [])[:6]],
        "",
        "selected_anchor_ids:",
        *[f"- {item}" for item in (payload.get("anchor_ids") or [])[:8]],
        "",
        "axis_source_map_keys:",
        *[f"- {item}" for item in sorted((payload.get("axis_source_map") or {}).keys())[:12]],
        "",
        "writing_guide_keys:",
        *[f"- {item}" for item in sorted((payload.get("writing_guide") or {}).keys())[:12]],
        "",
        "style_packet:",
        style_body,
    ]
    return "\n".join(lines).strip()


def _render_blueprint_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"paragraph_count: {payload.get('paragraph_count') or 0}",
        f"shape_note: {payload.get('shape_note') or ''}",
        f"entry_move: {payload.get('entry_move') or ''}",
        f"development_move: {payload.get('development_move') or ''}",
        f"turning_device: {payload.get('turning_device') or ''}",
        f"closure_residue: {payload.get('closure_residue') or ''}",
        "",
        "steps:",
        *[f"- {item}" for item in (payload.get("steps") or [])[:8]],
        "",
        "axis_map:",
        *[
            f"- {axis_key}: {(axis_value or {}).get('goal') or ''}"
            for axis_key, axis_value in list((payload.get("axis_map") or {}).items())[:8]
        ],
        "",
        "paragraph_map:",
        *[
            f"- P{item.get('paragraph_index')}: {item.get('role') or ''} | "
            f"{item.get('objective') or ''} | axes={', '.join(item.get('axis_keys') or [])}"
            for item in (payload.get("paragraph_map") or [])[:6]
            if isinstance(item, dict)
        ],
    ]
    return "\n".join(lines).strip()


def _collect_v3_trace_anchor_ids(
    bundle: StoneWritingAnalysisBundle,
    rerank: dict[str, Any],
    writing_packet: dict[str, Any],
    blueprint: dict[str, Any],
    revision_rounds: list[dict[str, Any]],
) -> list[str]:
    values: list[str] = []
    values.extend(_available_anchor_ids(bundle)[:12])
    values.extend(rerank.get("anchor_ids") or [])
    values.extend(writing_packet.get("anchor_ids") or [])
    values.extend(blueprint.get("anchor_ids") or [])
    for round_payload in revision_rounds:
        for critic in round_payload.get("critics") or []:
            values.extend(critic.get("anchor_ids") or [])
    return _unique_preserve_order(values)


def _build_trace_blocks_v3(
    analysis_bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    profile_selection: dict[str, Any],
    shortlist: dict[str, Any],
    rerank: dict[str, Any],
    writing_packet: dict[str, Any],
    packet_critic_rounds: list[dict[str, Any]],
    blueprint: dict[str, Any],
    revision_rounds: list[dict[str, Any]],
    revision_action: str,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "generation_packet",
            "label": f"Stone v3 baseline ready ({analysis_bundle.version_label})",
            "baseline": analysis_bundle.generation_packet.get("baseline", {}),
        },
        {
            "type": "stage",
            "stage": "request_adapter_v3",
            "label": "Request adapted into author space",
            "query_terms": request_adapter.get("query_terms") or [],
        },
        {
            "type": "stage",
            "stage": "profile_selection_v3",
            "label": "Request-conditioned profile evidence selected",
            "selected_profile_ids": list((profile_selection.get("summary") or {}).get("selected_profile_ids") or [])[:8],
            "top_families": list((profile_selection.get("summary") or {}).get("top_families") or [])[:6],
        },
        {
            "type": "stage",
            "stage": "candidate_shortlist_v3",
            "label": "Rule shortlist prepared",
            "candidate_count": len(shortlist.get("documents") or []),
        },
        {
            "type": "stage",
            "stage": "llm_rerank_v3",
            "label": "LLM rerank finalized evidence",
            "anchor_ids": rerank.get("anchor_ids") or [],
        },
        {
            "type": "stage",
            "stage": "writing_packet_v3",
            "label": "Writing packet ready",
            "anchor_ids": writing_packet.get("anchor_ids") or [],
            "coverage_warnings": list(writing_packet.get("coverage_warnings") or [])[:4],
            "selected_profile_ids": list(writing_packet.get("selected_profile_ids") or [])[:8],
            "axis_source_map": dict(writing_packet.get("axis_source_map") or {}),
        },
        {
            "type": "stage",
            "stage": "packet_critic",
            "label": "Packet critic completed",
            "rounds": len(packet_critic_rounds),
            "verdict": str((packet_critic_rounds[-1] or {}).get("verdict") or "") if packet_critic_rounds else "",
        },
        {
            "type": "stage",
            "stage": "blueprint_v3",
            "label": "Blueprint ready",
            "anchor_ids": blueprint.get("anchor_ids") or [],
            "paragraph_map": list(blueprint.get("paragraph_map") or [])[:6],
        },
        {
            "type": "stage",
            "stage": "draft_v3",
            "label": "First draft completed",
        },
    ]
    for round_payload in revision_rounds:
        blocks.append(
            {
                "type": "revision_round",
                "round": round_payload.get("round"),
                "stage": round_payload.get("stage"),
                "revision_action": round_payload.get("revision_action"),
                "word_count": round_payload.get("word_count"),
                "critic_count": len(round_payload.get("critics") or []),
            }
        )
    blocks.append(
        {
            "type": "stage",
            "stage": "revision",
            "label": f"Revision action: {revision_action}",
        }
    )
    return blocks


def _call_writer_json_stage_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    client: OpenAICompatibleClient | None,
    *,
    stage: str,
    label: str,
    messages: list[dict[str, Any]],
    stream_suffix: str | None = None,
) -> dict[str, Any]:
    if not client:
        raise WritingPipelineError(stage, f"{label} requires a configured writing model.")
    last_error: Exception | None = None
    stream_key = self._stream_key(state, stage, suffix=stream_suffix)
    for attempt in range(1, 4):
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind=stage,
            label=label,
            stage=stage,
            stream_key=stream_key,
            render_mode="plain",
        )
        response = None
        started_at = time.perf_counter()
        try:
            response = client.chat_completion_result(
                messages,
                model=client.config.model,
                temperature=0.2,
                max_tokens=3200,
                stream_handler=stream_handler,
            )
            finalize_stream()
            payload = parse_json_response(response.content, fallback=True)
            if not isinstance(payload, dict):
                raise ValueError(f"{stage} did not return a JSON object.")
            self._record_llm_usage(
                state,
                stage=stage,
                label=label,
                stream_key=stream_key,
                attempt=attempt,
                success=True,
                usage=getattr(response, "usage", None),
                duration_ms=int((time.perf_counter() - started_at) * 1000),
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            finalize_stream()
            if response is not None:
                self._record_llm_usage(
                    state,
                    stage=stage,
                    label=label,
                    stream_key=stream_key,
                    attempt=attempt,
                    success=False,
                    usage=getattr(response, "usage", None),
                    duration_ms=int((time.perf_counter() - started_at) * 1000),
                )
            last_error = exc
            if attempt < 3:
                self._emit_live_writer_message(
                    state,
                    message_kind=stage,
                    label=f"{label} retry {attempt + 1}",
                    body=f"{label} retrying after attempt {attempt} failed: {_trim_text(exc, 160)}",
                    stage=stage,
                    stream_key=stream_key,
                    render_mode="plain",
                )
    raise WritingPipelineError(stage, f"{label} failed after 3 attempts: {last_error}")


def _call_writer_text_stage_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    client: OpenAICompatibleClient | None,
    *,
    stage: str,
    label: str,
    messages: list[dict[str, Any]],
    temperature: float,
    stream_suffix: str | None = None,
) -> str:
    if not client:
        raise WritingPipelineError(stage, f"{label} requires a configured writing model.")
    last_error: Exception | None = None
    stream_key = self._stream_key(state, stage, suffix=stream_suffix)
    for attempt in range(1, 4):
        stream_handler, finalize_stream = self._make_stage_stream_handler(
            state,
            message_kind=stage,
            label=label,
            stage=stage,
            stream_key=stream_key,
        )
        response = None
        started_at = time.perf_counter()
        try:
            response = client.chat_completion_result(
                messages,
                model=client.config.model,
                temperature=temperature,
                max_tokens=None,
                stream_handler=stream_handler,
            )
            finalize_stream()
            candidate = _clean_model_text(response.content)
            if not candidate:
                raise ValueError(f"{stage} returned empty text.")
            if _contains_banned_meta(candidate):
                raise ValueError(f"{stage} leaked backstage prompt language.")
            self._record_llm_usage(
                state,
                stage=stage,
                label=label,
                stream_key=stream_key,
                attempt=attempt,
                success=True,
                usage=getattr(response, "usage", None),
                duration_ms=int((time.perf_counter() - started_at) * 1000),
            )
            return _light_trim_to_word_count(candidate, state.target_word_count)
        except Exception as exc:  # noqa: BLE001
            finalize_stream()
            if response is not None:
                self._record_llm_usage(
                    state,
                    stage=stage,
                    label=label,
                    stream_key=stream_key,
                    attempt=attempt,
                    success=False,
                    usage=getattr(response, "usage", None),
                    duration_ms=int((time.perf_counter() - started_at) * 1000),
                )
            last_error = exc
            if attempt < 3:
                self._emit_live_writer_message(
                    state,
                    message_kind=stage,
                    label=f"{label} retry {attempt + 1}",
                    body=f"{label} retrying after attempt {attempt} failed: {_trim_text(exc, 160)}",
                    stage=stage,
                    stream_key=stream_key,
                    render_mode="plain",
                )
    raise WritingPipelineError(stage, f"{label} failed after 3 attempts: {last_error}")

build_candidate_shortlist = _build_candidate_shortlist_v3
build_generation_packet = _build_generation_packet_v3
build_profile_index = _build_profile_index_v3
normalize_writing_packet = _normalize_writing_packet_v3

__all__ = [
    "_load_stone_profiles_v3",
    "_load_v3_asset_payload",
    "_build_source_anchors_v3",
    "_build_analysis_prompt_text_v3",
    "_build_generation_packet_v3",
    "_extract_analysis_evidence_ids_v3",
    "_compact_analysis_fewshots_v3",
    "_build_latest_analysis_v3",
    "_build_profile_index_v3",
    "_build_coverage_warnings_v3",
    "_score_profile_slice_for_request_v3",
    "_summarize_profile_selection_v3",
    "_select_profile_slices_for_request_v3",
    "_build_source_map_v3",
    "_compact_analysis_summary_for_prompt_v3",
    "_build_axis_source_map_v3",
    "_build_writing_packet_shell_v3",
    "_v3_keyword_units",
    "_resolve_length_band_v3",
    "_normalize_request_adapter_v3",
    "_score_v3_shortlist_candidate",
    "_build_candidate_shortlist_v3",
    "_compact_shortlist_for_prompt_v3",
    "_normalize_rerank_v3",
    "_selected_anchor_records_v3",
    "_build_selected_sample_style_context_v3",
    "_build_local_sample_packet_context_v3",
    "_normalize_style_packet_v3",
    "_normalize_blueprint_v3",
    "_normalize_blueprint_axis_map_v3",
    "_default_paragraph_map_v3",
    "_normalize_blueprint_packet_v3",
    "_normalize_v3_critic_payload",
    "_build_v3_author_floor",
    "_build_v3_draft_guardrails",
    "_build_v3_line_edit_brief",
    "_revision_action_v3",
    "_stone_json_chinese_instruction",
    "_STONE_BODY_CHINESE_ONLY",
    "_render_request_adapter_v3",
    "_render_profile_selection_v3",
    "_render_candidate_shortlist_v3",
    "_render_rerank_v3",
    "_render_style_packet_v3",
    "_normalize_writing_packet_v3",
    "_render_writing_packet_v3",
    "_render_blueprint_v3",
    "_collect_v3_trace_anchor_ids",
    "_build_trace_blocks_v3",
    "_call_writer_json_stage_v3",
    "_call_writer_text_stage_v3",
    "build_candidate_shortlist",
    "build_generation_packet",
    "build_profile_index",
    "normalize_writing_packet",
]

