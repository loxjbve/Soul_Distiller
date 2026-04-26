from collections import Counter
from typing import Any

from app.agents.base import AgentResult, AgentRunContext
from app.agents.markdown_runtime import MarkdownAgentSpec
from app.agents.registry import ToolBinding


def register_stone_behaviors(registry) -> None:
    registry.register("corpus_overview", _corpus_overview)
    registry.register("profile_selection", _profile_selection)
    registry.register("facet_analysis", _facet_analysis)
    registry.register("packet_composer", _packet_composer)
    registry.register("writing_planner", _writing_planner)
    registry.register("drafter", _drafter)
    registry.register("critic", _critic)


def _summary_and_task(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> tuple[str, str]:
    return spec.render_summary(context, tools).text, spec.render_task(context, tools).text


def _coverage_warnings(context: AgentRunContext) -> list[str]:
    warnings = context.payload.get("coverage_warnings")
    if isinstance(warnings, list):
        return [str(item).strip() for item in warnings if str(item).strip()][:8]
    packet = context.payload.get("writing_packet")
    if isinstance(packet, dict):
        return [str(item).strip() for item in list(packet.get("coverage_warnings") or []) if str(item).strip()][:8]
    return []


def _topic(context: AgentRunContext) -> str:
    return str(context.payload.get("topic") or "").strip()


def _target_word_count(context: AgentRunContext) -> int:
    return int(context.payload.get("target_word_count") or 800)


def _corpus_overview(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profile_slices = list(tools["list_profile_slices"].invoke(context.payload) or [])
    profile_index = dict(tools["get_profile_index"].invoke(context.payload) or {})
    analysis_facets = list(tools["get_analysis_facets"].invoke(context.payload) or [])
    summary, task = _summary_and_task(spec, context, tools)

    motif_counter = Counter()
    family_counter = Counter()
    for profile in profile_slices:
        motif_counter.update(str(item).strip() for item in list(profile.get("motif_tags") or []) if str(item).strip())
        family_counter.update([str(profile.get("prototype_family") or "").strip() or "unknown"])

    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "corpus_summary": {
                "profile_count": int(profile_index.get("profile_count") or len(profile_slices)),
                "sampled_profile_count": len(profile_slices),
                "analysis_facet_count": len(analysis_facets),
                "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
            },
            "top_motifs": [item for item, _count in motif_counter.most_common(8)],
            "top_families": [item for item, _count in family_counter.most_common(6)],
            "coverage_warnings": _coverage_warnings(context),
        },
    )


def _profile_selection(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profile_slices = list(tools["list_profile_slices"].invoke(context.payload) or [])
    profile_index = dict(tools["get_profile_index"].invoke(context.payload) or {})
    summary, task = _summary_and_task(spec, context, tools)
    limit = max(1, int(context.payload.get("profile_limit") or 8))

    selected_ids: list[str] = []
    family_seen: set[str] = set()
    for profile in profile_slices:
        document_id = str(profile.get("document_id") or "").strip()
        if not document_id:
            continue
        family = str(profile.get("prototype_family") or "").strip() or "unknown"
        if family not in family_seen or len(selected_ids) < min(limit, 4):
            selected_ids.append(document_id)
            family_seen.add(family)
        if len(selected_ids) >= limit:
            break
    if not selected_ids:
        selected_ids = [str(item).strip() for item in list(profile_index.get("selected_profile_ids") or []) if str(item).strip()][:limit]

    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "selected_profile_ids": selected_ids,
            "selected_count": len(selected_ids),
            "selection_policy": dict(profile_index.get("selection_policy") or {}),
            "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
            "coverage_warnings": _coverage_warnings(context),
        },
    )


def _facet_analysis(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    analysis_facets = list(tools["get_analysis_facets"].invoke(context.payload) or [])
    summary, task = _summary_and_task(spec, context, tools)
    axis_source_map: dict[str, Any] = {}
    for facet in analysis_facets:
        if not isinstance(facet, dict):
            continue
        facet_key = str(facet.get("facet_key") or "").strip()
        if not facet_key:
            continue
        axis_source_map[facet_key] = {
            "label": str(facet.get("label") or "").strip(),
            "summary": str(facet.get("summary") or "").strip(),
            "confidence": float(facet.get("confidence") or 0.0),
            "evidence_ids": list(facet.get("evidence_ids") or [])[:6],
            "anchor_ids": list(facet.get("anchor_ids") or [])[:4],
        }

    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "analysis_ready": bool((context.payload.get("analysis_summary") or {}).get("analysis_ready")),
            "axis_source_map": axis_source_map,
            "analysis_facet_count": len(axis_source_map),
            "coverage_warnings": _coverage_warnings(context),
        },
    )


def _packet_composer(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    summary, task = _summary_and_task(spec, context, tools)
    profile_index = dict(tools["get_profile_index"].invoke(context.payload) or {})
    writing_guide = dict(tools["get_writing_guide"].invoke(context.payload) or {})
    author_model = dict(tools["get_author_model"].invoke(context.payload) or {})
    prototype_index = dict(tools["get_prototype_index"].invoke(context.payload) or {})
    pipeline_results = dict(context.payload.get("pipeline_results") or {})
    selection = dict(pipeline_results.get("profile_selection") or {})
    facet_analysis = dict(pipeline_results.get("facet_analysis") or {})
    packet = dict(tools["get_writing_packet"].invoke(context.payload) or {})

    writing_packet = {
        **packet,
        "packet_version": "v3",
        "packet_kind": "writing_packet_v3",
        "profile_index": {
            "profile_count": int(profile_index.get("profile_count") or 0),
            "sampled_profile_count": int(profile_index.get("sampled_profile_count") or 0),
            "sparse_profile_mode": bool(profile_index.get("sparse_profile_mode")),
            "top_families": list(profile_index.get("top_families") or [])[:8],
        },
        "selected_profile_ids": list(selection.get("selected_profile_ids") or packet.get("selected_profile_ids") or [])[:24],
        "writing_guide": writing_guide,
        "axis_source_map": dict(facet_analysis.get("axis_source_map") or packet.get("axis_source_map") or {}),
        "coverage_warnings": _coverage_warnings(context),
        "asset_readiness": {
            "author_model_ready": bool(author_model),
            "prototype_index_ready": bool(prototype_index),
        },
    }

    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "writing_packet_v3": writing_packet,
        },
    )


def _writing_planner(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    summary, task = _summary_and_task(spec, context, tools)
    documents = list(tools["list_documents"].invoke(context.payload) or [])
    packet = dict(tools["get_writing_packet"].invoke(context.payload) or {})
    axis_source_map = dict(packet.get("axis_source_map") or {})
    paragraph_count = 3 if _target_word_count(context) <= 500 else 4 if _target_word_count(context) <= 900 else 5
    axis_keys = list(axis_source_map.keys())
    paragraph_map = []
    for index in range(paragraph_count):
        role = "opening" if index == 0 else "closing" if index == paragraph_count - 1 else "development"
        paragraph_map.append(
            {
                "paragraph_index": index + 1,
                "role": role,
                "objective": f"{role} paragraph for {_topic(context) or 'the assigned topic'}",
                "axis_keys": axis_keys[index::paragraph_count][:3] or axis_keys[:2],
                "anchor_ids": list(packet.get("anchor_ids") or [])[index::paragraph_count][:3] or list(packet.get("anchor_ids") or [])[:2],
            }
        )

    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "topic": _topic(context),
            "document_count": len(documents),
            "target_word_count": _target_word_count(context),
            "paragraph_count": paragraph_count,
            "axis_map": {
                axis_key: {
                    "goal": str((axis_source_map.get(axis_key) or {}).get("summary") or "").strip(),
                    "confidence": float((axis_source_map.get(axis_key) or {}).get("confidence") or 0.0),
                }
                for axis_key in axis_keys
            },
            "paragraph_map": paragraph_map,
            "coverage_warnings": _coverage_warnings(context),
        },
    )


def _drafter(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    summary, task = _summary_and_task(spec, context, tools)
    packet = dict(tools["get_writing_packet"].invoke(context.payload) or {})
    planner = dict((context.payload.get("pipeline_results") or {}).get("writing_planner") or {})
    selected_profile_ids = list(packet.get("selected_profile_ids") or [])
    coverage_warnings = _coverage_warnings(context)
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "draft_ready": bool(packet and packet.get("packet_kind") == "writing_packet_v3"),
            "packet_kind": packet.get("packet_kind"),
            "selected_profile_count": len(selected_profile_ids),
            "paragraph_map": list(planner.get("paragraph_map") or [])[:6],
            "binding_constraints": {
                "anchor_ids": list(packet.get("anchor_ids") or [])[:8],
                "coverage_warnings": coverage_warnings,
                "sparse_profile_mode": bool(packet.get("sparse_profile_mode")),
            },
        },
    )


def _critic(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    summary, task = _summary_and_task(spec, context, tools)
    packet = dict(tools["get_writing_packet"].invoke(context.payload) or {})
    dimensions = [
        "feature_density",
        "cross_domain_generalization",
        "rhythm_entropy",
        "extreme_state_handling",
        "ending_landing",
    ]
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "critic_dimensions": dimensions,
            "grounding_required": bool(packet.get("anchor_ids")),
            "anchor_ids": list(packet.get("anchor_ids") or [])[:8],
            "coverage_warnings": _coverage_warnings(context),
        },
    )
