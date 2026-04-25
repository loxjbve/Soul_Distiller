from __future__ import annotations

from collections import Counter
from typing import Any

from app.service.common.subagents.base import AgentResult, AgentRunContext
from app.service.common.subagents.markdown_runtime import MarkdownAgentSpec
from app.service.common.subagents.registry import ToolBinding


def register_stone_behaviors(registry) -> None:
    registry.register("corpus_overview", _corpus_overview)
    registry.register("profile_selection", _profile_selection)
    registry.register("facet_analysis", _facet_analysis)
    registry.register("writing_planner", _writing_planner)
    registry.register("drafter", _drafter)
    registry.register("critic", _critic)


def _corpus_overview(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profiles = list(tools["list_profiles"].invoke({"profiles": context.payload.get("profiles")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    motif_counter = Counter()
    for profile in profiles:
        bank = dict(profile.get("motif_and_scene_bank") or {})
        motif_counter.update(bank.get("motif_tags") or [])
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "profile_count": len(profiles),
            "top_motifs": [item for item, _count in motif_counter.most_common(8)],
        },
    )


def _profile_selection(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profiles = list(tools["list_profiles"].invoke({"profiles": context.payload.get("profiles")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    limit = int(context.payload.get("profile_limit") or 8)
    selected = profiles[: max(1, limit)]
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "selected_document_ids": [item.get("document_id") for item in selected if item.get("document_id")],
            "selected_count": len(selected),
        },
    )


def _facet_analysis(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profiles = list(tools["list_profiles"].invoke({"profiles": context.payload.get("profiles")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    facet_key = str(context.payload.get("facet_key") or "voice_signature")
    evidence = []
    for profile in profiles[:4]:
        anchors = dict(profile.get("anchor_windows") or {})
        signature_lines = list(anchors.get("signature_lines") or [])
        if signature_lines:
            evidence.append({"document_id": profile.get("document_id"), "quote": signature_lines[0]})
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "facet_key": facet_key,
            "evidence": evidence,
        },
    )


def _writing_planner(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    documents = list(tools["list_documents"].invoke({"documents": context.payload.get("documents")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "topic": str(context.payload.get("topic") or "").strip(),
            "document_count": len(documents),
            "target_word_count": int(context.payload.get("target_word_count") or 800),
        },
    )


def _drafter(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profiles = list(tools["list_profiles"].invoke({"profiles": context.payload.get("profiles")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "draft_ready": bool(profiles),
            "selected_profile_count": len(profiles[:6]),
        },
    )


def _critic(
    spec: MarkdownAgentSpec,
    context: AgentRunContext,
    tools: dict[str, ToolBinding],
) -> AgentResult:
    profiles = list(tools["list_profiles"].invoke({"profiles": context.payload.get("profiles")}) or [])
    summary = spec.render_summary(context, tools).text
    task = spec.render_task(context, tools).text
    return AgentResult(
        agent_name=spec.name,
        payload={
            "summary": summary,
            "task": task,
            "critic_count": 3,
            "grounding_required": bool(profiles),
        },
    )
