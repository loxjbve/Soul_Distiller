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

def _resolve_analysis_bundle_v3(self: WritingAgentService, session, project_id: str) -> StoneWritingAnalysisBundle:
    from app.analysis.stone_v3 import STONE_V3_PROFILE_KEY

    project = repository.get_project(session, project_id)
    if not project:
        raise ValueError("Project not found.")
    preprocess_run = get_latest_usable_stone_preprocess_run(
        session,
        project_id,
        profile_key=STONE_V3_PROFILE_KEY,
    )
    if preprocess_run:
        stone_profiles = _load_stone_profiles_v3(session, project_id)
        if not stone_profiles:
            raise ValueError("No Stone v3 profiles are available yet. Run Stone preprocess first.")
        author_model = _load_v3_asset_payload(session, project_id, asset_kind="stone_author_model_v3")
        prototype_index = _load_v3_asset_payload(session, project_id, asset_kind="stone_prototype_index_v3")
        if not author_model or not prototype_index:
            raise ValueError("Stone v3 baseline assets are incomplete. Generate Stone Author Model V3 and Stone Prototype Index V3 first.")

        analysis_summary = _build_latest_analysis_v3(session, project_id)
        profile_index, profile_slices, selected_profile_ids = _build_profile_index_v3(stone_profiles)
        source_anchors = _build_source_anchors_v3(prototype_index)
        version_label = (
            f"preprocess {preprocess_run.created_at.isoformat(timespec='minutes')}"
            if preprocess_run.created_at
            else "latest"
        )
        writing_guide = build_writing_guide_payload_from_facets(
            project_name=project.name,
            target_role=project.name,
            analysis_context="stone_v3_preprocess",
            summary_by_key=dict(analysis_summary.get("summary_by_key") or {}),
            evidence_by_key=dict(analysis_summary.get("evidence_by_key") or {}),
            stone_profiles=profile_slices,
        )
        coverage_warnings = _build_coverage_warnings_v3(
            analysis_summary=analysis_summary,
            profile_index=profile_index,
            author_model=author_model,
            prototype_index=prototype_index,
        )
        bundle = StoneWritingAnalysisBundle(
            run_id=preprocess_run.id,
            source="stone_v3_baseline",
            version_label=version_label,
            target_role=project.name,
            analysis_context="stone_v3_preprocess",
            facets=list(analysis_summary.get("facets") or []),
            prompt_text="",
            analysis_summary=analysis_summary,
            analysis_ready=bool(analysis_summary.get("analysis_ready")),
            writing_guide=writing_guide,
            guide_source="analysis_run_v3" if analysis_summary.get("run_id") else "derived",
            stone_profiles=stone_profiles,
            profile_index=profile_index,
            profile_slices=profile_slices,
            selected_profile_ids=selected_profile_ids,
            source_anchors=source_anchors,
            coverage_warnings=coverage_warnings,
            author_model=author_model,
            prototype_index=prototype_index,
            short_text_clusters=[],
        )
        bundle.prompt_text = _build_analysis_prompt_text_v3(bundle)
        bundle.generation_packet = _build_generation_packet_v3(bundle)
        bundle.writing_packet = _build_writing_packet_shell_v3(
            analysis_summary=analysis_summary,
            profile_index=profile_index,
            writing_guide=writing_guide,
            author_model=author_model,
            prototype_index=prototype_index,
        )
        return bundle

    raise ValueError("No Stone v3 baseline is available yet. Run Stone preprocess and generate the v3 baseline first.")

resolve_analysis_bundle = _resolve_analysis_bundle_v3

__all__ = [
    "_resolve_analysis_bundle_v3",
    "resolve_analysis_bundle",
]
