from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from app.agents.base import AgentRunContext
from app.agents.stone.orchestrator import StoneAgentOrchestrator
from app.agents.stone.writing import service as canonical_writing_service
from app.agents.stone.writing_service import WritingAgentService as legacy_writing_service
from app.analysis import engine_helpers as canonical_engine_helpers
from app.analysis import engine_runtime as canonical_engine_runtime
from app.analysis import stone_v3_runtime as canonical_stone_v3_runtime
from app.analysis import synthesizer_runtime as canonical_synthesizer_runtime
from app.core import AppContainer
from app.core.config import AppConfig
from app.db import Database
from app.db import models as canonical_models
from app.db.repositories import analysis as analysis_repository
from app.db.repositories import runtime as repository_runtime
from app.llm import client_runtime as canonical_llm_client_runtime
from app.llm.gateway import LLMGateway
from app.models import Project as legacy_project_model
from app.storage import repository as legacy_repository
from app.api.router import router as api_router
from app.schemas import ServiceConfig
from app.telegram.preprocess.runtime import (
    TelegramPreprocessManager as canonical_telegram_manager,
    TelegramPreprocessWorker as canonical_telegram_worker,
)
from app.telegram_preprocess import TelegramPreprocessManager, TelegramPreprocessWorker
from app.web.routes import router as web_router


def _test_root_dir() -> Path:
    root = Path("data") / f"arch-refactor-{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_api_router_only_contains_api_routes():
    api_paths = {route.path for route in api_router.routes}
    assert "/api/projects" in api_paths
    assert "/api/projects/{project_id}/analysis" in api_paths
    assert "/api/projects/{project_id}/preprocess/runs" in api_paths
    assert "/api/projects/{project_id}/preprocess/sessions" not in api_paths
    assert "/api/projects/{project_id}/documents/mentions" not in api_paths
    assert "/api/settings/models" in api_paths
    assert "/api/projects/{project_id}/assets/generate" in api_paths
    assert "/api/projects/{project_id}/writing/sessions" in api_paths
    assert "/api/projects/{project_id}/playground/chat" in api_paths
    assert all(path.startswith("/api/") for path in api_paths)
    assert all(not route.endpoint.__module__.startswith("app.web.routes") for route in api_router.routes if hasattr(route, "endpoint"))


def test_web_router_only_contains_page_routes():
    web_paths = {route.path for route in web_router.routes}
    assert "/" in web_paths
    assert "/settings" in web_paths
    assert "/projects/{project_id}" in web_paths
    assert "/projects/{project_id}/analysis" in web_paths
    assert "/projects/{project_id}/preprocess" in web_paths
    assert "/projects/{project_id}/writing" in web_paths
    assert all(not path.startswith("/api/") for path in web_paths)
    assert web_router is not api_router


def test_db_package_exports_database_class():
    config = AppConfig(root_dir=_test_root_dir())
    config.ensure_dirs()
    database = Database(config)
    try:
        assert database.engine is not None
        assert callable(database.create_all)
    finally:
        database.close()


def test_repository_facade_reexports_canonical_symbols():
    assert analysis_repository.create_analysis_run is repository_runtime.create_analysis_run
    assert analysis_repository.get_analysis_run is repository_runtime.get_analysis_run
    assert legacy_repository.create_analysis_run is repository_runtime.create_analysis_run
    assert legacy_repository.get_analysis_run is repository_runtime.get_analysis_run
    assert legacy_repository.PROJECT_LIFECYCLE_DELETING == repository_runtime.PROJECT_LIFECYCLE_DELETING


def test_model_facade_reexports_canonical_models():
    assert legacy_project_model is canonical_models.Project
    assert canonical_models.Base.metadata is legacy_project_model.metadata


def test_writing_and_telegram_facades_reexport_canonical_classes():
    assert legacy_writing_service is canonical_writing_service.WritingAgentService
    assert TelegramPreprocessManager is canonical_telegram_manager
    assert TelegramPreprocessWorker is canonical_telegram_worker


def test_analysis_engine_facade_reexports_canonical_runtime_and_helpers():
    import app.analysis.engine as legacy_engine

    assert legacy_engine.AnalysisEngine is canonical_engine_runtime.AnalysisEngine
    assert legacy_engine.AnalysisCancelledError is canonical_engine_runtime.AnalysisCancelledError
    assert legacy_engine.analyze_facet_worker is canonical_engine_runtime.analyze_facet_worker
    assert legacy_engine._normalize_facet_payload is canonical_engine_helpers._normalize_facet_payload
    assert legacy_engine.FACET_EVIDENCE_LIMIT == canonical_engine_helpers.FACET_EVIDENCE_LIMIT


def test_synthesizer_and_stone_v3_facades_reexport_canonical_runtime():
    import app.analysis.stone_v3 as legacy_stone_v3
    import app.analysis.synthesizer as legacy_synthesizer

    assert legacy_synthesizer.AssetSynthesizer is canonical_synthesizer_runtime.AssetSynthesizer
    assert legacy_synthesizer.SkillSynthesizer is canonical_synthesizer_runtime.SkillSynthesizer
    assert legacy_stone_v3.StoneV3BaselineSynthesizer is canonical_stone_v3_runtime.StoneV3BaselineSynthesizer
    assert legacy_stone_v3.STONE_V3_PROFILE_KEY == canonical_stone_v3_runtime.STONE_V3_PROFILE_KEY


def test_llm_client_facade_reexports_canonical_runtime():
    import app.llm.client as legacy_llm_client

    assert legacy_llm_client.OpenAICompatibleClient is canonical_llm_client_runtime.OpenAICompatibleClient
    assert legacy_llm_client.LLMError is canonical_llm_client_runtime.LLMError
    assert legacy_llm_client.parse_json_response is canonical_llm_client_runtime.parse_json_response
    assert legacy_llm_client.OFFICIAL_PROVIDER_BASE_URLS is canonical_llm_client_runtime.OFFICIAL_PROVIDER_BASE_URLS


def test_stone_agent_orchestrator_exposes_v3_subagents():
    orchestrator = StoneAgentOrchestrator()
    context = AgentRunContext(
        project_id="project-1",
        payload={
            "profile_slices": [
                {
                    "document_id": "doc-1",
                    "title": "Doc 1",
                    "motif_tags": ["night", "door"],
                    "prototype_family": "scene_vignette|night_cost",
                }
            ],
            "documents": [{"document_id": "doc-1", "title": "Doc 1"}],
            "profile_index": {
                "profile_count": 128,
                "sampled_profile_count": 1,
                "sparse_profile_mode": True,
                "selected_profile_ids": ["doc-1"],
                "selection_policy": {"method": "family-first"},
            },
            "analysis_summary": {
                "analysis_ready": True,
                "facet_packets": [
                    {
                        "facet_key": "voice_signature",
                        "label": "Voice Signature",
                        "summary": "Restrained, bodily, first-person pressure.",
                        "confidence": 0.92,
                        "evidence_ids": ["evidence:voice:1"],
                        "anchor_ids": ["anchor:doc-1:opening"],
                    }
                ],
            },
            "writing_guide": {"author_snapshot": "restrained and bodily"},
            "author_model": {"author_core": {"voice_summary": "restrained"}},
            "prototype_index": {"documents": [{"document_id": "doc-1", "title": "Doc 1"}]},
            "writing_packet": {
                "packet_kind": "writing_packet_v3",
                "anchor_ids": ["anchor:doc-1:opening"],
                "selected_profile_ids": ["doc-1"],
            },
            "topic": "night food",
            "target_word_count": 900,
            "profile_limit": 8,
        },
    )
    results = orchestrator.run_pipeline(context)
    assert [result.agent_name for result in results] == [
        "corpus_overview",
        "profile_selection",
        "facet_analysis",
        "packet_composer",
        "writing_planner",
        "drafter",
        "critic",
    ]
    assert [agent.spec.path.name for agent in orchestrator.subagents] == ["agent.md"] * 7
    assert all(len(agent.spec.sections) >= 6 for agent in orchestrator.subagents)
    assert all(isinstance(result.payload, dict) for result in results)
    assert "writing_packet_v3" in results[3].payload
    assert results[4].payload["paragraph_count"] >= 3

    first_agent = orchestrator.subagents[0]
    rendered = first_agent.spec.render_document(context, orchestrator.registry.resolve_many(first_agent.tool_names))
    assert "project-1" in rendered.text
    assert "list_profile_slices" in rendered.text
    assert not rendered.missing_placeholders


def test_llm_gateway_wraps_openai_compatible_client(monkeypatch):
    calls: list[tuple[str, object]] = []

    class FakeClient:
        def __init__(self, config, *, log_path=None):
            calls.append(("init", config.model))

        def resolve_model(self):
            calls.append(("resolve_model", None))
            return "demo-model"

    monkeypatch.setattr("app.llm.gateway.OpenAICompatibleClient", FakeClient)
    gateway = LLMGateway(ServiceConfig(base_url="https://example.com/v1", api_key="test", model="demo-model"))
    assert gateway.resolve_model() == "demo-model"
    assert calls == [("init", "demo-model"), ("resolve_model", None)]


def test_create_app_attaches_container_state():
    config = AppConfig(root_dir=_test_root_dir())
    config.ensure_dirs()
    container = AppContainer.build(config)
    try:
        from fastapi import FastAPI

        app = FastAPI()
        container.attach_to_app(app)
        assert app.state.container is container
        assert app.state.db is container.db
        assert app.state.stone_agents is container.stone_agents
        assert app.state.telegram_agents is container.telegram_agents
        assert not hasattr(app.state, "preprocess_agents")
        assert not hasattr(app.state, "preprocess_service")
    finally:
        container.project_deletion_manager.shutdown()
        container.analysis_runner.shutdown()
        container.writing_service.shutdown()
        container.telegram_preprocess_manager.shutdown()
        container.rechunk_manager.shutdown()
        container.ingest_task_manager.shutdown()
        container.vector_store_manager.save_all()
        container.db.close()


def test_stone_v2_asset_generation_is_rejected(client, app):
    with app.state.db.session() as session:
        project = legacy_repository.create_project(session, name="Stone V3 Only", mode="stone")
        project_id = project.id

    response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "stone_author_model_v2"},
    )

    assert response.status_code == 400
    assert "Stone v2" in response.text
    assert "v3" in response.text
