from __future__ import annotations

from app.agents.base import AgentRunContext
from app.agents.stone.orchestrator import StoneAgentOrchestrator
from app.core import AppContainer
from app.core.config import AppConfig
from app.db import Database
from app.db.repositories import analysis as analysis_repository
from app.llm.gateway import LLMGateway
from app.storage import repository as legacy_repository
from app.api.router import router as api_router
from app.web.routes import router as legacy_router
from app.schemas import ServiceConfig
from pathlib import Path
from uuid import uuid4


def _test_root_dir() -> Path:
    root = Path("data") / f"arch-refactor-{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_api_router_composes_legacy_and_split_routes():
    api_paths = {route.path for route in api_router.routes}
    legacy_paths = {route.path for route in legacy_router.routes}
    assert "/api/projects" in api_paths
    assert "/api/projects/{project_id}/analysis" in api_paths
    assert "/api/projects/{project_id}/preprocess/runs" in api_paths
    assert "/api/settings/models" in api_paths
    assert "/api/projects/{project_id}/assets/generate" in api_paths
    assert "/api/projects/{project_id}/writing/sessions" in api_paths
    assert "/api/projects" in legacy_paths
    assert "/api/projects/{project_id}/analysis" in legacy_paths
    assert "/api/projects/{project_id}/preprocess/runs" in legacy_paths
    assert "/api/settings/models" in legacy_paths
    assert "/api/projects/{project_id}/assets/generate" in legacy_paths
    assert "/api/projects/{project_id}/writing/sessions" in legacy_paths
    assert api_router is not legacy_router


def test_db_package_exports_database_class():
    config = AppConfig(root_dir=_test_root_dir())
    config.ensure_dirs()
    database = Database(config)
    try:
        assert database.engine is not None
        assert callable(database.create_all)
    finally:
        database.close()


def test_repository_split_reexports_legacy_functions():
    assert analysis_repository.create_analysis_run is legacy_repository.create_analysis_run
    assert analysis_repository.get_analysis_run is legacy_repository.get_analysis_run


def test_stone_agent_orchestrator_exposes_v3_subagents():
    orchestrator = StoneAgentOrchestrator()
    context = AgentRunContext(
        project_id="project-1",
        payload={
            "profiles": [
                {
                    "document_id": "doc-1",
                    "motif_and_scene_bank": {"motif_tags": ["night", "door"]},
                    "anchor_windows": {"signature_lines": ["A quiet opening."]},
                }
            ],
            "documents": [{"document_id": "doc-1", "title": "Doc 1"}],
            "topic": "night food",
            "target_word_count": 900,
        },
    )
    results = orchestrator.run_pipeline(context)
    assert [result.agent_name for result in results] == [
        "corpus_overview",
        "profile_selection",
        "facet_analysis",
        "writing_planner",
        "drafter",
        "critic",
    ]
    assert [agent.spec.path.name for agent in orchestrator.subagents] == ["agent.md"] * 6
    assert all(len(agent.spec.sections) >= 6 for agent in orchestrator.subagents)
    assert all(isinstance(result.payload, dict) for result in results)

    first_agent = orchestrator.subagents[0]
    rendered = first_agent.spec.render_document(context, orchestrator.registry.resolve_many(first_agent.tool_names))
    assert "project-1" in rendered.text
    assert "list_profiles" in rendered.text
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
        assert app.state.preprocess_agents is container.preprocess_agents
    finally:
        container.project_deletion_manager.shutdown()
        container.analysis_runner.shutdown()
        container.preprocess_service.shutdown()
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
