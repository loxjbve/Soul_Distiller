from __future__ import annotations

import re
from pathlib import Path
from uuid import uuid4

from app.service import ServiceRegistry
from app.service.common.subagents import load_markdown_agent_spec
from app.service.common.subagents.base import AgentRunContext
from app.service.stone.subagent_runner import StoneAgentOrchestrator
from app.core import AppContainer
from app.core.config import AppConfig
from app.db import Database
from app.db.repositories import analysis as analysis_repository
from app.service.common.llm.gateway import LLMGateway
from app.storage import repository as legacy_repository
from app.api.router import router as api_router
from app.web.routes import router as legacy_router
from app.schemas import ServiceConfig


LEGACY_IMPORT_PATTERN = re.compile(
    rb"\b(?:from|import)\s+app\.(agents|analysis|llm|pipeline|preprocess|stone_preprocess|telegram_preprocess)\b"
)


def _test_root_dir() -> Path:
    root = Path("data") / f"arch-refactor-{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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

    monkeypatch.setattr("app.service.common.llm.gateway.OpenAICompatibleClient", FakeClient)
    gateway = LLMGateway(ServiceConfig(base_url="https://example.com/v1", api_key="test", model="demo-model"))
    assert gateway.resolve_model() == "demo-model"
    assert calls == [("init", "demo-model"), ("resolve_model", None)]


def test_service_registry_routes_all_modes():
    registry = ServiceRegistry.build(
        single_preprocess="single-preprocess",
        group_preprocess="group-preprocess",
        telegram_preprocess="telegram-preprocess",
        stone_preprocess="stone-preprocess",
        single_analysis="single-analysis",
        group_analysis="group-analysis",
        telegram_analysis="telegram-analysis",
        stone_analysis="stone-analysis",
        single_assets="single-assets",
        group_assets="group-assets",
        telegram_assets="telegram-assets",
        stone_assets="stone-assets",
        stone_writing="stone-writing",
    )

    assert registry.for_mode("single").preprocess == "single-preprocess"
    assert registry.for_mode("group").analysis == "group-analysis"
    assert registry.for_mode("telegram").assets == "telegram-assets"
    assert registry.for_mode("stone").writing == "stone-writing"
    assert registry.for_mode(None) is registry.group
    assert registry.for_mode("unexpected-mode") is registry.group


def test_mode_subagent_markdown_specs_have_required_frontmatter():
    service_root = _repo_root() / "app" / "service"
    spec_paths = sorted(service_root.glob("*/subagents/*/agent.md"))

    assert spec_paths
    assert {path.parts[-4] for path in spec_paths} == {"single", "group", "telegram", "stone"}

    for path in spec_paths:
        spec = load_markdown_agent_spec(path)
        assert spec.name
        assert spec.summary
        assert spec.task
        assert spec.runtime in {"completion", "tool_loop"}
        assert spec.output_type in {"json", "markdown", "text"}
        assert spec.toolset
        assert spec.max_rounds >= 1


def test_no_legacy_implementation_imports_remain():
    repo_root = _repo_root()
    matches: list[str] = []

    for base in (repo_root / "app", repo_root / "tests"):
        for path in base.rglob("*.py"):
            if LEGACY_IMPORT_PATTERN.search(path.read_bytes()):
                matches.append(path.relative_to(repo_root).as_posix())

    assert matches == []


def test_create_app_attaches_container_state():
    config = AppConfig(root_dir=_test_root_dir())
    config.ensure_dirs()
    container = AppContainer.build(config)
    try:
        from fastapi import FastAPI

        app = FastAPI()
        container.attach_to_app(app)
        assert app.state.config is container.config
        assert app.state.db is container.db
        assert app.state.services is container.services
        assert app.state.services.stone_agents is container.stone_agents
        assert app.state.services.telegram_agents is container.telegram_agents
        assert app.state.services.preprocess_agents is container.preprocess_agents
        assert app.state.services.for_mode("single").preprocess is container.preprocess_service
        assert app.state.services.for_mode("telegram").preprocess is container.telegram_preprocess_manager
        assert app.state.services.for_mode("stone").writing is container.writing_service
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
