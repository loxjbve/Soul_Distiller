from __future__ import annotations

import io
import json
import time
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from app.config import AppConfig
from app.llm.client import OpenAICompatibleClient
from app.main import create_app
from app.models import TextChunk
from app.storage import repository


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 5.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _wait_for_rechunk(client, project_id: str, task_id: str, *, timeout_s: float = 8.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    return payload


def test_end_to_end_project_flow(client, app):
    create_response = client.post("/api/projects", json={"name": "Alice", "description": "Writer persona"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Alice writes concise diary entries about travel and tea."), "text/plain")},
    )
    assert upload_response.status_code == 200
    document_payload = upload_response.json()["documents"][0]
    assert document_payload["status"] == "ready"
    document_id = document_payload["id"]

    update_doc_response = client.post(
        f"/api/projects/{project_id}/documents/{document_id}",
        json={
            "title": "Travel diary",
            "source_type": "journal",
            "user_note": "These entries are written in first person and should imitate the author.",
        },
    )
    assert update_doc_response.status_code == 200
    assert update_doc_response.json()["title"] == "Travel diary"
    assert update_doc_response.json()["source_type"] == "journal"

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={
            "target_role": "Alice 本人",
            "analysis_context": "These notes are private journals and travel drafts. Focus on first-person expression.",
        },
    )
    assert analyze_response.status_code == 200
    analysis_payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])
    assert analysis_payload["status"] in {"completed", "partial_failed"}
    assert len(analysis_payload["facets"]) == 10
    assert analysis_payload["summary"]["target_role"] == "Alice 本人"
    assert "first-person" in analysis_payload["summary"]["analysis_context"]
    assert analysis_payload["events"]

    skill_response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "skill"},
    )
    assert skill_response.status_code == 200
    skill_draft = skill_response.json()
    assert skill_draft["asset_kind"] == "skill"
    assert "System Role" in skill_draft["markdown_text"]
    assert "Alice 本人" in skill_draft["markdown_text"]

    report_response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "profile_report"},
    )
    assert report_response.status_code == 200
    report_draft = report_response.json()
    assert report_draft["asset_kind"] == "profile_report"
    assert "全景侧写" in report_draft["markdown_text"]

    publish_response = client.post(f"/api/projects/{project_id}/skills/{skill_draft['id']}/publish")
    assert publish_response.status_code == 200
    assert publish_response.json()["asset_kind"] == "skill"
    assert publish_response.json()["version_number"] == 1

    publish_report_response = client.post(
        f"/api/projects/{project_id}/assets/{report_draft['id']}/publish",
        json={"asset_kind": "profile_report"},
    )
    assert publish_report_response.status_code == 200
    assert publish_report_response.json()["asset_kind"] == "profile_report"
    assert publish_report_response.json()["version_number"] == 1

    chat_response = client.post(
        f"/api/projects/{project_id}/playground/chat",
        json={"message": "她平时怎么说话？"},
    )
    assert chat_response.status_code == 200
    chat_payload = chat_response.json()
    assert chat_payload["trace"]["skill_version_number"] == 1
    assert chat_payload["trace"]["retrieval_mode"] == "lexical"
    assert chat_payload["trace"]["retrieval_trace"]["embedding_configured"] is False
    assert chat_payload["response"]

    with app.state.db.session() as session:
        skill_version = repository.get_latest_skill_version(session, project_id)
        report_version = repository.get_latest_asset_version(session, project_id, asset_kind="profile_report")
        assert skill_version is not None
        assert report_version is not None


def test_analysis_llm_parse_failure_is_logged_and_visible(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Debug"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Debug profile with sharp language and strong opinions."), "text/plain")},
    )
    client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai-compatible",
            "base_url": "https://example.com/v1",
            "api_key": "sk-test",
            "model": "demo-model",
            "api_mode": "responses",
        },
        follow_redirects=False,
    )

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        content = "this is not valid json, but it is the real model text"
        api_payload = {
            "id": "resp_debug",
            "model": "demo-model",
            "output": [{"type": "message", "content": [{"type": "output_text", "text": content}]}],
            "usage": {"input_tokens": 10, "output_tokens": 8, "total_tokens": 18},
        }
        self._append_log(
            {
                "timestamp": "2026-04-10T00:00:00Z",
                "method": "POST",
                "url": f"https://example.com/v1{path}",
                "provider_kind": self.config.provider_kind,
                "api_mode": self.config.api_mode,
                "request_body": payload,
                "status_code": 200,
                "response_text": json.dumps(api_payload, ensure_ascii=False),
                "ok": True,
            }
        )
        return api_payload, {
            "url": f"https://example.com/v1{path}",
            "status_code": 200,
            "response_text": json.dumps(api_payload, ensure_ascii=False),
        }

    def fake_post_stream_text_with_meta(self, path, payload, *, timeout, stream_handler, event_parser):
        del timeout, event_parser
        content = "this is not valid json, but it is the real model text"
        stream_handler(content)
        self._append_log(
            {
                "timestamp": "2026-04-10T00:00:00Z",
                "method": "POST",
                "url": f"https://example.com/v1{path}",
                "provider_kind": self.config.provider_kind,
                "api_mode": self.config.api_mode,
                "request_body": payload,
                "response_text": content,
                "raw_stream": content,
                "ok": True,
                "stream": True,
            }
        )
        return {
            "url": f"https://example.com/v1{path}",
            "response_text": content,
            "raw_stream": content,
            "content": content,
            "response_id": "resp_debug",
            "usage": {"input_tokens": 10, "output_tokens": 8, "total_tokens": 18},
        }

    monkeypatch.setattr(OpenAICompatibleClient, "_post_json_with_meta", fake_post_json_with_meta)
    monkeypatch.setattr(OpenAICompatibleClient, "_post_stream_text_with_meta", fake_post_stream_text_with_meta)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Debug 本人", "analysis_context": "Check logging and parse fallback."},
    )
    analysis_payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])
    assert analysis_payload["status"] == "completed"
    assert analysis_payload["summary"]["llm_failures"] == 10
    assert all(facet["status"] == "completed" for facet in analysis_payload["facets"])
    assert any(event["event_type"] == "llm_response" for event in analysis_payload["events"])
    assert any(
        "this is not valid json" in json.dumps(event["payload"], ensure_ascii=False)
        for event in analysis_payload["events"]
        if event["event_type"] == "llm_response"
    )

    log_text = app.state.config.llm_log_path.read_text(encoding="utf-8")
    assert "https://example.com/v1/responses" in log_text
    assert "this is not valid json" in log_text


def test_document_delete_removes_record(client, app):
    project_payload = client.post("/api/projects", json={"name": "Bob"}).json()
    project_id = project_payload["id"]
    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("note.txt", io.BytesIO(b"Bob likes tea."), "text/plain")},
    )
    document_id = upload_response.json()["documents"][0]["id"]

    delete_response = client.post(f"/api/projects/{project_id}/documents/{document_id}/delete")
    assert delete_response.status_code == 200
    assert delete_response.json()["ok"] is True

    with app.state.db.session() as session:
        assert repository.get_document(session, document_id) is None


def test_project_rechunk_task_rebuilds_chunks_and_embeddings(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Rechunk"}).json()
    project_id = project_payload["id"]
    text = ("Alpha notes with long context for chunk rebuild. " * 200).encode("utf-8")
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(text), "text/plain")},
    )
    client.post(
        "/settings/embedding",
        data={
            "provider_kind": "openai",
            "api_key": "sk-test",
            "model": "embed-test",
        },
        follow_redirects=False,
    )

    def fake_embeddings(self, inputs, *, model=None):
        del model
        return [[float(index + 1), float(len(item) % 13), 0.5] for index, item in enumerate(inputs)]

    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fake_embeddings)

    start = client.post(f"/api/projects/{project_id}/rechunk")
    assert start.status_code == 200
    task_id = start.json()["task_id"]

    status = _wait_for_rechunk(client, project_id, task_id)
    assert status["status"] == "completed"
    assert status["document_total"] >= 1
    assert status["chunk_processed"] >= 1
    assert status["embedding_processed"] >= 1

    with app.state.db.session() as session:
        chunks = list(session.scalars(select(TextChunk).where(TextChunk.project_id == project_id)))
        assert chunks
        assert all(len(chunk.content) <= 1800 for chunk in chunks)
        assert any(chunk.embedding_vector for chunk in chunks)


def test_analysis_stream_and_rerun_api(client, app):
    project_payload = client.post("/api/projects", json={"name": "Stream Debug"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Persona notes with repeated habits and boundaries."), "text/plain")},
    )

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Stream the run state."},
    )
    run_id = analyze_response.json()["id"]

    stream_response = client.get(f"/api/projects/{project_id}/analysis/stream", params={"run_id": run_id})
    assert stream_response.status_code == 200
    assert "event: snapshot" in stream_response.text

    rerun_response = client.post(f"/api/projects/{project_id}/analysis/personality/rerun")
    assert rerun_response.status_code == 200
    rerun_payload = rerun_response.json()
    assert rerun_payload["id"] == run_id
    assert len(rerun_payload["facets"]) == 10


def test_analysis_run_survives_single_facet_retrieval_failure(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Retrieval Failure"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Persona notes with enough text for fallback evidence."), "text/plain")},
    )

    engine = app.state.analysis_engine
    original = engine._retrieve_hits

    def flaky_retrieve(session, project_id, facet, **kwargs):
        if facet.key == "personality":
            raise RuntimeError("simulated retrieval failure")
        return original(session, project_id, facet, **kwargs)

    monkeypatch.setattr(engine, "_retrieve_hits", flaky_retrieve)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Keep going even if one facet retrieval fails."},
    )
    payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])

    assert payload["status"] == "partial_failed"
    personality = next(item for item in payload["facets"] if item["facet_key"] == "personality")
    assert personality["status"] == "failed"
    assert any(event["event_type"] == "retrieval" for event in payload["events"])
    assert any(
        event["event_type"] == "retrieval" and "simulated retrieval failure" in str(event["payload"])
        for event in payload["events"]
    )


def test_project_delete_cascades_records_and_files(client, app):
    project_payload = client.post("/api/projects", json={"name": "Delete Me"}).json()
    project_id = project_payload["id"]
    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("note.txt", io.BytesIO(b"Delete test content."), "text/plain")},
    )
    document_id = upload_response.json()["documents"][0]["id"]

    delete_response = client.delete(f"/api/projects/{project_id}")
    assert delete_response.status_code == 200
    assert delete_response.json()["ok"] is True

    with app.state.db.session() as session:
        assert repository.get_project(session, project_id) is None
        assert repository.get_document(session, document_id) is None

    assert not (app.state.config.upload_dir / project_id).exists()
    assert not (app.state.config.assets_dir / project_id).exists()
    assert not (app.state.config.output_dir / project_id).exists()


def test_create_app_recovers_stale_active_runs():
    root_dir = Path(".test-workspaces") / f"stale-run-recovery-{uuid4().hex}"
    root_dir.mkdir(parents=True, exist_ok=False)
    config = AppConfig(root_dir=root_dir)
    first_app = create_app(config)
    try:
        with first_app.state.db.session() as session:
            project = repository.create_project(session, "Recover Me", "test")
            run = repository.create_analysis_run(
                session,
                project_id=project.id,
                status="running",
                summary_json={"current_stage": "running", "current_facet": "personality"},
            )
            run_id = run.id
    finally:
        first_app.state.analysis_runner.shutdown()
        first_app.state.preprocess_service.shutdown()
        first_app.state.rechunk_manager.shutdown()
        first_app.state.db.close()

    second_app = create_app(config)
    try:
        with second_app.state.db.session() as session:
            recovered = repository.get_analysis_run(session, run_id)
            assert recovered is not None
            assert recovered.status == "failed"
            assert recovered.summary_json["current_stage"] == "服务重启，旧的后台任务已终止"
            assert any(
                event.event_type == "lifecycle" and event.payload_json.get("recovered_after_restart")
                for event in recovered.events
            )
    finally:
        second_app.state.analysis_runner.shutdown()
        second_app.state.preprocess_service.shutdown()
        second_app.state.rechunk_manager.shutdown()
        second_app.state.db.close()
        if root_dir.exists():
            import shutil

            shutil.rmtree(root_dir, ignore_errors=True)


def test_settings_accept_official_provider_without_base_url(client, app):
    response = client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai",
            "base_url": "",
            "api_key": "sk-test",
            "model": "gpt-4.1-mini",
            "api_mode": "responses",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    with app.state.db.session() as session:
        config = repository.get_service_config(session, "chat_service")
        assert config is not None
        assert config.provider_kind == "openai"
        assert config.base_url is None
        assert config.model == "gpt-4.1-mini"
        assert config.api_mode == "responses"


def test_custom_provider_requires_base_url(client):
    response = client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai-compatible",
            "base_url": "",
            "api_key": "sk-test",
            "model": "demo-model",
            "api_mode": "chat_completions",
        },
        follow_redirects=False,
    )
    assert response.status_code == 400
