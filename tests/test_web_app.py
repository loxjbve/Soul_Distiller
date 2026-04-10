from __future__ import annotations

import io
import time

from app.storage import repository


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
    analysis_payload = analyze_response.json()
    run_id = analysis_payload["id"]
    deadline = time.time() + 5
    while analysis_payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        analysis_payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    assert analysis_payload["status"] in {"completed", "partial_failed"}
    assert len(analysis_payload["facets"]) == 6
    assert analysis_payload["summary"]["target_role"] == "Alice 本人"
    assert "first-person" in analysis_payload["summary"]["analysis_context"]
    assert analysis_payload["events"]

    skill_response = client.post(f"/api/projects/{project_id}/skills/generate")
    assert skill_response.status_code == 200
    draft_payload = skill_response.json()
    draft_id = draft_payload["id"]
    assert "Skill" in draft_payload["markdown_text"]
    assert "Alice 本人" in draft_payload["markdown_text"]

    publish_response = client.post(f"/api/projects/{project_id}/skills/{draft_id}/publish")
    assert publish_response.status_code == 200
    assert publish_response.json()["version_number"] == 1

    chat_response = client.post(
        f"/api/projects/{project_id}/playground/chat",
        json={"message": "她平时怎么说话？"},
    )
    assert chat_response.status_code == 200
    chat_payload = chat_response.json()
    assert chat_payload["trace"]["skill_version_number"] == 1
    assert chat_payload["trace"]["retrieval_mode"] == "lexical"
    assert chat_payload["response"]

    with app.state.db.session() as session:
        version = repository.get_latest_skill_version(session, project_id)
        assert version is not None


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
