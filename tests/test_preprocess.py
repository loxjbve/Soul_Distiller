from __future__ import annotations

import io
import json

from app.service.common.llm.client import OpenAICompatibleClient
from app.service.common.tools import build_tool_schemas
from app.schemas import ServiceConfig


def _create_project_with_doc(client, *, mode: str = "group"):
    project = client.post("/api/projects", json={"name": "Workspace", "mode": mode}).json()
    project_id = project["id"]
    upload = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Alpha project notes about tea and travel."), "text/plain")},
    )
    document_id = upload.json()["documents"][0]["id"]
    return project_id, document_id


def test_preprocess_routes_are_disabled_for_single_and_group_projects(client):
    for mode in ("single", "group"):
        project_id, _ = _create_project_with_doc(client, mode=mode)

        page_response = client.get(f"/projects/{project_id}/preprocess")
        assert page_response.status_code == 404

        session_response = client.post(
            f"/api/projects/{project_id}/preprocess/sessions",
            json={"title": "Workspace Session"},
        )
        assert session_response.status_code == 404


def test_document_mentions_search_still_works_without_preprocess_agent(client):
    project_id, _ = _create_project_with_doc(client, mode="group")
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("guide.txt", io.BytesIO(b"A second memo about travel and tea."), "text/plain")},
    )

    response = client.get(f"/api/projects/{project_id}/documents/mentions", params={"q": "memo"})
    assert response.status_code == 200
    payload = response.json()
    assert any(item["filename"] == "memo.txt" for item in payload["items"])


def test_tool_round_uses_responses_payload(monkeypatch):
    captured = {}

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        captured["path"] = path
        captured["payload"] = payload
        data = {
            "id": "resp_1",
            "model": "demo-model",
            "output": [{"type": "message", "content": [{"type": "output_text", "text": "ok"}]}],
            "usage": {"input_tokens": 5, "output_tokens": 3, "total_tokens": 8},
        }
        return data, {"url": "https://example.com/v1/responses", "status_code": 200, "response_text": json.dumps(data)}

    monkeypatch.setattr(OpenAICompatibleClient, "_post_json_with_meta", fake_post_json_with_meta)
    client = OpenAICompatibleClient(
        ServiceConfig(base_url="https://example.com/v1", api_key="sk-test", model="demo-model", api_mode="responses")
    )
    result = client.tool_round([{"role": "user", "content": "hello"}], build_tool_schemas())
    assert result.content == "ok"
    assert captured["path"] == "/responses"
    assert "input" in captured["payload"]
    assert "tools" in captured["payload"]


def test_tool_round_uses_chat_completions_payload(monkeypatch):
    captured = {}

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        captured["path"] = path
        captured["payload"] = payload
        data = {
            "id": "chatcmpl_1",
            "model": "demo-model",
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "list_project_documents", "arguments": "{}"},
                            }
                        ],
                    }
                }
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        }
        return data, {"url": "https://example.com/v1/chat/completions", "status_code": 200, "response_text": json.dumps(data)}

    monkeypatch.setattr(OpenAICompatibleClient, "_post_json_with_meta", fake_post_json_with_meta)
    client = OpenAICompatibleClient(
        ServiceConfig(base_url="https://example.com/v1", api_key="sk-test", model="demo-model", api_mode="chat_completions")
    )
    result = client.tool_round([{"role": "user", "content": "hello"}], build_tool_schemas())
    assert result.tool_calls[0].name == "list_project_documents"
    assert captured["path"] == "/chat/completions"
    assert "messages" in captured["payload"]
    assert "tools" in captured["payload"]
