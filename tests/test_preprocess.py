from __future__ import annotations

import io
import json

from app.service.common.llm.client import OpenAICompatibleClient
from app.service.common.tools.workspace import build_tool_schemas
from app.schemas import LLMToolCall, ServiceConfig, ToolRoundResult


def _create_project_with_doc(client):
    project = client.post("/api/projects", json={"name": "Workspace"}).json()
    project_id = project["id"]
    upload = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Alpha project notes about tea and travel."), "text/plain")},
    )
    document_id = upload.json()["documents"][0]["id"]
    return project_id, document_id


def test_preprocess_session_crud_and_fallback_stream(client):
    project_id, _ = _create_project_with_doc(client)

    session_payload = client.post(
        f"/api/projects/{project_id}/preprocess/sessions",
        json={"title": "Workspace Session"},
    ).json()
    session_id = session_payload["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/preprocess/sessions/{session_id}/messages",
        json={"message": '@memo.txt summarize the file'},
    ).json()
    stream_id = message_payload["stream_id"]

    stream_response = client.get(
        f"/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}"
    )
    body = stream_response.text
    assert stream_response.status_code == 200
    assert "event: status" in body
    assert "event: assistant_done" in body

    session_detail = client.get(f"/api/projects/{project_id}/preprocess/sessions/{session_id}").json()
    assert session_detail["turns"][-1]["role"] == "assistant"
    assert session_detail["turns"][-1]["trace"]["resolved_mentions"][0]["filename"] == "memo.txt"


def test_preprocess_handles_ambiguous_mentions(client):
    project = client.post("/api/projects", json={"name": "Ambiguous"}).json()
    project_id = project["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files=[
            ("files", ("memo-one.txt", io.BytesIO(b"first"), "text/plain")),
            ("files", ("memo-two.txt", io.BytesIO(b"second"), "text/plain")),
        ],
    )
    session_id = client.post(
        f"/api/projects/{project_id}/preprocess/sessions",
        json={"title": "Ambiguous Session"},
    ).json()["id"]

    stream_id = client.post(
        f"/api/projects/{project_id}/preprocess/sessions/{session_id}/messages",
        json={"message": "@memo summarize"},
    ).json()["stream_id"]
    client.get(f"/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}")

    session_detail = client.get(f"/api/projects/{project_id}/preprocess/sessions/{session_id}").json()
    assert "多个文件" in session_detail["turns"][-1]["content"]


def test_preprocess_tool_loop_can_generate_artifact(client, app, monkeypatch):
    project_id, document_id = _create_project_with_doc(client)
    client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai",
            "api_key": "sk-test",
            "model": "gpt-4.1-mini",
            "api_mode": "responses",
        },
        follow_redirects=False,
    )

    rounds = [
        ToolRoundResult(
            content="",
            model="gpt-4.1-mini",
            usage={"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
            tool_calls=[
                LLMToolCall(
                    id="tool-1",
                    name="run_python_transform",
                    arguments_json=json.dumps(
                        {
                            "intent": "Write summary file",
                            "python_code": (
                                "from pathlib import Path\n"
                                "out = Path('outputs') / 'summary.txt'\n"
                                "out.write_text('summary artifact', encoding='utf-8')\n"
                            ),
                            "input_document_ids": [document_id],
                            "expected_output_files": ["summary.txt"],
                        }
                    ),
                    arguments={
                        "intent": "Write summary file",
                        "python_code": (
                            "from pathlib import Path\n"
                            "out = Path('outputs') / 'summary.txt'\n"
                            "out.write_text('summary artifact', encoding='utf-8')\n"
                        ),
                        "input_document_ids": [document_id],
                        "expected_output_files": ["summary.txt"],
                    },
                )
            ],
            provider_response_id="resp_1",
        ),
        ToolRoundResult(
            content="Created `summary.txt` for the session.",
            model="gpt-4.1-mini",
            usage={"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14},
            tool_calls=[],
            provider_response_id="resp_2",
        ),
    ]

    def fake_tool_round(self, messages, tools, **kwargs):
        return rounds.pop(0)

    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    session_id = client.post(
        f"/api/projects/{project_id}/preprocess/sessions",
        json={"title": "Artifact Session"},
    ).json()["id"]
    stream_id = client.post(
        f"/api/projects/{project_id}/preprocess/sessions/{session_id}/messages",
        json={"message": '@memo.txt make a summary artifact'},
    ).json()["stream_id"]
    response = client.get(f"/api/projects/{project_id}/preprocess/sessions/{session_id}/streams/{stream_id}")
    assert "event: tool_call" in response.text
    assert "event: tool_result" in response.text

    detail = client.get(f"/api/projects/{project_id}/preprocess/sessions/{session_id}").json()
    assert detail["artifacts"]
    artifact = detail["artifacts"][0]
    download = client.get(artifact["download_url"])
    assert download.status_code == 200
    assert b"summary artifact" in download.content


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
