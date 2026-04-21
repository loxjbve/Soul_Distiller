from __future__ import annotations

import json

from app.analysis.engine import _analyze_with_llm
from app.analysis.facets import FACETS
from app.llm.client import OpenAICompatibleClient
from app.preprocess.tools import build_tool_schemas
from app.schemas import ChatCompletionResult, ServiceConfig


def test_chat_completion_result_omits_responses_token_limit_when_disabled(monkeypatch):
    captured: dict[str, object] = {}

    def fake_post_stream_text_with_meta(self, path, payload, *, timeout, stream_handler, event_parser):
        del timeout, event_parser
        captured["path"] = path
        captured["payload"] = payload
        content = '{"summary":"ok","bullets":[],"confidence":0.8,"evidence":[],"conflicts":[],"notes":""}'
        stream_handler(content)
        return {
            "url": f"https://example.com/v1{path}",
            "response_text": content,
            "raw_stream": f"data: {content}",
            "content": content,
            "response_id": "resp_1",
            "usage": {"input_tokens": 5, "output_tokens": 3, "total_tokens": 8},
        }

    monkeypatch.setattr(OpenAICompatibleClient, "_post_stream_text_with_meta", fake_post_stream_text_with_meta)
    client = OpenAICompatibleClient(
        ServiceConfig(base_url="https://example.com/v1", api_key="sk-test", model="demo-model", api_mode="responses")
    )

    result = client.chat_completion_result(
        [{"role": "user", "content": "hello"}],
        max_tokens=None,
        stream_handler=lambda chunk: None,
    )

    assert result.content
    assert captured["path"] == "/responses"
    assert captured["payload"]["stream"] is True
    assert "max_output_tokens" not in captured["payload"]


def test_chat_completion_result_omits_chat_token_limit_when_disabled(monkeypatch):
    captured: dict[str, object] = {}

    def fake_post_stream_text_with_meta(self, path, payload, *, timeout, stream_handler, event_parser):
        del timeout, event_parser
        captured["path"] = path
        captured["payload"] = payload
        content = '{"summary":"ok","bullets":[],"confidence":0.8,"evidence":[],"conflicts":[],"notes":""}'
        stream_handler(content)
        return {
            "url": f"https://example.com/v1{path}",
            "response_text": content,
            "raw_stream": f"data: {content}",
            "content": content,
            "response_id": "chatcmpl_1",
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        }

    monkeypatch.setattr(OpenAICompatibleClient, "_post_stream_text_with_meta", fake_post_stream_text_with_meta)
    client = OpenAICompatibleClient(
        ServiceConfig(
            base_url="https://example.com/v1",
            api_key="sk-test",
            model="demo-model",
            api_mode="chat_completions",
        )
    )

    result = client.chat_completion_result(
        [{"role": "user", "content": "hello"}],
        max_tokens=None,
        stream_handler=lambda chunk: None,
    )

    assert result.content
    assert captured["path"] == "/chat/completions"
    assert captured["payload"]["stream"] is True
    assert "max_tokens" not in captured["payload"]


def test_tool_round_omits_responses_token_limit_when_disabled(monkeypatch):
    captured: dict[str, object] = {}

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        del timeout
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

    result = client.tool_round([{"role": "user", "content": "hello"}], build_tool_schemas(), max_tokens=None)

    assert result.content == "ok"
    assert captured["path"] == "/responses"
    assert "max_output_tokens" not in captured["payload"]


def test_tool_round_omits_chat_token_limit_when_disabled(monkeypatch):
    captured: dict[str, object] = {}

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        del timeout
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
        return data, {
            "url": "https://example.com/v1/chat/completions",
            "status_code": 200,
            "response_text": json.dumps(data),
        }

    monkeypatch.setattr(OpenAICompatibleClient, "_post_json_with_meta", fake_post_json_with_meta)
    client = OpenAICompatibleClient(
        ServiceConfig(
            base_url="https://example.com/v1",
            api_key="sk-test",
            model="demo-model",
            api_mode="chat_completions",
        )
    )

    result = client.tool_round([{"role": "user", "content": "hello"}], build_tool_schemas(), max_tokens=None)

    assert result.tool_calls[0].name == "list_project_documents"
    assert captured["path"] == "/chat/completions"
    assert "max_tokens" not in captured["payload"]


def test_analyze_with_llm_disables_token_limit(monkeypatch):
    captured: dict[str, object] = {}

    def fake_chat_completion_result(self, messages, **kwargs):
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return ChatCompletionResult(
            content='{"summary":"ok","bullets":[],"confidence":0.8,"evidence":[],"conflicts":[],"notes":""}',
            model="demo-model",
            usage={"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
            request_url="https://example.com/v1/responses",
            request_payload={"model": "demo-model"},
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)

    payload = _analyze_with_llm(
        FACETS[0],
        "Demo",
        [
            {
                "chunk_id": "chunk-1",
                "document_title": "memo",
                "filename": "memo.txt",
                "content": "Alpha alpha alpha",
                "page_number": None,
            }
        ],
        {
            "base_url": "https://example.com/v1",
            "api_key": "sk-test",
            "model": "demo-model",
            "provider_kind": "openai-compatible",
            "api_mode": "responses",
        },
        llm_log_path=None,
        target_role="Demo",
        analysis_context="Check token limit behavior.",
    )

    assert payload["summary"] == "ok"
    assert captured["kwargs"]["max_tokens"] is None


def test_chat_completion_result_falls_back_to_secondary_service_config(monkeypatch):
    calls: list[tuple[str | None, str | None]] = []

    def fake_chat_completion_once(
        self,
        messages,
        *,
        resolved_model,
        temperature,
        response_format=None,
        max_tokens=None,
        stream_handler=None,
    ):
        del messages, temperature, response_format, max_tokens, stream_handler
        calls.append((self.config.base_url, resolved_model))
        if self.config.base_url == "https://primary.example/v1":
            raise RuntimeError("primary unavailable")
        return ChatCompletionResult(
            content='{"summary":"ok","bullets":[],"confidence":0.8,"fewshots":[],"conflicts":[],"notes":""}',
            model=resolved_model or self.config.model,
            usage={"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        )

    monkeypatch.setattr(OpenAICompatibleClient, "_chat_completion_result_once", fake_chat_completion_once)

    client = OpenAICompatibleClient(
        ServiceConfig(
            base_url="https://primary.example/v1",
            api_key="sk-primary",
            model="primary-model",
            api_mode="responses",
            fallbacks=[
                ServiceConfig(
                    base_url="https://fallback.example/v1",
                    api_key="sk-fallback",
                    model="fallback-model",
                    api_mode="responses",
                )
            ],
        )
    )

    result = client.chat_completion_result([{"role": "user", "content": "hello"}], model="primary-model")

    assert result.model == "fallback-model"
    assert calls == [
        ("https://primary.example/v1", "primary-model"),
        ("https://fallback.example/v1", "fallback-model"),
    ]
