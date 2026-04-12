from __future__ import annotations

from types import SimpleNamespace

from app.analysis.synthesizer import AssetSynthesizer
from app.models import AnalysisFacet, Project
from app.schemas import ChatCompletionResult, ServiceConfig


def test_asset_synthesizer_excludes_large_internal_facet_fields(monkeypatch):
    captured: dict[str, object] = {}

    def fake_chat_completion_result(self, messages, **kwargs):
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return ChatCompletionResult(
            content=(
                '{"headline":"h","executive_summary":"e","reality_anchor":"r","social_dynamics":"s",'
                '"interpersonal_mechanics":"i","subculture_refuge":"u","core_values_and_triggers":"c",'
                '"linguistic_signature":"l","psychological_profile":"p","contradictions":[],"observer_conclusion":"o"}'
            ),
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        )

    monkeypatch.setattr("app.analysis.synthesizer.OpenAICompatibleClient.chat_completion_result", fake_chat_completion_result)

    facet = AnalysisFacet(
        facet_key="language_style",
        status="completed",
        confidence=0.92,
        findings_json={
            "label": "Language Style",
            "summary": "summary " * 80,
            "bullets": [("bullet " * 50)] * 8,
            "llm_response_text": "RAW_RESPONSE_" * 10000,
            "llm_request_payload": {"raw_prompt": "RAW_PROMPT_" * 10000},
            "retrieval_trace": {"query": "TRACE_QUERY_" * 10000},
            "llm_live_text": "LIVE_" * 10000,
        },
        conflicts_json=[
            {"title": "Conflict " * 20, "detail": "Detail " * 60},
            {"title": "Second " * 20, "detail": "Another " * 60},
        ],
    )
    project = Project(name="Demo")
    synth = AssetSynthesizer()

    bundle = synth.build(
        "profile_report",
        project,
        [facet],
        ServiceConfig(base_url="https://example.com/v1", api_key="sk-test", model="demo-model"),
    )

    assert bundle.json_payload["headline"] == "h"
    user_message = captured["messages"][1]["content"]
    assert "Language Style" in user_message
    assert "llm_response_text" not in user_message
    assert "llm_request_payload" not in user_message
    assert "retrieval_trace" not in user_message
    assert "RAW_RESPONSE_" not in user_message
    assert "RAW_PROMPT_" not in user_message
    assert "TRACE_QUERY_" not in user_message
    assert len(user_message) < 4000


def test_asset_synthesizer_truncates_search_context():
    synth = AssetSynthesizer()
    chunks = [
        SimpleNamespace(document_title="Doc A", content="A" * 1200),
        SimpleNamespace(filename="doc-b.txt", content="B" * 1200),
    ]

    context = synth._build_search_context(chunks)

    assert "[Doc A]" in context
    assert "[doc-b.txt]" in context
    assert "A" * 600 not in context
    assert "B" * 600 not in context
