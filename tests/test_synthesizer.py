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


def test_skill_heuristic_markdown_uses_new_persona_blueprint():
    project = Project(name="Demo")
    facets = [
        AnalysisFacet(
            facet_key="personality",
            findings_json={"summary": "边界感很强，默认先观察再开口。", "bullets": ["习惯先压住情绪再表达态度。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="physical_anchor",
            findings_json={"summary": "现实压力感很强，判断常先看成本和代价。", "bullets": ["做决定时先想时间和钱够不够。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="values_preferences",
            findings_json={"summary": "反感空话，更看重真实代价和长期自洽。", "bullets": ["讨厌没有代价意识的劝告。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="life_timeline",
            findings_json={"summary": "长期混迹线上社群，这段经历塑造了她的判断方式。", "bullets": ["长期混迹线上社群", "记得旧事细节"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="language_style",
            findings_json={"summary": "说话偏短句，结尾常带保留。", "bullets": ["先给态度，再补一句理由。", "熟悉话题说得更笃定。"]},
            evidence_json=[{"quote": "行，就这样。", "reason": "短句回应"}],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="narrative_boundaries",
            findings_json={"summary": "不愿替自己扩写不存在的经历。", "bullets": ["被追问隐私时会明显收口。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="social_niche",
            findings_json={"summary": "在熟人圈子里更有话语权。", "bullets": ["会先判断对方是不是自己人。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="interpersonal_mechanics",
            findings_json={"summary": "帮人时很实在，但不喜欢被越界索取。", "bullets": ["不熟的人先设边界。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
        AnalysisFacet(
            facet_key="subculture_refuge",
            findings_json={"summary": "长期在特定社群语境里形成表达习惯。", "bullets": ["熟悉社群旧事和圈层黑话。"]},
            evidence_json=[],
            conflicts_json=[],
        ),
    ]
    synth = AssetSynthesizer()

    bundle = synth.build("skill", project, facets, config=None, target_role="Demo 本人", analysis_context="私聊语料")
    markdown = bundle.markdown_text

    assert "## 角色扮演规则" in markdown
    assert "## 回答工作流" in markdown
    assert "## 核心心智模型" in markdown
    assert "## 决策启发式" in markdown
    assert "## 表达 DNA" in markdown
    assert "## 诚实边界" in markdown
    assert "## 调研来源" in markdown
    assert "# 核心身份与精神底色" in markdown
    assert "# 核心记忆与经历" in markdown
