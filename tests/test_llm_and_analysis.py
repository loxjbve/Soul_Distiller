from __future__ import annotations

import pytest

from app.analysis.engine import FACET_EVIDENCE_LIMIT, _normalize_facet_payload
from app.analysis.facets import FACETS
from app.agents.analysis.prompts import build_facet_analysis_messages
from app.llm.client import OpenAICompatibleClient
from app.schemas import ServiceConfig


@pytest.mark.parametrize(
    ("provider_kind", "expected_base_url"),
    [
        ("openai", "https://api.openai.com/v1"),
        ("xai", "https://api.x.ai/v1"),
        ("gemini", "https://generativelanguage.googleapis.com/v1beta/openai"),
    ],
)
def test_official_provider_uses_default_base_url(provider_kind: str, expected_base_url: str):
    client = OpenAICompatibleClient(
        ServiceConfig(
            base_url=None,
            api_key="sk-test",
            model="demo-model",
            provider_kind=provider_kind,
        )
    )
    assert client._url("/models") == f"{expected_base_url}/models"


def test_normalize_facet_payload_backfills_to_evidence_limit():
    facet = next(item for item in FACETS if item.key == "personality")
    chunks = [
        {
            "chunk_id": f"chunk-{index}",
            "document_id": "doc-1",
            "document_title": "Test Doc",
            "filename": "test.txt",
            "source_type": "txt",
            "content": f"sample content {index}",
            "score": 1.0,
            "page_number": None,
            "metadata": {},
        }
        for index in range(FACET_EVIDENCE_LIMIT + 12)
    ]

    normalized = _normalize_facet_payload(
        {
            "summary": "summary",
            "bullets": [],
            "confidence": 0.7,
            "evidence": [],
            "conflicts": [],
        },
        chunks,
        facet,
    )

    assert len(normalized["evidence"]) == FACET_EVIDENCE_LIMIT
    assert normalized["evidence"][0]["chunk_id"] == "chunk-0"
    assert normalized["evidence"][-1]["chunk_id"] == f"chunk-{FACET_EVIDENCE_LIMIT - 1}"


def test_build_facet_analysis_messages_stays_facet_scoped():
    facet = next(item for item in FACETS if item.key == "narrative_boundaries")

    messages = build_facet_analysis_messages(
        "Kurumi",
        facet,
        "[chunk-1] memo.txt\n会用缩写回避敏感话题。",
        target_role="Kurumi 本人",
        analysis_context="Focus on boundary management.",
    )

    system_text = messages[0]["content"]
    user_text = messages[1]["content"]

    assert "你当前只负责分析一个维度" in system_text
    assert "不要把人物十维总卡" in system_text
    assert "除了该维度本身，还要尽量抽取" not in user_text
    assert "只分析这个维度，不要顺手总结其他维度" in user_text
    assert "本维度的输出结构要求" in user_text


def test_normalize_facet_payload_filters_off_facet_bullets_and_rebuilds_summary():
    facet = next(item for item in FACETS if item.key == "narrative_boundaries")
    chunks = [
        {
            "chunk_id": "chunk-1",
            "document_id": "doc-1",
            "document_title": "Test Doc",
            "filename": "test.txt",
            "source_type": "txt",
            "content": "他会用缩写和昵称重构敏感话题，遇到政治追问就收口。",
            "score": 1.0,
            "page_number": None,
            "metadata": {},
        }
    ]

    normalized = _normalize_facet_payload(
        {
            "summary": "Kurumi 是年轻交易员，常靠市场过度反应模型做判断，也会向老师请教。",
            "bullets": [
                "角色规则：在群里扮演直言不讳的交易员。",
                "收口方式：遇到敏感追问会立刻收口，不继续展开。",
                "自保/重构：会用缩写和昵称改写敏感话题，降低风险。",
            ],
            "confidence": 0.7,
            "evidence": [{"chunk_id": "chunk-1", "reason": "代表片段", "quote": "会用缩写和昵称重构敏感话题"}],
            "conflicts": [],
            "notes": "",
        },
        chunks,
        facet,
    )

    assert normalized["summary"].startswith("围绕 自我叙事与禁区边界")
    assert normalized["bullets"] == [
        "收口方式：遇到敏感追问会立刻收口，不继续展开。",
        "自保/重构：会用缩写和昵称改写敏感话题，降低风险。",
    ]
    assert "off-facet bullet" in (normalized["notes"] or "")
