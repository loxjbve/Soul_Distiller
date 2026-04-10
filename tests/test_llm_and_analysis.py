from __future__ import annotations

import pytest

from app.analysis.engine import FACET_EVIDENCE_LIMIT, _normalize_facet_payload
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
    )

    assert len(normalized["evidence"]) == FACET_EVIDENCE_LIMIT
    assert normalized["evidence"][0]["chunk_id"] == "chunk-0"
    assert normalized["evidence"][-1]["chunk_id"] == f"chunk-{FACET_EVIDENCE_LIMIT - 1}"
