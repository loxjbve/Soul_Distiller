from __future__ import annotations

import json
from typing import Any

from app.service.common.llm.client import OpenAICompatibleClient, parse_json_response
from app.schemas import ServiceConfig

REWRITE_PROMPT = """You are an expert search query optimization assistant for a technical Retrieval-Augmented Generation (RAG) system.
Your task is to analyze the user's search query and provide two outputs to significantly improve document retrieval:
1. "hyde_document": A brief hypothetical document or paragraph (2-4 sentences) that directly answers the query using relevant domain terminology. Do not worry about factual accuracy, just generate the structure and vocabulary that a real matching document would contain. This will be used for dense vector similarity search (HyDE).
2. "expanded_keywords": A space-separated list of synonyms, related technical terms, and variations of the core concepts in the query. This will be used to enhance BM25 lexical search.

You MUST respond strictly in valid JSON format matching this schema:
{
    "hyde_document": "...",
    "expanded_keywords": "..."
}

User Query:
{query}
"""


def rewrite_query(
    query: str,
    config: ServiceConfig,
    log_path: str | None = None,
) -> tuple[str, str]:
    """
    Uses LLM to rewrite the query into a hypothetical document (HyDE) and expanded keywords.
    Returns a tuple of (hyde_document, expanded_keywords).
    """
    if not query or not query.strip():
        return "", ""

    client = OpenAICompatibleClient(config, log_path=log_path)
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": REWRITE_PROMPT.replace("{query}", query)}
    ]
    try:
        completion = client.chat_completion_result(
            messages,
            model=config.model,
            temperature=0.3,
            max_tokens=400,
        )
        parsed = parse_json_response(completion.content)
        hyde = str(parsed.get("hyde_document", "")).strip()
        keywords = str(parsed.get("expanded_keywords", "")).strip()
        return hyde, keywords
    except Exception:
        # Graceful degradation on failure: return empty strings to fallback to original query
        return "", ""
