from __future__ import annotations

from sqlalchemy.orm import Session

from app.llm.client import OpenAICompatibleClient
from app.retrieval.base import RetrievalFilters
from app.retrieval.embedding import EmbeddingRetriever
from app.retrieval.lexical import LexicalRetriever
from app.schemas import RetrievedChunk, ServiceConfig


class RetrievalService:
    def __init__(self, vector_store=None) -> None:
        self.lexical = LexicalRetriever()
        self.embedding = EmbeddingRetriever(vector_store=vector_store)

    def set_vector_store(self, vector_store) -> None:
        self.embedding.set_vector_store(vector_store)

    def search(
        self,
        session: Session,
        *,
        project_id: str,
        query: str,
        embedding_config: ServiceConfig | None,
        log_path: str | None = None,
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> tuple[list[RetrievedChunk], str, dict[str, object]]:
        trace: dict[str, object] = {
            "mode": "lexical",
            "embedding_configured": bool(embedding_config),
            "embedding_attempted": False,
            "embedding_api_called": False,
            "embedding_success": False,
            "embedding_error": None,
            "embedding_url": None,
            "embedding_skip_reason": "embedding_not_configured" if not embedding_config else None,
            "lexical_candidate_count": 0,
            "lexical_result_count": 0,
            "fallback_reason": "embedding_not_configured" if not embedding_config else None,
        }
        if embedding_config:
            trace["embedding_url"] = OpenAICompatibleClient(embedding_config, log_path=log_path).endpoint_url("/embeddings")
            trace["embedding_attempted"] = True
            try:
                results, embedding_trace = self.embedding.search(
                    session,
                    project_id=project_id,
                    query=query,
                    config=embedding_config,
                    log_path=log_path,
                    limit=limit,
                    filters=filters,
                )
                trace.update(embedding_trace)
                trace["embedding_attempted"] = bool(trace.get("embedding_attempted"))
                trace["embedding_api_called"] = bool(trace.get("embedding_api_called"))
                if results:
                    trace["mode"] = "hybrid"
                    trace["embedding_success"] = True
                    trace["lexical_result_count"] = len(results)
                    return results, "hybrid", trace
                trace["embedding_success"] = bool(trace.get("embedding_api_called"))
                trace["fallback_reason"] = (
                    str(trace.get("embedding_skip_reason"))
                    if trace.get("embedding_skip_reason")
                    else "empty_hybrid_results"
                )
            except Exception as exc:
                trace["embedding_error"] = _format_exception(exc)
                trace["fallback_reason"] = "embedding_exception"
                trace["embedding_success"] = False
                trace["embedding_api_called"] = True
        results = self.lexical.search(
            session,
            project_id=project_id,
            query=query,
            limit=limit,
            filters=filters,
        )
        trace["mode"] = "lexical"
        trace["lexical_result_count"] = len(results)
        return results, "lexical", trace


def _format_exception(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text
    return exc.__class__.__name__
