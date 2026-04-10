from __future__ import annotations

from sqlalchemy.orm import Session

from app.retrieval.base import RetrievalFilters
from app.retrieval.embedding import EmbeddingRetriever
from app.retrieval.lexical import LexicalRetriever
from app.schemas import RetrievedChunk, ServiceConfig


class RetrievalService:
    def __init__(self) -> None:
        self.lexical = LexicalRetriever()
        self.embedding = EmbeddingRetriever()

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
            "embedding_success": False,
            "embedding_error": None,
            "embedding_url": None,
            "fallback_reason": "embedding_not_configured" if not embedding_config else None,
        }
        if embedding_config:
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
                if results:
                    trace["mode"] = "hybrid"
                    trace["embedding_success"] = True
                    return results, "hybrid", trace
                trace["embedding_success"] = True
                trace["fallback_reason"] = "empty_hybrid_results"
            except Exception as exc:
                trace["embedding_error"] = str(exc)
                trace["fallback_reason"] = "embedding_exception"
                pass
        results = self.lexical.search(
            session,
            project_id=project_id,
            query=query,
            limit=limit,
            filters=filters,
        )
        trace["mode"] = "lexical"
        return results, "lexical", trace
