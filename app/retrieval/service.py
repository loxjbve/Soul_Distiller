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
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> tuple[list[RetrievedChunk], str]:
        if embedding_config:
            try:
                results = self.embedding.search(
                    session,
                    project_id=project_id,
                    query=query,
                    config=embedding_config,
                    limit=limit,
                    filters=filters,
                )
                if results:
                    return results, "hybrid"
            except Exception:
                pass
        results = self.lexical.search(
            session,
            project_id=project_id,
            query=query,
            limit=limit,
            filters=filters,
        )
        return results, "lexical"
