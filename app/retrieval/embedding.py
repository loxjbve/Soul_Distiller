from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.llm.client import OpenAICompatibleClient
from app.models import TextChunk
from app.retrieval.base import RetrievalFilters
from app.retrieval.lexical import LexicalRetriever
from app.schemas import RetrievedChunk, ServiceConfig
from app.utils.text import cosine_similarity


class EmbeddingRetriever:
    def __init__(self) -> None:
        self.lexical = LexicalRetriever()

    def search(
        self,
        session: Session,
        *,
        project_id: str,
        query: str,
        config: ServiceConfig,
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> list[RetrievedChunk]:
        lexical_hits = self.lexical.search(
            session,
            project_id=project_id,
            query=query,
            limit=max(limit * 3, 12),
            filters=filters,
        )
        if not lexical_hits:
            return []
        client = OpenAICompatibleClient(config)
        query_vector = client.embeddings([query], model=config.model)[0]
        hits_by_id = {hit.chunk_id: hit for hit in lexical_hits}
        chunks = list(session.scalars(select(TextChunk).where(TextChunk.id.in_(list(hits_by_id)))))
        missing = [chunk for chunk in chunks if not chunk.embedding_vector or chunk.embedding_model != config.model]
        if missing:
            vectors = client.embeddings([chunk.content for chunk in missing], model=config.model)
            for chunk, vector in zip(missing, vectors):
                chunk.embedding_vector = vector
                chunk.embedding_model = config.model
            session.flush()
        chunk_map = {chunk.id: chunk for chunk in chunks}
        combined: list[RetrievedChunk] = []
        for hit in lexical_hits:
            chunk = chunk_map[hit.chunk_id]
            vector_score = cosine_similarity(query_vector, chunk.embedding_vector or [])
            hit.score = round((hit.score * 0.45) + (max(vector_score, 0.0) * 6.0), 4)
            combined.append(hit)
        combined.sort(key=lambda item: item.score, reverse=True)
        return combined[:limit]
