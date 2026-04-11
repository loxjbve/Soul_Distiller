from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session

from app.llm.client import OpenAICompatibleClient
from app.retrieval.base import RetrievalFilters
from app.retrieval.embedding import EmbeddingRetriever
from app.retrieval.lexical import LexicalRetriever
from app.retrieval.rewrite import rewrite_query
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
        llm_config: ServiceConfig | None = None,
        log_path: str | None = None,
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> tuple[list[RetrievedChunk], str, dict[str, object]]:
        trace: dict[str, object] = {
            "mode": "lexical",
            "embedding_configured": bool(embedding_config),
            "llm_configured": bool(llm_config),
            "query_rewritten": False,
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
        
        # Query Rewriting
        hyde_text = ""
        expanded_keywords = ""
        if llm_config:
            try:
                hyde_text, expanded_keywords = rewrite_query(query, llm_config, log_path)
                if hyde_text or expanded_keywords:
                    trace["query_rewritten"] = True
                    trace["hyde_text"] = hyde_text
                    trace["expanded_keywords"] = expanded_keywords
            except Exception as e:
                trace["rewrite_error"] = _format_exception(e)
                
        lexical_query = f"{query} {expanded_keywords}".strip() if expanded_keywords else query
        vector_query = hyde_text if hyde_text else query

        lexical_results: list[RetrievedChunk] = []
        embedding_results: list[RetrievedChunk] = []
        embedding_trace: dict[str, object] = {}

        if embedding_config:
            trace["embedding_url"] = OpenAICompatibleClient(embedding_config, log_path=log_path).endpoint_url("/embeddings")
            trace["embedding_attempted"] = True
            
            def run_embedding() -> tuple[list[RetrievedChunk], dict[str, object]]:
                return self.embedding.search(
                    session,
                    project_id=project_id,
                    query=vector_query,
                    config=embedding_config,
                    log_path=log_path,
                    limit=limit * 2,
                    filters=filters,
                )
            
            # Use ThreadPoolExecutor to run vector search concurrently to save time.
            # Lexical (BM25) runs on the main thread using the original SQLAlchemy session.
            with ThreadPoolExecutor(max_workers=1) as executor:
                future_emb = executor.submit(run_embedding)
                
                # While embedding runs, do lexical search
                try:
                    lexical_results = self.lexical.search(
                        session,
                        project_id=project_id,
                        query=lexical_query,
                        limit=limit * 2,
                        filters=filters,
                    )
                except Exception as e:
                    trace["lexical_error"] = _format_exception(e)
                
                # Wait for embedding search
                try:
                    embedding_results, embedding_trace = future_emb.result()
                    trace.update(embedding_trace)
                    trace["embedding_attempted"] = bool(trace.get("embedding_attempted"))
                    trace["embedding_api_called"] = bool(trace.get("embedding_api_called"))
                    if embedding_results:
                        trace["embedding_success"] = True
                    else:
                        trace["embedding_success"] = bool(trace.get("embedding_api_called"))
                        trace["fallback_reason"] = str(trace.get("embedding_skip_reason")) or "empty_hybrid_results"
                except Exception as exc:
                    trace["embedding_error"] = _format_exception(exc)
                    trace["fallback_reason"] = "embedding_exception"
                    trace["embedding_success"] = False
                    trace["embedding_api_called"] = True
        else:
            # Fallback lexical only
            lexical_results = self.lexical.search(
                session,
                project_id=project_id,
                query=lexical_query,
                limit=limit,
                filters=filters,
            )

        if embedding_config and trace.get("embedding_success"):
            trace["mode"] = "hybrid"
            merged_results = self._rrf_merge(lexical_results, embedding_results, limit=limit)
            trace["lexical_result_count"] = len(lexical_results)
            return merged_results, "hybrid", trace

        trace["mode"] = "lexical"
        trace["lexical_result_count"] = len(lexical_results)
        return lexical_results[:limit], "lexical", trace

    def _rrf_merge(
        self,
        lexical_results: list[RetrievedChunk],
        embedding_results: list[RetrievedChunk],
        limit: int,
        k: int = 60,
    ) -> list[RetrievedChunk]:
        scores: dict[str, float] = {}
        chunk_map: dict[str, RetrievedChunk] = {}

        for rank, chunk in enumerate(lexical_results):
            scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (k + rank + 1)
            chunk_map[chunk.chunk_id] = chunk

        for rank, chunk in enumerate(embedding_results):
            scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (k + rank + 1)
            chunk_map[chunk.chunk_id] = chunk

        sorted_ids = sorted(scores.keys(), key=lambda cid: scores[cid], reverse=True)
        merged: list[RetrievedChunk] = []
        for cid in sorted_ids[:limit]:
            chunk = chunk_map[cid]
            chunk.score = scores[cid]
            merged.append(chunk)
            
        return merged


def _format_exception(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text
    return exc.__class__.__name__
