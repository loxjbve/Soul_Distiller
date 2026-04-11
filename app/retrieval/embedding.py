from __future__ import annotations

import heapq
from collections import defaultdict
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.llm.client import OpenAICompatibleClient
from app.models import DocumentRecord, TextChunk
from app.retrieval.base import RetrievalFilters
from app.schemas import RetrievedChunk, ServiceConfig
from app.utils.text import cosine_similarity

EMBEDDING_BATCH_SIZE = 64
SEMANTIC_POOL_MIN = 240
SEMANTIC_POOL_MULTIPLIER = 25
CONTEXT_TARGET_CHARS = 900
CONTEXT_MAX_CHARS = 1100
CONTEXT_NEIGHBOR_WINDOW = 6
PER_DOCUMENT_CAP = 3


class EmbeddingRetriever:
    def __init__(self, vector_store=None):
        self._vector_store = vector_store

    def set_vector_store(self, vector_store):
        self._vector_store = vector_store

    def search(
        self,
        session: Session,
        *,
        project_id: str,
        query: str,
        config: ServiceConfig,
        log_path: str | None = None,
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> tuple[list[RetrievedChunk], dict[str, object]]:
        trace: dict[str, object] = {
            "embedding_attempted": True,
            "embedding_api_called": True,
            "embedding_url": OpenAICompatibleClient(config, log_path=log_path).endpoint_url("/embeddings"),
            "lexical_candidate_count": 0,
            "new_chunk_embeddings": 0,
            "embedding_skip_reason": None,
            "candidate_chunks": 0,
            "candidate_documents": 0,
            "selected_documents": 0,
            "per_document_cap_applied": False,
            "backfill_batches": 0,
            "missing_embeddings_before": 0,
            "vector_store_used": self._vector_store is not None,
        }
        client = OpenAICompatibleClient(config, log_path=log_path)
        query_vector = client.embeddings([query], model=config.model)[0]

        if self._vector_store is not None:
            return self._search_from_vector_store(
                session,
                project_id=project_id,
                query_vector=query_vector,
                config=config,
                client=client,
                limit=limit,
                filters=filters,
                trace=trace,
            )

        backfill = self._backfill_missing_embeddings(
            session,
            project_id=project_id,
            config=config,
            client=client,
            filters=filters,
        )
        trace["new_chunk_embeddings"] = backfill["embedded_chunks"]
        trace["backfill_batches"] = backfill["batches"]
        trace["missing_embeddings_before"] = backfill["missing_before"]

        candidates, candidate_chunks, candidate_docs = self._rank_semantic_candidates(
            session,
            project_id=project_id,
            query_vector=query_vector,
            config=config,
            limit=limit,
            filters=filters,
        )
        trace["candidate_chunks"] = candidate_chunks
        trace["candidate_documents"] = candidate_docs
        if not candidates:
            trace["embedding_skip_reason"] = "no_semantic_candidates"
            return [], trace

        selected, selected_docs, cap_applied = self._apply_document_cap(
            candidates,
            limit=limit,
            per_document_cap=PER_DOCUMENT_CAP,
        )
        trace["selected_documents"] = selected_docs
        trace["per_document_cap_applied"] = cap_applied

        hits = [
            self._expand_context_hit(
                session,
                project_id=project_id,
                anchor=anchor,
                target_chars=CONTEXT_TARGET_CHARS,
            )
            for anchor in selected
        ]
        return hits, trace

    def _search_from_vector_store(
        self,
        session: Session,
        *,
        project_id: str,
        query_vector: list[float],
        config: ServiceConfig,
        client: OpenAICompatibleClient,
        limit: int,
        filters: RetrievalFilters | None,
        trace: dict[str, object],
    ) -> tuple[list[RetrievedChunk], dict[str, object]]:
        try:
            search_results = self._vector_store.search(query_vector, top_k=limit * PER_DOCUMENT_CAP)
            if not search_results:
                trace["embedding_skip_reason"] = "vector_store_empty"
                return [], trace

            chunk_ids = [r.get("id") for r in search_results if r.get("id")]
            if not chunk_ids:
                trace["embedding_skip_reason"] = "no_chunk_ids_from_vector_store"
                return [], trace

            stmt = (
                select(
                    TextChunk.id.label("chunk_id"),
                    TextChunk.document_id.label("document_id"),
                    TextChunk.chunk_index.label("chunk_index"),
                    TextChunk.page_number.label("page_number"),
                    TextChunk.content.label("content"),
                    TextChunk.metadata_json.label("metadata_json"),
                    DocumentRecord.filename.label("filename"),
                    DocumentRecord.title.label("document_title"),
                    DocumentRecord.source_type.label("source_type"),
                )
                .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
                .where(
                    TextChunk.id.in_(chunk_ids),
                    TextChunk.project_id == project_id,
                    DocumentRecord.ingest_status == "ready",
                )
            )
            if filters and filters.source_types:
                stmt = stmt.where(DocumentRecord.source_type.in_(filters.source_types))

            rows = session.execute(stmt).all()
            id_to_row = {str(row.chunk_id): row for row in rows}
            id_to_score = {str(r.get("id", "")): r.get("score", 0.0) for r in search_results}

            candidates = []
            for row in rows:
                chunk_id = str(row.chunk_id)
                score = id_to_score.get(chunk_id, 0.0)
                candidates.append({
                    "chunk_id": chunk_id,
                    "document_id": str(row.document_id),
                    "chunk_index": int(row.chunk_index),
                    "page_number": row.page_number,
                    "content": str(row.content or ""),
                    "metadata": dict(row.metadata_json or {}),
                    "filename": str(row.filename or ""),
                    "document_title": str(row.document_title or row.filename or ""),
                    "source_type": str(row.source_type or "document"),
                    "score": float(score),
                })

            selected, selected_docs, cap_applied = self._apply_document_cap(
                candidates,
                limit=limit,
                per_document_cap=PER_DOCUMENT_CAP,
            )
            trace["selected_documents"] = selected_docs
            trace["per_document_cap_applied"] = cap_applied
            trace["candidate_chunks"] = len(candidates)
            trace["candidate_documents"] = len({c.get("document_id") for c in candidates})
            trace["vector_store_hit_count"] = len(search_results)

            hits = [
                self._expand_context_hit(
                    session,
                    project_id=project_id,
                    anchor=anchor,
                    target_chars=CONTEXT_TARGET_CHARS,
                )
                for anchor in selected
            ]
            return hits, trace
        except Exception as exc:
            trace["vector_store_error"] = str(exc)
            trace["fallback_to_db"] = True
            return self._search_from_db(
                session,
                project_id=project_id,
                query_vector=query_vector,
                config=config,
                client=client,
                limit=limit,
                filters=filters,
                trace=trace,
            )

    def _search_from_db(
        self,
        session: Session,
        *,
        project_id: str,
        query_vector: list[float],
        config: ServiceConfig,
        client: OpenAICompatibleClient,
        limit: int,
        filters: RetrievalFilters | None,
        trace: dict[str, object],
    ) -> tuple[list[RetrievedChunk], dict[str, object]]:
        backfill = self._backfill_missing_embeddings(
            session,
            project_id=project_id,
            config=config,
            client=client,
            filters=filters,
        )
        trace["new_chunk_embeddings"] = backfill["embedded_chunks"]
        trace["backfill_batches"] = backfill["batches"]
        trace["missing_embeddings_before"] = backfill["missing_before"]

        candidates, candidate_chunks, candidate_docs = self._rank_semantic_candidates(
            session,
            project_id=project_id,
            query_vector=query_vector,
            config=config,
            limit=limit,
            filters=filters,
        )
        trace["candidate_chunks"] = candidate_chunks
        trace["candidate_documents"] = candidate_docs
        if not candidates:
            trace["embedding_skip_reason"] = "no_semantic_candidates"
            return [], trace

        selected, selected_docs, cap_applied = self._apply_document_cap(
            candidates,
            limit=limit,
            per_document_cap=PER_DOCUMENT_CAP,
        )
        trace["selected_documents"] = selected_docs
        trace["per_document_cap_applied"] = cap_applied

        hits = [
            self._expand_context_hit(
                session,
                project_id=project_id,
                anchor=anchor,
                target_chars=CONTEXT_TARGET_CHARS,
            )
            for anchor in selected
        ]
        return hits, trace

    def _backfill_missing_embeddings(
        self,
        session: Session,
        *,
        project_id: str,
        config: ServiceConfig,
        client: OpenAICompatibleClient,
        filters: RetrievalFilters | None,
    ) -> dict[str, int]:
        stmt = (
            select(TextChunk.id, TextChunk.content)
            .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
            .where(
                TextChunk.project_id == project_id,
                DocumentRecord.ingest_status == "ready",
                or_(TextChunk.embedding_vector.is_(None), TextChunk.embedding_model != config.model),
            )
        )
        if filters and filters.source_types:
            stmt = stmt.where(DocumentRecord.source_type.in_(filters.source_types))
        result = session.execute(stmt)
        missing_before = 0
        embedded_chunks = 0
        batches = 0
        while True:
            rows = result.fetchmany(EMBEDDING_BATCH_SIZE)
            if not rows:
                break
            missing_before += len(rows)
            batch_ids = [str(row.id) for row in rows]
            batch_inputs = [str(row.content or "") for row in rows]
            vectors = client.embeddings(batch_inputs, model=config.model)
            chunks = list(session.scalars(select(TextChunk).where(TextChunk.id.in_(batch_ids))))
            chunk_map = {chunk.id: chunk for chunk in chunks}
            for chunk_id, vector in zip(batch_ids, vectors):
                chunk = chunk_map.get(chunk_id)
                if not chunk:
                    continue
                chunk.embedding_vector = vector
                chunk.embedding_model = config.model
                embedded_chunks += 1
            batches += 1
            session.flush()
        return {
            "missing_before": missing_before,
            "embedded_chunks": embedded_chunks,
            "batches": batches,
        }

    def _rank_semantic_candidates(
        self,
        session: Session,
        *,
        project_id: str,
        query_vector: list[float],
        config: ServiceConfig,
        limit: int,
        filters: RetrievalFilters | None,
    ) -> tuple[list[dict[str, Any]], int, int]:
        pool_size = max(limit * SEMANTIC_POOL_MULTIPLIER, SEMANTIC_POOL_MIN)
        stmt = (
            select(
                TextChunk.id.label("chunk_id"),
                TextChunk.document_id.label("document_id"),
                TextChunk.chunk_index.label("chunk_index"),
                TextChunk.page_number.label("page_number"),
                TextChunk.content.label("content"),
                TextChunk.embedding_vector.label("embedding_vector"),
                TextChunk.metadata_json.label("metadata_json"),
                DocumentRecord.filename.label("filename"),
                DocumentRecord.title.label("document_title"),
                DocumentRecord.source_type.label("source_type"),
            )
            .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
            .where(
                TextChunk.project_id == project_id,
                DocumentRecord.ingest_status == "ready",
                TextChunk.embedding_model == config.model,
                TextChunk.embedding_vector.is_not(None),
            )
        )
        if filters and filters.source_types:
            stmt = stmt.where(DocumentRecord.source_type.in_(filters.source_types))
        result = session.execute(stmt)

        candidate_chunks = 0
        candidate_docs: set[str] = set()
        heap: list[tuple[float, int, dict[str, Any]]] = []
        serial = 0
        while True:
            rows = result.fetchmany(256)
            if not rows:
                break
            for row in rows:
                candidate_chunks += 1
                doc_id = str(row.document_id)
                candidate_docs.add(doc_id)
                vector = row.embedding_vector
                if not isinstance(vector, (list, tuple)):
                    continue
                score = cosine_similarity(query_vector, list(vector))
                payload = {
                    "chunk_id": str(row.chunk_id),
                    "document_id": doc_id,
                    "chunk_index": int(row.chunk_index),
                    "page_number": row.page_number,
                    "content": str(row.content or ""),
                    "metadata": dict(row.metadata_json or {}),
                    "filename": str(row.filename or ""),
                    "document_title": str(row.document_title or row.filename or ""),
                    "source_type": str(row.source_type or "document"),
                    "score": float(score),
                }
                entry = (float(score), serial, payload)
                serial += 1
                if len(heap) < pool_size:
                    heapq.heappush(heap, entry)
                elif score > heap[0][0]:
                    heapq.heapreplace(heap, entry)
        ordered = sorted(heap, key=lambda item: item[0], reverse=True)
        return [item[2] for item in ordered], candidate_chunks, len(candidate_docs)

    @staticmethod
    def _apply_document_cap(
        candidates: list[dict[str, Any]],
        *,
        limit: int,
        per_document_cap: int,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        if limit <= 0:
            return [], 0, False
        selected: list[dict[str, Any]] = []
        overflow: list[dict[str, Any]] = []
        doc_counts: dict[str, int] = defaultdict(int)
        selected_ids: set[str] = set()
        cap_applied = False
        for item in candidates:
            chunk_id = str(item.get("chunk_id") or "")
            if not chunk_id:
                continue
            doc_id = str(item.get("document_id") or "")
            if len(selected) < limit and doc_counts[doc_id] < per_document_cap:
                selected.append(item)
                selected_ids.add(chunk_id)
                doc_counts[doc_id] += 1
            else:
                if doc_counts[doc_id] >= per_document_cap:
                    cap_applied = True
                overflow.append(item)
        if len(selected) < limit:
            for item in overflow:
                chunk_id = str(item.get("chunk_id") or "")
                if not chunk_id or chunk_id in selected_ids:
                    continue
                selected.append(item)
                selected_ids.add(chunk_id)
                if len(selected) >= limit:
                    break
        selected_docs = len({str(item.get("document_id") or "") for item in selected if item.get("document_id")})
        return selected[:limit], selected_docs, cap_applied

    def _expand_context_hit(
        self,
        session: Session,
        *,
        project_id: str,
        anchor: dict[str, Any],
        target_chars: int,
    ) -> RetrievedChunk:
        anchor_text = str(anchor.get("content") or "").strip()
        anchor_index = int(anchor.get("chunk_index") or 0)
        if len(anchor_text) >= target_chars:
            snippet = anchor_text[:target_chars]
            return RetrievedChunk(
                chunk_id=str(anchor.get("chunk_id") or ""),
                document_id=str(anchor.get("document_id") or ""),
                document_title=str(anchor.get("document_title") or ""),
                filename=str(anchor.get("filename") or ""),
                source_type=str(anchor.get("source_type") or "document"),
                content=snippet,
                score=float(anchor.get("score") or 0.0),
                page_number=anchor.get("page_number"),
                metadata=dict(anchor.get("metadata") or {}),
                anchor_chunk_id=str(anchor.get("chunk_id") or ""),
                anchor_chunk_index=anchor_index,
                context_span={"left": 0, "right": 0, "total_chars": len(snippet)},
            )

        stmt = (
            select(
                TextChunk.id.label("chunk_id"),
                TextChunk.chunk_index.label("chunk_index"),
                TextChunk.content.label("content"),
                TextChunk.page_number.label("page_number"),
            )
            .where(
                TextChunk.project_id == project_id,
                TextChunk.document_id == str(anchor.get("document_id") or ""),
                TextChunk.chunk_index >= anchor_index - CONTEXT_NEIGHBOR_WINDOW,
                TextChunk.chunk_index <= anchor_index + CONTEXT_NEIGHBOR_WINDOW,
            )
            .order_by(TextChunk.chunk_index.asc())
        )
        rows = session.execute(stmt).all()
        row_map = {int(row.chunk_index): row for row in rows}
        if anchor_index not in row_map:
            snippet = anchor_text[:target_chars]
            return RetrievedChunk(
                chunk_id=str(anchor.get("chunk_id") or ""),
                document_id=str(anchor.get("document_id") or ""),
                document_title=str(anchor.get("document_title") or ""),
                filename=str(anchor.get("filename") or ""),
                source_type=str(anchor.get("source_type") or "document"),
                content=snippet,
                score=float(anchor.get("score") or 0.0),
                page_number=anchor.get("page_number"),
                metadata=dict(anchor.get("metadata") or {}),
                anchor_chunk_id=str(anchor.get("chunk_id") or ""),
                anchor_chunk_index=anchor_index,
                context_span={"left": 0, "right": 0, "total_chars": len(snippet)},
            )

        chosen_indices = {anchor_index}
        total_chars = len(str(row_map[anchor_index].content or "").strip())
        left_cursor = anchor_index - 1
        right_cursor = anchor_index + 1
        left_added = 0
        right_added = 0
        min_index = min(row_map)
        max_index = max(row_map)
        while total_chars < target_chars and (left_cursor >= min_index or right_cursor <= max_index):
            progressed = False
            if right_cursor <= max_index:
                right_row = row_map.get(right_cursor)
                right_cursor += 1
                if right_row is not None:
                    right_text = str(right_row.content or "").strip()
                    if right_text:
                        chosen_indices.add(int(right_row.chunk_index))
                        total_chars += len(right_text) + 1
                        right_added += 1
                        progressed = True
            if total_chars >= target_chars:
                break
            if left_cursor >= min_index:
                left_row = row_map.get(left_cursor)
                left_cursor -= 1
                if left_row is not None:
                    left_text = str(left_row.content or "").strip()
                    if left_text:
                        chosen_indices.add(int(left_row.chunk_index))
                        total_chars += len(left_text) + 1
                        left_added += 1
                        progressed = True
            if not progressed and left_cursor < min_index and right_cursor > max_index:
                break

        snippets = [
            str(row_map[index].content or "").strip()
            for index in sorted(chosen_indices)
            if index in row_map and str(row_map[index].content or "").strip()
        ]
        combined = "\n".join(snippets).strip()
        if len(combined) > CONTEXT_MAX_CHARS:
            combined = combined[:CONTEXT_MAX_CHARS].rstrip()

        anchor_row = row_map.get(anchor_index)
        page_number = anchor_row.page_number if anchor_row else anchor.get("page_number")
        return RetrievedChunk(
            chunk_id=str(anchor.get("chunk_id") or ""),
            document_id=str(anchor.get("document_id") or ""),
            document_title=str(anchor.get("document_title") or ""),
            filename=str(anchor.get("filename") or ""),
            source_type=str(anchor.get("source_type") or "document"),
            content=combined,
            score=float(anchor.get("score") or 0.0),
            page_number=page_number,
            metadata=dict(anchor.get("metadata") or {}),
            anchor_chunk_id=str(anchor.get("chunk_id") or ""),
            anchor_chunk_index=anchor_index,
            context_span={"left": left_added, "right": right_added, "total_chars": len(combined)},
        )
