from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.llm.client import OpenAICompatibleClient
from app.models import DocumentRecord, TextChunk
from app.retrieval.base import RetrievalFilters
from app.retrieval.vector_store import VectorStoreResolution, model_key_for
from app.schemas import RetrievedChunk, ServiceConfig
from app.storage import repository

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
        target_project_id = repository.get_target_project_id(session, project_id)
        client = OpenAICompatibleClient(config, log_path=log_path)
        resolved_model = config.model or client.resolve_model()
        trace: dict[str, object] = {
            "embedding_attempted": True,
            "embedding_api_called": False,
            "embedding_url": client.endpoint_url("/embeddings"),
            "embedding_skip_reason": None,
            "candidate_chunks": 0,
            "candidate_documents": 0,
            "selected_documents": 0,
            "per_document_cap_applied": False,
            "vector_store_used": self._vector_store is not None,
            "vector_store_backend": None,
            "vector_store_provider": None,
            "vector_store_model": resolved_model,
            "vector_store_available": False,
            "vector_store_error": None,
            "semantic_degraded": False,
            "degraded_reason": None,
        }

        resolution = self._resolve_vector_store(project_id=target_project_id, model=resolved_model)
        trace.update(resolution.to_trace())
        trace["vector_store_used"] = resolution.store is not None
        if not resolution.available or resolution.store is None:
            trace["embedding_skip_reason"] = str(trace.get("degraded_reason") or "vector_store_unavailable")
            return [], trace

        query_vector = client.embeddings([query], model=resolved_model)[0]
        trace["embedding_api_called"] = True
        return self._search_from_vector_store(
            session,
            project_id=target_project_id,
            query_vector=query_vector,
            limit=limit,
            filters=filters,
            trace=trace,
            resolution=resolution,
        )

    def _resolve_vector_store(self, *, project_id: str, model: str | None) -> VectorStoreResolution:
        resolver = getattr(self._vector_store, "resolve_store", None)
        if callable(resolver):
            return resolver(project_id, provider="auto", model=model, allow_memory=False)

        store = self._vector_store
        if store is not None and callable(getattr(store, "search", None)):
            backend_name = type(store).__name__.replace("VectorStore", "").lower() or "custom"
            return VectorStoreResolution(
                store=store,
                backend=backend_name,
                provider="legacy",
                model=model,
                model_key=model_key_for(model),
                available=True,
            )

        return VectorStoreResolution(
            store=None,
            backend="disabled",
            provider="auto",
            model=model,
            model_key=model_key_for(model),
            available=False,
            degraded_reason="vector_store_unavailable",
        )

    def _search_from_vector_store(
        self,
        session: Session,
        *,
        project_id: str,
        query_vector: list[float],
        limit: int,
        filters: RetrievalFilters | None,
        trace: dict[str, object],
        resolution: VectorStoreResolution,
    ) -> tuple[list[RetrievedChunk], dict[str, object]]:
        try:
            search_results = resolution.store.search(query_vector, top_k=limit * PER_DOCUMENT_CAP) if resolution.store else []
        except Exception as exc:
            trace["vector_store_error"] = _format_exception(exc)
            trace["vector_store_available"] = False
            trace["semantic_degraded"] = True
            trace["degraded_reason"] = "vector_store_search_failed"
            trace["embedding_skip_reason"] = "vector_store_search_failed"
            return [], trace

        trace["vector_store_hit_count"] = len(search_results)
        if not search_results:
            trace["embedding_skip_reason"] = "vector_store_empty"
            return [], trace

        chunk_ids = [str(item.get("id") or "") for item in search_results if str(item.get("id") or "").strip()]
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
        id_to_score = {str(item.get("id") or ""): float(item.get("score") or 0.0) for item in search_results}
        id_to_candidate = {
            str(row.chunk_id): {
                "chunk_id": str(row.chunk_id),
                "document_id": str(row.document_id),
                "chunk_index": int(row.chunk_index),
                "page_number": row.page_number,
                "content": str(row.content or ""),
                "metadata": dict(row.metadata_json or {}),
                "filename": str(row.filename or ""),
                "document_title": str(row.document_title or row.filename or ""),
                "source_type": str(row.source_type or "document"),
                "score": id_to_score.get(str(row.chunk_id), 0.0),
            }
            for row in rows
        }
        candidates = [id_to_candidate[chunk_id] for chunk_id in chunk_ids if chunk_id in id_to_candidate]
        trace["candidate_chunks"] = len(candidates)
        trace["candidate_documents"] = len({candidate.get("document_id") for candidate in candidates})

        if not candidates:
            trace["embedding_skip_reason"] = "vector_store_rows_missing"
            return [], trace

        selected, selected_docs, cap_applied = self._apply_document_cap(
            candidates,
            limit=limit,
            per_document_cap=PER_DOCUMENT_CAP,
        )
        trace["selected_documents"] = selected_docs
        trace["per_document_cap_applied"] = cap_applied

        hits = self._expand_context_hits(
            session,
            project_id=project_id,
            anchors=selected,
            target_chars=CONTEXT_TARGET_CHARS,
        )
        return hits, trace

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

    def _expand_context_hits(
        self,
        session: Session,
        *,
        project_id: str,
        anchors: list[dict[str, Any]],
        target_chars: int,
    ) -> list[RetrievedChunk]:
        if not anchors:
            return []

        direct_hits: dict[str, RetrievedChunk] = {}
        intervals_by_document: dict[str, list[tuple[int, int]]] = defaultdict(list)

        for anchor in anchors:
            chunk_id = str(anchor.get("chunk_id") or "")
            anchor_text = str(anchor.get("content") or "").strip()
            if not chunk_id:
                continue
            if len(anchor_text) >= target_chars:
                direct_hits[chunk_id] = self._build_trimmed_hit(anchor, target_chars)
                continue
            document_id = str(anchor.get("document_id") or "")
            if not document_id:
                direct_hits[chunk_id] = self._build_trimmed_hit(anchor, target_chars)
                continue
            anchor_index = int(anchor.get("chunk_index") or 0)
            intervals_by_document[document_id].append(
                (
                    anchor_index - CONTEXT_NEIGHBOR_WINDOW,
                    anchor_index + CONTEXT_NEIGHBOR_WINDOW,
                )
            )

        conditions = []
        for document_id, ranges in intervals_by_document.items():
            for start, end in self._merge_ranges(ranges):
                conditions.append(
                    and_(
                        TextChunk.document_id == document_id,
                        TextChunk.chunk_index >= start,
                        TextChunk.chunk_index <= end,
                    )
                )

        row_maps: dict[str, dict[int, Any]] = defaultdict(dict)
        if conditions:
            stmt = (
                select(
                    TextChunk.document_id.label("document_id"),
                    TextChunk.id.label("chunk_id"),
                    TextChunk.chunk_index.label("chunk_index"),
                    TextChunk.content.label("content"),
                    TextChunk.page_number.label("page_number"),
                )
                .where(TextChunk.project_id == project_id, or_(*conditions))
                .order_by(TextChunk.document_id.asc(), TextChunk.chunk_index.asc())
            )
            rows = session.execute(stmt).all()
            for row in rows:
                row_maps[str(row.document_id)][int(row.chunk_index)] = row

        return [
            direct_hits.get(str(anchor.get("chunk_id") or ""))
            or self._build_context_hit(
                anchor,
                row_maps.get(str(anchor.get("document_id") or ""), {}),
                target_chars,
            )
            for anchor in anchors
        ]

    @staticmethod
    def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
        if not ranges:
            return []
        ordered = sorted(ranges, key=lambda item: item[0])
        merged = [ordered[0]]
        for start, end in ordered[1:]:
            current_start, current_end = merged[-1]
            if start <= current_end + 1:
                merged[-1] = (current_start, max(current_end, end))
            else:
                merged.append((start, end))
        return merged

    def _build_trimmed_hit(self, anchor: dict[str, Any], target_chars: int) -> RetrievedChunk:
        snippet = str(anchor.get("content") or "").strip()[:target_chars]
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
            anchor_chunk_index=int(anchor.get("chunk_index") or 0),
            context_span={"left": 0, "right": 0, "total_chars": len(snippet)},
        )

    def _build_context_hit(
        self,
        anchor: dict[str, Any],
        row_map: dict[int, Any],
        target_chars: int,
    ) -> RetrievedChunk:
        anchor_index = int(anchor.get("chunk_index") or 0)
        if anchor_index not in row_map:
            return self._build_trimmed_hit(anchor, target_chars)

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


def _format_exception(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text
    return exc.__class__.__name__
