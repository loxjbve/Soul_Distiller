from __future__ import annotations

import math
from collections import Counter

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.models import DocumentRecord, TextChunk
from app.retrieval.base import RetrievalFilters
from app.schemas import RetrievedChunk
from app.utils.text import tokenize

MAX_LEXICAL_CANDIDATES = 1200
MIN_FILTER_TERM_LEN = 2
MAX_FILTER_TERMS = 8


class LexicalRetriever:
    def search(
        self,
        session: Session,
        *,
        project_id: str,
        query: str,
        limit: int = 8,
        filters: RetrievalFilters | None = None,
    ) -> list[RetrievedChunk]:
        query_terms = tokenize(query)
        if not query_terms:
            return []

        stmt = (
            select(TextChunk, DocumentRecord)
            .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
            .where(TextChunk.project_id == project_id, DocumentRecord.ingest_status == "ready")
        )
        if filters and filters.source_types:
            stmt = stmt.where(DocumentRecord.source_type.in_(filters.source_types))

        filter_terms = _build_filter_terms(query_terms)
        if filter_terms:
            stmt = stmt.where(or_(*(TextChunk.content.ilike(f"%{term}%") for term in filter_terms)))

        candidate_limit = min(max(limit * 40, 200), MAX_LEXICAL_CANDIDATES)
        rows = session.execute(stmt.limit(candidate_limit)).all()
        if not rows:
            return []

        doc_tokens = [tokenize(chunk.content) for chunk, _ in rows]
        avg_len = sum(len(tokens) for tokens in doc_tokens) / max(len(doc_tokens), 1)
        doc_freq: Counter[str] = Counter()
        for tokens in doc_tokens:
            doc_freq.update(set(tokens))
        scored: list[tuple[float, RetrievedChunk]] = []
        for (chunk, document), tokens in zip(rows, doc_tokens):
            score = _bm25(query_terms, tokens, doc_freq, len(doc_tokens), avg_len)
            content_lower = chunk.content.lower()
            if query.lower() in content_lower:
                score += 2.0
            if score <= 0:
                continue
            scored.append(
                (
                    score,
                    RetrievedChunk(
                        chunk_id=chunk.id,
                        document_id=document.id,
                        document_title=document.title or document.filename,
                        filename=document.filename,
                        source_type=document.source_type,
                        content=chunk.content,
                        score=score,
                        page_number=chunk.page_number,
                        metadata=chunk.metadata_json or {},
                    ),
                )
            )
        scored.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in scored[:limit]]


def _build_filter_terms(query_terms: list[str]) -> list[str]:
    ranked: list[str] = []
    seen: set[str] = set()
    for term in sorted(query_terms, key=len, reverse=True):
        if len(term) < MIN_FILTER_TERM_LEN:
            continue
        if term in seen:
            continue
        ranked.append(term)
        seen.add(term)
        if len(ranked) >= MAX_FILTER_TERMS:
            break
    if ranked:
        return ranked
    return query_terms[:1]


def _bm25(
    query_terms: list[str],
    document_terms: list[str],
    document_frequency: Counter[str],
    document_count: int,
    avg_len: float,
    *,
    k1: float = 1.5,
    b: float = 0.75,
) -> float:
    if not document_terms:
        return 0.0
    term_freq = Counter(document_terms)
    score = 0.0
    length = len(document_terms)
    for term in query_terms:
        if term not in term_freq:
            continue
        freq = term_freq[term]
        idf = math.log(1 + (document_count - document_frequency[term] + 0.5) / (document_frequency[term] + 0.5))
        numerator = freq * (k1 + 1)
        denominator = freq + k1 * (1 - b + b * (length / max(avg_len, 1)))
        score += idf * (numerator / denominator)
    return score
