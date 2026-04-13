from __future__ import annotations

import shutil
from typing import Any
from pathlib import Path
from uuid import uuid4

from app.llm.client import OpenAICompatibleClient
from app.pipeline.chunking import chunk_segments
from app.retrieval.service import RetrievalService
from app.retrieval.vector_store import VectorStoreBatch, VectorStoreManager, VectorStoreResolution, model_key_for
from app.schemas import ExtractedSegment, ServiceConfig
from app.storage import repository


class DummyVectorStore:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self.results = results
        self.queries: list[tuple[list[float], int]] = []

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        self.queries.append((list(query_vector), top_k))
        return list(self.results[:top_k])


class DummyVectorStoreManager:
    def __init__(self, resolution: VectorStoreResolution) -> None:
        self.resolution = resolution
        self.calls: list[dict[str, Any]] = []

    def resolve_store(self, project_id: str, provider: str = "auto", model: str | None = None, *, allow_memory: bool | None = None):
        self.calls.append(
            {
                "project_id": project_id,
                "provider": provider,
                "model": model,
                "allow_memory": allow_memory,
            }
        )
        return self.resolution


def _patch_embedding_client(
    monkeypatch,
    *,
    vector: list[float] | None = None,
    embeddings_side_effect: Exception | None = None,
):
    resolved_vector = vector or [0.4, 0.2, 0.1]

    def fake_endpoint_url(self, path: str) -> str:
        return f"https://embeddings.test{path}"

    def fake_resolve_model(self) -> str:
        return self.config.model or "resolved-embedding-model"

    def fake_embeddings(self, inputs, *, model=None, timeout=None):
        del model, timeout
        if embeddings_side_effect is not None:
            raise embeddings_side_effect
        return [list(resolved_vector) for _ in inputs]

    monkeypatch.setattr(OpenAICompatibleClient, "endpoint_url", fake_endpoint_url)
    monkeypatch.setattr(OpenAICompatibleClient, "resolve_model", fake_resolve_model)
    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fake_embeddings)


def _create_document_with_chunks(session, *, project_id: str, filename: str, contents: list[str]):
    joined = " ".join(contents)
    document = repository.create_document(
        session,
        project_id=project_id,
        filename=filename,
        mime_type="text/plain",
        extension=".txt",
        source_type="text",
        title=filename,
        author_guess=None,
        created_at_guess=None,
        raw_text=joined,
        clean_text=joined,
        language="en",
        metadata_json={},
        ingest_status="ready",
        error_message=None,
        storage_path=filename,
    )
    repository.replace_document_chunks(
        session,
        document.id,
        [
            {
                "project_id": project_id,
                "chunk_index": index,
                "content": content,
                "start_offset": index * 100,
                "end_offset": index * 100 + len(content),
                "page_number": None,
                "token_count": max(1, len(content.split())),
                "metadata_json": {},
            }
            for index, content in enumerate(contents)
        ],
    )
    return document


def test_chunking_preserves_page_number():
    segments = [ExtractedSegment(text="A" * 1500, metadata={"page_number": 3})]
    chunks = chunk_segments(segments, chunk_size=600, overlap=100)
    assert len(chunks) >= 3
    assert all(chunk.page_number == 3 for chunk in chunks)
    assert chunks[0].start_offset == 0
    assert chunks[1].start_offset < chunks[0].end_offset


def test_lexical_retrieval_returns_relevant_chunk(app):
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Alice", "test")
        _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="memo.txt",
            contents=[
                "Alice likes long walks and writing poetry.",
                "She also collects teacups.",
            ],
        )
        retrieval = RetrievalService()
        hits, mode, trace = retrieval.search(
            session,
            project_id=project.id,
            query="writing poetry",
            embedding_config=None,
            limit=3,
        )
        assert mode == "lexical"
        assert trace["embedding_configured"] is False
        assert hits
        assert hits[0].filename == "memo.txt"
        assert "poetry" in hits[0].content


def test_embedding_degrades_to_lexical_when_vector_store_is_unavailable(app, monkeypatch):
    _patch_embedding_client(monkeypatch, embeddings_side_effect=AssertionError("embeddings should not be called"))
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Skip Embedding", "test")
        _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="memo.txt",
            contents=["Only tea notes live here."],
        )
        hits, mode, trace = app.state.retrieval.search(
            session,
            project_id=project.id,
            query="tea notes",
            embedding_config=ServiceConfig(
                base_url="https://example.com/v1",
                api_key="sk-test",
                model="text-embedding-3-small",
                provider_kind="openai-compatible",
            ),
            limit=3,
        )
        assert mode == "lexical"
        assert hits
        assert trace["embedding_configured"] is True
        assert trace["embedding_attempted"] is True
        assert trace["embedding_api_called"] is False
        assert trace["semantic_degraded"] is True
        assert trace["degraded_reason"] == "vector_store_unavailable"
        assert trace["vector_store_available"] is False
        assert trace["fallback_reason"] == "vector_store_unavailable"
        assert "AttributeError" not in str(trace.get("vector_store_error") or "")
        assert str(trace["embedding_url"]).endswith("/embeddings")


def test_embedding_search_uses_manager_resolved_store_without_db_fallback(app, monkeypatch):
    _patch_embedding_client(monkeypatch)
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Hybrid", "test")
        document = _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="memo.txt",
            contents=["Alpha note about tea rituals."],
        )
        chunk_id = str(session.execute(select_chunk_id_stmt(document.id)).scalar_one())
        store = DummyVectorStore([{"id": chunk_id, "score": 0.98}])
        manager = DummyVectorStoreManager(
            VectorStoreResolution(
                store=store,
                backend="faiss",
                provider="auto",
                model="text-embedding-3-small",
                model_key=model_key_for("text-embedding-3-small"),
                available=True,
            )
        )
        retrieval = RetrievalService(vector_store=manager)
        hits, mode, trace = retrieval.search(
            session,
            project_id=project.id,
            query="tea rituals",
            embedding_config=ServiceConfig(
                base_url="https://example.com/v1",
                api_key="sk-test",
                model="text-embedding-3-small",
                provider_kind="openai-compatible",
            ),
            limit=3,
        )
        assert mode == "hybrid"
        assert hits
        assert manager.calls
        assert trace["embedding_success"] is True
        assert trace["vector_store_backend"] == "faiss"
        assert trace["vector_store_available"] is True
        assert trace["semantic_degraded"] is False
        assert trace.get("vector_store_error") is None
        assert trace.get("fallback_reason") is None


def test_vector_store_results_keep_score_order_before_document_cap(app, monkeypatch):
    _patch_embedding_client(monkeypatch)
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Score Order", "test")
        lower_doc = _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="lower.txt",
            contents=["lower score chunk"],
        )
        higher_doc = _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="higher.txt",
            contents=["higher score chunk"],
        )
        lower_chunk_id = session.execute(select_chunk_id_stmt(lower_doc.id)).scalar_one()
        higher_chunk_id = session.execute(select_chunk_id_stmt(higher_doc.id)).scalar_one()
        manager = DummyVectorStoreManager(
            VectorStoreResolution(
                store=DummyVectorStore(
                    [
                        {"id": str(higher_chunk_id), "score": 0.91},
                        {"id": str(lower_chunk_id), "score": 0.30},
                    ]
                ),
                backend="faiss",
                provider="auto",
                model="text-embedding-3-small",
                model_key=model_key_for("text-embedding-3-small"),
                available=True,
            )
        )
        retrieval = RetrievalService(vector_store=manager)
        hits, trace = retrieval.embedding.search(
            session,
            project_id=project.id,
            query="score",
            config=ServiceConfig(
                base_url="https://example.com/v1",
                api_key="sk-test",
                model="text-embedding-3-small",
                provider_kind="openai-compatible",
            ),
            limit=1,
        )
        assert hits
        assert hits[0].document_id == higher_doc.id
        assert trace["candidate_chunks"] == 2
        assert trace["selected_documents"] == 1


def test_embedding_context_expansion_batches_neighbor_query(app, monkeypatch):
    _patch_embedding_client(monkeypatch)
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Context", "test")
        document = _create_document_with_chunks(
            session,
            project_id=project.id,
            filename="context.txt",
            contents=[
                "zero left context",
                "one anchor text",
                "two middle bridge",
                "three anchor text",
                "four right context",
            ],
        )
        chunk_ids = list(session.execute(select_chunk_ids_stmt(document.id)).scalars().all())
        manager = DummyVectorStoreManager(
            VectorStoreResolution(
                store=DummyVectorStore(
                    [
                        {"id": str(chunk_ids[1]), "score": 0.88},
                        {"id": str(chunk_ids[3]), "score": 0.79},
                    ]
                ),
                backend="faiss",
                provider="auto",
                model="text-embedding-3-small",
                model_key=model_key_for("text-embedding-3-small"),
                available=True,
            )
        )
        retrieval = RetrievalService(vector_store=manager)
        original_execute = session.execute
        counters = {"context_queries": 0}

        def wrapped_execute(statement, *args, **kwargs):
            text = str(statement)
            if "chunks.document_id AS document_id" in text and "ORDER BY chunks.document_id ASC, chunks.chunk_index ASC" in text:
                counters["context_queries"] += 1
            return original_execute(statement, *args, **kwargs)

        monkeypatch.setattr(session, "execute", wrapped_execute)
        hits, trace = retrieval.embedding.search(
            session,
            project_id=project.id,
            query="anchor",
            config=ServiceConfig(
                base_url="https://example.com/v1",
                api_key="sk-test",
                model="text-embedding-3-small",
                provider_kind="openai-compatible",
            ),
            limit=2,
        )
        assert len(hits) == 2
        assert counters["context_queries"] == 1
        assert "zero left context" in hits[0].content
        assert "four right context" in hits[1].content
        assert trace["candidate_chunks"] == 2


def test_vector_store_manager_scopes_indexes_by_model_and_rebuilds():
    workspace_root = Path("e:\\Dev\\--\\.test-workspaces")
    workspace_root.mkdir(parents=True, exist_ok=True)
    root_dir = workspace_root / f"vector-store-manager-{uuid4().hex}"
    root_dir.mkdir(parents=True, exist_ok=False)
    try:
        manager = VectorStoreManager(root_dir, allow_memory_fallback=True)
        manager.rebuild_project(
            "project-1",
            {
                "model-a": VectorStoreBatch(ids=["a-1"], vectors=[[1.0, 0.0]]),
                "model-b": VectorStoreBatch(ids=["b-1"], vectors=[[0.0, 1.0]]),
            },
        )

        store_a = manager.resolve_store("project-1", model="model-a").store
        store_b = manager.resolve_store("project-1", model="model-b").store
        assert store_a is not None
        assert store_b is not None
        assert store_a.search([1.0, 0.0], top_k=5)[0]["id"] == "a-1"
        assert store_b.search([0.0, 1.0], top_k=5)[0]["id"] == "b-1"

        manager.rebuild_project(
            "project-1",
            {
                "model-b": VectorStoreBatch(ids=["b-2"], vectors=[[0.0, 1.0]]),
            },
        )

        refreshed_a = manager.resolve_store("project-1", model="model-a").store
        refreshed_b = manager.resolve_store("project-1", model="model-b").store
        assert refreshed_a is not None
        assert refreshed_b is not None
        assert refreshed_a.search([1.0, 0.0], top_k=5) == []
        assert refreshed_b.search([0.0, 1.0], top_k=5)[0]["id"] == "b-2"
    finally:
        shutil.rmtree(root_dir, ignore_errors=True)


def select_chunk_id_stmt(document_id: str):
    from sqlalchemy import select

    from app.models import TextChunk

    return select(TextChunk.id).where(TextChunk.document_id == document_id, TextChunk.chunk_index == 0)


def select_chunk_ids_stmt(document_id: str):
    from sqlalchemy import select

    from app.models import TextChunk

    return select(TextChunk.id).where(TextChunk.document_id == document_id).order_by(TextChunk.chunk_index.asc())
