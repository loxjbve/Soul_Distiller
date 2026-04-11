from __future__ import annotations

from app.pipeline.chunking import chunk_segments
from app.retrieval.service import RetrievalService
from app.schemas import ExtractedSegment, ServiceConfig
from app.storage import repository


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
        document = repository.create_document(
            session,
            project_id=project.id,
            filename="memo.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="text",
            title="Memo",
            author_guess=None,
            created_at_guess=None,
            raw_text="Alice likes long walks and writing poetry.",
            clean_text="Alice likes long walks and writing poetry.",
            language="en",
            metadata_json={},
            ingest_status="ready",
            error_message=None,
            storage_path="memo.txt",
        )
        repository.replace_document_chunks(
            session,
            document.id,
            [
                {
                    "project_id": project.id,
                    "chunk_index": 0,
                    "content": "Alice likes long walks and writing poetry.",
                    "start_offset": 0,
                    "end_offset": 43,
                    "page_number": None,
                    "token_count": 7,
                    "metadata_json": {},
                },
                {
                    "project_id": project.id,
                    "chunk_index": 1,
                    "content": "She also collects teacups.",
                    "start_offset": 44,
                    "end_offset": 71,
                    "page_number": None,
                    "token_count": 4,
                    "metadata_json": {},
                },
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


def test_embedding_trace_records_embedding_exception_and_fallback(app):
    db = app.state.db
    with db.session() as session:
        project = repository.create_project(session, "Skip Embedding", "test")
        document = repository.create_document(
            session,
            project_id=project.id,
            filename="memo.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="text",
            title="Memo",
            author_guess=None,
            created_at_guess=None,
            raw_text="Only tea notes live here.",
            clean_text="Only tea notes live here.",
            language="en",
            metadata_json={},
            ingest_status="ready",
            error_message=None,
            storage_path="memo.txt",
        )
        repository.replace_document_chunks(
            session,
            document.id,
            [
                {
                    "project_id": project.id,
                    "chunk_index": 0,
                    "content": "Only tea notes live here.",
                    "start_offset": 0,
                    "end_offset": 25,
                    "page_number": None,
                    "token_count": 5,
                    "metadata_json": {},
                }
            ],
        )
        retrieval = RetrievalService()
        hits, mode, trace = retrieval.search(
            session,
            project_id=project.id,
            query="completely unrelated spaceship term",
            embedding_config=ServiceConfig(
                base_url="https://example.com/v1",
                api_key="sk-test",
                model="text-embedding-3-small",
                provider_kind="openai-compatible",
            ),
            limit=3,
        )
        assert mode == "lexical"
        assert hits == []
        assert trace["embedding_configured"] is True
        assert trace["embedding_attempted"] is True
        assert trace["embedding_api_called"] is True
        assert trace["fallback_reason"] == "embedding_exception"
        assert trace["embedding_error"]
        assert str(trace["embedding_url"]).endswith("/embeddings")
