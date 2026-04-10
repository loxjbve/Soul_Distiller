from __future__ import annotations

from app.pipeline.chunking import chunk_segments
from app.retrieval.service import RetrievalService
from app.schemas import ExtractedSegment
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
