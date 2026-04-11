import os
import json
from app.db import Database
from app.storage import repository
from app.retrieval.service import RetrievalService
from app.schemas import ServiceConfig


def test_hybrid_search():
    from app.config import AppConfig
    config = AppConfig()
    db = Database(config.database_url)
    
    with db.session() as session:
        # Create dummy project
        project = repository.create_project(session, "Hybrid Test", "hybrid-test")
        
        # Add some chunks
        doc = repository.create_document(
            session,
            project_id=project.id,
            filename="test.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="text",
            title="Test Doc",
            raw_text="This is a test document about RAG architecture.",
            clean_text="This is a test document about RAG architecture.",
            ingest_status="ready"
        )
        
        repository.replace_document_chunks(
            session,
            doc.id,
            [
                {
                    "project_id": project.id,
                    "chunk_index": 0,
                    "content": "RAG architecture uses embeddings and vector search to find relevant context.",
                    "start_offset": 0,
                    "end_offset": 75,
                    "token_count": 10,
                    "metadata_json": {},
                },
                {
                    "project_id": project.id,
                    "chunk_index": 1,
                    "content": "BM25 is a lexical search algorithm based on term frequency and inverse document frequency.",
                    "start_offset": 76,
                    "end_offset": 160,
                    "token_count": 12,
                    "metadata_json": {},
                }
            ]
        )
        
        # Test RetrievalService
        service = RetrievalService()
        
        # Create a mock LLM config
        llm_config = ServiceConfig(
            provider_kind="openai",
            api_mode="chat",
            model="gpt-4o-mini",
            credentials={"api_key": os.environ.get("OPENAI_API_KEY", "dummy")}
        )
        
        embedding_config = ServiceConfig(
            provider_kind="openai",
            api_mode="embeddings",
            model="text-embedding-3-small",
            credentials={"api_key": os.environ.get("OPENAI_API_KEY", "dummy")}
        )
        
        # Test query
        hits, mode, trace = service.search(
            session,
            project_id=project.id,
            query="What is BM25?",
            embedding_config=embedding_config,
            llm_config=llm_config,
            limit=2
        )
        
        print("Mode:", mode)
        print("Trace:", json.dumps(trace, indent=2, ensure_ascii=False))
        print("Hits:")
        for hit in hits:
            print(f"- [{hit.score:.4f}] {hit.content}")

if __name__ == "__main__":
    test_hybrid_search()