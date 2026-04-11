import asyncio
from app.db import Database
from app.config import AppConfig
from app.schemas import ServiceConfig
from pathlib import Path
from app.retrieval.vector_store import VectorStoreManager
from app.pipeline.ingest_task import IngestTaskManager
from app.storage.repository import create_project, create_document
from sqlalchemy import select
from app.models import DocumentRecord

import logging
logging.basicConfig(level=logging.DEBUG)

db = Database(AppConfig(database_url="sqlite:////workspace/data/trae.db"))

vsm = VectorStoreManager(Path("/workspace/data"))
tm = IngestTaskManager(db, vsm)
tm.set_embedding_config(ServiceConfig(
    provider_kind="openai",
    model="text-embedding-3-small",
    api_key="sk-test",
    api_mode="embeddings"
))

with db.session() as session:
    proj = create_project(session, name="Test Project", description="")
    session.flush()
    doc = create_document(
        session,
        id="doc1",
        project_id=proj.id,
        filename="test.txt",
        extension=".txt",
        mime_type="text/plain",
        source_type="document",
        title="test.txt",
        raw_text="",
        clean_text="",
        language="en",
        metadata_json={},
        ingest_status="pending",
        storage_path="/tmp/test.txt"
    )
    session.commit()

with open("/tmp/test.txt", "w") as f:
    f.write("Hello world! " * 1000)

task = tm.submit(proj.id, "doc1", "test.txt", "/tmp/test.txt")

import time
for i in range(15):
    time.sleep(1)
    with db.session() as session:
        doc_db = session.scalar(select(DocumentRecord).where(DocumentRecord.id == "doc1"))
        print(f"Status in DB: {doc_db.ingest_status}")
        if doc_db.ingest_status in ["ready", "failed"]:
            print(f"Finished! Error: {doc_db.error_message}")
            break
