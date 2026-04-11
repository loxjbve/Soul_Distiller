import time
from app.db import Database
from app.config import config
from app.schemas import ServiceConfig
from pathlib import Path
from app.retrieval.vector_store import VectorStoreManager
from app.pipeline.ingest_task import IngestTaskManager
from app.storage.repository import create_project, create_document
from sqlalchemy import select
from app.models import DocumentRecord
import logging
import shutil

logging.basicConfig(level=logging.ERROR)

db = Database(config)
db.create_all()

vsm = VectorStoreManager(Path("/tmp/vectors"))
shutil.rmtree("/tmp/vectors", ignore_errors=True)

tm = IngestTaskManager(db, vsm)
tm.set_embedding_config(ServiceConfig(
    provider_kind="openai",
    model="text-embedding-3-small",
    api_key="sk-test",
    api_mode="embeddings"
))

with db.session() as session:
    proj = create_project(session, name="Test Project Multiple", description="")
    session.flush()
    docs = []
    for i in range(3):
        doc = create_document(
            session,
            id=f"doc{i}",
            project_id=proj.id,
            filename=f"test{i}.txt",
            extension=".txt",
            mime_type="text/plain",
            source_type="document",
            title=f"test{i}.txt",
            raw_text="",
            clean_text="",
            language="en",
            metadata_json={},
            ingest_status="pending",
            storage_path=f"/tmp/test{i}.txt"
        )
        docs.append(doc)
        with open(f"/tmp/test{i}.txt", "w") as f:
            f.write(f"Hello world! {i} " * 100)
    session.commit()

for i in range(3):
    tm.submit(proj.id, f"doc{i}", f"test{i}.txt", f"/tmp/test{i}.txt")

for _ in range(10):
    time.sleep(1)
    with db.session() as session:
        all_done = True
        for i in range(3):
            doc_db = session.scalar(select(DocumentRecord).where(DocumentRecord.id == f"doc{i}"))
            print(f"Doc {i} status: {doc_db.ingest_status}, error: {doc_db.error_message}")
            if doc_db.ingest_status not in ["ready", "failed"]:
                all_done = False
        if all_done:
            break
