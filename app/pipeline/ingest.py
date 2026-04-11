from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models import DocumentRecord, utcnow
from app.storage import repository


class DocumentIngestService:
    def __init__(self, config) -> None:
        self._config = config

    def _store_upload(self, project_id: str, document_id: str, filename: str, content: bytes) -> Path:
        upload_dir = self._config.upload_dir / project_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        storage_path = upload_dir / f"{document_id}{Path(filename).suffix.lower()}"
        with open(storage_path, "wb") as f:
            f.write(content)
        return storage_path

    def _infer_source_type(self, filename: str) -> str:
        ext = Path(filename).suffix.lower()
        source_map = {
            ".json": "json",
            ".jsonl": "jsonl",
            ".txt": "text",
            ".md": "markdown",
            ".log": "log",
            ".docx": "docx",
            ".pdf": "pdf",
            ".html": "html",
            ".htm": "html",
        }
        return source_map.get(ext, "document")

    def ingest_bytes(
        self,
        session: Session,
        *,
        project_id: str,
        filename: str,
        content: bytes,
        mime_type: str | None = None,
        source_type: str | None = None,
    ):
        source = source_type or self._infer_source_type(filename)
        document_id = str(uuid4())
        storage_path = self._store_upload(project_id, document_id, filename, content)

        document = repository.create_document(
            session,
            id=document_id,
            project_id=project_id,
            filename=filename,
            mime_type=mime_type,
            extension=Path(filename).suffix.lower(),
            source_type=source,
            title=filename,
            author_guess=None,
            created_at_guess=None,
            raw_text="",
            clean_text="",
            language="unknown",
            metadata_json={},
            ingest_status="pending",
            error_message=None,
            storage_path=str(storage_path),
        )
        session.flush()
        return document
