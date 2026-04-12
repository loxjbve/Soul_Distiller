from __future__ import annotations

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

    async def create_documents_from_uploads(
        self,
        session: Session,
        *,
        project_id: str,
        uploads: list,
    ) -> list[DocumentRecord]:
        created: list[DocumentRecord] = []
        upload_dir = self._config.upload_dir / project_id
        upload_dir.mkdir(parents=True, exist_ok=True)

        for upload in uploads:
            document_id = str(uuid4())
            filename = upload.filename or "upload.bin"
            storage_path = upload_dir / f"{document_id}{Path(filename).suffix.lower()}"
            with storage_path.open("wb") as handle:
                while chunk := await upload.read(1024 * 1024):
                    handle.write(chunk)
            created.append(
                self.ingest_file(
                    session,
                    project_id=project_id,
                    document_id=document_id,
                    filename=filename,
                    storage_path=storage_path,
                    mime_type=upload.content_type,
                )
            )
        session.flush()
        return created

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

    def ingest_file(
        self,
        session: Session,
        *,
        project_id: str,
        document_id: str,
        filename: str,
        storage_path: Path,
        mime_type: str | None = None,
        source_type: str | None = None,
    ):
        source = source_type or self._infer_source_type(filename)
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
