from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from app.config import AppConfig
from app.pipeline.chunking import chunk_segments
from app.pipeline.extractors import ExtractionError, extract_text
from app.storage import repository


class DocumentIngestService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

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
        try:
            result = extract_text(filename, content)
            chunks = chunk_segments(result.segments)
            document = repository.create_document(
                session,
                id=document_id,
                project_id=project_id,
                filename=filename,
                mime_type=mime_type,
                extension=Path(filename).suffix.lower(),
                source_type=source,
                title=result.title or filename,
                author_guess=result.author_guess,
                created_at_guess=result.created_at_guess,
                raw_text=result.raw_text,
                clean_text=result.clean_text,
                language=result.language,
                metadata_json=result.metadata,
                ingest_status="ready",
                error_message=None,
                storage_path=str(storage_path),
            )
            repository.replace_document_chunks(
                session,
                document_id=document.id,
                chunks=[
                    {
                        "project_id": project_id,
                        "chunk_index": chunk.chunk_index,
                        "content": chunk.content,
                        "start_offset": chunk.start_offset,
                        "end_offset": chunk.end_offset,
                        "page_number": chunk.page_number,
                        "token_count": chunk.token_count,
                        "metadata_json": chunk.metadata,
                    }
                    for chunk in chunks
                ],
            )
            return document
        except ExtractionError as exc:
            return repository.create_document(
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
                metadata_json={"format": Path(filename).suffix.lower()},
                ingest_status="failed",
                error_message=str(exc),
                storage_path=str(storage_path),
            )

    def _store_upload(self, project_id: str, document_id: str, filename: str, content: bytes) -> Path:
        project_dir = self.config.upload_dir / project_id
        project_dir.mkdir(parents=True, exist_ok=True)
        safe_name = f"{document_id}_{Path(filename).name}"
        destination = project_dir / safe_name
        destination.write_bytes(content)
        return destination

    @staticmethod
    def _infer_source_type(filename: str) -> str:
        extension = Path(filename).suffix.lower()
        mapping = {
            ".html": "html",
            ".htm": "html",
            ".json": "json",
            ".docx": "docx",
            ".pdf": "pdf",
            ".txt": "text",
            ".md": "markdown",
            ".log": "chat-log",
        }
        return mapping.get(extension, "document")
