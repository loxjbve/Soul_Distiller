from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile
from sqlalchemy.orm import Session

from app.models import DocumentRecord
from app.storage import repository


class DocumentIngestService:
    def __init__(self, config) -> None:
        self._config = config

    async def create_documents_from_uploads(
        self,
        session: Session,
        *,
        project_id: str,
        uploads: list[UploadFile],
    ) -> list[DocumentRecord]:
        created: list[DocumentRecord] = []
        for upload in uploads:
            filename = (upload.filename or "").strip()
            if not filename:
                await upload.close()
                continue
            content = await upload.read()
            created.append(
                self.ingest_bytes(
                    session,
                    project_id=project_id,
                    filename=filename,
                    content=content,
                    mime_type=upload.content_type,
                )
            )
            await upload.close()
        session.flush()
        return created

    def create_text_document(
        self,
        session: Session,
        *,
        project_id: str,
        title: str,
        content: str,
        source_type: str | None = None,
        user_note: str | None = None,
    ) -> DocumentRecord:
        normalized_title = (title or "").strip() or f"text-{uuid4().hex[:8]}"
        filename_stem = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in normalized_title).strip("._")
        filename = f"{filename_stem or 'stone-text'}.txt"
        document = self.ingest_bytes(
            session,
            project_id=project_id,
            filename=filename,
            content=str(content or "").encode("utf-8"),
            mime_type="text/plain",
            source_type=source_type or "text",
        )
        metadata = dict(document.metadata_json or {})
        metadata["user_note"] = (user_note or "").strip()
        metadata["stone_text_entry"] = True
        document.metadata_json = metadata
        document.title = normalized_title
        session.flush()
        return document

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
        project = repository.get_project(session, project_id)
        source = source_type or self._infer_source_type(filename)
        if project and project.mode == "telegram" and Path(filename).suffix.lower() == ".json":
            source = "telegram_export"
            try:
                import json
                data = json.loads(content.decode("utf-8", errors="ignore"))
                group_name = data.get("name")
                if group_name and project.name == "未命名 Telegram 项目":
                    project.name = group_name
            except Exception:
                pass
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
