from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import UploadFile
from sqlalchemy.orm import Session

from app.models import DocumentRecord
from app.storage import repository
from app.utils.text import normalize_whitespace

STONE_MIN_ARTICLE_CHARS = 50
STONE_IGNORED_RICH_TEXT_TYPES = {"link", "hashtag", "mention", "email", "phone", "bot_command"}


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
        project = repository.get_project(session, project_id)
        created: list[DocumentRecord] = []
        for upload in uploads:
            filename = (upload.filename or "").strip()
            if not filename:
                await upload.close()
                continue
            content = await upload.read()
            extension = Path(filename).suffix.lower()
            if project and project.mode == "stone" and extension == ".json":
                created.extend(
                    self.create_text_documents_from_stone_json(
                        session,
                        project_id=project_id,
                        filename=filename,
                        content=content,
                    )
                )
            else:
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
        title: str | None,
        content: str,
        source_type: str | None = None,
        user_note: str | None = None,
        metadata_extra: dict[str, Any] | None = None,
    ) -> DocumentRecord:
        normalized_title = (title or "").strip()
        filename_seed = normalized_title or self._derive_stone_title(content) or f"text-{uuid4().hex[:8]}"
        safe_seed = filename_seed[:80]
        normalized_title = normalized_title or None
        filename_stem = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in safe_seed).strip("._")
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
        if user_note:
            metadata["user_note"] = (user_note or "").strip()
        metadata["stone_text_entry"] = True
        for key, value in dict(metadata_extra or {}).items():
            metadata[key] = value
        document.metadata_json = metadata
        document.title = normalized_title
        session.flush()
        return document

    def create_text_documents_from_stone_json(
        self,
        session: Session,
        *,
        project_id: str,
        filename: str,
        content: bytes,
    ) -> list[DocumentRecord]:
        payload = self._load_stone_json_payload(content)
        candidates = self._extract_stone_json_articles(payload, source_filename=filename)
        if not candidates:
            raise ValueError("No valid articles were found in the uploaded JSON.")
        created: list[DocumentRecord] = []
        for index, candidate in enumerate(candidates, start=1):
            document = self.create_text_document(
                session,
                project_id=project_id,
                title=candidate.get("title"),
                content=str(candidate.get("content") or ""),
                source_type=str(candidate.get("source_type") or "stone_json_article"),
                metadata_extra={
                    "stone_json_import": {
                        **dict(candidate.get("metadata") or {}),
                        "source_filename": filename,
                        "article_index": index,
                    }
                },
            )
            created.append(document)
        return created

    @staticmethod
    def _load_stone_json_payload(content: bytes) -> Any:
        encodings = ("utf-8-sig", "utf-8", "gb18030", "big5")
        last_error: Exception | None = None
        for encoding in encodings:
            try:
                return json.loads(content.decode(encoding))
            except Exception as exc:
                last_error = exc
        raise ValueError(f"Invalid JSON payload: {last_error}")

    def _extract_stone_json_articles(self, payload: Any, *, source_filename: str) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("messages"), list):
            root_title = str(payload.get("name") or payload.get("title") or Path(source_filename).stem).strip() or None
            return self._extract_articles_from_message_feed(payload.get("messages") or [], root_title=root_title)
        articles: list[dict[str, Any]] = []
        self._walk_generic_json_articles(payload, articles, source_filename=source_filename)
        return articles

    def _extract_articles_from_message_feed(
        self,
        messages: list[Any],
        *,
        root_title: str | None,
    ) -> list[dict[str, Any]]:
        articles: list[dict[str, Any]] = []
        for index, item in enumerate(messages, start=1):
            if not isinstance(item, dict):
                continue
            text = self._flatten_message_text(item)
            if not self._is_valid_stone_article(text):
                continue
            title = self._derive_stone_title(text)
            metadata = {
                "import_kind": "telegram_export_json",
                "message_id": item.get("id"),
                "date": item.get("date"),
                "author": item.get("author") or item.get("from"),
                "root_title": root_title,
                "message_index": index,
            }
            articles.append(
                {
                    "title": title,
                    "content": text,
                    "source_type": "stone_json_article",
                    "metadata": metadata,
                }
            )
        return articles

    def _walk_generic_json_articles(
        self,
        value: Any,
        articles: list[dict[str, Any]],
        *,
        source_filename: str,
        path: str = "root",
    ) -> None:
        if isinstance(value, dict):
            if any(key in value for key in ("text", "content", "body", "article")):
                text = self._flatten_rich_text(
                    value.get("text")
                    if "text" in value
                    else value.get("content")
                    if "content" in value
                    else value.get("body")
                    if "body" in value
                    else value.get("article")
                )
                if self._is_valid_stone_article(text):
                    title_seed = value.get("title") or value.get("name") or value.get("subject")
                    articles.append(
                        {
                            "title": str(title_seed).strip() or self._derive_stone_title(text),
                            "content": text,
                            "source_type": "stone_json_article",
                            "metadata": {
                                "import_kind": "generic_json",
                                "json_path": path,
                                "source_filename": source_filename,
                            },
                        }
                    )
            for key, child in value.items():
                self._walk_generic_json_articles(child, articles, source_filename=source_filename, path=f"{path}.{key}")
            return
        if isinstance(value, list):
            for index, child in enumerate(value):
                self._walk_generic_json_articles(child, articles, source_filename=source_filename, path=f"{path}[{index}]")

    def _flatten_message_text(self, message: dict[str, Any]) -> str:
        if isinstance(message.get("text_entities"), list) and message.get("text_entities"):
            return self._flatten_rich_text(message.get("text_entities"))
        return self._flatten_rich_text(message.get("text"))

    def _flatten_rich_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return normalize_whitespace(value)
        if isinstance(value, dict):
            kind = str(value.get("type") or "").strip().lower()
            if kind in STONE_IGNORED_RICH_TEXT_TYPES:
                return ""
            return self._flatten_rich_text(value.get("text"))
        if isinstance(value, list):
            parts = [self._flatten_rich_text(item) for item in value]
            return normalize_whitespace("\n".join(part for part in parts if part))
        return normalize_whitespace(str(value))

    @staticmethod
    def _is_valid_stone_article(text: str) -> bool:
        normalized = normalize_whitespace(text or "")
        if len(normalized) <= STONE_MIN_ARTICLE_CHARS:
            return False
        return re.search(r"[\u4e00-\u9fff]", normalized) is not None

    @staticmethod
    def _derive_stone_title(content: str, *, limit: int = 48) -> str | None:
        normalized = normalize_whitespace(content or "")
        if not normalized:
            return None
        first_line = next((line.strip() for line in normalized.splitlines() if line.strip()), normalized)
        first_line = re.sub(r"\s+", " ", first_line).strip()
        if not first_line:
            return None
        return first_line[:limit]

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
