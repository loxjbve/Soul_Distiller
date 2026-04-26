from __future__ import annotations

from pydantic import BaseModel


class ProjectCreatePayload(BaseModel):
    name: str | None = None
    description: str | None = None
    mode: str = "group"


class ChatPayload(BaseModel):
    message: str
    session_id: str | None = None


class DocumentUpdatePayload(BaseModel):
    title: str | None = None
    source_type: str | None = None
    user_note: str | None = None


class TextDocumentCreatePayload(BaseModel):
    title: str | None = None
    content: str
    source_type: str | None = None
    user_note: str | None = None
