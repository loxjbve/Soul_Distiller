from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class AssetGeneratePayload(BaseModel):
    asset_kind: str = "cc_skill"


class AssetSavePayload(BaseModel):
    asset_kind: str = "cc_skill"
    markdown_text: str
    json_payload: dict[str, Any]
    prompt_text: str
    notes: str | None = None
