from __future__ import annotations

from pydantic import BaseModel, Field


class ServiceSettingConfigPayload(BaseModel):
    id: str | None = None
    label: str | None = None
    provider_kind: str = "openai"
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    api_mode: str | None = None
    available_models: list[str] = Field(default_factory=list)


class ServiceSettingsBundlePayload(BaseModel):
    active_config_id: str | None = None
    discover_config_id: str | None = None
    fallback_order: list[str] = Field(default_factory=list)
    configs: list[ServiceSettingConfigPayload] = Field(default_factory=list)
