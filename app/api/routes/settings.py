from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from app.api.schemas.settings import ServiceSettingsBundlePayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["settings"])


@router.post("/api/settings/{service_name}")
def save_service_settings_api(
    request: Request,
    service_name: str,
    payload: ServiceSettingsBundlePayload,
    session: SessionDep,
):
    return runtime.save_service_settings_api(request, service_name, payload, session)


@router.get("/api/settings/models")
def list_models_api(
    request: Request,
    service: Annotated[str, Query(pattern="^(chat|embedding)$")],
    session: SessionDep,
    config_id: str | None = Query(default=None),
):
    return runtime.list_models_api(request, service, session, config_id)
