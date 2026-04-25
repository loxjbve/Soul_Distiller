from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from app.web import routes as legacy

router = APIRouter(tags=["settings"])


@router.post("/api/settings/{service_name}")
def save_service_settings_api(
    request: Request,
    service_name: str,
    payload: legacy.ServiceSettingsBundlePayload,
    session: legacy.SessionDep,
):
    return legacy.save_service_settings_api(request, service_name, payload, session)


@router.get("/api/settings/models")
def list_models_api(
    request: Request,
    service: Annotated[str, Query(pattern="^(chat|embedding)$")],
    session: legacy.SessionDep,
    config_id: str | None = Query(default=None),
):
    return legacy.list_models_api(request, service, session, config_id)
