from __future__ import annotations

from fastapi import APIRouter, Request

from app.web import routes as legacy

router = APIRouter(tags=["assets"])


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/{document_key}")
def download_asset_draft_document_api(project_id: str, draft_id: str, document_key: str, session: legacy.SessionDep):
    return legacy.download_asset_draft_document_api(project_id, draft_id, document_key, session)


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/bundle")
def download_asset_draft_bundle_api(project_id: str, draft_id: str, session: legacy.SessionDep):
    return legacy.download_asset_draft_bundle_api(project_id, draft_id, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/{document_key}")
def download_asset_version_document_api(project_id: str, version_id: str, document_key: str, session: legacy.SessionDep):
    return legacy.download_asset_version_document_api(project_id, version_id, document_key, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/bundle")
def download_asset_version_bundle_api(project_id: str, version_id: str, session: legacy.SessionDep):
    return legacy.download_asset_version_bundle_api(project_id, version_id, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/download")
def download_asset_version_api(project_id: str, version_id: str, session: legacy.SessionDep):
    return legacy.download_asset_version_api(project_id, version_id, session)


@router.post("/api/projects/{project_id}/assets/generate/stream")
def generate_asset_stream_api(request: Request, project_id: str, payload: legacy.AssetGeneratePayload):
    return legacy.generate_asset_stream_api(request, project_id, payload)


@router.post("/api/projects/{project_id}/assets/generate")
def generate_asset_api(
    request: Request,
    project_id: str,
    payload: legacy.AssetGeneratePayload,
    session: legacy.SessionDep,
):
    return legacy.generate_asset_api(request, project_id, payload, session)


@router.post("/api/projects/{project_id}/assets/{draft_id}/save")
def save_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: legacy.AssetSavePayload,
    session: legacy.SessionDep,
):
    return legacy.save_asset_api(request, project_id, draft_id, payload, session)


@router.post("/api/projects/{project_id}/assets/{draft_id}/publish")
def publish_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: legacy.AssetGeneratePayload,
    session: legacy.SessionDep,
):
    return legacy.publish_asset_api(request, project_id, draft_id, payload, session)
