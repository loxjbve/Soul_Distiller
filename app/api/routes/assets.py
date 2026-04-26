from __future__ import annotations

from fastapi import APIRouter, Request

from app.api.schemas.assets import AssetGeneratePayload, AssetSavePayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["assets"])


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/{document_key}")
def download_asset_draft_document_api(project_id: str, draft_id: str, document_key: str, session: SessionDep):
    return runtime.download_asset_draft_document_api(project_id, draft_id, document_key, session)


@router.get("/api/projects/{project_id}/assets/{draft_id}/exports/bundle")
def download_asset_draft_bundle_api(project_id: str, draft_id: str, session: SessionDep):
    return runtime.download_asset_draft_bundle_api(project_id, draft_id, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/{document_key}")
def download_asset_version_document_api(project_id: str, version_id: str, document_key: str, session: SessionDep):
    return runtime.download_asset_version_document_api(project_id, version_id, document_key, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/exports/bundle")
def download_asset_version_bundle_api(project_id: str, version_id: str, session: SessionDep):
    return runtime.download_asset_version_bundle_api(project_id, version_id, session)


@router.get("/api/projects/{project_id}/asset-versions/{version_id}/download")
def download_asset_version_api(project_id: str, version_id: str, session: SessionDep):
    return runtime.download_asset_version_api(project_id, version_id, session)


@router.post("/api/projects/{project_id}/assets/generate/stream")
def generate_asset_stream_api(request: Request, project_id: str, payload: AssetGeneratePayload):
    return runtime.generate_asset_stream_api(request, project_id, payload)


@router.post("/api/projects/{project_id}/assets/generate")
def generate_asset_api(
    request: Request,
    project_id: str,
    payload: AssetGeneratePayload,
    session: SessionDep,
):
    return runtime.generate_asset_api(request, project_id, payload, session)


@router.post("/api/projects/{project_id}/assets/{draft_id}/save")
def save_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: AssetSavePayload,
    session: SessionDep,
):
    return runtime.save_asset_api(request, project_id, draft_id, payload, session)


@router.post("/api/projects/{project_id}/assets/{draft_id}/publish")
def publish_asset_api(
    request: Request,
    project_id: str,
    draft_id: str,
    payload: AssetGeneratePayload,
    session: SessionDep,
):
    return runtime.publish_asset_api(request, project_id, draft_id, payload, session)


@router.post("/api/projects/{project_id}/skills/generate")
def generate_skill_api(request: Request, project_id: str, session: SessionDep):
    return runtime.generate_skill_api(request, project_id, session)


@router.post("/api/projects/{project_id}/skills/{draft_id}/publish")
def publish_skill_api(request: Request, project_id: str, draft_id: str, session: SessionDep):
    return runtime.publish_skill_api(request, project_id, draft_id, session)
