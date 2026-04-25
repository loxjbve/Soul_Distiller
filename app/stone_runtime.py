from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from app.storage import repository

AssetPayloadValidator = Callable[[str, dict[str, Any] | None], bool]


def count_ready_documents_with_profile(
    session: Session,
    project_id: str,
    *,
    profile_key: str,
) -> tuple[int, int]:
    documents = repository.list_project_documents(session, project_id)
    ready_documents = [document for document in documents if document.ingest_status == "ready"]
    profile_count = sum(
        1
        for document in ready_documents
        if isinstance(dict(document.metadata_json or {}).get(profile_key), dict)
    )
    return len(ready_documents), profile_count


def get_latest_usable_stone_preprocess_run(
    session: Session,
    project_id: str,
    *,
    profile_key: str,
) -> Any | None:
    run = repository.get_latest_successful_stone_preprocess_run(session, project_id)
    if run:
        return run
    _ready_count, profile_count = count_ready_documents_with_profile(
        session,
        project_id,
        profile_key=profile_key,
    )
    if profile_count <= 0:
        return None
    return repository.get_latest_stone_preprocess_run(session, project_id)


def load_latest_valid_asset_payload(
    session: Session,
    project_id: str,
    *,
    asset_kind: str,
    validator: AssetPayloadValidator,
) -> dict[str, Any]:
    for version in repository.list_asset_versions(session, project_id, asset_kind=asset_kind):
        if isinstance(version.json_payload, dict) and validator(asset_kind, version.json_payload):
            return dict(version.json_payload)
    for draft in repository.list_asset_drafts(session, project_id, asset_kind=asset_kind):
        if isinstance(draft.json_payload, dict) and validator(asset_kind, draft.json_payload):
            return dict(draft.json_payload)
    return {}


def has_valid_asset_payload(
    session: Session,
    project_id: str,
    *,
    asset_kind: str,
    validator: AssetPayloadValidator,
) -> bool:
    return bool(
        load_latest_valid_asset_payload(
            session,
            project_id,
            asset_kind=asset_kind,
            validator=validator,
        )
    )
