from __future__ import annotations

from typing import Any

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from app.db.models import GeneratedArtifact, SkillDraft, SkillVersion


def delete_generated_artifacts_by_ids(session: Session, artifact_ids: list[str]) -> int:
    if not artifact_ids:
        return 0
    return session.execute(delete(GeneratedArtifact).where(GeneratedArtifact.id.in_(artifact_ids))).rowcount or 0


def delete_skill_versions_by_ids(session: Session, version_ids: list[str]) -> int:
    if not version_ids:
        return 0
    return session.execute(delete(SkillVersion).where(SkillVersion.id.in_(version_ids))).rowcount or 0


def delete_skill_drafts_by_ids(session: Session, draft_ids: list[str]) -> int:
    if not draft_ids:
        return 0
    return session.execute(delete(SkillDraft).where(SkillDraft.id.in_(draft_ids))).rowcount or 0


def create_asset_draft(
    session: Session,
    *,
    project_id: str,
    run_id: str | None,
    asset_kind: str,
    markdown_text: str,
    json_payload: dict[str, Any],
    prompt_text: str,
    notes: str | None = None,
) -> SkillDraft:
    draft = SkillDraft(
        project_id=project_id,
        run_id=run_id,
        asset_kind=asset_kind,
        markdown_text=markdown_text,
        json_payload=json_payload,
        system_prompt=prompt_text,
        notes=notes,
    )
    session.add(draft)
    session.flush()
    return draft


def create_skill_draft(
    session: Session,
    *,
    project_id: str,
    run_id: str | None,
    markdown_text: str,
    json_payload: dict[str, Any],
    system_prompt: str,
    notes: str | None = None,
) -> SkillDraft:
    return create_asset_draft(
        session,
        project_id=project_id,
        run_id=run_id,
        asset_kind="skill",
        markdown_text=markdown_text,
        json_payload=json_payload,
        prompt_text=system_prompt,
        notes=notes,
    )


def get_latest_asset_draft(session: Session, project_id: str, *, asset_kind: str) -> SkillDraft | None:
    stmt = (
        select(SkillDraft)
        .where(SkillDraft.project_id == project_id, SkillDraft.asset_kind == asset_kind)
        .order_by(desc(SkillDraft.created_at))
    )
    return session.scalars(stmt).first()


def list_asset_drafts(session: Session, project_id: str, *, asset_kind: str) -> list[SkillDraft]:
    stmt = (
        select(SkillDraft)
        .where(SkillDraft.project_id == project_id, SkillDraft.asset_kind == asset_kind)
        .order_by(desc(SkillDraft.created_at))
    )
    return list(session.scalars(stmt))


def get_latest_skill_draft(session: Session, project_id: str) -> SkillDraft | None:
    return get_latest_asset_draft(session, project_id, asset_kind="cc_skill")


def get_asset_draft(session: Session, draft_id: str, *, asset_kind: str | None = None) -> SkillDraft | None:
    stmt = select(SkillDraft).where(SkillDraft.id == draft_id)
    if asset_kind:
        stmt = stmt.where(SkillDraft.asset_kind == asset_kind)
    return session.scalar(stmt)


def get_skill_draft(session: Session, draft_id: str) -> SkillDraft | None:
    return get_asset_draft(session, draft_id, asset_kind="cc_skill")


def get_asset_version(session: Session, version_id: str, *, asset_kind: str | None = None) -> SkillVersion | None:
    stmt = select(SkillVersion).where(SkillVersion.id == version_id)
    if asset_kind:
        stmt = stmt.where(SkillVersion.asset_kind == asset_kind)
    return session.scalar(stmt)


def list_asset_versions(session: Session, project_id: str, *, asset_kind: str) -> list[SkillVersion]:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return list(session.scalars(stmt))


def list_skill_versions(session: Session, project_id: str) -> list[SkillVersion]:
    return list_asset_versions(session, project_id, asset_kind="cc_skill")


def get_latest_asset_version(session: Session, project_id: str, *, asset_kind: str) -> SkillVersion | None:
    stmt = (
        select(SkillVersion)
        .where(SkillVersion.project_id == project_id, SkillVersion.asset_kind == asset_kind)
        .order_by(desc(SkillVersion.version_number))
    )
    return session.scalars(stmt).first()


def delete_asset_version(session: Session, version: SkillVersion) -> None:
    session.execute(delete(SkillVersion).where(SkillVersion.id == version.id))


def get_latest_skill_version(session: Session, project_id: str) -> SkillVersion | None:
    return get_latest_asset_version(session, project_id, asset_kind="cc_skill")


def publish_asset_draft(session: Session, project_id: str, draft: SkillDraft) -> SkillVersion:
    latest = get_latest_asset_version(session, project_id, asset_kind=draft.asset_kind)
    next_version = (latest.version_number if latest else 0) + 1
    version = SkillVersion(
        project_id=project_id,
        draft_id=draft.id,
        asset_kind=draft.asset_kind,
        version_number=next_version,
        markdown_text=draft.markdown_text,
        json_payload=draft.json_payload,
        system_prompt=draft.system_prompt,
    )
    session.add(version)
    session.flush()
    return version


def publish_skill_draft(session: Session, project_id: str, draft: SkillDraft) -> SkillVersion:
    if draft.asset_kind not in {"skill", "cc_skill"}:
        raise ValueError("Draft is not a Claude Code Skill asset.")
    return publish_asset_draft(session, project_id, draft)


__all__ = [name for name in globals() if not name.startswith("_")]
