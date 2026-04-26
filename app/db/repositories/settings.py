from __future__ import annotations

from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db.models import AppSetting
from app.llm.client_helpers import normalize_api_mode, normalize_provider_kind
from app.schemas import ServiceConfig


def upsert_setting(session: Session, key: str, value_json: dict[str, Any]) -> AppSetting:
    setting = session.get(AppSetting, key)
    if not setting:
        setting = AppSetting(key=key, value_json=value_json)
        session.add(setting)
    else:
        setting.value_json = value_json
    session.flush()
    return setting


def get_setting(session: Session, key: str) -> AppSetting | None:
    return session.get(AppSetting, key)


def get_service_setting_bundle(
    session: Session,
    key: str,
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> dict[str, Any]:
    setting = get_setting(session, key)
    return _normalize_service_setting_bundle(
        setting.value_json if setting else {},
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )


def upsert_service_setting_bundle(
    session: Session,
    key: str,
    bundle: dict[str, Any],
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> AppSetting:
    normalized_bundle = _normalize_service_setting_bundle(
        bundle,
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )
    return upsert_setting(session, key, normalized_bundle)


def get_service_setting_config(
    session: Session,
    key: str,
    config_id: str,
    *,
    default_provider: str = "openai",
    default_api_mode: str = "responses",
) -> dict[str, Any] | None:
    bundle = get_service_setting_bundle(
        session,
        key,
        default_provider=default_provider,
        default_api_mode=default_api_mode,
    )
    target_id = str(config_id or "").strip()
    if not target_id:
        return None
    for config in bundle["configs"]:
        if config["id"] == target_id:
            return dict(config)
    return None


def get_service_config(session: Session, key: str) -> ServiceConfig | None:
    bundle = get_service_setting_bundle(session, key)
    ordered_configs = _ordered_service_setting_configs(bundle)
    resolved_configs = [config for config in (_build_service_config(item) for item in ordered_configs) if config]
    if not resolved_configs:
        return None
    primary, *fallbacks = resolved_configs
    primary.fallbacks = fallbacks
    return primary


def _normalize_service_setting_bundle(
    payload: dict[str, Any] | None,
    *,
    default_provider: str,
    default_api_mode: str,
) -> dict[str, Any]:
    source = dict(payload or {})
    raw_configs = source.get("configs") if isinstance(source.get("configs"), list) else None
    configs: list[dict[str, Any]] = []

    if raw_configs is None:
        configs.append(
            _normalize_service_setting_config(
                source,
                default_provider=default_provider,
                default_api_mode=default_api_mode,
                fallback_label="Default",
            )
        )
    else:
        for index, item in enumerate(raw_configs, start=1):
            config_payload = item if isinstance(item, dict) else {}
            configs.append(
                _normalize_service_setting_config(
                    config_payload,
                    default_provider=default_provider,
                    default_api_mode=default_api_mode,
                    fallback_label=f"Config {index}",
                )
            )

    if not configs:
        configs.append(
            _normalize_service_setting_config(
                {},
                default_provider=default_provider,
                default_api_mode=default_api_mode,
                fallback_label="Default",
            )
        )

    config_ids = [config["id"] for config in configs]
    active_config_id = str(source.get("active_config_id") or "").strip()
    if active_config_id not in config_ids:
        active_config_id = config_ids[0]

    fallback_order: list[str] = []
    seen_ids = {active_config_id}
    for item in source.get("fallback_order") or []:
        config_id = str(item or "").strip()
        if config_id and config_id in config_ids and config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)
    for config_id in config_ids:
        if config_id not in seen_ids:
            fallback_order.append(config_id)
            seen_ids.add(config_id)

    return {
        "version": 2,
        "active_config_id": active_config_id,
        "fallback_order": fallback_order,
        "configs": configs,
    }


def _normalize_service_setting_config(
    payload: dict[str, Any] | None,
    *,
    default_provider: str,
    default_api_mode: str,
    fallback_label: str,
) -> dict[str, Any]:
    source = dict(payload or {})
    normalized_base_url = str(source.get("base_url") or "").strip()
    normalized_provider = normalize_provider_kind(
        source.get("provider_kind") or ("openai-compatible" if normalized_base_url else default_provider)
    )
    available_models: list[str] = []
    seen_models: set[str] = set()
    for item in source.get("available_models") or []:
        model_name = str(item or "").strip()
        if model_name and model_name not in seen_models:
            available_models.append(model_name)
            seen_models.add(model_name)
    return {
        "id": str(source.get("id") or uuid4().hex).strip() or uuid4().hex,
        "label": str(source.get("label") or fallback_label).strip() or fallback_label,
        "provider_kind": normalized_provider,
        "base_url": normalized_base_url,
        "api_key": str(source.get("api_key") or "").strip(),
        "model": str(source.get("model") or "").strip(),
        "api_mode": normalize_api_mode(source.get("api_mode") or default_api_mode),
        "available_models": available_models,
    }


def _ordered_service_setting_configs(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    configs = [dict(item) for item in bundle.get("configs") or [] if isinstance(item, dict)]
    if not configs:
        return []
    by_id = {config["id"]: config for config in configs}
    ordered_ids = [bundle.get("active_config_id"), *(bundle.get("fallback_order") or [])]
    ordered_configs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for config_id in ordered_ids:
        normalized_id = str(config_id or "").strip()
        if normalized_id and normalized_id in by_id and normalized_id not in seen_ids:
            ordered_configs.append(by_id[normalized_id])
            seen_ids.add(normalized_id)
    for config in configs:
        if config["id"] not in seen_ids:
            ordered_configs.append(config)
            seen_ids.add(config["id"])
    return ordered_configs


def _build_service_config(payload: dict[str, Any]) -> ServiceConfig | None:
    api_key = str(payload.get("api_key") or "").strip()
    if not api_key:
        return None
    base_url = str(payload.get("base_url") or "").strip() or None
    provider_kind = normalize_provider_kind(payload.get("provider_kind") or ("openai-compatible" if base_url else "openai"))
    if provider_kind == "openai-compatible" and not base_url:
        return None
    return ServiceConfig(
        base_url=base_url,
        api_key=api_key,
        model=str(payload.get("model") or "").strip() or None,
        provider_kind=provider_kind,
        api_mode=normalize_api_mode(payload.get("api_mode")),
    )


__all__ = [name for name in globals() if not name.startswith("__")]
