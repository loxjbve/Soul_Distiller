from __future__ import annotations

from typing import Any

PERSONA_MODE = "persona"
PERSONA_PLUS_LABEL = "Persona+"
PERSONA_PLUS_METADATA_KEY = "persona_plus"
PERSONA_ROLE_KEY = "role"
PERSONA_ROLE_DEFAULT = "self"
PERSONA_ROLE_OPTIONS: tuple[str, ...] = (
    "self",
    "colleague",
    "mentor",
    "family",
    "partner",
    "friend",
    "public-figure",
)

PERSONA_FACET_DOCUMENT_MAP: dict[str, tuple[str, ...]] = {
    "procedure": ("values_preferences", "physical_anchor", "social_niche", "life_timeline"),
    "interaction": ("language_style", "interpersonal_mechanics", "social_niche", "narrative_boundaries", "relationship_network"),
    "memory": ("life_timeline", "relationship_network", "physical_anchor", "subculture_refuge"),
    "personality": ("personality", "values_preferences", "narrative_boundaries", "subculture_refuge", "interpersonal_mechanics"),
}

PERSONA_DOCUMENT_FILENAMES: dict[str, str] = {
    "skill": "SKILL.md",
    "procedure": "references/procedure.md",
    "interaction": "references/interaction.md",
    "memory": "references/memory.md",
    "personality": "references/personality.md",
    "conflicts": "conflicts.md",
    "analysis": "references/analysis.md",
}

PERSONA_ROLE_DOCUMENT_ORDER: dict[str, tuple[str, ...]] = {
    "self": ("skill", "procedure", "interaction", "memory", "personality", "conflicts", "analysis"),
    "mentor": ("skill", "procedure", "interaction", "memory", "personality", "conflicts", "analysis"),
    "public-figure": ("skill", "procedure", "interaction", "memory", "personality", "conflicts", "analysis"),
    "colleague": ("skill", "procedure", "interaction", "conflicts", "analysis"),
    "family": ("skill", "interaction", "memory", "personality", "conflicts", "analysis"),
    "partner": ("skill", "interaction", "memory", "personality", "conflicts", "analysis"),
    "friend": ("skill", "interaction", "memory", "personality", "conflicts", "analysis"),
}


def is_persona_mode(mode: str | None) -> bool:
    return str(mode or "").strip().lower() == PERSONA_MODE


def normalize_persona_role(value: Any, *, default: str = PERSONA_ROLE_DEFAULT) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in PERSONA_ROLE_OPTIONS:
        return candidate
    return default


def persona_metadata_with_role(metadata: dict[str, Any] | None, role: Any) -> dict[str, Any]:
    normalized = dict(metadata or {})
    payload = dict(normalized.get(PERSONA_PLUS_METADATA_KEY) or {})
    payload[PERSONA_ROLE_KEY] = normalize_persona_role(role)
    normalized[PERSONA_PLUS_METADATA_KEY] = payload
    return normalized


def get_persona_role_from_metadata(metadata: dict[str, Any] | None) -> str:
    payload = dict(metadata or {})
    nested = payload.get(PERSONA_PLUS_METADATA_KEY)
    if isinstance(nested, dict):
        return normalize_persona_role(nested.get(PERSONA_ROLE_KEY))
    return PERSONA_ROLE_DEFAULT


def persona_document_order_for_role(role: Any) -> tuple[str, ...]:
    normalized = normalize_persona_role(role)
    return PERSONA_ROLE_DOCUMENT_ORDER.get(normalized, PERSONA_ROLE_DOCUMENT_ORDER[PERSONA_ROLE_DEFAULT])


__all__ = [
    "PERSONA_DOCUMENT_FILENAMES",
    "PERSONA_FACET_DOCUMENT_MAP",
    "PERSONA_MODE",
    "PERSONA_PLUS_LABEL",
    "PERSONA_PLUS_METADATA_KEY",
    "PERSONA_ROLE_DEFAULT",
    "PERSONA_ROLE_DOCUMENT_ORDER",
    "PERSONA_ROLE_KEY",
    "PERSONA_ROLE_OPTIONS",
    "get_persona_role_from_metadata",
    "is_persona_mode",
    "normalize_persona_role",
    "persona_document_order_for_role",
    "persona_metadata_with_role",
]
