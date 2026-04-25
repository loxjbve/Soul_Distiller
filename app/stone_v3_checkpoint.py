from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def stone_v3_checkpoint_path(assets_dir: Path, project_id: str) -> Path:
    checkpoint_dir = assets_dir / project_id / "_stone_v3_baseline"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    return checkpoint_dir / "checkpoint.json"


def load_stone_v3_checkpoint(assets_dir: Path, project_id: str) -> dict[str, Any]:
    path = stone_v3_checkpoint_path(assets_dir, project_id)
    if not path.exists() or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_stone_v3_checkpoint(assets_dir: Path, project_id: str, payload: dict[str, Any]) -> Path:
    path = stone_v3_checkpoint_path(assets_dir, project_id)
    temp_path = path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)
    return path


def clear_stone_v3_checkpoint(assets_dir: Path, project_id: str) -> None:
    path = stone_v3_checkpoint_path(assets_dir, project_id)
    if path.exists() and path.is_file():
        path.unlink()
