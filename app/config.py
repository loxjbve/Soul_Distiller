from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class AppConfig:
    root_dir: Path

    @property
    def data_dir(self) -> Path:
        return self.root_dir / "data"

    @property
    def upload_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def skill_dir(self) -> Path:
        return self.data_dir / "skills"

    @property
    def assets_dir(self) -> Path:
        return self.data_dir / "assets"

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "outputs"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def llm_log_path(self) -> Path:
        return self.log_dir / "llm_requests.jsonl"

    @property
    def analysis_error_log_path(self) -> Path:
        return self.log_dir / "analysis_errors.jsonl"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def database_url(self) -> str:
        return f"sqlite:///{self.db_path.as_posix()}"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.skill_dir.mkdir(parents=True, exist_ok=True)
        self.assets_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)


def default_config() -> AppConfig:
    root_dir = Path(__file__).resolve().parents[1]
    config = AppConfig(root_dir=root_dir)
    config.ensure_dirs()
    return config
