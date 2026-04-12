from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event, inspect
from sqlalchemy.orm import Session, sessionmaker

from app.config import AppConfig, default_config
from app.models import Base


class Database:
    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or default_config()
        self.engine = create_engine(
            self.config.database_url,
            future=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        if self.engine.url.get_backend_name() == "sqlite":
            event.listen(self.engine, "connect", _configure_sqlite_connection)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            future=True,
            expire_on_commit=False,
        )

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)
        upgrade_schema(self.engine)

    def close(self) -> None:
        self.engine.dispose()

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def upgrade_schema(engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        if "projects" in tables:
            project_columns = {column["name"] for column in inspector.get_columns("projects")}
            if "mode" not in project_columns:
                connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN mode VARCHAR(32) DEFAULT 'group'")
            if "parent_id" not in project_columns:
                connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN parent_id VARCHAR(36) DEFAULT NULL")
            connection.exec_driver_sql(
                "UPDATE projects SET mode = 'group' WHERE mode IS NULL OR mode = ''"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_projects_parent_updated ON projects (parent_id, updated_at)"
            )

        if "documents" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_documents_project_status_created ON documents (project_id, ingest_status, created_at)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_documents_project_created ON documents (project_id, created_at)"
            )

        if "chunks" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_chunks_project_document_chunk ON chunks (project_id, document_id, chunk_index)"
            )

        if "chat_sessions" in tables:
            columns = {column["name"] for column in inspector.get_columns("chat_sessions")}
            if "session_kind" not in columns:
                connection.exec_driver_sql(
                    "ALTER TABLE chat_sessions ADD COLUMN session_kind VARCHAR(32) DEFAULT 'playground'"
                )
            if "title" not in columns:
                connection.exec_driver_sql("ALTER TABLE chat_sessions ADD COLUMN title VARCHAR(255)")
            if "last_active_at" not in columns:
                connection.exec_driver_sql("ALTER TABLE chat_sessions ADD COLUMN last_active_at DATETIME")
            connection.exec_driver_sql(
                "UPDATE chat_sessions SET session_kind = 'playground' WHERE session_kind IS NULL OR session_kind = ''"
            )
            connection.exec_driver_sql(
                "UPDATE chat_sessions SET last_active_at = created_at WHERE last_active_at IS NULL"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_chat_sessions_session_kind ON chat_sessions (session_kind)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_chat_sessions_project_kind_active ON chat_sessions (project_id, session_kind, last_active_at)"
            )

        if "skill_drafts" in tables:
            skill_draft_columns = {column["name"] for column in inspector.get_columns("skill_drafts")}
            if "asset_kind" not in skill_draft_columns:
                connection.exec_driver_sql("ALTER TABLE skill_drafts ADD COLUMN asset_kind VARCHAR(32) DEFAULT 'skill'")
            connection.exec_driver_sql(
                "UPDATE skill_drafts SET asset_kind = 'skill' WHERE asset_kind IS NULL OR asset_kind = ''"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_skill_drafts_asset_kind ON skill_drafts (asset_kind)"
            )

        if "skill_versions" in tables:
            skill_version_columns = {column["name"] for column in inspector.get_columns("skill_versions")}
            if "asset_kind" not in skill_version_columns:
                connection.exec_driver_sql(
                    "ALTER TABLE skill_versions ADD COLUMN asset_kind VARCHAR(32) DEFAULT 'skill'"
                )
            connection.exec_driver_sql(
                "UPDATE skill_versions SET asset_kind = 'skill' WHERE asset_kind IS NULL OR asset_kind = ''"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_skill_versions_asset_kind ON skill_versions (asset_kind)"
            )

        if "analysis_runs" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_analysis_runs_project_status_created ON analysis_runs (project_id, status, created_at)"
            )

        if "analysis_facets" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_analysis_facets_run_status_key ON analysis_facets (run_id, status, facet_key)"
            )


def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
    finally:
        cursor.close()
