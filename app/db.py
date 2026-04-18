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
            if "lifecycle_state" not in project_columns:
                connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN lifecycle_state VARCHAR(32) DEFAULT 'active'")
            if "delete_requested_at" not in project_columns:
                connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN delete_requested_at DATETIME")
            if "deletion_error" not in project_columns:
                connection.exec_driver_sql("ALTER TABLE projects ADD COLUMN deletion_error TEXT")
            connection.exec_driver_sql(
                "UPDATE projects SET mode = 'group' WHERE mode IS NULL OR mode = ''"
            )
            connection.exec_driver_sql(
                "UPDATE projects SET lifecycle_state = 'active' WHERE lifecycle_state IS NULL OR lifecycle_state = ''"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_projects_parent_updated ON projects (parent_id, updated_at)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_projects_lifecycle_state ON projects (lifecycle_state, updated_at)"
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

        if "telegram_chats" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_chats_project_document ON telegram_chats (project_id, document_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_chats_project_title ON telegram_chats (project_id, title)"
            )

        if "telegram_participants" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_participants_chat_key ON telegram_participants (chat_id, participant_key)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_participants_project_user ON telegram_participants (project_id, telegram_user_id)"
            )

        if "telegram_messages" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_messages_chat_message ON telegram_messages (chat_id, telegram_message_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_messages_project_sent_at ON telegram_messages (project_id, sent_at)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_messages_project_reply ON telegram_messages (project_id, reply_to_message_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_messages_chat_participant ON telegram_messages (chat_id, participant_id, sent_at)"
            )

        if "telegram_topic_reports" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_topic_reports_chat_stage ON telegram_topic_reports (chat_id, stage_index)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_topic_reports_project_status ON telegram_topic_reports (project_id, status, created_at)"
            )

        if "telegram_preprocess_runs" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_runs_project_status_created ON telegram_preprocess_runs (project_id, status, created_at)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_runs_project_finished ON telegram_preprocess_runs (project_id, finished_at)"
            )

        if "telegram_preprocess_top_users" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_top_users_run_rank ON telegram_preprocess_top_users (run_id, rank)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_top_users_project_run ON telegram_preprocess_top_users (project_id, run_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_top_users_project_participant ON telegram_preprocess_top_users (project_id, participant_id)"
            )

        if "telegram_preprocess_weekly_topic_candidates" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_weekly_candidates_run_week ON telegram_preprocess_weekly_topic_candidates (run_id, week_key)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_weekly_candidates_project_run ON telegram_preprocess_weekly_topic_candidates (project_id, run_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_weekly_candidates_project_time ON telegram_preprocess_weekly_topic_candidates (project_id, start_at, end_at)"
            )

        if "telegram_preprocess_topics" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_topics_run_topic ON telegram_preprocess_topics (run_id, topic_index)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_topics_project_time ON telegram_preprocess_topics (project_id, start_at, end_at)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_topics_project_messages ON telegram_preprocess_topics (project_id, start_message_id, end_message_id)"
            )

        if "telegram_preprocess_topic_participants" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_topic_participants_topic_participant ON telegram_preprocess_topic_participants (topic_id, participant_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_topic_participants_run_participant ON telegram_preprocess_topic_participants (run_id, participant_id)"
            )

        if "telegram_preprocess_active_users" in tables:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_active_users_run_rank ON telegram_preprocess_active_users (run_id, rank)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_active_users_project_participant ON telegram_preprocess_active_users (project_id, participant_id)"
            )
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_telegram_preprocess_active_users_project_uid ON telegram_preprocess_active_users (project_id, uid)"
            )


def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
    finally:
        cursor.close()
