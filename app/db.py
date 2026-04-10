from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from app.config import AppConfig, default_config
from app.models import Base


class Database:
    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or default_config()
        self.engine = create_engine(
            self.config.database_url,
            future=True,
            connect_args={"check_same_thread": False},
        )
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
    if "chat_sessions" not in tables:
        return

    columns = {column["name"] for column in inspector.get_columns("chat_sessions")}
    with engine.begin() as connection:
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
