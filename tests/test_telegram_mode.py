from __future__ import annotations

import io
import json
import time
from datetime import datetime, timedelta

from sqlalchemy import func, select

from app.analysis.telegram_agent import TelegramFacetAnalysisResult
from app.llm.client import OpenAICompatibleClient
from app.models import TextChunk
from app.storage import repository
from app.telegram_preprocess import TelegramPreprocessWorker


def _telegram_export_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _sample_telegram_export() -> bytes:
    payload = {
        "name": "Telegram Test Group",
        "type": "supergroup",
        "id": 123456789,
        "messages": [
            {
                "id": 1,
                "type": "message",
                "date": "2025-01-01T10:00:00",
                "date_unixtime": "1735725600",
                "from": "Alice",
                "from_id": "user1",
                "text": "We should add a Telegram mode first.",
            },
            {
                "id": 2,
                "type": "message",
                "date": "2025-01-01T10:02:00",
                "date_unixtime": "1735725720",
                "from": "Bob",
                "from_id": "user2",
                "text": [
                    "I think we should remove ",
                    {"type": "bold", "text": "embeddings"},
                    " and rely on agents plus SQL.",
                ],
            },
            {
                "id": 3,
                "type": "service",
                "date": "2025-01-01T10:03:00",
                "date_unixtime": "1735725780",
                "actor": "Alice",
                "actor_id": "user1",
                "action": "invite_members",
                "members": ["Carol"],
            },
            {
                "id": 4,
                "type": "message",
                "date": "2025-01-01T10:05:00",
                "date_unixtime": "1735725900",
                "from": "Carol",
                "from_id": "user3",
                "reply_to_message_id": 2,
                "text": "Agreed. Later analysis can use reports first and fetch evidence from DB.",
            },
        ],
    }
    return _telegram_export_bytes(payload)


def _bot_heavy_export() -> bytes:
    payload = {
        "name": "Bot Heavy Group",
        "type": "supergroup",
        "id": 987654321,
        "messages": [
            {"id": 1, "type": "message", "date": "2025-01-01T10:00:00", "date_unixtime": "1735725600", "from": "Alice", "from_id": "alice", "text": "hello"},
            {"id": 2, "type": "message", "date": "2025-01-01T10:01:00", "date_unixtime": "1735725660", "from": "helperbot", "from_id": "helperbot", "text": "bot 1"},
            {"id": 3, "type": "message", "date": "2025-01-01T10:02:00", "date_unixtime": "1735725720", "from": "helperbot", "from_id": "helperbot", "text": "bot 2"},
            {"id": 4, "type": "message", "date": "2025-01-01T10:03:00", "date_unixtime": "1735725780", "from": "helperbot", "from_id": "helperbot", "text": "bot 3"},
            {"id": 5, "type": "message", "date": "2025-01-01T10:04:00", "date_unixtime": "1735725840", "from": "Bob", "from_id": "bob", "text": "human 1"},
            {"id": 6, "type": "message", "date": "2025-01-01T10:05:00", "date_unixtime": "1735725900", "from": "Bob", "from_id": "bob", "text": "human 2"},
            {"id": 7, "type": "message", "date": "2025-01-01T10:06:00", "date_unixtime": "1735725960", "from": "Bob", "from_id": "bob", "text": "human 3"},
        ],
    }
    return _telegram_export_bytes(payload)


def _dense_week_export() -> bytes:
    start = datetime(2025, 1, 4, 12, 0, 0)
    messages = []
    message_id = 1
    for index in range(20):
        current = datetime(2025, 1, 1, 0, 0, 0) + timedelta(hours=index * 4)
        messages.append(
            {
                "id": message_id,
                "type": "message",
                "date": current.isoformat(),
                "date_unixtime": str(int(current.timestamp())),
                "from": "Alice",
                "from_id": "alice",
                "text": f"sparse-before-{index}",
            }
        )
        message_id += 1
    dense_start = message_id
    for index in range(300):
        current = start + timedelta(seconds=index * 12)
        messages.append(
            {
                "id": message_id,
                "type": "message",
                "date": current.isoformat(),
                "date_unixtime": str(int(current.timestamp())),
                "from": "Bob" if index % 2 else "Alice",
                "from_id": "bob" if index % 2 else "alice",
                "text": f"dense-{index}",
            }
        )
        message_id += 1
    dense_end = message_id - 1
    for index in range(30):
        current = datetime(2025, 1, 5, 20, 0, 0) + timedelta(minutes=index * 30)
        messages.append(
            {
                "id": message_id,
                "type": "message",
                "date": current.isoformat(),
                "date_unixtime": str(int(current.timestamp())),
                "from": "Carol",
                "from_id": "carol",
                "text": f"sparse-after-{index}",
            }
        )
        message_id += 1
    payload = {
        "name": "Dense Week Group",
        "type": "supergroup",
        "id": 55555,
        "messages": messages,
        "_expected_dense_start": dense_start,
        "_expected_dense_end": dense_end,
    }
    return _telegram_export_bytes(payload)


def _wait_for_ready(client, project_id: str, *, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        payload = client.get(f"/api/projects/{project_id}/documents").json()
        documents = payload.get("documents", [])
        if documents and documents[0].get("ingest_status") == "ready":
            return
        time.sleep(0.05)
    raise AssertionError("Telegram document did not become ready in time.")


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 8.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _wait_for_preprocess(client, project_id: str, run_id: str, *, timeout_s: float = 8.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    return payload


def _wait_for_rechunk(client, project_id: str, task_id: str, *, timeout_s: float = 8.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    return payload


def _ensure_service_config(app, service_name: str, *, model: str) -> None:
    with app.state.db.session() as session:
        repository.upsert_setting(
            session,
            service_name,
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": model,
                "api_mode": "responses",
            },
        )


def _ingest_export_bytes(client, app, monkeypatch, export_bytes: bytes, *, project_name: str = "Telegram Workspace") -> str:
    project_payload = client.post(
        "/api/projects",
        json={"name": project_name, "description": "tg export", "mode": "telegram"},
    ).json()
    project_id = project_payload["id"]
    assert project_payload["mode"] == "telegram"

    _ensure_service_config(app, "embedding_service", model="embed-test")

    def fail_embeddings(self, inputs, *, model=None, timeout=None):
        del self, inputs, model, timeout
        raise AssertionError("Telegram mode should not call embeddings.")

    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fail_embeddings)

    upload = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("telegram.json", io.BytesIO(export_bytes), "application/json")},
    )
    assert upload.status_code == 200

    process = client.post(f"/api/projects/{project_id}/process-all")
    assert process.status_code == 200
    _wait_for_ready(client, project_id)
    return project_id


def _create_ingested_telegram_project(client, app, monkeypatch) -> str:
    return _ingest_export_bytes(client, app, monkeypatch, _sample_telegram_export())


def _seed_preprocess_tables(app, project_id: str) -> str:
    with app.state.db.session() as session:
        chat = repository.get_latest_telegram_chat(session, project_id)
        participants = repository.list_telegram_participants(session, project_id, limit=10)
        alice = next(item for item in participants if item.display_name == "Alice")
        bob = next(item for item in participants if item.display_name == "Bob")
        carol = next(item for item in participants if item.display_name == "Carol")
        run = repository.create_telegram_preprocess_run(
            session,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            status="completed",
            llm_model="demo-model",
            summary_json={
                "current_stage": "completed",
                "progress_percent": 100,
                "top_user_count": 2,
                "weekly_candidate_count": 1,
                "trace_event_count": 2,
                "trace_events": [
                    {
                        "seq": 1,
                        "timestamp": "2025-01-01T10:00:00",
                        "kind": "llm_request_started",
                        "stage": "weekly_topic_summary",
                        "agent": "weekly_topic_agent",
                        "request_key": "weekly-2025-W01-attempt-1-round-1",
                        "label": "Weekly topic summary 2025-W01 round 1",
                    },
                    {
                        "seq": 2,
                        "timestamp": "2025-01-01T10:00:01",
                        "kind": "llm_request_completed",
                        "stage": "weekly_topic_summary",
                        "agent": "weekly_topic_agent",
                        "request_key": "weekly-2025-W01-attempt-1-round-1",
                        "label": "Weekly topic summary 2025-W01 round 1",
                        "response_text_preview": "{\"topics\":[]}",
                    },
                ],
            },
        )
        repository.replace_telegram_preprocess_top_users(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            top_users=[
                {
                    "rank": 1,
                    "participant_id": bob.id,
                    "uid": bob.telegram_user_id,
                    "username": bob.username,
                    "display_name": bob.display_name,
                    "message_count": 1,
                    "first_seen_at": bob.first_seen_at,
                    "last_seen_at": bob.last_seen_at,
                    "metadata_json": {"source": "sql_materialize"},
                },
                {
                    "rank": 2,
                    "participant_id": alice.id,
                    "uid": alice.telegram_user_id,
                    "username": alice.username,
                    "display_name": alice.display_name,
                    "message_count": 1,
                    "first_seen_at": alice.first_seen_at,
                    "last_seen_at": alice.last_seen_at,
                    "metadata_json": {"source": "sql_materialize"},
                },
            ],
        )
        repository.replace_telegram_preprocess_weekly_topic_candidates(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            weekly_candidates=[
                {
                    "week_key": "2025-W01",
                    "start_at": alice.first_seen_at,
                    "end_at": carol.last_seen_at,
                    "start_message_id": 1,
                    "end_message_id": 4,
                    "message_count": 3,
                    "participant_count": 3,
                    "top_participants_json": [
                        {"participant_id": alice.id, "display_name": "Alice", "message_count": 1},
                        {"participant_id": bob.id, "display_name": "Bob", "message_count": 1},
                        {"participant_id": carol.id, "display_name": "Carol", "message_count": 1},
                    ],
                    "sample_messages_json": [
                        {"message_id": 1, "participant_id": alice.id, "sender_name": "Alice", "sent_at": "2025-01-01T10:00:00", "text": "We should add a Telegram mode first."},
                        {"message_id": 2, "participant_id": bob.id, "sender_name": "Bob", "sent_at": "2025-01-01T10:02:00", "text": "I think we should remove embeddings and rely on agents plus SQL."},
                        {"message_id": 4, "participant_id": carol.id, "sender_name": "Carol", "sent_at": "2025-01-01T10:05:00", "text": "Agreed. Later analysis can use reports first and fetch evidence from DB."},
                    ],
                    "metadata_json": {"source": "sql_materialize"},
                }
            ],
        )
        repository.replace_telegram_preprocess_topics(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            topics=[
                {
                    "topic_index": 1,
                    "title": "Telegram mode discussion",
                    "summary": "The group discusses removing embeddings and relying on agent plus SQL evidence.",
                    "start_message_id": 1,
                    "end_message_id": 4,
                    "message_count": 4,
                    "participant_count": 3,
                    "keywords_json": ["telegram mode", "sql evidence"],
                    "evidence_json": [
                        {
                            "message_id": 2,
                            "sender_name": "Bob",
                            "sent_at": "2025-01-01T10:02:00",
                            "quote": "I think we should remove embeddings and rely on agents plus SQL.",
                        }
                    ],
                    "participants": [
                        {"participant_id": alice.id, "role_hint": "initiator", "message_count": 1, "mention_count": 0},
                        {"participant_id": bob.id, "role_hint": "proposer", "message_count": 1, "mention_count": 0},
                        {"participant_id": carol.id, "role_hint": "supporter", "message_count": 1, "mention_count": 0},
                    ],
                }
            ],
        )
        repository.replace_telegram_preprocess_active_users(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            active_users=[
                {
                    "rank": 1,
                    "participant_id": bob.id,
                    "uid": bob.telegram_user_id,
                    "username": bob.username,
                    "display_name": bob.display_name,
                    "primary_alias": "Bob",
                    "aliases_json": ["Bob", "user2"],
                    "message_count": 1,
                    "first_seen_at": bob.first_seen_at,
                    "last_seen_at": bob.last_seen_at,
                    "evidence_json": [],
                },
                {
                    "rank": 2,
                    "participant_id": alice.id,
                    "uid": alice.telegram_user_id,
                    "username": alice.username,
                    "display_name": alice.display_name,
                    "primary_alias": "Alice",
                    "aliases_json": ["Alice", "user1"],
                    "message_count": 1,
                    "first_seen_at": alice.first_seen_at,
                    "last_seen_at": alice.last_seen_at,
                    "evidence_json": [],
                },
            ],
        )
        run.window_count = 1
        run.topic_count = 1
        run.active_user_count = 2
        run.progress_percent = 100
        run.current_stage = "completed"
        return run.id


def test_telegram_ingest_stores_sql_rows_and_skips_embeddings_and_rechunk(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)

    with app.state.db.session() as session:
        document = repository.list_project_documents(session, project_id)[0]
        assert document.source_type == "telegram_export"
        assert document.metadata_json["format"] == "telegram_export"
        assert len(repository.list_telegram_chats(session, project_id)) == 1
        assert len(repository.list_telegram_participants(session, project_id, limit=10)) == 3
        assert len(repository.list_telegram_messages(session, project_id, limit=10, ascending=True)) == 4
        assert (session.scalar(select(func.count()).select_from(TextChunk).where(TextChunk.project_id == project_id)) or 0) == 0

    start = client.post(f"/api/projects/{project_id}/rechunk")
    assert start.status_code == 200
    status = _wait_for_rechunk(client, project_id, start.json()["task_id"])
    assert status["status"] == "completed"

    with app.state.db.session() as session:
        assert (session.scalar(select(func.count()).select_from(TextChunk).where(TextChunk.project_id == project_id)) or 0) == 0


def test_telegram_preprocess_run_persists_topics_active_users_and_new_counts(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)

    def fake_process(self, run, *, progress_callback=None):
        if progress_callback:
            progress_callback("sql_bootstrap", 20, {"window_count": 1})
            progress_callback("sql_materialize", 36, {"top_user_count": 1, "weekly_candidate_count": 1, "window_count": 1})
            progress_callback("weekly_topic_summary", 60, {"topic_count": 1})
            progress_callback("active_users", 90, {"active_user_count": 1})
        return {
            "bootstrap": {"message_count": 4},
            "window_count": 1,
            "top_user_count": 1,
            "weekly_candidate_count": 1,
            "topic_count": 1,
            "active_user_count": 1,
            "topics": [
                {
                    "topic_index": 1,
                    "title": "Telegram mode discussion",
                    "summary": "The group discusses removing embeddings and relying on SQL-backed agents.",
                    "start_message_id": 1,
                    "end_message_id": 4,
                    "message_count": 4,
                    "participant_count": 3,
                    "keywords_json": ["telegram", "agent", "sql"],
                    "evidence_json": [{"message_id": 2, "quote": "remove embeddings"}],
                    "participants": [],
                }
            ],
            "active_users": [
                {
                    "rank": 1,
                    "participant_id": repository.list_telegram_participants(self.session, self.project.id, limit=10)[0].id,
                    "uid": "user1",
                    "username": None,
                    "display_name": "Alice",
                    "primary_alias": "Alice",
                    "aliases_json": ["Alice"],
                    "message_count": 1,
                    "first_seen_at": None,
                    "last_seen_at": None,
                    "evidence_json": [],
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18, "cache_creation_tokens": 0, "cache_read_tokens": 0},
        }

    monkeypatch.setattr("app.telegram_preprocess.TelegramPreprocessWorker.process", fake_process)

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    payload = _wait_for_preprocess(client, project_id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["topic_count"] == 1
    assert payload["active_user_count"] == 1
    assert payload["top_user_count"] == 1
    assert payload["weekly_candidate_count"] == 1
    assert payload["current_stage"] == "completed"

    topics_payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{payload['id']}/topics").json()
    users_payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{payload['id']}/active-users").json()

    assert topics_payload["topics"][0]["title"] == "Telegram mode discussion"
    assert users_payload["active_users"][0]["primary_alias"] == "Alice"


def test_telegram_analysis_requires_successful_preprocess_run(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _ensure_service_config(app, "chat_service", model="demo-model")

    response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_user_query": "Bob", "analysis_context": "Focus on Telegram evidence."},
    )

    assert response.status_code == 400
    assert "预处理" in response.text


def test_telegram_analysis_uses_preprocess_tables_and_skips_retrieval(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")

    def fail_search(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Telegram analysis should not call retrieval.search.")

    monkeypatch.setattr(app.state.retrieval, "search", fail_search)

    def fake_analyze_facet(self, facet, *, target_user_query, participant_id, analysis_context, preprocess_run_id=None):
        del self, target_user_query, analysis_context
        assert participant_id
        assert preprocess_run_id
        return TelegramFacetAnalysisResult(
            payload={
                "summary": f"{facet.key} from telegram preprocess tables",
                "bullets": [f"{facet.key} bullet"],
                "confidence": 0.82,
                "evidence": [
                    {
                        "message_id": 2,
                        "sender_name": "Bob",
                        "sent_at": "2025-01-01T10:02:00",
                        "quote": "I think we should remove embeddings and rely on agents plus SQL.",
                        "reason": "Telegram direct evidence",
                    }
                ],
                "conflicts": [],
                "notes": None,
                "_meta": {
                    "llm_called": True,
                    "llm_success": True,
                    "llm_attempts": 1,
                    "provider_kind": "openai-compatible",
                    "api_mode": "responses",
                    "llm_model": "demo-model",
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "cache_creation_tokens": 0,
                    "cache_read_tokens": 0,
                    "request_url": "https://example.com/v1/responses",
                    "request_payload": {"facet": facet.key, "mode": "telegram_user_analysis"},
                    "raw_text": "{}",
                    "llm_error": None,
                    "log_path": None,
                },
            },
            retrieval_trace={
                "mode": "telegram_agent",
                "tool_calls": [{"tool": "list_related_topics"}, {"tool": "query_telegram_messages"}],
                "preprocess_run_id": preprocess_run_id,
                "target_user": {"participant_id": participant_id, "label": "Bob"},
                "topic_ids": ["topic-1"],
                "queried_message_ids": [2],
                "topic_count_used": 1,
            },
            hit_count=1,
        )

    monkeypatch.setattr("app.analysis.telegram_agent.TelegramAnalysisAgent.analyze_facet", fake_analyze_facet)

    with app.state.db.session() as session:
        active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=preprocess_run_id)
        participant_id = active_users[0].participant_id

    response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"participant_id": participant_id, "target_user_query": "Bob", "analysis_context": "Focus on Telegram evidence."},
    )
    assert response.status_code == 200
    payload = _wait_for_analysis(client, project_id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["summary"]["chunk_count"] == 0
    assert payload["summary"]["preprocess_run_id"] == preprocess_run_id
    assert payload["summary"]["telegram_message_count"] == 4
    assert payload["summary"]["telegram_participant_count"] == 3
    assert payload["summary"]["topic_count_used"] == 1
    assert payload["summary"]["weekly_candidate_count"] == 1
    assert payload["summary"]["weekly_topic_count"] == 1
    assert payload["summary"]["target_user"]["participant_id"] == participant_id
    assert all(facet["findings"]["retrieval_mode"] == "telegram_agent" for facet in payload["facets"])


def test_telegram_asset_generation_only_consumes_facets(client, app, monkeypatch):
    project_payload = client.post(
        "/api/projects",
        json={"name": "Telegram Assets", "description": "tg skill", "mode": "telegram"},
    ).json()
    project_id = project_payload["id"]
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "target_role": "TG Persona",
                "target_user": {"participant_id": "p1", "label": "TG Persona"},
                "analysis_context": "telegram skill",
            },
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.9,
            findings_json={"label": "Personality", "summary": "Calm and direct", "bullets": ["Uses concise replies"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    def fail_search(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Telegram asset synthesis should not call retrieval.search.")

    monkeypatch.setattr(app.state.retrieval, "search", fail_search)

    response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "skill"})
    assert response.status_code == 200
    payload = response.json()
    documents = payload["json_payload"]["documents"]

    assert payload["asset_kind"] == "skill"
    assert documents["skill"]["markdown"].startswith("# System Role:")
    assert "TG Persona" in payload["markdown_text"]
    assert documents["merge"]["markdown"] == payload["markdown_text"]
    assert payload["prompt_text"] == payload["markdown_text"]


def test_telegram_preprocess_page_renders_intermediate_and_final_tables(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    run_id = _seed_preprocess_tables(app, project_id)

    response = client.get(f"/projects/{project_id}/preprocess", params={"run_id": run_id})

    assert response.status_code == 200
    assert "Weekly Candidates" in response.text
    assert "Top Users" in response.text
    assert "Subagent" in response.text
    assert "Telegram mode discussion" in response.text
    assert "Alice" in response.text or "Bob" in response.text
    assert "telegram_preprocess.js" in response.text


def test_telegram_preprocess_run_detail_includes_intermediate_tables_and_trace(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    run_id = _seed_preprocess_tables(app, project_id)

    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()

    assert payload["status"] == "completed"
    assert payload["top_user_count"] == 2
    assert payload["weekly_candidate_count"] == 1
    assert payload["weekly_candidates"][0]["week_key"] == "2025-W01"
    assert payload["top_users"][0]["display_name"] == "Bob"
    assert payload["topics"][0]["title"] == "Telegram mode discussion"
    assert payload["active_users"][0]["primary_alias"] == "Bob"
    assert payload["trace_events"][0]["kind"] == "llm_request_started"
    assert payload["trace_events"][0]["stage"] == "weekly_topic_summary"


def test_telegram_preprocess_top_users_excludes_bot_accounts(client, app, monkeypatch):
    project_id = _ingest_export_bytes(client, app, monkeypatch, _bot_heavy_export(), project_name="Bot Workspace")
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        chat = repository.get_latest_telegram_chat(session, project_id)
        run = repository.create_telegram_preprocess_run(
            session,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            status="queued",
            llm_model="demo-model",
            summary_json={},
        )
        worker = TelegramPreprocessWorker(session, project, llm_config=None)
        worker._materialize_top_users(run.id, chat.id)
        top_users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=run.id)

    usernames = [item.username for item in top_users]
    assert "helperbot" not in usernames
    assert top_users[0].display_name == "Bob"


def test_telegram_weekly_candidates_select_densest_300_messages(client, app, monkeypatch):
    export_bytes = _dense_week_export()
    project_id = _ingest_export_bytes(client, app, monkeypatch, export_bytes, project_name="Dense Week Workspace")
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        chat = repository.get_latest_telegram_chat(session, project_id)
        run = repository.create_telegram_preprocess_run(
            session,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            status="queued",
            llm_model="demo-model",
            summary_json={},
        )
        worker = TelegramPreprocessWorker(session, project, llm_config=None)
        worker._materialize_weekly_topic_candidates(run.id, chat.id)
        candidates = repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=run.id)

    densest = max(candidates, key=lambda item: item.message_count)
    assert densest.message_count == 300
    assert densest.start_message_id == 21
    assert densest.end_message_id == 320


def test_telegram_preprocess_failure_keeps_sql_intermediate_tables_without_final_results(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _ensure_service_config(app, "chat_service", model="demo-model")

    def fail_weekly_summary(self, run_id, chat_id, *, progress_callback=None):
        del self, run_id, chat_id, progress_callback
        raise RuntimeError("weekly topic summary boom")

    monkeypatch.setattr("app.telegram_preprocess.TelegramPreprocessWorker._run_weekly_topic_summary", fail_weekly_summary)

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    payload = _wait_for_preprocess(client, project_id, response.json()["id"])

    assert payload["status"] == "failed"
    assert payload["topics"] == []
    assert payload["active_users"] == []
    assert payload["top_user_count"] >= 1
    assert payload["weekly_candidate_count"] >= 1
    assert all((event.get("agent") or "") != "window_topic_agent" for event in payload["trace_events"])
    assert all(event.get("kind") != "fallback" for event in payload["trace_events"])

    with app.state.db.session() as session:
        assert repository.list_telegram_preprocess_top_users(session, project_id, run_id=payload["id"])
        assert repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=payload["id"])
        assert repository.list_telegram_preprocess_topics(session, project_id, run_id=payload["id"]) == []
        assert repository.list_telegram_preprocess_active_users(session, project_id, run_id=payload["id"]) == []
