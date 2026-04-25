from __future__ import annotations

import io
import json
import threading
import time
from datetime import datetime, timedelta

from sqlalchemy import func, select

from app.service.common.facets import FacetDefinition
from app.service.common.pipeline.telegram_analysis_runtime import TELEGRAM_TOOL_LOOP_MAX_STEPS, TelegramAnalysisAgent, TelegramFacetAnalysisResult
from app.service.common.llm.client import OpenAICompatibleClient
from app.models import TextChunk
from app.schemas import LLMToolCall, ServiceConfig, ToolRoundResult
from app.storage import repository
from app.service.common.pipeline.telegram_runtime import TelegramPreprocessWorker


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


def _two_week_export() -> bytes:
    payload = {
        "name": "Two Week Group",
        "type": "supergroup",
        "id": 24680,
        "messages": [
            {
                "id": 1,
                "type": "message",
                "date": "2025-01-01T09:00:00",
                "date_unixtime": "1735693200",
                "from": "Alice",
                "from_id": "alice",
                "text": "Week one planning starts here.",
            },
            {
                "id": 2,
                "type": "message",
                "date": "2025-01-01T09:01:00",
                "date_unixtime": "1735693260",
                "from": "Bob",
                "from_id": "bob",
                "text": "Week one prefers SQL-first preprocessing.",
            },
            {
                "id": 3,
                "type": "message",
                "date": "2025-01-02T10:00:00",
                "date_unixtime": "1735783200",
                "from": "Alice",
                "from_id": "alice",
                "text": "Week one wraps up with a checkpoint.",
            },
            {
                "id": 4,
                "type": "message",
                "date": "2025-01-08T09:00:00",
                "date_unixtime": "1736298000",
                "from": "Bob",
                "from_id": "bob",
                "text": "Week two resumes from the saved checkpoint.",
            },
            {
                "id": 5,
                "type": "message",
                "date": "2025-01-08T09:02:00",
                "date_unixtime": "1736298120",
                "from": "Alice",
                "from_id": "alice",
                "text": "Week two only needs the remaining summary.",
            },
            {
                "id": 6,
                "type": "message",
                "date": "2025-01-09T11:00:00",
                "date_unixtime": "1736420400",
                "from": "Bob",
                "from_id": "bob",
                "text": "Week two finishes without starting over.",
            },
        ],
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
                "completed_week_count": 1,
                "remaining_week_count": 0,
                "current_topic_index": 1,
                "current_topic_total": 1,
                "current_topic_label": "Telegram mode discussion",
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
                    "message_count": 1,
                    "first_seen_at": alice.first_seen_at,
                    "last_seen_at": alice.last_seen_at,
                    "evidence_json": [],
                },
                {
                    "rank": 3,
                    "participant_id": carol.id,
                    "uid": carol.telegram_user_id,
                    "username": carol.username,
                    "display_name": carol.display_name,
                    "primary_alias": "Carol",
                    "message_count": 1,
                    "first_seen_at": carol.first_seen_at,
                    "last_seen_at": carol.last_seen_at,
                    "evidence_json": [],
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
                        {"participant_id": alice.id, "role_hint": "initiator", "stance_summary": "提出要先补齐 Telegram mode。", "message_count": 1, "mention_count": 0},
                        {"participant_id": bob.id, "role_hint": "proposer", "stance_summary": "主张移除 embeddings，改成 agent + SQL。", "message_count": 1, "mention_count": 0},
                        {"participant_id": carol.id, "role_hint": "supporter", "stance_summary": "补充支持先看报告再回原始证据。", "message_count": 1, "mention_count": 0},
                    ],
                    "participant_quotes": [
                        {"participant_id": alice.id, "message_id": 1, "sent_at": "2025-01-01T10:00:00", "quote": "We should add a Telegram mode first.", "rank": 1},
                        {"participant_id": bob.id, "message_id": 2, "sent_at": "2025-01-01T10:02:00", "quote": "I think we should remove embeddings and rely on agents plus SQL.", "rank": 1},
                        {"participant_id": carol.id, "message_id": 4, "sent_at": "2025-01-01T10:05:00", "quote": "Agreed. Later analysis can use reports first and fetch evidence from DB.", "rank": 1},
                    ],
                    "metadata_json": {
                        "week_key": "2025-W01",
                        "source": "weekly_topic_agent",
                        "candidate_id": "candidate-week-1",
                        "subtopics": ["Telegram mode", "SQL evidence"],
                        "interaction_patterns": ["Bob 提方案，Alice 起头，Carol 补充支持"],
                        "participant_viewpoints": [
                            {
                                "participant_id": alice.id,
                                "display_name": "Alice",
                                "stance_summary": "提出要先补齐 Telegram mode。",
                                "notable_points": ["先落地 Telegram 模式"],
                                "evidence_message_ids": [1],
                            },
                            {
                                "participant_id": bob.id,
                                "display_name": "Bob",
                                "stance_summary": "主张移除 embeddings，改成 agent + SQL。",
                                "notable_points": ["强调 SQL 证据优先"],
                                "evidence_message_ids": [2],
                            },
                        ],
                    },
                }
            ],
        )
        relationship_snapshot = repository.create_or_replace_telegram_relationship_snapshot(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            status="completed",
            analyzed_user_count=3,
            candidate_pair_count=2,
            llm_pair_count=2,
            summary_json={
                "friendly_count": 1,
                "neutral_count": 1,
                "tense_count": 0,
                "unclear_count": 0,
                "edge_count": 2,
                "central_users": [
                    {"participant_id": bob.id, "label": "Bob", "weighted_degree": 1.03, "edge_count": 2},
                    {"participant_id": carol.id, "label": "Carol", "weighted_degree": 0.61, "edge_count": 1},
                    {"participant_id": alice.id, "label": "Alice", "weighted_degree": 0.42, "edge_count": 1},
                ],
                "isolated_users": [],
                "snapshot_notes": [],
            },
        )
        repository.replace_telegram_relationship_edges(
            session,
            snapshot_id=relationship_snapshot.id,
            project_id=project_id,
            edges=[
                {
                    "participant_a_id": alice.id,
                    "participant_b_id": bob.id,
                    "interaction_strength": 0.42,
                    "confidence": 0.56,
                    "relation_label": "neutral",
                    "summary": "Alice and Bob keep working through the same rollout topic without obvious warmth or friction.",
                    "evidence_json": [
                        {
                            "kind": "topic",
                            "title": "Telegram mode discussion",
                            "summary": "Alice starts the topic and Bob responds with a competing implementation idea.",
                            "interaction_patterns": ["Alice opens the topic and Bob reframes the implementation plan."],
                            "participant_a_stance": "Pushes to ship Telegram mode first.",
                            "participant_b_stance": "Prefers an agent plus SQL workflow.",
                            "quotes": [
                                {"participant_id": alice.id, "display_name": "Alice", "quote": "We should add a Telegram mode first."},
                                {"participant_id": bob.id, "display_name": "Bob", "quote": "I think we should remove embeddings and rely on agents plus SQL."},
                            ],
                        }
                    ],
                    "counterevidence_json": [],
                    "metrics_json": {
                        "reply_total": 0,
                        "reply_a_to_b": 0,
                        "reply_b_to_a": 0,
                        "shared_topic_count": 1,
                        "shared_topics_with_both_quotes": 1,
                        "reply_score": 0.0,
                        "shared_topic_score": 0.2,
                        "co_quote_score": 0.25,
                        "heuristic_label": "neutral",
                        "supporting_signals": ["Repeated topic overlap."],
                        "counter_signals": [],
                    },
                },
                {
                    "participant_a_id": bob.id,
                    "participant_b_id": carol.id,
                    "interaction_strength": 0.61,
                    "confidence": 0.74,
                    "relation_label": "friendly",
                    "summary": "Carol directly backs Bob's proposal and extends it with a compatible next step.",
                    "evidence_json": [
                        {
                            "kind": "reply_context",
                            "anchor_message_id": 4,
                            "summary": "Carol replied to Bob",
                            "messages": [
                                {"message_id": 2, "participant_id": bob.id, "sender_name": "Bob", "text": "I think we should remove embeddings and rely on agents plus SQL."},
                                {"message_id": 4, "participant_id": carol.id, "sender_name": "Carol", "text": "Agreed. Later analysis can use reports first and fetch evidence from DB."},
                            ],
                        },
                        {
                            "kind": "topic",
                            "title": "Telegram mode discussion",
                            "summary": "Carol reinforces Bob's SQL-first direction and adds a retrieval plan.",
                            "interaction_patterns": ["Bob proposes and Carol reinforces the same path."],
                            "participant_a_stance": "Prefers an agent plus SQL workflow.",
                            "participant_b_stance": "Supports the report-first workflow and DB evidence lookup.",
                            "quotes": [
                                {"participant_id": bob.id, "display_name": "Bob", "quote": "I think we should remove embeddings and rely on agents plus SQL."},
                                {"participant_id": carol.id, "display_name": "Carol", "quote": "Agreed. Later analysis can use reports first and fetch evidence from DB."},
                            ],
                        },
                    ],
                    "counterevidence_json": [],
                    "metrics_json": {
                        "reply_total": 1,
                        "reply_a_to_b": 0,
                        "reply_b_to_a": 1,
                        "shared_topic_count": 1,
                        "shared_topics_with_both_quotes": 1,
                        "reply_score": 0.1667,
                        "shared_topic_score": 0.2,
                        "co_quote_score": 0.25,
                        "heuristic_label": "friendly",
                        "supporting_signals": ["Direct agreement in reply.", "Shared topic with aligned quotes."],
                        "counter_signals": [],
                    },
                },
            ],
        )
        run.window_count = 1
        run.topic_count = 1
        run.top_user_count = 2
        run.weekly_candidate_count = 1
        run.active_user_count = 3
        run.progress_percent = 100
        run.current_stage = "completed"
        run.summary_json = {
            **(run.summary_json or {}),
            "active_user_count": 3,
            "relationship_snapshot_id": relationship_snapshot.id,
            "relationship_status": "completed",
            "relationship_edge_count": 2,
            "relationship_summary": dict(relationship_snapshot.summary_json or {}),
        }
        return run.id


def _seed_two_week_preprocess_tables(app, project_id: str) -> tuple[str, str]:
    with app.state.db.session() as session:
        chat = repository.get_latest_telegram_chat(session, project_id)
        participants = repository.list_telegram_participants(session, project_id, limit=10)
        alice = next(item for item in participants if item.display_name == "Alice")
        bob = next(item for item in participants if item.display_name == "Bob")
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
                "weekly_candidate_count": 2,
                "topic_count": 2,
                "trace_event_count": 0,
                "trace_events": [],
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
                    "message_count": bob.message_count,
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
                    "message_count": alice.message_count,
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
                    "start_at": datetime.fromisoformat("2025-01-01T09:00:00"),
                    "end_at": datetime.fromisoformat("2025-01-02T10:00:00"),
                    "start_message_id": 1,
                    "end_message_id": 3,
                    "message_count": 3,
                    "participant_count": 2,
                    "top_participants_json": [
                        {"participant_id": alice.id, "display_name": "Alice", "message_count": 2},
                        {"participant_id": bob.id, "display_name": "Bob", "message_count": 1},
                    ],
                    "sample_messages_json": [
                        {"message_id": 1, "participant_id": alice.id, "sender_name": "Alice", "sent_at": "2025-01-01T09:00:00", "text": "Week one planning starts here."},
                        {"message_id": 2, "participant_id": bob.id, "sender_name": "Bob", "sent_at": "2025-01-01T09:01:00", "text": "Week one prefers SQL-first preprocessing."},
                        {"message_id": 3, "participant_id": alice.id, "sender_name": "Alice", "sent_at": "2025-01-02T10:00:00", "text": "Week one wraps up with a checkpoint."},
                    ],
                    "metadata_json": {"source": "sql_materialize"},
                },
                {
                    "week_key": "2025-W02",
                    "start_at": datetime.fromisoformat("2025-01-08T09:00:00"),
                    "end_at": datetime.fromisoformat("2025-01-09T11:00:00"),
                    "start_message_id": 4,
                    "end_message_id": 6,
                    "message_count": 3,
                    "participant_count": 2,
                    "top_participants_json": [
                        {"participant_id": bob.id, "display_name": "Bob", "message_count": 2},
                        {"participant_id": alice.id, "display_name": "Alice", "message_count": 1},
                    ],
                    "sample_messages_json": [
                        {"message_id": 4, "participant_id": bob.id, "sender_name": "Bob", "sent_at": "2025-01-08T09:00:00", "text": "Week two resumes from the saved checkpoint."},
                        {"message_id": 5, "participant_id": alice.id, "sender_name": "Alice", "sent_at": "2025-01-08T09:02:00", "text": "Week two only needs the remaining summary."},
                        {"message_id": 6, "participant_id": bob.id, "sender_name": "Bob", "sent_at": "2025-01-09T11:00:00", "text": "Week two finishes without starting over."},
                    ],
                    "metadata_json": {"source": "sql_materialize"},
                },
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
                    "title": "Week one SQL-first plan",
                    "summary": "Alice and Bob discuss using SQL-first preprocessing in week one.",
                    "start_at": datetime.fromisoformat("2025-01-01T09:00:00"),
                    "end_at": datetime.fromisoformat("2025-01-02T10:00:00"),
                    "start_message_id": 1,
                    "end_message_id": 3,
                    "message_count": 3,
                    "participant_count": 2,
                    "keywords_json": ["week one", "sql-first", "preprocess"],
                    "evidence_json": [
                        {
                            "message_id": 2,
                            "sender_name": "Bob",
                            "sent_at": "2025-01-01T09:01:00",
                            "quote": "Week one prefers SQL-first preprocessing.",
                        }
                    ],
                    "participants": [
                        {"participant_id": alice.id, "role_hint": "planner", "stance_summary": "负责规划和收束周内推进节奏。", "message_count": 2, "mention_count": 0},
                        {"participant_id": bob.id, "role_hint": "sql advocate", "stance_summary": "明确支持 SQL-first 的预处理方案。", "message_count": 1, "mention_count": 0},
                    ],
                    "participant_quotes": [
                        {"participant_id": alice.id, "message_id": 1, "sent_at": "2025-01-01T09:00:00", "quote": "Week one planning starts here.", "rank": 1},
                        {"participant_id": bob.id, "message_id": 2, "sent_at": "2025-01-01T09:01:00", "quote": "Week one prefers SQL-first preprocessing.", "rank": 1},
                        {"participant_id": alice.id, "message_id": 3, "sent_at": "2025-01-02T10:00:00", "quote": "Week one wraps up with a checkpoint.", "rank": 2},
                    ],
                    "metadata_json": {
                        "week_key": "2025-W01",
                        "source": "weekly_topic_agent",
                        "candidate_id": "candidate-week-1",
                        "subtopics": ["SQL-first", "week one checkpoint"],
                        "interaction_patterns": ["Alice 规划，Bob 提技术路线"],
                        "participant_viewpoints": [
                            {
                                "participant_id": alice.id,
                                "display_name": "Alice",
                                "stance_summary": "负责规划和收束周内推进节奏。",
                                "notable_points": ["启动计划", "补充阶段性 checkpoint"],
                                "evidence_message_ids": [1, 3],
                            },
                            {
                                "participant_id": bob.id,
                                "display_name": "Bob",
                                "stance_summary": "明确支持 SQL-first 的预处理方案。",
                                "notable_points": ["强调 SQL-first preprocessing"],
                                "evidence_message_ids": [2],
                            },
                        ],
                    },
                },
                {
                    "topic_index": 2,
                    "title": "Week two resume flow",
                    "summary": "Alice and Bob discuss resuming from the saved checkpoint in week two.",
                    "start_at": datetime.fromisoformat("2025-01-08T09:00:00"),
                    "end_at": datetime.fromisoformat("2025-01-09T11:00:00"),
                    "start_message_id": 4,
                    "end_message_id": 6,
                    "message_count": 3,
                    "participant_count": 2,
                    "keywords_json": ["week two", "resume", "checkpoint"],
                    "evidence_json": [
                        {
                            "message_id": 4,
                            "sender_name": "Bob",
                            "sent_at": "2025-01-08T09:00:00",
                            "quote": "Week two resumes from the saved checkpoint.",
                        }
                    ],
                    "participants": [
                        {"participant_id": alice.id, "role_hint": "planner", "stance_summary": "认同续跑方案，补充剩余总结即可。", "message_count": 1, "mention_count": 0},
                        {"participant_id": bob.id, "role_hint": "resume advocate", "stance_summary": "强调从已保存 checkpoint 继续，而不是整体重来。", "message_count": 2, "mention_count": 0},
                    ],
                    "participant_quotes": [
                        {"participant_id": bob.id, "message_id": 4, "sent_at": "2025-01-08T09:00:00", "quote": "Week two resumes from the saved checkpoint.", "rank": 1},
                        {"participant_id": alice.id, "message_id": 5, "sent_at": "2025-01-08T09:02:00", "quote": "Week two only needs the remaining summary.", "rank": 1},
                        {"participant_id": bob.id, "message_id": 6, "sent_at": "2025-01-09T11:00:00", "quote": "Week two finishes without starting over.", "rank": 2},
                    ],
                    "metadata_json": {
                        "week_key": "2025-W02",
                        "source": "weekly_topic_agent",
                        "candidate_id": "candidate-week-2",
                        "subtopics": ["resume flow", "saved checkpoint"],
                        "interaction_patterns": ["Bob 推进恢复，Alice 确认剩余工作"],
                        "participant_viewpoints": [
                            {
                                "participant_id": bob.id,
                                "display_name": "Bob",
                                "stance_summary": "强调从已保存 checkpoint 继续，而不是整体重来。",
                                "notable_points": ["resume from checkpoint", "avoid restart"],
                                "evidence_message_ids": [4, 6],
                            },
                            {
                                "participant_id": alice.id,
                                "display_name": "Alice",
                                "stance_summary": "认同续跑方案，补充剩余总结即可。",
                                "notable_points": ["只补剩余总结"],
                                "evidence_message_ids": [5],
                            },
                        ],
                    },
                },
            ],
        )
        run.window_count = 2
        run.topic_count = 2
        run.progress_percent = 100
        run.current_stage = "completed"
        session.commit()
        return run.id, bob.id


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


def test_telegram_preprocess_run_persists_topics_top_users_and_new_counts(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)

    def fake_process(self, run, *, progress_callback=None):
        participant = repository.list_telegram_participants(self.session, self.project.id, limit=1)[0]
        repository.replace_telegram_preprocess_top_users(
            self.session,
            run_id=run.id,
            project_id=self.project.id,
            chat_id=run.chat_id,
            top_users=[
                {
                    "rank": 1,
                    "participant_id": participant.id,
                    "uid": participant.telegram_user_id,
                    "username": participant.username,
                    "display_name": participant.display_name,
                    "message_count": participant.message_count,
                    "first_seen_at": participant.first_seen_at,
                    "last_seen_at": participant.last_seen_at,
                    "metadata_json": {"source": "test"},
                }
            ],
        )
        if progress_callback:
            progress_callback("sql_bootstrap", 20, {"window_count": 1})
            progress_callback("sql_materialize", 36, {"top_user_count": 1, "weekly_candidate_count": 1, "window_count": 1})
            progress_callback("weekly_topic_summary", 60, {"topic_count": 1})
        return {
            "bootstrap": {"message_count": 4},
            "window_count": 1,
            "top_user_count": 1,
            "weekly_candidate_count": 1,
            "topic_count": 1,
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
            "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18, "cache_creation_tokens": 0, "cache_read_tokens": 0},
        }

    monkeypatch.setattr("app.service.common.pipeline.telegram_runtime.TelegramPreprocessWorker.process", fake_process)

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    payload = _wait_for_preprocess(client, project_id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["topic_count"] == 1
    assert payload["top_user_count"] == 1
    assert payload["weekly_candidate_count"] == 1
    assert payload["current_stage"] == "completed"

    topics_payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{payload['id']}/topics").json()
    top_users_payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{payload['id']}/top-users").json()
    active_users_payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{payload['id']}/active-users").json()
    relationship_payload = client.get(f"/api/projects/{project_id}/relationships/latest").json()

    assert topics_payload["topics"][0]["title"] == "Telegram mode discussion"
    assert top_users_payload["top_users"][0]["display_name"] == "Alice"
    assert len(active_users_payload["active_users"]) == 1
    assert active_users_payload["active_users"][0]["display_name"] == "Alice"
    assert relationship_payload["snapshot"]["status"] == "completed"
    assert relationship_payload["snapshot"]["analyzed_user_count"] == 1
    assert relationship_payload["snapshot"]["summary"]["edge_count"] == 0


def test_telegram_preprocess_run_accepts_weekly_summary_concurrency(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    requested_concurrency = 64

    def fake_process(self, run, *, progress_callback=None):
        del self
        assert (run.summary_json or {}).get("weekly_summary_concurrency") == requested_concurrency
        if progress_callback:
            progress_callback("sql_bootstrap", 20, {"window_count": 1})
            progress_callback(
                "sql_materialize",
                36,
                {"top_user_count": 1, "weekly_candidate_count": 1, "window_count": 1},
            )
            progress_callback(
                "weekly_topic_summary",
                92,
                {"topic_count": 0, "weekly_summary_concurrency": requested_concurrency},
            )
        return {
            "bootstrap": {"message_count": 4},
            "window_count": 1,
            "top_user_count": 1,
            "weekly_candidate_count": 1,
            "topic_count": 0,
            "topics": [],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "cache_creation_tokens": 0,
                "cache_read_tokens": 0,
            },
        }

    monkeypatch.setattr("app.service.common.pipeline.telegram_runtime.TelegramPreprocessWorker.process", fake_process)

    response = client.post(
        f"/api/projects/{project_id}/preprocess/runs",
        json={"weekly_summary_concurrency": requested_concurrency},
    )
    assert response.status_code == 200

    payload = _wait_for_preprocess(client, project_id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["weekly_summary_concurrency"] == requested_concurrency


def test_telegram_preprocess_worker_does_not_cap_weekly_summary_concurrency():
    assert TelegramPreprocessWorker._normalize_weekly_summary_concurrency(64) == 64


def test_telegram_analysis_tool_iterations_limit_is_raised():
    assert TELEGRAM_TOOL_LOOP_MAX_STEPS == 25


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

    monkeypatch.setattr(app.state.services.retrieval, "search", fail_search)

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

    monkeypatch.setattr("app.service.common.pipeline.telegram_analysis_runtime.TelegramAnalysisAgent.analyze_facet", fake_analyze_facet)

    with app.state.db.session() as session:
        top_users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=preprocess_run_id)
        participant_id = top_users[0].participant_id

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


def test_telegram_analysis_query_messages_balances_across_related_topics(client, app, monkeypatch):
    project_id = _ingest_export_bytes(client, app, monkeypatch, _two_week_export(), project_name="Balanced Topics Workspace")
    preprocess_run_id, participant_id = _seed_two_week_preprocess_tables(app, project_id)

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        agent = TelegramAnalysisAgent(session, project, llm_config=None)
        target_user = agent.resolve_target_user(
            target_user_query="Bob",
            participant_id=participant_id,
            preprocess_run_id=preprocess_run_id,
        )
        topics = repository.list_telegram_preprocess_topics(session, project_id, run_id=preprocess_run_id)
        topic_ids = [topic.id for topic in topics]
        output, state = agent._execute_tool(
            "query_telegram_messages",
            {
                "participant_id": participant_id,
                "topic_ids": topic_ids,
                "limit": 4,
            },
            target_user,
            preprocess_run_id,
        )

    message_ids = {item["message_id"] for item in output["messages"]}
    assert any(message_id <= 3 for message_id in message_ids)
    assert any(message_id >= 4 for message_id in message_ids)
    assert state["week_keys"] == {"2025-W01", "2025-W02"}


def test_telegram_analysis_related_topics_expose_participant_viewpoints(client, app, monkeypatch):
    project_id = _ingest_export_bytes(client, app, monkeypatch, _two_week_export(), project_name="Related Topics Workspace")
    preprocess_run_id, participant_id = _seed_two_week_preprocess_tables(app, project_id)

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        agent = TelegramAnalysisAgent(session, project, llm_config=None)
        target_user = agent.resolve_target_user(
            target_user_query="Bob",
            participant_id=participant_id,
            preprocess_run_id=preprocess_run_id,
        )
        output, state = agent._execute_tool(
            "list_related_topics",
            {"participant_id": participant_id, "limit": 6},
            target_user,
            preprocess_run_id,
        )

    assert output["topics"][0]["participant_viewpoints"][0]["stance_summary"]
    assert output["topics"][0]["subtopics"]
    assert state["week_keys"] == {"2025-W01", "2025-W02"}


def test_telegram_analysis_blocks_raw_reads_before_topic_overview(client, app, monkeypatch):
    project_id = _ingest_export_bytes(client, app, monkeypatch, _two_week_export(), project_name="Topic Guard Workspace")
    preprocess_run_id, participant_id = _seed_two_week_preprocess_tables(app, project_id)
    llm_config = ServiceConfig(
        base_url="https://example.com/v1",
        api_key="sk-test",
        model="demo-model",
        api_mode="responses",
    )
    round_counter = {"count": 0}

    def fail_raw_reads(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Raw Telegram reads should be blocked before topic overviews are listed.")

    def fake_tool_round(self, messages, tools, **kwargs):
        del self, tools, kwargs
        round_counter["count"] += 1
        if round_counter["count"] == 1:
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
                tool_calls=[
                    LLMToolCall(
                        id="call-1",
                        name="query_telegram_messages",
                        arguments_json=json.dumps({"topic_ids": ["topic-a"], "limit": 2}),
                        arguments={"topic_ids": ["topic-a"], "limit": 2},
                    )
                ],
            )
        assert "请先调用 list_related_topics" in messages[-1]["content"]
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "在查看话题概要前，原始消息读取已被阻止。",
                    "bullets": ["必须先走 list_related_topics。"],
                    "confidence": 0.73,
                    "evidence": [],
                    "conflicts": [],
                    "notes": "tool loop guard worked",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14},
            tool_calls=[],
        )

    monkeypatch.setattr(repository, "list_telegram_messages", fail_raw_reads)
    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        agent = TelegramAnalysisAgent(session, project, llm_config=llm_config)
        result = agent.analyze_facet(
            FacetDefinition(
                key="personality",
                label="人格特征",
                purpose="验证 Telegram facet agent 的话题优先编排。",
                search_query="人格",
            ),
            target_user_query="Bob",
            participant_id=participant_id,
            analysis_context="验证先话题、后原始消息。",
            preprocess_run_id=preprocess_run_id,
        )

    assert round_counter["count"] == 2
    assert result.payload["summary"] == "在查看话题概要前，原始消息读取已被阻止。"
    assert result.retrieval_trace["tool_calls"][0]["tool"] == "query_telegram_messages"
    assert "请先调用 list_related_topics" in result.retrieval_trace["tool_calls"][0]["result_preview"]


def test_telegram_analysis_stream_emits_trace_events(client, app, monkeypatch):
    project_payload = client.post(
        "/api/projects",
        json={"name": "Telegram Stream Trace", "description": "trace", "mode": "telegram"},
    ).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="running",
            summary_json={
                "current_phase": "telegram_agent",
                "active_facets": ["personality"],
                "queued_facets": ["language_style"],
            },
        )
        run_id = run.id
        session.commit()

    def push_trace_then_finish():
        time.sleep(0.2)
        app.state.services.analysis_stream_hub.publish(
            run_id,
            event="trace",
            payload={
                "kind": "tool_call",
                "agent": "telegram_facet_agent",
                "facet_key": "personality",
                "request_key": "personality-round-1",
                "tool_name": "list_related_topics",
                "arguments_preview": "{\"limit\": 6}",
            },
        )
        with app.state.db.session() as session:
            live_run = repository.get_analysis_run(session, run_id)
            live_run.status = "completed"
            summary = dict(live_run.summary_json or {})
            summary["active_facets"] = []
            summary["queued_facets"] = []
            summary["current_phase"] = "completed"
            live_run.summary_json = summary
            session.commit()
        app.state.services.analysis_stream_hub.publish(run_id)

    worker = threading.Thread(target=push_trace_then_finish, daemon=True)
    worker.start()

    stream_response = client.get(
        f"/api/projects/{project_id}/analysis/stream",
        params={"run_id": run_id},
    )
    worker.join(timeout=2.0)

    assert stream_response.status_code == 200
    assert "event: trace" in stream_response.text
    assert '"tool_name": "list_related_topics"' in stream_response.text


def test_telegram_profile_children_share_parent_preprocess_and_run_independently(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")

    profile_response = client.post(
        f"/projects/{project_id}/profiles",
        data={"name": "Bob Persona", "description": "聚焦 Bob 的长期表达"},
        follow_redirects=False,
    )
    assert profile_response.status_code == 303

    with app.state.db.session() as session:
        child = repository.list_child_projects(session, project_id)[0]
        assert child.mode == "telegram"
        participant_id = repository.list_telegram_preprocess_top_users(session, project_id, run_id=preprocess_run_id)[0].participant_id

    child_page = client.get(f"/projects/{child.id}")
    assert child_page.status_code == 200
    assert 'telegram-bind-needed-form' in child_page.text
    assert 'data-telegram-target-picker' in child_page.text
    assert 'data-top-user-card' in child_page.text
    assert 'name="concurrency"' in child_page.text

    shared_preprocess_run_id = preprocess_run_id

    def fake_analyze_facet(self, facet, *, target_user_query, participant_id, analysis_context, preprocess_run_id=None):
        assert self.project.id == project_id
        assert target_user_query == "Bob"
        assert preprocess_run_id == shared_preprocess_run_id
        return TelegramFacetAnalysisResult(
            payload={
                "summary": f"{facet.key} for child telegram profile",
                "bullets": [],
                "confidence": 0.8,
                "evidence": [],
                "conflicts": [],
                "notes": None,
                "_meta": {
                    "llm_called": True,
                    "llm_success": True,
                    "llm_attempts": 1,
                    "provider_kind": "openai-compatible",
                    "api_mode": "responses",
                    "llm_model": "demo-model",
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
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
                "tool_calls": [{"tool": "list_related_topics"}],
                "preprocess_run_id": shared_preprocess_run_id,
                "target_user": {"participant_id": participant_id, "label": "Bob"},
                "topic_ids": ["topic-1"],
                "topic_weeks_used": ["2025-W01"],
                "queried_message_ids": [],
                "topic_count_used": 1,
            },
            hit_count=0,
        )

    monkeypatch.setattr("app.service.common.pipeline.telegram_analysis_runtime.TelegramAnalysisAgent.analyze_facet", fake_analyze_facet)

    response = client.post(
        f"/api/projects/{child.id}/analyze",
        json={"participant_id": participant_id, "target_user_query": "Bob", "analysis_context": "child telegram profile"},
    )
    assert response.status_code == 200
    payload = _wait_for_analysis(client, child.id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["summary"]["preprocess_run_id"] == shared_preprocess_run_id
    assert payload["summary"]["telegram_message_count"] == 4

    rebound_child_page = client.get(f"/projects/{child.id}")
    assert rebound_child_page.status_code == 200
    assert 'type="hidden" name="participant_id"' in rebound_child_page.text
    assert 'data-telegram-target-picker' not in rebound_child_page.text
    assert 'data-top-user-card' not in rebound_child_page.text
    assert 'name="concurrency"' in rebound_child_page.text


def test_telegram_relationship_latest_api_returns_snapshot_bundle(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)

    latest_payload = client.get(f"/api/projects/{project_id}/relationships/latest").json()

    assert latest_payload["snapshot"]["run_id"] == preprocess_run_id
    assert latest_payload["snapshot"]["status"] == "completed"
    assert latest_payload["snapshot"]["summary"]["friendly_count"] == 1
    assert latest_payload["snapshot"]["summary"]["edge_count"] == 2
    assert {item["label"] for item in latest_payload["users"]} >= {"Alice", "Bob", "Carol"}
    assert any(edge["relation_label"] == "friendly" for edge in latest_payload["edges"])
    assert any(edge["relation_label"] == "neutral" for edge in latest_payload["edges"])

    snapshot_id = latest_payload["snapshot"]["id"]
    snapshot_payload = client.get(f"/api/projects/{project_id}/relationships/{snapshot_id}").json()

    assert snapshot_payload["snapshot"]["id"] == snapshot_id
    assert len(snapshot_payload["edges"]) == 2


def test_telegram_relationship_latest_api_resolves_parent_snapshot_for_child_persona(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)

    profile_response = client.post(
        f"/projects/{project_id}/profiles",
        data={"name": "Bob Persona", "description": "bound child"},
        follow_redirects=False,
    )
    assert profile_response.status_code == 303

    with app.state.db.session() as session:
        child = repository.list_child_projects(session, project_id)[0]

    payload = client.get(f"/api/projects/{child.id}/relationships/latest").json()

    assert payload["snapshot"]["project_id"] == project_id
    assert payload["snapshot"]["run_id"] == preprocess_run_id
    assert payload["snapshot"]["status"] == "completed"
    assert len(payload["edges"]) == 2


def test_telegram_project_detail_renders_relationship_panel(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _seed_preprocess_tables(app, project_id)

    response = client.get(f"/projects/{project_id}")

    assert response.status_code == 200
    assert 'id="telegram-relationship-panel"' in response.text
    assert 'id="telegram-relationship-member-select"' in response.text
    assert 'id="telegram-relationship-friendly-list"' in response.text
    assert 'id="telegram-relationship-tense-list"' in response.text
    assert 'class="persona-top-user-card' in response.text


def test_telegram_preprocess_relationship_snapshot_llm_failure_degrades_to_partial(client, app, monkeypatch):
    export_payload = {
        "name": "Relationship Failure Group",
        "type": "supergroup",
        "id": 333444555,
        "messages": [
            {
                "id": 1,
                "type": "message",
                "date": "2025-01-01T10:00:00",
                "date_unixtime": "1735725600",
                "from": "Alice",
                "from_id": "alice",
                "text": "Let's ship Telegram mode first.",
            },
            {
                "id": 2,
                "type": "message",
                "date": "2025-01-01T10:01:00",
                "date_unixtime": "1735725660",
                "from": "Bob",
                "from_id": "bob",
                "reply_to_message_id": 1,
                "text": "We should wire SQL-backed evidence into it.",
            },
            {
                "id": 3,
                "type": "message",
                "date": "2025-01-01T10:02:00",
                "date_unixtime": "1735725720",
                "from": "Alice",
                "from_id": "alice",
                "reply_to_message_id": 2,
                "text": "That works if the rollout stays simple.",
            },
            {
                "id": 4,
                "type": "message",
                "date": "2025-01-01T10:03:00",
                "date_unixtime": "1735725780",
                "from": "Bob",
                "from_id": "bob",
                "reply_to_message_id": 3,
                "text": "Agreed, we can keep the first pass narrow.",
            },
        ],
    }
    project_id = _ingest_export_bytes(
        client,
        app,
        monkeypatch,
        _telegram_export_bytes(export_payload),
        project_name="Relationship Failure Workspace",
    )
    _ensure_service_config(app, "chat_service", model="demo-model")

    def fake_process(self, run, *, progress_callback=None):
        del progress_callback
        participants = repository.list_telegram_participants(self.session, self.project.id, limit=10)
        alice = next(item for item in participants if item.display_name == "Alice")
        bob = next(item for item in participants if item.display_name == "Bob")
        repository.replace_telegram_preprocess_top_users(
            self.session,
            run_id=run.id,
            project_id=self.project.id,
            chat_id=run.chat_id,
            top_users=[
                {
                    "rank": 1,
                    "participant_id": bob.id,
                    "uid": bob.telegram_user_id,
                    "username": bob.username,
                    "display_name": bob.display_name,
                    "message_count": bob.message_count,
                    "first_seen_at": bob.first_seen_at,
                    "last_seen_at": bob.last_seen_at,
                    "metadata_json": {"source": "test"},
                },
                {
                    "rank": 2,
                    "participant_id": alice.id,
                    "uid": alice.telegram_user_id,
                    "username": alice.username,
                    "display_name": alice.display_name,
                    "message_count": alice.message_count,
                    "first_seen_at": alice.first_seen_at,
                    "last_seen_at": alice.last_seen_at,
                    "metadata_json": {"source": "test"},
                },
            ],
        )
        return {
            "bootstrap": {"message_count": 4},
            "window_count": 1,
            "top_user_count": 2,
            "weekly_candidate_count": 1,
            "topic_count": 1,
            "topics": [
                {
                    "topic_index": 1,
                    "title": "Telegram rollout debate",
                    "summary": "Alice and Bob align on a narrow Telegram rollout with SQL-backed evidence.",
                    "start_message_id": 1,
                    "end_message_id": 4,
                    "message_count": 4,
                    "participant_count": 2,
                    "keywords_json": ["telegram", "sql"],
                    "evidence_json": [
                        {"message_id": 1, "quote": "Let's ship Telegram mode first."},
                        {"message_id": 4, "quote": "Agreed, we can keep the first pass narrow."},
                    ],
                    "participants": [
                        {"participant_id": alice.id, "role_hint": "initiator", "stance_summary": "Wants the first rollout to stay focused.", "message_count": 2, "mention_count": 0},
                        {"participant_id": bob.id, "role_hint": "builder", "stance_summary": "Pushes SQL-backed evidence while staying aligned on scope.", "message_count": 2, "mention_count": 0},
                    ],
                    "participant_quotes": [
                        {"participant_id": alice.id, "message_id": 1, "sent_at": "2025-01-01T10:00:00", "quote": "Let's ship Telegram mode first.", "rank": 1},
                        {"participant_id": bob.id, "message_id": 4, "sent_at": "2025-01-01T10:03:00", "quote": "Agreed, we can keep the first pass narrow.", "rank": 1},
                    ],
                    "metadata_json": {
                        "week_key": "2025-W01",
                        "interaction_patterns": ["Alice and Bob keep replying directly while refining the same plan."],
                        "participant_viewpoints": [
                            {"participant_id": alice.id, "display_name": "Alice", "stance_summary": "Keep the rollout focused.", "notable_points": ["wants a narrow first pass"], "evidence_message_ids": [1, 3]},
                            {"participant_id": bob.id, "display_name": "Bob", "stance_summary": "Add SQL-backed evidence without expanding scope.", "notable_points": ["keeps replying directly"], "evidence_message_ids": [2, 4]},
                        ],
                    },
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 8,
                "total_tokens": 18,
                "cache_creation_tokens": 0,
                "cache_read_tokens": 0,
            },
        }

    def fail_relationship_summary(self, candidate, participant_lookup):
        del self, candidate, participant_lookup
        raise RuntimeError("relationship llm boom")

    monkeypatch.setattr("app.service.common.pipeline.telegram_runtime.TelegramPreprocessWorker.process", fake_process)
    monkeypatch.setattr(
        "app.service.common.pipeline.telegram_runtime.TelegramPreprocessWorker._summarize_relationship_edge",
        fail_relationship_summary,
    )

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    payload = _wait_for_preprocess(client, project_id, response.json()["id"])
    relationship_payload = client.get(f"/api/projects/{project_id}/relationships/latest").json()

    assert payload["status"] == "completed"
    assert payload["relationship_status"] == "partial"
    assert payload["relationship_edge_count"] == 1
    assert relationship_payload["snapshot"]["status"] == "partial"
    assert relationship_payload["snapshot"]["summary"]["unclear_count"] == 1
    assert relationship_payload["edges"][0]["relation_label"] == "unclear"
    assert relationship_payload["edges"][0]["summary"] is None


def test_telegram_parent_persona_studio_auto_analyzes_and_redirects(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)
    expected_preprocess_run_id = preprocess_run_id
    requested_concurrency = 3
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        top_user = repository.list_telegram_preprocess_top_users(session, project_id, run_id=preprocess_run_id)[0]

    parent_page = client.get(f"/projects/{project_id}")
    assert parent_page.status_code == 200
    assert 'data-telegram-persona-studio' in parent_page.text
    assert 'name="auto_analyze" value="1"' in parent_page.text
    assert 'name="concurrency"' in parent_page.text
    assert 'add-profile-modal' not in parent_page.text

    def fake_analyze_facet(self, facet, *, target_user_query, participant_id, analysis_context, preprocess_run_id=None):
        assert self.project.id == project_id
        assert target_user_query == "Bob"
        assert participant_id == top_user.participant_id
        assert analysis_context == "Auto analyze Bob"
        assert preprocess_run_id == expected_preprocess_run_id
        return TelegramFacetAnalysisResult(
            payload={
                "summary": f"{facet.key} auto run",
                "bullets": [],
                "confidence": 0.9,
                "evidence": [],
                "conflicts": [],
                "notes": None,
                "_meta": {
                    "llm_called": True,
                    "llm_success": True,
                    "llm_attempts": 1,
                    "provider_kind": "openai-compatible",
                    "api_mode": "responses",
                    "llm_model": "demo-model",
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
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
                "tool_calls": [{"tool": "list_related_topics"}],
                "preprocess_run_id": expected_preprocess_run_id,
                "target_user": {"participant_id": participant_id, "label": "Bob"},
                "topic_ids": ["topic-1"],
                "topic_weeks_used": ["2025-W01"],
                "queried_message_ids": [],
                "topic_count_used": 1,
            },
            hit_count=0,
        )

    monkeypatch.setattr("app.service.common.pipeline.telegram_analysis_runtime.TelegramAnalysisAgent.analyze_facet", fake_analyze_facet)

    response = client.post(
        f"/projects/{project_id}/profiles",
        data={
            "participant_id": top_user.participant_id,
            "target_user_query": "Bob",
            "analysis_context": "Auto analyze Bob",
            "concurrency": requested_concurrency,
            "auto_analyze": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    with app.state.db.session() as session:
        children = repository.list_child_projects(session, project_id)
        assert len(children) == 1
        child = children[0]
        assert child.mode == "telegram"
        assert child.name == "Bob"
        assert child.description == "Auto analyze Bob"
        child_run = repository.get_latest_analysis_run(session, child.id)
        assert child_run is not None
        assert (child_run.summary_json or {}).get("concurrency") == requested_concurrency

    assert response.headers["location"] == f"/projects/{child.id}/analysis?run_id={child_run.id}"


def test_telegram_legacy_child_page_uses_parent_preprocess_for_binding(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _seed_preprocess_tables(app, project_id)

    with app.state.db.session() as session:
        child = repository.create_project(
            session,
            name="Legacy Telegram Persona",
            description="Needs binding",
            mode="telegram",
            parent_id=project_id,
        )
        child_id = child.id

    child_page = client.get(f"/projects/{child_id}")
    assert child_page.status_code == 200
    assert 'telegram-bind-needed-form' in child_page.text
    assert 'data-telegram-target-picker' in child_page.text
    assert 'data-top-user-card' in child_page.text
    assert 'name="target_user_query"' in child_page.text
    assert 'name="concurrency"' in child_page.text


def test_telegram_analysis_page_renders_agent_center_shell(client, app):
    project_payload = client.post(
        "/api/projects",
        json={"name": "Telegram Analysis UI", "description": "trace shell", "mode": "telegram"},
    ).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "progress_percent": 100,
                "current_stage": "completed",
                "current_facet": "personality",
                "concurrency": 1,
                "active_facets": 0,
                "completed_facets": 1,
                "failed_facets": 0,
                "total_facets": 1,
                "analysis_context": "Telegram trace shell",
                "target_user": {"label": "Bob"},
            },
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={
                "label": "Personality",
                "summary": "Calm and direct",
                "bullets": ["Answers tersely"],
                "llm_response_text": "Final markdown response",
                "retrieval_trace": {"tool_calls": [{"tool": "list_related_topics"}]},
            },
            evidence_json=[
                {
                    "message_id": 12,
                    "sender_name": "TG Persona",
                    "sent_at": "2025-01-01T12:00:00",
                    "situation": "回应他人关于 Telegram 模式优先级的讨论",
                    "expression": "先给明确判断，再补一条执行偏好",
                    "quote": "我还是觉得 Telegram mode 要先做，后面再补别的。",
                    "context_before": "Alice: 我们是不是应该先把 Telegram 群聊链路跑通？",
                    "context_after": "Carol: 那就先把 SQL 和 agent 证据链收紧。",
                }
            ],
            conflicts_json=[],
            error_message=None,
        )
        run_id = run.id

    response = client.get(f"/projects/{project_id}/analysis", params={"run_id": run_id})
    assert response.status_code == 200
    assert 'id="analysis-page-bootstrap"' in response.text
    assert 'id="analysis-feed"' in response.text
    assert 'id="analysis-diagnostics-list"' in response.text
    assert 'id="analysis-agent-lanes"' in response.text
    assert 'id="analysis-result-nav"' in response.text
    assert 'id="analysis-live-pill"' in response.text
    assert 'id="analysis-completed-count"' in response.text
    assert 'analysis-live-output' not in response.text
    assert 'analysis-trace-list' not in response.text


def test_telegram_analysis_summary_includes_concurrency_and_agent_tracks(client, app):
    project_payload = client.post(
        "/api/projects",
        json={"name": "Telegram Analysis Summary", "description": "summary", "mode": "telegram"},
    ).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="running",
            summary_json={
                "progress_percent": 20,
                "current_stage": "running",
                "current_facet": "personality",
                "requested_concurrency": 3,
                "concurrency": 3,
            },
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="running",
            confidence=0.8,
            findings_json={
                "label": "Personality",
                "phase": "llm",
                "summary": "Streaming",
                "started_at": "2025-01-01T00:00:00",
                "retrieval_trace": {
                    "tool_calls": [
                        {"tool": "list_related_topics", "request_key": "personality-1"},
                        {"tool": "query_telegram_messages", "request_key": "personality-1"},
                    ]
                },
            },
            evidence_json=[
                {
                    "message_id": 12,
                    "sender_name": "TG Persona",
                    "sent_at": "2025-01-01T12:00:00",
                    "situation": "回应他人关于 Telegram 模式优先级的讨论",
                    "expression": "先给明确判断，再补一条执行偏好",
                    "quote": "我还是觉得 Telegram mode 要先做，后面再补别的。",
                    "context_before": "Alice: 我们是不是应该先把 Telegram 群聊链路跑通？",
                    "context_after": "Carol: 那就先把 SQL 和 agent 证据链收紧。",
                }
            ],
            conflicts_json=[],
            error_message=None,
        )
        repository.upsert_facet(
            session,
            run.id,
            "language_style",
            status="queued",
            confidence=0.0,
            findings_json={"label": "Language Style"},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        run_id = run.id
        session.commit()

    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()

    assert payload["summary"]["requested_concurrency"] == 3
    assert payload["summary"]["effective_concurrency"] == 3
    assert payload["summary"]["effective_active_agents"] == 1
    assert payload["summary"]["agent_tracks"][0]["facet_key"] == "personality"
    assert payload["summary"]["agent_tracks"][0]["tool_call_count"] == 2
    assert payload["summary"]["agent_tracks"][0]["request_keys"] == ["personality-1"]


def test_telegram_analysis_runs_facets_concurrently(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    preprocess_run_id = _seed_preprocess_tables(app, project_id)
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        top_user = repository.list_telegram_preprocess_top_users(session, project_id, run_id=preprocess_run_id)[0]

    counter = {"active": 0, "peak": 0}
    lock = threading.Lock()

    def fake_analyze_facet(self, facet, *, target_user_query, participant_id, analysis_context, preprocess_run_id=None):
        del self, facet, target_user_query, analysis_context
        assert participant_id == top_user.participant_id
        assert preprocess_run_id
        with lock:
            counter["active"] += 1
            counter["peak"] = max(counter["peak"], counter["active"])
        time.sleep(0.08)
        with lock:
            counter["active"] -= 1
        return TelegramFacetAnalysisResult(
            payload={
                "summary": "facet completed",
                "bullets": [],
                "confidence": 0.8,
                "evidence": [],
                "conflicts": [],
                "notes": None,
                "_meta": {
                    "llm_called": True,
                    "llm_success": True,
                    "llm_attempts": 1,
                    "provider_kind": "openai-compatible",
                    "api_mode": "responses",
                    "llm_model": "demo-model",
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                    "cache_creation_tokens": 0,
                    "cache_read_tokens": 0,
                    "request_url": "https://example.com/v1/responses",
                    "request_payload": {"mode": "telegram_user_analysis"},
                    "raw_text": "{}",
                    "llm_error": None,
                    "log_path": None,
                },
            },
            retrieval_trace={
                "mode": "telegram_agent",
                "tool_calls": [{"tool": "list_related_topics", "request_key": "shared"}],
                "preprocess_run_id": preprocess_run_id,
                "target_user": {"participant_id": participant_id, "label": "Bob"},
                "topic_ids": ["topic-1"],
                "queried_message_ids": [],
                "topic_count_used": 1,
            },
            hit_count=0,
        )

    monkeypatch.setattr("app.service.common.pipeline.telegram_analysis_runtime.TelegramAnalysisAgent.analyze_facet", fake_analyze_facet)

    response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={
            "participant_id": top_user.participant_id,
            "target_user_query": "Bob",
            "analysis_context": "concurrency check",
            "concurrency": 3,
        },
    )
    assert response.status_code == 200

    payload = _wait_for_analysis(client, project_id, response.json()["id"])

    assert payload["status"] == "completed"
    assert payload["summary"]["requested_concurrency"] == 3
    assert payload["summary"]["effective_concurrency"] == 3
    assert counter["peak"] >= 2


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
            evidence_json=[
                {
                    "message_id": 12,
                    "sender_name": "TG Persona",
                    "sent_at": "2025-01-01T12:00:00",
                    "situation": "回应他人关于 Telegram 模式优先级的讨论",
                    "expression": "先给明确判断，再补一条执行偏好",
                    "quote": "我还是觉得 Telegram mode 要先做，后面再补别的。",
                    "context_before": "Alice: 我们是不是应该先把 Telegram 群聊链路跑通？",
                    "context_after": "Carol: 那就先把 SQL 和 agent 证据链收紧。",
                }
            ],
            conflicts_json=[],
            error_message=None,
        )

    def fail_search(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Telegram asset synthesis should not call retrieval.search.")

    monkeypatch.setattr(app.state.services.retrieval, "search", fail_search)

    response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"})
    assert response.status_code == 200
    payload = response.json()
    documents = payload["json_payload"]["documents"]

    assert payload["asset_kind"] == "cc_skill"
    assert documents["skill"]["markdown"].startswith("---")
    assert "# System Role:" in documents["skill"]["markdown"]
    assert documents["personality"]["markdown"].strip()
    assert documents["memories"]["markdown"].strip()
    assert documents["analysis"]["markdown"].startswith("# 十维分析摘要")
    assert "上文：" in documents["analysis"]["markdown"]
    assert "目标用户的表达方式：" in documents["analysis"]["markdown"]
    assert "目标用户原话：" in documents["analysis"]["markdown"]
    assert "下文：" in documents["analysis"]["markdown"]
    assert "TG Persona" in payload["markdown_text"]
    assert payload["prompt_text"] == payload["markdown_text"]


def test_telegram_preprocess_page_renders_compact_hub(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    run_id = _seed_preprocess_tables(app, project_id)

    response = client.get(f"/projects/{project_id}/preprocess", params={"run_id": run_id})

    assert response.status_code == 200
    assert "Topic Indicators" in response.text
    assert "Input Tokens" in response.text
    assert "Output Tokens" in response.text
    assert "Telegram mode discussion" in response.text
    assert "telegram_preprocess.js" in response.text
    assert 'id="telegram-preprocess-live-pill"' in response.text
    assert 'id="telegram-preprocess-topic-lamps"' in response.text
    assert 'id="telegram-preprocess-agent-list"' in response.text
    assert 'id="telegram-preprocess-agent-modal"' in response.text
    assert "Weekly Candidates" not in response.text
    assert "Top Users" not in response.text
    assert "Active Users" not in response.text
    assert "Workers" not in response.text


def test_telegram_preprocess_run_detail_includes_intermediate_tables_and_trace(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    run_id = _seed_preprocess_tables(app, project_id)

    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()

    assert payload["status"] == "completed"
    assert payload["top_user_count"] == 2
    assert payload["weekly_candidate_count"] == 1
    assert payload["weekly_candidates"][0]["week_key"] == "2025-W01"
    assert payload["top_users"][0]["display_name"] == "Bob"
    assert payload["active_user_count"] == 3
    assert payload["active_users"][0]["display_name"] == "Bob"
    assert payload["relationship_snapshot"]["status"] == "completed"
    assert payload["relationship_snapshot"]["summary"]["friendly_count"] == 1
    assert payload["relationship_snapshot_id"]
    assert payload["relationship_status"] == "completed"
    assert payload["relationship_edge_count"] == 2
    assert payload["topics"][0]["title"] == "Telegram mode discussion"
    assert payload["topics"][0]["participant_viewpoints"][0]["stance_summary"] == "提出要先补齐 Telegram mode。"
    assert payload["topics"][0]["subtopics"] == ["Telegram mode", "SQL evidence"]
    assert payload["trace_events"][0]["kind"] == "llm_request_started"
    assert payload["trace_events"][0]["stage"] == "weekly_topic_summary"
    assert "snapshot_version" in payload
    assert "requested_weekly_concurrency" in payload
    assert "completed_week_count" in payload
    assert "remaining_week_count" in payload
    assert payload["current_topic_index"] == 1
    assert payload["current_topic_total"] == 1
    assert payload["current_topic_label"] == "Telegram mode discussion"


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


def test_telegram_weekly_candidates_select_up_to_two_densest_250_message_windows(client, app, monkeypatch):
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
    densest_week_candidates = [item for item in candidates if item.week_key == densest.week_key]
    assert len(densest_week_candidates) == 2
    assert densest.message_count == 250
    assert sum(item.message_count for item in densest_week_candidates) <= 500
    assert all(item.message_count <= 250 for item in densest_week_candidates)
    assert sorted(item.window_index for item in densest_week_candidates) == [1, 2]


def test_telegram_preprocess_failure_keeps_sql_intermediate_tables_without_final_results(client, app, monkeypatch):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _ensure_service_config(app, "chat_service", model="demo-model")

    def fail_weekly_summary(self, run_id, chat_id, *, progress_callback=None):
        del self, run_id, chat_id, progress_callback
        raise RuntimeError("weekly topic summary boom")

    monkeypatch.setattr("app.service.common.pipeline.telegram_runtime.TelegramPreprocessWorker._run_weekly_topic_summary", fail_weekly_summary)

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    payload = _wait_for_preprocess(client, project_id, response.json()["id"])

    assert payload["status"] == "failed"
    assert payload["topics"] == []
    assert payload["top_user_count"] >= 1
    assert payload["weekly_candidate_count"] >= 1
    assert all((event.get("agent") or "") != "window_topic_agent" for event in payload["trace_events"])
    assert all(event.get("kind") != "fallback" for event in payload["trace_events"])

    with app.state.db.session() as session:
        assert repository.list_telegram_preprocess_top_users(session, project_id, run_id=payload["id"])
        assert repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=payload["id"])
        assert repository.list_telegram_preprocess_topics(session, project_id, run_id=payload["id"]) == []


def test_telegram_preprocess_resumes_existing_failed_run_from_checkpoint(client, app, monkeypatch):
    project_id = _ingest_export_bytes(client, app, monkeypatch, _two_week_export(), project_name="Resume Workspace")
    _ensure_service_config(app, "chat_service", model="demo-model")

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        chat = repository.get_latest_telegram_chat(session, project_id)
        run = repository.create_telegram_preprocess_run(
            session,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            status="failed",
            llm_model="demo-model",
            summary_json={
                "current_stage": "failed",
                "progress_percent": 52,
                "top_user_count": 0,
                "weekly_candidate_count": 0,
                "topic_count": 1,
                "trace_events": [],
                "trace_event_count": 0,
                "resume_count": 0,
                "resume_available": True,
            },
        )
        worker = TelegramPreprocessWorker(session, project, llm_config=None)
        top_users = worker._materialize_top_users(run.id, chat.id)
        candidates = worker._materialize_weekly_topic_candidates(run.id, chat.id)
        first_candidate = candidates[0]
        first_participants = [
            {
                "participant_id": item["participant_id"],
                "role_hint": None,
                "message_count": int(item.get("message_count") or 0),
                "mention_count": 0,
            }
            for item in list(first_candidate.top_participants_json or [])
            if item.get("participant_id")
        ]
        repository.replace_telegram_preprocess_topics(
            session,
            run_id=run.id,
            project_id=project_id,
            chat_id=chat.id if chat else None,
            topics=[
                {
                    "topic_index": 1,
                    "title": f"{first_candidate.week_key} 周话题",
                    "summary": "已保存的第一周总结。",
                    "start_at": first_candidate.start_at,
                    "end_at": first_candidate.end_at,
                    "start_message_id": first_candidate.start_message_id,
                    "end_message_id": first_candidate.end_message_id,
                    "message_count": first_candidate.message_count,
                    "participant_count": first_candidate.participant_count,
                    "keywords_json": ["第一周", "checkpoint"],
                    "evidence_json": [
                        {
                            "message_id": item["message_id"],
                            "sender_name": item["sender_name"],
                            "sent_at": item["sent_at"],
                            "quote": item["text"],
                        }
                        for item in list(first_candidate.sample_messages_json or [])[:1]
                        if item.get("message_id") is not None
                    ],
                    "participants": first_participants,
                    "participant_quotes": [
                        {
                            "participant_id": item["participant_id"],
                            "message_id": item["message_id"],
                            "sent_at": item["sent_at"],
                            "quote": item["text"],
                            "rank": 1,
                        }
                        for item in list(first_candidate.sample_messages_json or [])[:1]
                        if item.get("participant_id") and item.get("message_id") is not None
                    ],
                    "metadata_json": {
                        "week_key": first_candidate.week_key,
                        "source": "weekly_topic_agent",
                        "candidate_id": first_candidate.id,
                    },
                }
            ],
        )
        session.commit()
        resumable_run_id = run.id
        remaining_week_key = candidates[1].week_key

    called_weeks: list[str] = []

    def fake_weekly_agent(self, run_id, candidate, *, attempt):
        del self, run_id, attempt
        called_weeks.append(candidate.week_key)
        return [
            {
                "week_key": candidate.week_key,
                "week_topic_index": 1,
                "title": f"{candidate.week_key} 周话题",
                "summary": f"{candidate.week_key} 只补完剩余周，不重新扫描已完成周。",
                "start_at": candidate.start_at,
                "end_at": candidate.end_at,
                "start_message_id": candidate.start_message_id,
                "end_message_id": candidate.end_message_id,
                "message_count": candidate.message_count,
                "participant_count": candidate.participant_count,
                "keywords_json": ["续跑", "checkpoint"],
                "evidence_json": [
                    {
                        "message_id": item["message_id"],
                        "sender_name": item["sender_name"],
                        "sent_at": item["sent_at"],
                        "quote": item["text"],
                    }
                    for item in list(candidate.sample_messages_json or [])[:1]
                    if item.get("message_id") is not None
                ],
                "participants": [
                    {
                        "participant_id": item["participant_id"],
                        "role_hint": None,
                        "stance_summary": None,
                        "message_count": int(item.get("message_count") or 0),
                        "mention_count": 0,
                    }
                    for item in list(candidate.top_participants_json or [])
                    if item.get("participant_id")
                ],
                "participant_quotes": [
                    {
                        "participant_id": item["participant_id"],
                        "message_id": item["message_id"],
                        "sent_at": item["sent_at"],
                        "quote": item["text"],
                        "rank": 1,
                    }
                    for item in list(candidate.sample_messages_json or [])[:1]
                    if item.get("participant_id") and item.get("message_id") is not None
                ],
                "metadata_json": {
                    "week_key": candidate.week_key,
                    "source": "weekly_topic_agent",
                    "candidate_id": candidate.id,
                },
            }
        ]

    monkeypatch.setattr(TelegramPreprocessWorker, "_run_weekly_topic_agent", fake_weekly_agent)

    response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert response.status_code == 200
    assert response.json()["id"] == resumable_run_id

    payload = _wait_for_preprocess(client, project_id, resumable_run_id)

    assert payload["status"] == "completed"
    assert payload["id"] == resumable_run_id
    assert payload["topic_count"] == 2
    assert called_weeks == [remaining_week_key]
    assert any("saved checkpoint" in str(event.get("message") or "") for event in payload["trace_events"])
    assert all((event.get("agent") or "") != "active_user_alias_agent" for event in payload["trace_events"])
