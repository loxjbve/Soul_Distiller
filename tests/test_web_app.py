from __future__ import annotations

import io
import json
import time
import zipfile
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.analysis.facets import FACETS
from app.config import AppConfig
from app.llm.client import OpenAICompatibleClient
from app.main import create_app
from app.models import TextChunk
from app.schemas import AssetBundle, ChatCompletionResult, DEFAULT_ANALYSIS_CONCURRENCY, ExtractedSegment, ExtractionResult, RetrievedChunk
from app.storage import repository



def _wait_for_ready(client, project_id: str):
    import time
    for _ in range(20):
        doc_res = client.get(f"/api/projects/{project_id}/documents")
        docs = doc_res.json().get("documents", [])
        if docs and docs[0].get("ingest_status") == "ready":
            return
        time.sleep(0.5)

def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 5.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _collect_analysis_snapshots(client, project_id: str, run_id: str, *, timeout_s: float = 8.0) -> list[dict]:
    deadline = time.time() + timeout_s
    snapshots: list[dict] = []
    while time.time() < deadline:
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
        snapshots.append(payload)
        if payload["status"] not in {"queued", "running"}:
            break
        time.sleep(0.02)
    return snapshots


def _wait_for_rechunk(client, project_id: str, task_id: str, *, timeout_s: float = 8.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/rechunk/{task_id}").json()
    return payload


def test_end_to_end_project_flow(client, app):
    create_response = client.post("/api/projects", json={"name": "Alice", "description": "Writer persona"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Alice writes concise diary entries about travel and tea."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)
    assert upload_response.status_code == 200
    document_payload = upload_response.json()["documents"][0]
    assert document_payload["ingest_status"] == "pending"
    document_id = document_payload["id"]

    update_doc_response = client.post(
        f"/api/projects/{project_id}/documents/{document_id}",
        json={
            "title": "Travel diary",
            "source_type": "journal",
            "user_note": "These entries are written in first person and should imitate the author.",
        },
    )
    assert update_doc_response.status_code == 200
    assert update_doc_response.json()["title"] == "Travel diary"
    assert update_doc_response.json()["source_type"] == "journal"

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={
            "target_role": "Alice 本人",
            "analysis_context": "These notes are private journals and travel drafts. Focus on first-person expression.",
        },
    )
    assert analyze_response.status_code == 200
    analysis_payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])
    assert analysis_payload["status"] in {"completed", "partial_failed"}
    assert len(analysis_payload["facets"]) == 10
    assert analysis_payload["summary"]["target_role"] == "Alice 本人"
    assert "first-person" in analysis_payload["summary"]["analysis_context"]
    assert analysis_payload["events"]

    skill_response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "cc_skill"},
    )
    assert skill_response.status_code == 200
    skill_draft = skill_response.json()
    assert skill_draft["asset_kind"] == "cc_skill"
    assert skill_draft["markdown_text"].startswith("---")
    assert "System Role" in skill_draft["markdown_text"]
    assert "Alice 本人" in skill_draft["markdown_text"]
    assert "## 回答工作流" in skill_draft["markdown_text"]
    assert "## 核心心智模型" in skill_draft["markdown_text"]
    assert "## 诚实边界" in skill_draft["markdown_text"]
    assert "documents" in skill_draft["json_payload"]
    assert "references/personality.md" in skill_draft["markdown_text"]
    assert "references/memories.md" in skill_draft["markdown_text"]
    assert "references/analysis.md" in skill_draft["markdown_text"]
    assert skill_draft["prompt_text"] == skill_draft["markdown_text"]

    report_response = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "profile_report"},
    )
    assert report_response.status_code == 200
    report_draft = report_response.json()
    assert report_draft["asset_kind"] == "profile_report"
    assert "用户画像报告" in report_draft["markdown_text"]

    publish_response = client.post(f"/api/projects/{project_id}/skills/{skill_draft['id']}/publish")
    assert publish_response.status_code == 200
    assert publish_response.json()["asset_kind"] == "cc_skill"
    assert publish_response.json()["version_number"] == 1

    publish_report_response = client.post(
        f"/api/projects/{project_id}/assets/{report_draft['id']}/publish",
        json={"asset_kind": "profile_report"},
    )
    assert publish_report_response.status_code == 200
    assert publish_report_response.json()["asset_kind"] == "profile_report"
    assert publish_report_response.json()["version_number"] == 1

    chat_response = client.post(
        f"/api/projects/{project_id}/playground/chat",
        json={"message": "她平时怎么说话？"},
    )
    assert chat_response.status_code == 200
    chat_payload = chat_response.json()
    assert chat_payload["trace"]["skill_version_number"] == 1
    assert "retrieval_mode" not in chat_payload["trace"]
    assert chat_payload["response"]

    with app.state.db.session() as session:
        skill_version = repository.get_latest_skill_version(session, project_id)
        report_version = repository.get_latest_asset_version(session, project_id, asset_kind="profile_report")
        assert skill_version is not None
        assert report_version is not None


def test_analysis_llm_parse_failure_is_logged_and_visible(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Debug"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Debug profile with sharp language and strong opinions."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)
    client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai-compatible",
            "base_url": "https://example.com/v1",
            "api_key": "sk-test",
            "model": "demo-model",
            "api_mode": "responses",
        },
        follow_redirects=False,
    )

    def fake_post_json_with_meta(self, path, payload, *, timeout=90.0):
        content = "this is not valid json, but it is the real model text"
        api_payload = {
            "id": "resp_debug",
            "model": "demo-model",
            "output": [{"type": "message", "content": [{"type": "output_text", "text": content}]}],
            "usage": {"input_tokens": 10, "output_tokens": 8, "total_tokens": 18},
        }
        self._append_log(
            {
                "timestamp": "2026-04-10T00:00:00Z",
                "method": "POST",
                "url": f"https://example.com/v1{path}",
                "provider_kind": self.config.provider_kind,
                "api_mode": self.config.api_mode,
                "request_body": payload,
                "status_code": 200,
                "response_text": json.dumps(api_payload, ensure_ascii=False),
                "ok": True,
            }
        )
        return api_payload, {
            "url": f"https://example.com/v1{path}",
            "status_code": 200,
            "response_text": json.dumps(api_payload, ensure_ascii=False),
        }

    def fake_post_stream_text_with_meta(self, path, payload, *, timeout, stream_handler, event_parser):
        del timeout, event_parser
        content = "this is not valid json, but it is the real model text"
        stream_handler(content)
        self._append_log(
            {
                "timestamp": "2026-04-10T00:00:00Z",
                "method": "POST",
                "url": f"https://example.com/v1{path}",
                "provider_kind": self.config.provider_kind,
                "api_mode": self.config.api_mode,
                "request_body": payload,
                "response_text": content,
                "raw_stream": content,
                "ok": True,
                "stream": True,
            }
        )
        return {
            "url": f"https://example.com/v1{path}",
            "response_text": content,
            "raw_stream": content,
            "content": content,
            "response_id": "resp_debug",
            "usage": {"input_tokens": 10, "output_tokens": 8, "total_tokens": 18},
        }

    monkeypatch.setattr(OpenAICompatibleClient, "_post_json_with_meta", fake_post_json_with_meta)
    monkeypatch.setattr(OpenAICompatibleClient, "_post_stream_text_with_meta", fake_post_stream_text_with_meta)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Debug 本人", "analysis_context": "Check logging and parse fallback."},
    )
    analysis_payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])
    assert analysis_payload["status"] == "completed"
    assert analysis_payload["summary"]["llm_failures"] == 10
    assert all(facet["status"] == "completed" for facet in analysis_payload["facets"])
    assert any(event["event_type"] == "llm_response" for event in analysis_payload["events"])
    assert any(
        "this is not valid json" in json.dumps(event["payload"], ensure_ascii=False)
        for event in analysis_payload["events"]
        if event["event_type"] == "llm_response"
    )

    log_text = app.state.config.llm_log_path.read_text(encoding="utf-8")
    assert "https://example.com/v1/responses" in log_text
    assert "this is not valid json" in log_text


def test_analysis_api_truncates_large_preview_fields(client, app):
    project_payload = client.post("/api/projects", json={"name": "Preview Limits"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"progress_percent": 100, "total_facets": 1, "completed_facets": 1, "failed_facets": 0},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.7,
            findings_json={
                "label": "Personality",
                "summary": "S" * 900,
                "bullets": [],
                "llm_live_text": "L" * 5000,
                "llm_response_text": "R" * 4200,
                "llm_request_payload": {"messages": ["M" * 3000]},
            },
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="llm_response",
            message="Large payload",
            payload_json={"response_text": "E" * 3500, "request_payload": {"body": "Q" * 2000}},
        )
        run_id = run.id

    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()

    facet = payload["facets"][0]
    assert facet["findings"]["summary_truncated"] is True
    assert len(facet["findings"]["summary"]) < 900
    assert facet["findings"]["llm_live_text_truncated"] is True
    assert len(facet["findings"]["llm_live_text"]) < 5000
    assert facet["findings"]["llm_response_text_truncated"] is True
    assert "llm_request_payload" not in facet["findings"]

    event_payload = payload["events"][0]["payload"]
    assert event_payload["response_text_truncated"] is True
    assert "request_payload" not in event_payload


def test_analysis_api_backfills_queue_fields_for_legacy_runs(client, app):
    project_payload = client.post("/api/projects", json={"name": "Legacy Queue"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="queued",
            summary_json={"current_stage": "legacy"},
        )
        for facet in FACETS:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="pending",
                confidence=0.0,
                findings_json={"label": facet.label, "summary": ""},
                evidence_json=[],
                conflicts_json=[],
                error_message=None,
            )
        run_id = run.id

    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()

    assert payload["summary"]["concurrency"] == DEFAULT_ANALYSIS_CONCURRENCY
    assert payload["summary"]["active_facets"] == 0
    assert payload["summary"]["queued_facets"] == len(FACETS)
    assert payload["summary"]["current_phase"] == "queued"
    assert [facet["findings"]["queue_position"] for facet in payload["facets"]] == list(range(1, len(FACETS) + 1))
    assert all(facet["status"] == "queued" for facet in payload["facets"])
    assert all("phase" in facet["findings"] for facet in payload["facets"])
    assert all("started_at" in facet["findings"] for facet in payload["facets"])
    assert all("finished_at" in facet["findings"] for facet in payload["facets"])


def test_analysis_concurrency_one_is_strictly_serial(client, app, monkeypatch):
    import app.analysis.engine as analysis_engine_module

    project_payload = client.post("/api/projects", json={"name": "Serial Run"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Serial analysis test content."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)

    app.state.analysis_runner.run_inline = False

    def fake_retrieve(session, project_id, facet, **kwargs):
        del session, project_id, kwargs
        return (
            [
                RetrievedChunk(
                    chunk_id=f"{facet.key}-chunk",
                    document_id="doc-1",
                    document_title="Memo",
                    filename="memo.txt",
                    source_type="text",
                    content=f"Evidence for {facet.key}",
                    score=1.0,
                    page_number=None,
                    metadata={},
                    anchor_chunk_id=f"{facet.key}-chunk",
                    anchor_chunk_index=0,
                    context_span={"left": 0, "right": 0, "total_chars": 32},
                )
            ],
            "keyword",
            {"query": facet.key},
        )

    def fake_worker(
        facet,
        project_name,
        chunks,
        llm_config,
        llm_log_path,
        target_role,
        analysis_context,
        stream_callback=None,
    ):
        del project_name, chunks, llm_config, llm_log_path, target_role, analysis_context, stream_callback
        time.sleep(0.12)
        return {
            "facet_key": facet.key,
            "status": "completed",
            "confidence": 0.75,
            "summary": f"{facet.key} complete",
            "bullets": [f"{facet.key} bullet"],
            "evidence": [],
            "conflicts": [],
            "notes": None,
            "raw_payload": {
                "_meta": {
                    "llm_called": False,
                    "llm_success": False,
                    "llm_attempts": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "duration_ms": 120,
                }
            },
        }

    monkeypatch.setattr(app.state.analysis_engine, "_retrieve_hits", fake_retrieve)
    monkeypatch.setattr(analysis_engine_module, "analyze_facet_worker", fake_worker)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Strict serial", "concurrency": 1},
    )
    run_id = analyze_response.json()["id"]
    snapshots = _collect_analysis_snapshots(client, project_id, run_id)

    assert snapshots
    assert max(snapshot["summary"]["active_facets"] for snapshot in snapshots) <= 1
    assert all(
        sum(1 for facet in snapshot["facets"] if facet["status"] in {"preparing", "running"}) <= 1
        for snapshot in snapshots
    )
    assert any(
        snapshot["summary"]["active_facets"] == 1 and snapshot["summary"]["queued_facets"] >= 1
        for snapshot in snapshots
        if snapshot["status"] == "running"
    )
    assert snapshots[-1]["summary"]["concurrency"] == 1
    assert snapshots[-1]["status"] == "completed"


def test_analysis_concurrency_two_caps_active_slots(client, app, monkeypatch):
    import app.analysis.engine as analysis_engine_module

    project_payload = client.post("/api/projects", json={"name": "Parallel Cap"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Parallel analysis test content."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)

    app.state.analysis_runner.run_inline = False

    def fake_retrieve(session, project_id, facet, **kwargs):
        del session, project_id, kwargs
        return (
            [
                RetrievedChunk(
                    chunk_id=f"{facet.key}-chunk",
                    document_id="doc-1",
                    document_title="Memo",
                    filename="memo.txt",
                    source_type="text",
                    content=f"Evidence for {facet.key}",
                    score=1.0,
                    page_number=None,
                    metadata={},
                    anchor_chunk_id=f"{facet.key}-chunk",
                    anchor_chunk_index=0,
                    context_span={"left": 0, "right": 0, "total_chars": 32},
                )
            ],
            "keyword",
            {"query": facet.key},
        )

    def fake_worker(
        facet,
        project_name,
        chunks,
        llm_config,
        llm_log_path,
        target_role,
        analysis_context,
        stream_callback=None,
    ):
        del project_name, chunks, llm_config, llm_log_path, target_role, analysis_context, stream_callback
        time.sleep(0.12)
        return {
            "facet_key": facet.key,
            "status": "completed",
            "confidence": 0.75,
            "summary": f"{facet.key} complete",
            "bullets": [f"{facet.key} bullet"],
            "evidence": [],
            "conflicts": [],
            "notes": None,
            "raw_payload": {
                "_meta": {
                    "llm_called": False,
                    "llm_success": False,
                    "llm_attempts": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "duration_ms": 120,
                }
            },
        }

    monkeypatch.setattr(app.state.analysis_engine, "_retrieve_hits", fake_retrieve)
    monkeypatch.setattr(analysis_engine_module, "analyze_facet_worker", fake_worker)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Cap active slots", "concurrency": 2},
    )
    run_id = analyze_response.json()["id"]
    snapshots = _collect_analysis_snapshots(client, project_id, run_id)

    assert snapshots
    assert max(snapshot["summary"]["active_facets"] for snapshot in snapshots) <= 2
    assert all(
        sum(1 for facet in snapshot["facets"] if facet["status"] in {"preparing", "running"}) <= 2
        for snapshot in snapshots
    )
    assert any(snapshot["summary"]["active_facets"] == 2 for snapshot in snapshots if snapshot["status"] == "running")
    assert snapshots[-1]["summary"]["concurrency"] == 2
    assert snapshots[-1]["status"] == "completed"


def test_asset_generation_stream_emits_status_events(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Asset Stream"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Tester", "analysis_context": "stream asset status"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Personality", "summary": "ready", "bullets": []},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    def fake_build(asset_kind, project, facets, config, **kwargs):
        progress_callback = kwargs.get("progress_callback")
        stream_callback = kwargs.get("stream_callback")
        if callable(progress_callback):
            progress_callback({"phase": "synthesis", "progress_percent": 52, "message": "LLM 正在生成结构化草稿"})
        if callable(stream_callback):
            stream_callback("partial output")
        return AssetBundle(
            asset_kind=asset_kind,
            markdown_text="# Draft",
            json_payload={"headline": "Preview"},
            prompt_text="Prompt",
        )

    monkeypatch.setattr(app.state.asset_synthesizer, "build", fake_build)

    response = client.post(f"/api/projects/{project_id}/assets/generate/stream", json={"asset_kind": "profile_report"})
    assert response.status_code == 200
    assert "event: status" in response.text
    assert "event: delta" in response.text
    assert "event: done" in response.text
    assert '"document_key": "asset"' in response.text


def test_skill_asset_stream_emits_document_specific_deltas(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Skill Stream"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Tester", "analysis_context": "skill stream status"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Personality", "summary": "ready", "bullets": ["direct"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    def fake_build(asset_kind, project, facets, config, **kwargs):
        del asset_kind, project, facets, config
        progress_callback = kwargs.get("progress_callback")
        stream_callback = kwargs.get("stream_callback")
        if callable(progress_callback):
            progress_callback(
                {
                    "phase": "personality_context",
                    "progress_percent": 24,
                    "message": "Building personality.md",
                    "document_key": "personality",
                }
            )
        if callable(stream_callback):
            stream_callback({"document_key": "personality", "chunk": "personality chunk"})
            stream_callback({"document_key": "memories", "chunk": "memories chunk"})
            stream_callback({"document_key": "skill", "chunk": "skill chunk"})
        return AssetBundle(
            asset_kind="skill",
            markdown_text="# Draft",
            json_payload={
                "documents": {
                    "skill": {"filename": "Skill.md", "markdown": "skill chunk"},
                    "personality": {"filename": "personality.md", "markdown": "personality chunk"},
                    "memories": {"filename": "memories.md", "markdown": "memories chunk"},
                    "merge": {"filename": "Skill_merge.md", "markdown": "# Draft"},
                }
            },
            prompt_text="# Draft",
        )

    monkeypatch.setattr(app.state.asset_synthesizer, "build", fake_build)

    response = client.post(f"/api/projects/{project_id}/assets/generate/stream", json={"asset_kind": "skill"})
    assert response.status_code == 200
    assert '"document_key": "personality"' in response.text
    assert '"document_key": "memories"' in response.text
    assert '"document_key": "skill"' in response.text


def test_skill_generation_with_llm_creates_split_documents(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Alice"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        repository.upsert_setting(
            session,
            "chat_service",
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": "demo-model",
                "api_mode": "responses",
            },
        )
        repository.upsert_setting(
            session,
            "embedding_service",
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": "demo-embedding",
                "api_mode": "responses",
            },
        )
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Alice 本人", "analysis_context": "Focus on realistic imitation."},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.9,
            findings_json={"label": "Personality", "summary": "冷静、克制、强自我边界", "bullets": ["自我认知明确"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        repository.upsert_facet(
            session,
            run.id,
            "language_style",
            status="completed",
            confidence=0.9,
            findings_json={"label": "Language", "summary": "句子短，语气直接", "bullets": ["常用短句", "不铺垫"]},
            evidence_json=[{"quote": "行，就这样。", "reason": "短句", "filename": "memo.txt"}],
            conflicts_json=[],
            error_message=None,
        )
        repository.upsert_facet(
            session,
            run.id,
            "life_timeline",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Timeline", "summary": "长期围绕线上社群活动展开", "bullets": ["长期混迹线上社群", "对旧事记得很细"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    retrieval_queries = []

    def fake_search(session, *, project_id, query, embedding_config, **kwargs):
        del session, project_id, embedding_config, kwargs
        retrieval_queries.append(query)
        return (
            [
                RetrievedChunk(
                    chunk_id="chunk-1",
                    document_id="doc-1",
                    document_title="Memo",
                    filename="memo.txt",
                    source_type="text",
                    content="Alice 反复强调自己记得以前发生的细节，也会明确描述自己的状态。",
                    score=1.0,
                    page_number=None,
                    metadata={},
                )
            ],
            "hybrid",
            {},
        )

    monkeypatch.setattr(app.state.retrieval, "search", fake_search)

    llm_calls = []

    def fake_chat_completion_result(self, messages, *, model, temperature, max_tokens=None, stream_handler=None):
        del self, model, temperature, max_tokens
        llm_calls.append(messages)
        index = len(llm_calls)
        if index == 1:
            content = "# 核心身份与精神底色\n\n## 核心身份\nAlice 本人处于强自我边界的第一人称角色位。\n\n## 精神底色\n长期冷静、克制，但保持警惕。"
        elif index == 2:
            content = "# 核心记忆与经历\n\n## 关键记忆\n- 记得旧事细节\n- 长期混迹线上社群\n\n## 长期经历脉络\n这些经历塑造了她对社群秩序和旧账细节的敏感。"
        else:
            content = "# System Role: 扮演 Alice 本人\n\n## 角色扮演规则\n- 保持第一人称、边界清楚。\n- 不把自己说成万能助手。\n\n## 回答工作流\n- 先判断是否属于高置信领域。\n- 如果系统提供检索，则先看记忆切片再回答。\n\n## 身份卡\n- 我是谁：Alice 本人，强自我边界的第一人称角色位。\n- 我的现实坐标：长期冷静、克制，但保持警惕。\n\n## 核心心智模型\n- 模型：边界先于热情。证据：不编造未证实经历。场景：被追问隐私或现实细节时。局限：面对极熟的人会松动。\n\n## 决策启发式\n- 快捷规则：先短句回应，再决定是否展开。\n\n## 高置信领域\n- 线上社群互动\n\n## 表达 DNA\n- 常用短句\n- 语气直接\n- 节奏偏向先表态再补理由\n\n## 人物时间线\n- 长期混迹线上社群\n- 记得旧事细节\n\n## 价值观与反模式\n- 追求什么：说话算数，边界清楚。\n- 拒绝什么：不编造未证实经历。\n- 还没想清楚的：想保持距离，但又会记住社群旧事。\n\n## 智识谱系\n- 文化母体：社群旧事和圈层经验。\n\n## 诚实边界\n- 未证实的现实细节不展开。\n\n## Few-Shot 切片\n### 语气切片 1\n- Context: 社群旧事\n- Reply: 行，就这样。\n\n## 调研来源\n- 语言风格：2 条证据切片，主要锚定短句和直接语气。\n\n## 冲突备注\n- 无"
            if callable(stream_handler):
                stream_handler(content)
        return ChatCompletionResult(
            content=content,
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)

    response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"})
    assert response.status_code == 200
    payload = response.json()
    documents = payload["json_payload"]["documents"]

    assert len(llm_calls) == 3
    assert retrieval_queries == [
        "话题总结 高频表达 决策方式 互动模式 原话 证据 语料",
        "话题总结 性格特质 精神状态 自我认知 核心身份 内在张力 原话 证据",
        "话题总结 核心记忆 经历 过往重要事件 长期背景 时间线 原话 证据",
    ]
    assert "证据语料包：" in llm_calls[0][1]["content"]
    assert "Retrieved evidence corpus:" in llm_calls[2][1]["content"]
    assert documents["skill"]["markdown"].startswith("---")
    assert "# System Role:" in documents["skill"]["markdown"]
    assert "## 回答工作流" in documents["skill"]["markdown"]
    assert "## 调研来源" in documents["skill"]["markdown"]
    assert documents["personality"]["markdown"].startswith("# 核心身份与精神底色")
    assert documents["memories"]["markdown"].startswith("# 核心记忆与经历")
    assert documents["analysis"]["markdown"].startswith("# 十维分析摘要")
    assert payload["prompt_text"] == payload["markdown_text"]


def test_cc_skill_generation_with_llm_creates_skill_md_frontmatter(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Alice"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        repository.upsert_setting(
            session,
            "chat_service",
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": "demo-model",
                "api_mode": "responses",
            },
        )
        repository.upsert_setting(
            session,
            "embedding_service",
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": "demo-embedding",
                "api_mode": "responses",
            },
        )
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Alice 本人", "analysis_context": "Focus on realistic imitation."},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.9,
            findings_json={"label": "Personality", "summary": "冷静、克制、强自我边界", "bullets": ["自我认知明确"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    retrieval_queries = []

    def fake_search(session, *, project_id, query, embedding_config, **kwargs):
        del session, project_id, embedding_config, kwargs
        retrieval_queries.append(query)
        return (
            [
                RetrievedChunk(
                    chunk_id="chunk-1",
                    document_id="doc-1",
                    document_title="Memo",
                    filename="memo.txt",
                    source_type="text",
                    content="Alice 反复强调自己记得以前发生的细节，也会明确描述自己的状态。",
                    score=1.0,
                    page_number=None,
                    metadata={},
                )
            ],
            "hybrid",
            {},
        )

    monkeypatch.setattr(app.state.retrieval, "search", fake_search)

    llm_calls = []

    def fake_chat_completion_result(self, messages, *, model, temperature, max_tokens=None, stream_handler=None):
        del self, model, temperature, max_tokens
        llm_calls.append(messages)
        index = len(llm_calls)
        if index == 1:
            content = "# 核心身份与精神底色\n\n## 核心身份\nAlice 本人处于强自我边界的第一人称角色位。\n\n## 精神底色\n长期冷静、克制，但保持警惕。"
        elif index == 2:
            content = "# 核心记忆与经历\n\n## 关键记忆\n- 记得旧事细节\n\n## 长期经历脉络\n这些经历塑造了她对社群秩序和旧账细节的敏感。"
        else:
            content = (
                "---\n"
                "name: roleplay-alice\n"
                "description: 当需要以 Alice 本人 的语气、立场与规则进行输出时使用。\n"
                "---\n\n"
                "# System Role: 扮演 Alice 本人\n\n"
                "## 角色扮演规则\n- 保持第一人称、边界清楚。\n\n"
                "更多人格底色见 references/personality.md。\n\n"
                "更多记忆与经历见 references/memories.md。\n"
            )
            if callable(stream_handler):
                stream_handler(content)
        return ChatCompletionResult(
            content=content,
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)

    response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"})
    assert response.status_code == 200
    payload = response.json()
    documents = payload["json_payload"]["documents"]

    assert len(llm_calls) == 3
    assert retrieval_queries == [
        "话题总结 高频表达 决策方式 互动模式 原话 证据 语料",
        "话题总结 性格特质 精神状态 自我认知 核心身份 内在张力 原话 证据",
        "话题总结 核心记忆 经历 过往重要事件 长期背景 时间线 原话 证据",
    ]
    assert "证据语料包：" in llm_calls[0][1]["content"]
    assert "Retrieved evidence corpus:" in llm_calls[2][1]["content"]
    assert documents["skill"]["filename"] == "SKILL.md"
    assert documents["skill"]["markdown"].startswith("---")
    assert "name: roleplay-alice" in documents["skill"]["markdown"]
    assert "references/personality.md" in documents["skill"]["markdown"]
    assert "references/memories.md" in documents["skill"]["markdown"]
    assert payload["markdown_text"].startswith("---")
    assert payload["prompt_text"] == payload["markdown_text"]


def test_skill_split_document_exports_work_for_draft_and_version(client, app):
    project_payload = client.post("/api/projects", json={"name": "Export Skill"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Export Skill 本人", "analysis_context": "export docs"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Personality", "summary": "边界清楚", "bullets": ["不多话"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        repository.upsert_facet(
            session,
            run.id,
            "life_timeline",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Timeline", "summary": "长期在线活动", "bullets": ["长期在线活动"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    draft_payload = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"}).json()
    draft_id = draft_payload["id"]

    document_expectations = {
        "skill": "---",
        "personality": "# 核心身份与精神底色",
        "memories": "# 核心记忆与经历",
        "analysis": "# 十维分析摘要",
    }
    for key, marker in document_expectations.items():
        response = client.get(f"/api/projects/{project_id}/assets/{draft_id}/exports/{key}")
        assert response.status_code == 200
        assert marker in response.text

    bundle_response = client.get(f"/api/projects/{project_id}/assets/{draft_id}/exports/bundle")
    assert bundle_response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(bundle_response.content)) as archive:
        assert set(archive.namelist()) == {"SKILL.md", "references/personality.md", "references/memories.md", "references/analysis.md"}

    publish_payload = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "cc_skill"},
    ).json()
    version_id = publish_payload["id"]

    version_response = client.get(f"/api/projects/{project_id}/asset-versions/{version_id}/exports/analysis")
    assert version_response.status_code == 200
    assert "# 十维分析摘要" in version_response.text


def test_cc_skill_split_document_exports_work_for_draft_and_version(client, app):
    project_payload = client.post("/api/projects", json={"name": "Export CC Skill"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Export CC Skill 本人", "analysis_context": "export docs"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Personality", "summary": "边界清楚", "bullets": ["不多话"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    draft_payload = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"}).json()
    draft_id = draft_payload["id"]

    document_expectations = {
        "skill": "---",
        "personality": "# 核心身份与精神底色",
        "memories": "# 核心记忆与经历",
    }
    for key, marker in document_expectations.items():
        response = client.get(f"/api/projects/{project_id}/assets/{draft_id}/exports/{key}")
        assert response.status_code == 200
        assert marker in response.text

    bundle_response = client.get(f"/api/projects/{project_id}/assets/{draft_id}/exports/bundle")
    assert bundle_response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(bundle_response.content)) as archive:
        assert set(archive.namelist()) == {"SKILL.md", "references/personality.md", "references/memories.md", "references/analysis.md"}

    publish_payload = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "cc_skill"},
    ).json()
    version_id = publish_payload["id"]

    version_response = client.get(f"/api/projects/{project_id}/asset-versions/{version_id}/exports/skill")
    assert version_response.status_code == 200
    assert "---" in version_response.text


def test_analysis_export_uses_utf8_filename_for_unicode_project_names(client, app):
    project_payload = client.post("/api/projects", json={"name": "中文项目"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(session, project_id, status="completed", summary_json={})
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.7,
            findings_json={"label": "Personality", "summary": "ready", "bullets": []},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        run_id = run.id

    response = client.get(f"/projects/{project_id}/analysis/export", params={"run_id": run_id})
    assert response.status_code == 200
    disposition = response.headers["content-disposition"]
    assert "filename=analysis_export_" in disposition
    assert "filename*=UTF-8''analysis_export_%E4%B8%AD%E6%96%87%E9%A1%B9%E7%9B%AE_" in disposition


def test_document_delete_removes_record(client, app):
    project_payload = client.post("/api/projects", json={"name": "Bob"}).json()
    project_id = project_payload["id"]
    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("note.txt", io.BytesIO(b"Bob likes tea."), "text/plain")},
    )
    document_id = upload_response.json()["documents"][0]["id"]

    delete_response = client.post(f"/api/projects/{project_id}/documents/{document_id}/delete")
    assert delete_response.status_code == 200
    assert delete_response.json()["ok"] is True

    with app.state.db.session() as session:
        assert repository.get_document(session, document_id) is None


def test_project_rechunk_task_rebuilds_chunks_and_embeddings(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Rechunk"}).json()
    project_id = project_payload["id"]
    text = ("Alpha notes with long context for chunk rebuild. " * 200).encode("utf-8")
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(text), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)
    client.post(
        "/settings/embedding",
        data={
            "provider_kind": "openai",
            "api_key": "sk-test",
            "model": "embed-test",
        },
        follow_redirects=False,
    )

    def fake_embeddings(self, inputs, *, model=None, timeout=None):
            del model
            del timeout
            return [[float(index + 1), float(len(item) % 13), 0.5] for index, item in enumerate(inputs)]

    monkeypatch.setattr(OpenAICompatibleClient, "embeddings", fake_embeddings)

    start = client.post(f"/api/projects/{project_id}/rechunk")
    assert start.status_code == 200
    task_id = start.json()["task_id"]

    status = _wait_for_rechunk(client, project_id, task_id)
    if status["status"] == "failed":
        print(f"Rechunk failed: {status.get('error')}")
    assert status["status"] == "completed"
    assert status["document_total"] >= 1
    assert status["chunk_processed"] >= 1
    assert status["embedding_processed"] >= 1

    with app.state.db.session() as session:
        chunks = list(session.scalars(select(TextChunk).where(TextChunk.project_id == project_id)))
        assert chunks
        assert all(len(chunk.content) <= 1800 for chunk in chunks)
        assert any(chunk.embedding_vector for chunk in chunks)


def test_analysis_stream_and_rerun_api(client, app):
    project_payload = client.post("/api/projects", json={"name": "Stream Debug"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Persona notes with repeated habits and boundaries."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Stream the run state."},
    )
    run_id = analyze_response.json()["id"]

    stream_response = client.get(f"/api/projects/{project_id}/analysis/stream", params={"run_id": run_id})
    assert stream_response.status_code == 200
    assert "event: snapshot" in stream_response.text
    assert '"active_facets"' in stream_response.text
    assert '"queued_facets"' in stream_response.text
    assert '"current_phase"' in stream_response.text

    rerun_response = client.post(f"/api/projects/{project_id}/analysis/personality/rerun")
    assert rerun_response.status_code == 200
    rerun_payload = rerun_response.json()
    assert rerun_payload["id"] == run_id
    assert len(rerun_payload["facets"]) == 10
    assert "active_facets" in rerun_payload["summary"]
    assert "queued_facets" in rerun_payload["summary"]
    assert "current_phase" in rerun_payload["summary"]
    assert all("phase" in facet["findings"] for facet in rerun_payload["facets"])
    assert all("started_at" in facet["findings"] for facet in rerun_payload["facets"])
    assert all("finished_at" in facet["findings"] for facet in rerun_payload["facets"])


def test_analysis_run_survives_single_facet_retrieval_failure(client, app, monkeypatch):
    project_payload = client.post("/api/projects", json={"name": "Retrieval Failure"}).json()
    project_id = project_payload["id"]
    client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"Persona notes with enough text for fallback evidence."), "text/plain")},
    )
    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)

    engine = app.state.analysis_engine
    original = engine._retrieve_hits

    def flaky_retrieve(session, project_id, facet, **kwargs):
        if facet.key == "personality":
            raise RuntimeError("simulated retrieval failure")
        return original(session, project_id, facet, **kwargs)

    monkeypatch.setattr(engine, "_retrieve_hits", flaky_retrieve)

    analyze_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"target_role": "Tester", "analysis_context": "Keep going even if one facet retrieval fails."},
    )
    payload = _wait_for_analysis(client, project_id, analyze_response.json()["id"])

    assert payload["status"] == "partial_failed"
    personality = next(item for item in payload["facets"] if item["facet_key"] == "personality")
    assert personality["status"] == "failed"
    assert any(event["event_type"] == "retrieval" for event in payload["events"])
    assert any(
        event["event_type"] == "retrieval" and "simulated retrieval failure" in str(event["payload"])
        for event in payload["events"]
    )


def test_project_delete_cascades_records_and_files(client, app):
    project_payload = client.post("/api/projects", json={"name": "Delete Me"}).json()
    project_id = project_payload["id"]
    upload_response = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("note.txt", io.BytesIO(b"Delete test content."), "text/plain")},
    )
    document_id = upload_response.json()["documents"][0]["id"]

    delete_response = client.delete(f"/api/projects/{project_id}")
    assert delete_response.status_code == 200
    assert delete_response.json()["ok"] is True

    with app.state.db.session() as session:
        assert repository.get_project(session, project_id) is None
        assert repository.get_document(session, document_id) is None

    assert not (app.state.config.upload_dir / project_id).exists()
    assert not (app.state.config.assets_dir / project_id).exists()
    assert not (app.state.config.output_dir / project_id).exists()


def test_create_app_recovers_stale_active_runs():
    root_dir = Path(".test-workspaces") / f"stale-run-recovery-{uuid4().hex}"
    root_dir.mkdir(parents=True, exist_ok=False)
    config = AppConfig(root_dir=root_dir)
    first_app = create_app(config)
    try:
        with first_app.state.db.session() as session:
            project = repository.create_project(session, "Recover Me", "test")
            run = repository.create_analysis_run(
                session,
                project_id=project.id,
                status="running",
                summary_json={"current_stage": "running", "current_facet": "personality"},
            )
            run_id = run.id
    finally:
        first_app.state.analysis_runner.shutdown()
        first_app.state.telegram_preprocess_manager.shutdown()
        first_app.state.rechunk_manager.shutdown()
        first_app.state.db.close()

    second_app = create_app(config)
    try:
        with second_app.state.db.session() as session:
            recovered = repository.get_analysis_run(session, run_id)
            assert recovered is not None
            assert recovered.status == "failed"
            assert recovered.summary_json["current_stage"] == "服务重启，旧的后台任务已终止"
            assert any(
                event.event_type == "lifecycle" and event.payload_json.get("recovered_after_restart")
                for event in recovered.events
            )
    finally:
        second_app.state.analysis_runner.shutdown()
        second_app.state.telegram_preprocess_manager.shutdown()
        second_app.state.rechunk_manager.shutdown()
        second_app.state.db.close()
        if root_dir.exists():
            import shutil

            shutil.rmtree(root_dir, ignore_errors=True)


def test_settings_accept_official_provider_without_base_url(client, app):
    response = client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai",
            "base_url": "",
            "api_key": "sk-test",
            "model": "gpt-4.1-mini",
            "api_mode": "responses",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    with app.state.db.session() as session:
        config = repository.get_service_config(session, "chat_service")
        assert config is not None
        assert config.provider_kind == "openai"
        assert config.base_url is None
        assert config.model == "gpt-4.1-mini"
        assert config.api_mode == "responses"


def test_custom_provider_requires_base_url(client):
    response = client.post(
        "/settings/chat",
        data={
            "provider_kind": "openai-compatible",
            "base_url": "",
            "api_key": "sk-test",
            "model": "demo-model",
            "api_mode": "chat_completions",
        },
        follow_redirects=False,
    )
    assert response.status_code == 400


def test_settings_api_persists_multi_config_bundle_and_auto_discovers_models(client, app, monkeypatch):
    def fake_list_models(self):
        return ["gpt-4.1-mini", "gpt-4o-mini"]

    monkeypatch.setattr(OpenAICompatibleClient, "list_models", fake_list_models)

    response = client.post(
        "/api/settings/chat",
        json={
            "active_config_id": "primary",
            "discover_config_id": "primary",
            "fallback_order": ["backup"],
            "configs": [
                {
                    "id": "primary",
                    "label": "Primary",
                    "provider_kind": "openai",
                    "base_url": "",
                    "api_key": "sk-primary",
                    "model": "",
                    "api_mode": "responses",
                    "available_models": [],
                },
                {
                    "id": "backup",
                    "label": "Backup",
                    "provider_kind": "openai-compatible",
                    "base_url": "https://fallback.example/v1",
                    "api_key": "sk-backup",
                    "model": "fallback-model",
                    "api_mode": "chat_completions",
                    "available_models": ["fallback-model"],
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "chat"
    assert payload["discovered_config_id"] == "primary"
    assert payload["discovered_models"] == ["gpt-4.1-mini", "gpt-4o-mini"]
    assert payload["bundle"]["configs"][0]["available_models"] == ["gpt-4.1-mini", "gpt-4o-mini"]
    assert payload["bundle"]["configs"][0]["model"] == "gpt-4.1-mini"

    with app.state.db.session() as session:
        stored_bundle = repository.get_service_setting_bundle(session, "chat_service")
        assert stored_bundle["active_config_id"] == "primary"
        assert stored_bundle["fallback_order"] == ["backup"]
        config = repository.get_service_config(session, "chat_service")
        assert config is not None
        assert config.model == "gpt-4.1-mini"
        assert len(config.fallbacks) == 1
        assert config.fallbacks[0].model == "fallback-model"


def test_asset_version_download_and_delete(client, app):
    project_id = client.post("/api/projects", json={"name": "Asset version ops"}).json()["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "Asset version ops 本人", "analysis_context": "version ops"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.9,
            findings_json={"label": "Personality", "summary": "边界清楚", "bullets": ["不多话"]},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    draft_payload = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "cc_skill"}).json()
    publish_payload = client.post(
        f"/api/projects/{project_id}/assets/{draft_payload['id']}/publish",
        json={"asset_kind": "cc_skill"},
    ).json()
    version_id = publish_payload["id"]

    download_response = client.get(f"/api/projects/{project_id}/asset-versions/{version_id}/download")
    assert download_response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(download_response.content)) as archive:
        assert set(archive.namelist()) == {"SKILL.md", "references/personality.md", "references/memories.md", "references/analysis.md"}

    asset_dir = app.state.config.assets_dir / project_id / "cc_skill"
    assert (asset_dir / "published_v1.md").exists()

    delete_response = client.post(
        f"/projects/{project_id}/asset-versions/{version_id}/delete",
        follow_redirects=False,
    )
    assert delete_response.status_code == 303
    assert delete_response.headers["location"].endswith("/projects/{}/assets?kind=cc_skill".format(project_id))

    with app.state.db.session() as session:
        assert repository.get_asset_version(session, version_id) is None
    assert not (asset_dir / "published_v1.md").exists()


def test_pages_render_simplified_chinese_and_lang(client):
    project_id = client.post("/api/projects", json={"name": "页面检查"}).json()["id"]

    pages = {
        "/": "项目总览",
        f"/projects/{project_id}": "项目控制中心",
        f"/projects/{project_id}/analysis": "分析监控",
        f"/projects/{project_id}/assets?kind=skill": "资产输出工作台",
        f"/projects/{project_id}/playground": "沉浸式对话验证",
        "/settings": "配置 Chat LLM 与 Embedding 服务",
    }

    for path, expected_text in pages.items():
        response = client.get(path)
        assert response.status_code == 200
        assert b'lang="zh-CN"' in response.content
        assert expected_text.encode("utf-8") in response.content
        if path == "/settings":
            assert b"OpenAI \\u5b98\\u65b9" in response.content
            assert b"Gemini \\u5b98\\u65b9" in response.content
        assert b"\xef\xbf\xbd" not in response.content
        assert b"zh-Hant" not in response.content


def test_legacy_preprocess_page_is_removed_for_group_projects(client):
    project_id = client.post("/api/projects", json={"name": "预分析已移除"}).json()["id"]

    response = client.get(f"/projects/{project_id}/preprocess")

    assert response.status_code == 404
    assert "removed for this project mode" in response.text


def test_repository_rejects_legacy_preprocess_session_kind(app):
    with app.state.db.session() as session:
        project = repository.create_project(session, name="Legacy Session Kind")
        with pytest.raises(ValueError, match="Unsupported chat session kind"):
            repository.create_chat_session(session, project_id=project.id, session_kind="preprocess")


def test_localized_api_messages_and_status_fields(client, app, monkeypatch):
    create_payload = client.post("/api/projects", json={"name": "接口本地化"}).json()
    project_id = create_payload["id"]
    assert create_payload["status"] == "ok"
    assert "项目已创建" in create_payload["message"]

    upload_payload = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("memo.txt", io.BytesIO(b"localized api payload"), "text/plain")},
    ).json()
    assert upload_payload["status"] == "ok"
    assert "文档上传完成" in upload_payload["message"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={"target_role": "测试角色", "analysis_context": "接口文案检查"},
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.8,
            findings_json={"label": "Personality", "summary": "ready", "bullets": []},
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )

    def fake_build(asset_kind, project, facets, config, **kwargs):
        return AssetBundle(
            asset_kind=asset_kind,
            markdown_text="# Draft",
            json_payload={"headline": "Preview"},
            prompt_text="Prompt",
        )

    monkeypatch.setattr(app.state.asset_synthesizer, "build", fake_build)

    draft_payload = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "skill"},
    ).json()
    assert draft_payload["request_status"] == "ok"
    assert "资产草稿已生成" in draft_payload["message"]

    publish_payload = client.post(f"/api/projects/{project_id}/skills/{draft_payload['id']}/publish").json()
    assert publish_payload["request_status"] == "ok"
    assert "Skill 版本已发布" in publish_payload["message"]

    chat_payload = client.post(
        f"/api/projects/{project_id}/playground/chat",
        json={"message": "你好，介绍一下自己。"},
    ).json()
    assert chat_payload["status"] == "ok"
    assert "试聊回复已生成" in chat_payload["message"]


def test_ingest_processing_uses_real_extractor_pipeline(client, app, monkeypatch):
    import app.pipeline.ingest_task as ingest_task_module

    calls = []

    def fake_extract_text(filename: str, content: bytes):
        calls.append((filename, content))
        return ExtractionResult(
            raw_text="原始抽取文本",
            clean_text="清洗后的文本",
            title="提取标题",
            author_guess=None,
            created_at_guess=None,
            language="zh",
            metadata={"format": "fake"},
            segments=[ExtractedSegment(text="第一段", metadata={})],
        )

    monkeypatch.setattr(ingest_task_module, "extract_text", fake_extract_text)

    project_id = client.post("/api/projects", json={"name": "抽取回归"}).json()["id"]
    raw_bytes = b"\xff\xfe\x00real extractor path"
    upload_payload = client.post(
        f"/api/projects/{project_id}/documents",
        files={"files": ("broken.txt", io.BytesIO(raw_bytes), "text/plain")},
    ).json()
    document_id = upload_payload["documents"][0]["id"]

    client.post(f"/api/projects/{project_id}/process-all")
    _wait_for_ready(client, project_id)

    assert calls == [("broken.txt", raw_bytes)]
    with app.state.db.session() as session:
        document = repository.get_document(session, document_id)
        assert document is not None
        assert document.title == "提取标题"
        assert document.clean_text == "清洗后的文本"
