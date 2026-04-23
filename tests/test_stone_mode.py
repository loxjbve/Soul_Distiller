from __future__ import annotations

import json
import time
from pathlib import Path
from uuid import uuid4

from app.analysis.facets import get_facets_for_mode
from app.storage import repository


def _wait_for_ready(client, project_id: str, document_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    latest = {}
    while time.time() < deadline:
        latest = client.get(f"/api/projects/{project_id}/documents").json()
        for item in latest.get("documents", []):
            if item["id"] == document_id and item["ingest_status"] == "ready":
                return item
        time.sleep(0.1)
    raise AssertionError(f"document {document_id} did not become ready: {latest}")


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _collect_sse_events(client, url: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    with client.stream("GET", url) as response:
        assert response.status_code == 200
        current_event: str | None = None
        data_lines: list[str] = []
        for raw_line in response.iter_lines():
            line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
            if line == "":
                if current_event is not None:
                    payload = json.loads("\n".join(data_lines)) if data_lines else {}
                    events.append((current_event, payload))
                current_event = None
                data_lines = []
                continue
            if line.startswith("event: "):
                current_event = line[7:]
            elif line.startswith("data: "):
                data_lines.append(line[6:])
    return events


def _seed_stone_analysis(app, project_id: str) -> None:
    facet_catalog = get_facets_for_mode("stone")
    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    storage_path = upload_dir / "seed-stone.txt"
    storage_path.write_text("夜里写字的人总会回到代价、关系和沉默。", encoding="utf-8")

    with app.state.db.session() as session:
        repository.create_document(
            session,
            id=str(uuid4()),
            project_id=project_id,
            filename="seed-stone.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="essay",
            title="Seed Stone",
            author_guess="Author",
            created_at_guess=None,
            raw_text="夜里写字的人总会回到代价、关系和沉默。",
            clean_text="夜里写字的人总会回到代价、关系和沉默。",
            language="zh",
            metadata_json={
                "stone_profile": {
                    "article_theme": "代价、关系、沉默",
                    "narrative_pov": "first_person",
                    "tone": "cool_and_observational",
                    "structure_template": "setup_then_turn",
                    "lexical_markers": ["代价", "关系", "沉默"],
                    "emotional_progression": "steady_pressure_with_small_turns",
                    "nonclinical_signals": ["边界/防御线索偏强：克制、沉默"],
                    "representative_lines": ["夜里写字的人总会回到代价、关系和沉默。"],
                }
            },
            ingest_status="ready",
            error_message=None,
            storage_path=str(storage_path),
        )
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "facet_keys": [facet.key for facet in facet_catalog],
                "facet_labels": [facet.label for facet in facet_catalog],
                "target_role": "Author",
                "analysis_context": "stone corpus",
            },
        )
        for facet in facet_catalog:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="completed",
                confidence=0.84,
                findings_json={
                    "label": facet.label,
                    "summary": f"{facet.label} summary",
                    "bullets": [f"{facet.key} bullet 1", f"{facet.key} bullet 2"],
                },
                evidence_json=[],
                conflicts_json=[],
                error_message=None,
            )


def test_stone_mode_text_document_api_and_analysis_flow(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Project", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    home = client.get("/")
    assert home.status_code == 200
    assert 'value="stone"' in home.text

    project_page = client.get(f"/projects/{project_id}")
    assert project_page.status_code == 200
    assert "写作台" in project_page.text
    assert "添加文章" in project_page.text
    assert 'id="upload-dropzone"' not in project_page.text

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "title": "夜车",
            "content": "我总觉得夜车不是为了把人送到哪里，而是为了把白天没有说完的话重新摇出来。",
            "source_type": "essay",
            "user_note": "first import",
        },
    )
    assert create_doc.status_code == 200
    document_payload = create_doc.json()
    document_id = document_payload["id"]
    assert document_payload["request_status"] == "ok"
    assert document_payload["source_type"] == "essay"
    assert document_payload["status"] in {"queued", "parsing", "chunking", "embedding", "storing", "completed"}

    document_detail = _wait_for_ready(client, project_id, document_id)
    assert document_detail["ingest_status"] == "ready"
    assert document_detail["metadata_json"]["user_note"] == "first import"
    assert document_detail["metadata_json"]["stone_text_entry"] is True

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone corpus", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    run_id = run_response.json()["id"]

    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"
    stone_keys = [facet.key for facet in get_facets_for_mode("stone")]
    assert analysis_payload["summary"]["facet_keys"] == stone_keys
    assert len(analysis_payload["facets"]) == len(stone_keys)

    refreshed_docs = client.get(f"/api/projects/{project_id}/documents").json()["documents"]
    profiled_doc = next(item for item in refreshed_docs if item["id"] == document_id)
    stone_profile = profiled_doc["metadata_json"]["stone_profile"]
    assert stone_profile["article_theme"]
    assert stone_profile["narrative_pov"]
    assert stone_profile["tone"]
    assert stone_profile["structure_template"]


def test_stone_writing_guide_generation_and_writing_workspace_prefers_published_version(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)

    assets_page = client.get(f"/projects/{project_id}/assets")
    assert assets_page.status_code == 200
    assert "Writing Guide" in assets_page.text
    assert "Claude Code Skill" not in assets_page.text

    draft_response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "writing_guide"})
    assert draft_response.status_code == 200
    draft_payload = draft_response.json()
    draft_id = draft_payload["id"]
    assert draft_payload["asset_kind"] == "writing_guide"
    assert draft_payload["json_payload"]["external_slots"] == {
        "clinical_profile": {},
        "vulnerability_map": {},
        "reserved_external": True,
    }
    assert "## external_slots" in draft_payload["markdown_text"]

    writing_page = client.get(f"/projects/{project_id}/writing")
    assert writing_page.status_code == 200
    assert "写作台" in writing_page.text

    session_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Draft Session"},
    ).json()
    session_id = session_payload["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "雨夜的站台", "target_word_count": 600, "extra_requirements": "保持冷静克制"},
    ).json()
    stream_id = message_payload["stream_id"]
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    event_names = [name for name, _payload in events]
    assert "stage" in event_names
    assert "done" in event_names

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    assert assistant_turns
    latest_turn = assistant_turns[-1]
    assert latest_turn["trace"]["guide_source"] == "draft"
    assert len(latest_turn["trace"]["reviews"]) == 5
    assert latest_turn["trace"]["final_judge"]

    publish_response = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "writing_guide"},
    )
    assert publish_response.status_code == 200
    assert publish_response.json()["asset_kind"] == "writing_guide"

    published_session = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Published Session"},
    ).json()
    published_session_id = published_session["id"]
    published_message = client.post(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/messages",
        json={"topic": "凌晨的窗台", "target_word_count": 550, "extra_requirements": "留一点余味"},
    ).json()
    published_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/streams/{published_message['stream_id']}",
    )
    assert "done" in [name for name, _payload in published_events]

    published_detail = client.get(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}"
    ).json()
    published_assistant_turns = [turn for turn in published_detail["turns"] if turn["role"] == "assistant"]
    assert published_assistant_turns[-1]["trace"]["guide_source"] == "published"
