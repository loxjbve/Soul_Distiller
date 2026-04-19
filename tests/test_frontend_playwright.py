from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

from app.storage import repository

from tests.test_telegram_mode import _create_ingested_telegram_project, _seed_preprocess_tables


@pytest.fixture()
def live_server(app):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()

    server = uvicorn.Server(
        uvicorn.Config(app, host=host, port=port, log_level="error", lifespan="on")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:
        server.should_exit = True
        thread.join(timeout=5)
        raise RuntimeError("Timed out starting live frontend test server.")

    try:
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.mark.frontend
def test_shell_mode_switches_between_fixed_and_relaxed(page, live_server):
    page.set_viewport_size({"width": 1440, "height": 960})
    page.goto(f"{live_server}/", wait_until="networkidle")
    assert page.evaluate("document.documentElement.dataset.shellMode") == "fixed"

    page.set_viewport_size({"width": 1280, "height": 720})
    page.reload(wait_until="networkidle")
    assert page.evaluate("document.documentElement.dataset.shellMode") == "relaxed"


@pytest.mark.frontend
def test_telegram_persona_studio_scroller_stays_interactive(page, client, app, monkeypatch, live_server):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    _seed_preprocess_tables(app, project_id)

    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(f"{live_server}/projects/{project_id}", wait_until="networkidle")

    scrollable = page.locator("[data-telegram-persona-studio] [data-panel-scroll]").first
    scrollable.evaluate(
        """
        (node) => {
            const grid = node.querySelector('.persona-top-user-grid');
            if (!grid) return;
            const template = grid.querySelector('[data-top-user-card]');
            for (let index = 0; index < 18; index += 1) {
                const clone = template.cloneNode(true);
                clone.dataset.label = `${template.dataset.label} ${index}`;
                clone.querySelector('strong').textContent = `${template.dataset.label} ${index}`;
                grid.appendChild(clone);
            }
        }
        """
    )

    shell_mode = page.evaluate("document.documentElement.dataset.shellMode")
    if shell_mode == "fixed":
        metrics = scrollable.evaluate(
            "(node) => ({ client: node.clientHeight, scroll: node.scrollHeight, topBefore: node.scrollTop })"
        )
        assert metrics["scroll"] > metrics["client"]

        top_after = scrollable.evaluate(
            "(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }"
        )
        assert top_after > metrics["topBefore"]
        scrollable.evaluate("(node) => { node.scrollTop = 0; }")
    else:
        page_metrics = page.evaluate(
            """
            () => ({
                client: document.scrollingElement.clientHeight,
                scroll: document.scrollingElement.scrollHeight
            })
            """
        )
        assert page_metrics["scroll"] > page_metrics["client"]

    page.locator("[data-top-user-card]").first.click()
    assert page.locator("[data-target-submit]").is_enabled()


@pytest.mark.frontend
def test_analysis_feed_scrolls_within_fixed_shell(page, client, app, live_server):
    project_payload = client.post("/api/projects", json={"name": "Frontend Analysis", "description": "scroll shell"}).json()
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
                "analysis_context": "Playwright smoke",
                "target_role": "Tester",
                "requested_concurrency": 2,
                "effective_concurrency": 2,
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
                "summary": "Stable and concise",
                "llm_response_text": "Rendered bubble",
                "retrieval_trace": {"tool_calls": [{"tool": "query_messages"}]},
            },
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        run_id = run.id

    page.set_viewport_size({"width": 1440, "height": 960})
    page.goto(f"{live_server}/projects/{project_id}/analysis?run_id={run_id}", wait_until="networkidle")

    feed = page.locator("#analysis-feed")
    feed.evaluate(
        """
        (node) => {
            for (let index = 0; index < 32; index += 1) {
                const bubble = document.createElement('article');
                bubble.className = 'bubble bubble--assistant';
                bubble.textContent = `Synthetic bubble ${index}`;
                node.appendChild(bubble);
            }
        }
        """
    )

    metrics = feed.evaluate("(node) => ({ client: node.clientHeight, scroll: node.scrollHeight, topBefore: node.scrollTop })")
    assert metrics["scroll"] > metrics["client"]

    top_after = feed.evaluate("(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }")
    assert top_after > metrics["topBefore"]
