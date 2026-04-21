from __future__ import annotations

import importlib.util
import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from app.storage import repository

_telegram_mode_spec = importlib.util.spec_from_file_location(
    "local_test_telegram_mode",
    Path(__file__).with_name("test_telegram_mode.py"),
)
assert _telegram_mode_spec and _telegram_mode_spec.loader
_telegram_mode = importlib.util.module_from_spec(_telegram_mode_spec)
_telegram_mode_spec.loader.exec_module(_telegram_mode)
_create_ingested_telegram_project = _telegram_mode._create_ingested_telegram_project
_seed_preprocess_tables = _telegram_mode._seed_preprocess_tables


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


@pytest.fixture()
def page():
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("playwright is not installed in the active test environment")

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": 1600, "height": 1200})
            try:
                yield page
            finally:
                browser.close()
    except PlaywrightError as exc:
        pytest.skip(f"playwright browser is unavailable: {exc}")


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

    page.set_viewport_size({"width": 1600, "height": 1100})
    page.goto(f"{live_server}/projects/{project_id}", wait_until="networkidle")

    assert page.evaluate("document.documentElement.dataset.shellMode") == "fixed"

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

    metrics = scrollable.evaluate(
        "(node) => ({ client: node.clientHeight, scroll: node.scrollHeight, topBefore: node.scrollTop })"
    )
    assert metrics["client"] > 120
    assert metrics["scroll"] > metrics["client"]

    top_after = scrollable.evaluate(
        "(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }"
    )
    assert top_after > metrics["topBefore"]
    scrollable.evaluate("(node) => { node.scrollTop = 0; }")

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


@pytest.mark.frontend
def test_telegram_preprocess_layout_stays_contained_in_fixed_shell(page, client, app, monkeypatch, live_server):
    project_id = _create_ingested_telegram_project(client, app, monkeypatch)
    run_id = _seed_preprocess_tables(app, project_id)

    page.set_viewport_size({"width": 1766, "height": 931})
    page.goto(f"{live_server}/projects/{project_id}/preprocess?run_id={run_id}", wait_until="networkidle")

    assert page.evaluate("document.documentElement.dataset.shellMode") == "fixed"

    page.evaluate(
        """
        () => {
            const duplicateCards = (selector, count) => {
                const board = document.querySelector(selector);
                const template = board?.querySelector('.document-card');
                if (!board || !template) return;
                for (let index = 0; index < count; index += 1) {
                    const clone = template.cloneNode(true);
                    clone.querySelectorAll('strong').forEach((node) => {
                        node.textContent = `${node.textContent} ${index}`;
                    });
                    board.appendChild(clone);
                }
            };

            duplicateCards('#telegram-preprocess-weekly-candidates', 12);
            duplicateCards('#telegram-preprocess-top-users', 12);
            duplicateCards('#telegram-preprocess-topics', 10);

            const traceList = document.querySelector('#telegram-preprocess-trace-list');
            if (traceList) {
                for (let index = 0; index < 18; index += 1) {
                    const bubble = document.createElement('div');
                    bubble.className = 'bubble-context-row';
                    bubble.innerHTML = `
                        <div class="bubble bubble--context bubble--context-processing">
                            <span class="bubble__dot" aria-hidden="true"></span>
                            <span>Synthetic trace ${index}</span>
                            <small>weekly_topic_agent</small>
                        </div>
                    `;
                    traceList.appendChild(bubble);
                }
            }
        }
        """
    )

    metrics = page.evaluate(
        """
        () => {
            const rect = (selector) => {
                const node = document.querySelector(selector);
                if (!node) return null;
                const box = node.getBoundingClientRect();
                return {
                    top: box.top,
                    bottom: box.bottom,
                    height: box.height,
                };
            };

            const scrollState = (selector) => {
                const node = document.querySelector(selector);
                if (!node) return null;
                return {
                    clientHeight: node.clientHeight,
                    scrollHeight: node.scrollHeight,
                    topBefore: node.scrollTop,
                };
            };

            return {
                content: rect('.page-content'),
                pulse: rect('.telegram-preprocess-panel--pulse'),
                status: rect('.telegram-preprocess-panel--status'),
                trace: rect('.telegram-preprocess-panel--trace'),
                data: rect('.telegram-preprocess-panel--data'),
                weeklyBoard: scrollState('#telegram-preprocess-weekly-candidates'),
            };
        }
        """
    )

    assert metrics["pulse"]["height"] > 120
    assert metrics["status"]["height"] > 120
    assert metrics["trace"]["bottom"] <= metrics["content"]["bottom"] + 1
    assert metrics["data"]["bottom"] <= metrics["content"]["bottom"] + 1
    assert metrics["weeklyBoard"]["scrollHeight"] > metrics["weeklyBoard"]["clientHeight"]

    weekly_board = page.locator("#telegram-preprocess-weekly-candidates")
    top_after = weekly_board.evaluate("(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }")
    assert top_after > metrics["weeklyBoard"]["topBefore"]
