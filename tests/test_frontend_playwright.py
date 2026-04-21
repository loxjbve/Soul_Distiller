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
        """
        (node) => {
            const firstCard = node.querySelector('[data-top-user-card]');
            return {
                client: node.clientHeight,
                scroll: node.scrollHeight,
                topBefore: node.scrollTop,
                firstCardHeight: firstCard ? firstCard.getBoundingClientRect().height : 0,
            };
        }
        """
    )
    assert metrics["client"] > 120
    assert metrics["scroll"] > metrics["client"]
    assert 60 <= metrics["firstCardHeight"] <= 96

    top_after = scrollable.evaluate(
        "(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }"
    )
    assert top_after > metrics["topBefore"]
    scrollable.evaluate("(node) => { node.scrollTop = 0; }")

    page.locator("[data-top-user-card]").first.click()
    assert page.locator("[data-target-submit]").is_enabled()
    assert "Bob" in page.locator("[data-target-summary]").inner_text()


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
            const board = document.querySelector('#telegram-preprocess-topic-lamps');
            const template = board?.querySelector('.telegram-topic-lamp');
            if (!board || !template) return;
            for (let index = 0; index < 28; index += 1) {
                const clone = template.cloneNode(true);
                clone.classList.remove('status-completed', 'status-running', 'status-failed');
                clone.classList.add(index % 4 === 0 ? 'status-running' : 'status-queued');
                const title = clone.querySelector('strong');
                const meta = clone.querySelector('small');
                const order = clone.querySelector('.telegram-topic-lamp__index');
                if (title) title.textContent = `Synthetic Topic ${index + 2}`;
                if (meta) meta.textContent = `2025-W${String(index + 2).padStart(2, '0')}`;
                if (order) order.textContent = String(index + 2).padStart(2, '0');
                board.appendChild(clone);
            }

            const progressShell = document.querySelector('.telegram-preprocess-progress-spotlight');
            if (progressShell) {
                progressShell.style.minHeight = '180px';
            }
            const metrics = document.querySelector('.telegram-preprocess-metrics');
            if (metrics) {
                metrics.style.minHeight = '140px';
            }
            const topicBoard = document.querySelector('.telegram-preprocess-topic-board');
            if (topicBoard) {
                topicBoard.style.minHeight = '420px';
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
                hub: rect('.telegram-preprocess-hub'),
                spotlight: rect('.telegram-preprocess-progress-spotlight'),
                board: rect('.telegram-preprocess-topic-board'),
                lamps: scrollState('#telegram-preprocess-topic-lamps'),
                hasLegacyPanels: Boolean(
                    document.querySelector('.telegram-preprocess-panel--trace')
                    || document.querySelector('.telegram-preprocess-panel--data')
                    || document.querySelector('#telegram-preprocess-weekly-candidates')
                ),
            };
        }
        """
    )

    assert metrics["hub"]["bottom"] <= metrics["content"]["bottom"] + 1
    assert metrics["spotlight"]["height"] > 120
    assert metrics["board"]["height"] > 220
    assert metrics["lamps"]["scrollHeight"] > metrics["lamps"]["clientHeight"]
    assert metrics["hasLegacyPanels"] is False

    lamp_board = page.locator("#telegram-preprocess-topic-lamps")
    top_after = lamp_board.evaluate("(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }")
    assert top_after > metrics["lamps"]["topBefore"]


@pytest.mark.frontend
def test_profile_report_assets_page_keeps_publish_clickable_in_fixed_shell(page, client, app, live_server):
    project_payload = client.post("/api/projects", json={"name": "Frontend Assets", "description": "profile shell"}).json()
    project_id = project_payload["id"]

    with app.state.db.session() as session:
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "target_role": "Frontend Assets 本人",
                "analysis_context": "profile report shell",
            },
        )
        repository.upsert_facet(
            session,
            run.id,
            "personality",
            status="completed",
            confidence=0.86,
            findings_json={
                "label": "Personality",
                "summary": "Stable and reflective",
                "bullets": ["Answers with explicit tradeoffs", "Keeps emotional distance under pressure"],
            },
            evidence_json=[
                {
                    "situation": "回应团队对方案优先级的追问",
                    "expression": "先确认约束，再给出简明判断",
                    "quote": "先把主链路跑通，再谈扩展。",
                    "context_before": "Teammate: 我们要不要先做更多功能？",
                    "context_after": "Another teammate: 那就先收紧当前版本。",
                }
            ],
            conflicts_json=[],
            error_message=None,
        )

    draft_payload = client.post(
        f"/api/projects/{project_id}/assets/generate",
        json={"asset_kind": "profile_report"},
    ).json()
    assert draft_payload["asset_kind"] == "profile_report"

    page.set_viewport_size({"width": 1600, "height": 1100})
    page.goto(f"{live_server}/projects/{project_id}/assets?kind=profile_report", wait_until="networkidle")

    assert page.evaluate("document.documentElement.dataset.shellMode") == "fixed"
    assert page.locator("#asset-publish-btn").is_enabled()

    page.evaluate(
        """
        () => {
            const scroll = document.querySelector('.asset-draft-scroll');
            if (!scroll) return;
            const filler = document.createElement('div');
            filler.style.height = '960px';
            filler.dataset.syntheticFill = '1';
            scroll.appendChild(filler);
        }
        """
    )

    metrics = page.evaluate(
        """
        () => {
            const scroll = document.querySelector('.asset-draft-scroll');
            const publish = document.querySelector('#asset-publish-btn');
            if (!scroll || !publish) return null;
            const publishBox = publish.getBoundingClientRect();
            return {
                scrollHeight: scroll.scrollHeight,
                clientHeight: scroll.clientHeight,
                topBefore: scroll.scrollTop,
                publishTop: publishBox.top,
                publishBottom: publishBox.bottom,
                viewportHeight: window.innerHeight,
            };
        }
        """
    )

    assert metrics is not None
    assert metrics["scrollHeight"] > metrics["clientHeight"]
    assert metrics["publishTop"] >= 0
    assert metrics["publishBottom"] <= metrics["viewportHeight"]

    top_after = page.locator(".asset-draft-scroll").evaluate(
        "(node) => { node.scrollTop = node.scrollHeight; return node.scrollTop; }"
    )
    assert top_after > metrics["topBefore"]

    assert page.locator(".version-card").count() == 0
    page.locator("#asset-publish-btn").click()
    page.wait_for_function("() => document.querySelectorAll('.version-card').length > 0", timeout=4000)
    assert page.locator(".version-card").count() >= 1
