import {
    clampPercent,
    escapeHtml,
    fetchJson,
    formatDateTime,
    renderMarkdownInto,
    safeParseJson,
    setStatusTone,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("analysis-page-bootstrap")?.textContent, {});

if (bootstrap?.project_id && bootstrap?.run_id) {
    const ui = bootstrap.ui_strings || {};
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id,
        payload: bootstrap.initial_run ? safeParseJson(bootstrap.initial_run, null) : null,
        pollTimer: null,
        stream: null,
        traceEvents: [],
        liveOutputByRequest: {},
        activeRequestKey: null,
    };

    const elements = {
        percent: document.getElementById("analysis-percent"),
        stage: document.getElementById("analysis-stage"),
        concurrency: document.getElementById("analysis-concurrency"),
        slotUsage: document.getElementById("analysis-slot-usage"),
        currentFacet: document.getElementById("analysis-current-facet"),
        lastUpdated: document.getElementById("analysis-last-updated"),
        statusChip: document.getElementById("analysis-status-chip"),
        progressLabel: document.getElementById("analysis-progress-label"),
        progressCaption: document.getElementById("analysis-progress-caption"),
        progressFill: document.getElementById("analysis-progress-fill"),
        facetList: document.getElementById("facet-status-list"),
        eventList: document.getElementById("analysis-events"),
        resultList: document.getElementById("analysis-result-list"),
        liveTitle: document.getElementById("analysis-live-title"),
        liveOutput: document.getElementById("analysis-live-output"),
        traceList: document.getElementById("analysis-trace-list"),
    };

    if (state.payload) {
        render(state.payload);
    }
    connectStream();

    function connectStream() {
        state.stream = new EventSource(`/api/projects/${state.projectId}/analysis/stream?run_id=${encodeURIComponent(state.runId)}`);
        state.stream.addEventListener("snapshot", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            state.payload = payload;
            render(payload);
            if (!isRunning(payload.status)) {
                stopStream();
            }
        });
        state.stream.addEventListener("trace", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            handleTraceEvent(payload);
        });
        state.stream.addEventListener("done", () => stopStream());
        state.stream.onerror = () => {
            stopStream();
            beginPolling();
        };
    }

    function stopStream() {
        if (state.stream) {
            state.stream.close();
            state.stream = null;
        }
    }

    function beginPolling() {
        if (state.pollTimer) {
            return;
        }
        state.pollTimer = window.setInterval(async () => {
            const payload = await fetchJson(`/api/projects/${state.projectId}/analysis?run_id=${encodeURIComponent(state.runId)}`);
            state.payload = payload;
            render(payload);
            if (!isRunning(payload.status)) {
                window.clearInterval(state.pollTimer);
                state.pollTimer = null;
            }
        }, 1800);
    }

    function handleTraceEvent(event) {
        if (event.kind === "llm_delta") {
            if (event.request_key) {
                state.activeRequestKey = event.request_key;
                state.liveOutputByRequest[event.request_key] = String(event.text_preview || "");
            }
            renderLiveOutput(event);
            return;
        }
        pushTraceEvent(event);
        if (event.kind === "llm_request_started" && event.request_key) {
            state.activeRequestKey = event.request_key;
            state.liveOutputByRequest[event.request_key] = "";
        }
        if (event.kind === "llm_request_completed" && event.request_key) {
            state.activeRequestKey = event.request_key;
            state.liveOutputByRequest[event.request_key] = String(
                event.response_text_preview || state.liveOutputByRequest[event.request_key] || ""
            );
        }
        renderTraceList();
        renderLiveOutput(event);
    }

    function pushTraceEvent(event) {
        const key = `${event.timestamp || ""}:${event.kind || ""}:${event.request_key || ""}:${event.tool_name || ""}:${event.round_index || ""}`;
        if (state.traceEvents.some((item) => item._key === key)) {
            return;
        }
        state.traceEvents.push({ ...event, _key: key });
        state.traceEvents = state.traceEvents.slice(-120);
    }

    function render(payload) {
        const summary = payload.summary || {};
        const facets = payload.facets || [];
        const events = payload.events || [];
        const completed = summary.completed_facets || 0;
        const total = summary.total_facets || facets.length || 0;
        const percent = clampPercent(summary.progress_percent || 0);

        updateText(elements.percent, `${percent}%`);
        updateText(elements.stage, summary.current_stage || ui.waiting || "等待中");
        updateText(elements.concurrency, summary.concurrency || 0);
        updateText(elements.slotUsage, `${summary.active_facets || 0} / ${summary.concurrency || 0}`);
        updateText(elements.currentFacet, summary.current_facet || ui.waiting || "等待中");
        updateText(elements.lastUpdated, `${ui.last_updated || "最近更新时间"} · ${formatDateTime(payload.finished_at || payload.started_at || events[0]?.created_at)}`);
        updateText(elements.progressLabel, summary.current_stage || ui.waiting || "等待中");
        updateText(elements.progressCaption, `${completed} / ${total}`);
        if (elements.progressFill) {
            elements.progressFill.style.width = `${percent}%`;
        }
        setStatusTone(elements.statusChip, payload.status, payload.status);

        renderFacetList(facets);
        renderEvents(events);
        renderResults(facets);
        renderTraceList();
        renderLiveOutput();
    }

    function renderFacetList(facets) {
        elements.facetList.innerHTML = "";
        facets.forEach((facet) => {
            const card = document.createElement("article");
            card.className = "facet-card";
            card.innerHTML = `
                <div class="project-card__title-row">
                    <strong>${escapeHtml(facet.findings?.label || facet.facet_key)}</strong>
                    <span class="status-chip tone-${normalizeTone(facet.status)}">${escapeHtml(facet.status)}</span>
                </div>
                <p class="helper-text">${escapeHtml(facet.findings?.phase || "")}</p>
                <div class="document-card__meta">
                    <span>${escapeHtml(ui.queue_position || "排队位置")}：${facet.findings?.queue_position ?? "--"}</span>
                    <span>置信度：${Number(facet.confidence || 0).toFixed(2)}</span>
                </div>
            `;
            elements.facetList.appendChild(card);
        });
    }

    function renderEvents(events) {
        elements.eventList.innerHTML = "";
        if (!events.length) {
            const empty = document.createElement("div");
            empty.className = "empty-panel";
            empty.textContent = ui.no_events || "暂无事件";
            elements.eventList.appendChild(empty);
            return;
        }
        events.forEach((event) => {
            const card = document.createElement("article");
            card.className = "event-card";
            card.innerHTML = `
                <div class="project-card__title-row">
                    <strong>${escapeHtml(event.event_type)}</strong>
                    <span class="status-chip tone-${event.level === "error" ? "failed" : event.level === "warning" ? "warning" : "ready"}">${escapeHtml(event.level || "info")}</span>
                </div>
                <p>${escapeHtml(event.message || "")}</p>
                <div class="event-card__meta top-gap">${escapeHtml(formatDateTime(event.created_at))}</div>
                ${event.payload && Object.keys(event.payload).length ? `<details class="trace-disclosure top-gap"><summary class="trace-preview-line">${escapeHtml(collapseToSingleLine(JSON.stringify(event.payload, null, 2)))}</summary><pre class="trace-box trace-box--expanded top-gap">${escapeHtml(JSON.stringify(event.payload, null, 2))}</pre></details>` : ""}
            `;
            elements.eventList.appendChild(card);
        });
    }

    function renderResults(facets) {
        elements.resultList.innerHTML = "";
        facets.forEach((facet) => {
            const card = document.createElement("article");
            card.className = "result-card";

            const titleRow = document.createElement("div");
            titleRow.className = "project-card__title-row";
            titleRow.innerHTML = `
                <strong>${escapeHtml(facet.findings?.label || facet.facet_key)}</strong>
                <span class="status-chip tone-${normalizeTone(facet.status)}">${escapeHtml(facet.status)}</span>
            `;
            card.appendChild(titleRow);

            const summary = document.createElement("div");
            renderMarkdownInto(summary, facet.findings?.summary || "暂无摘要");
            card.appendChild(summary);

            if (facet.evidence?.length) {
                const evidence = document.createElement("div");
                evidence.className = "top-gap";
                evidence.innerHTML = `<strong>${escapeHtml(ui.evidence || "证据")}</strong>`;
                const list = document.createElement("ul");
                facet.evidence.slice(0, 6).forEach((item) => {
                    const li = document.createElement("li");
                    li.textContent = typeof item === "string" ? item : JSON.stringify(item, null, 2);
                    list.appendChild(li);
                });
                evidence.appendChild(list);
                card.appendChild(evidence);
            }

            const retrievalTrace = facet.findings?.retrieval_trace || null;
            if (retrievalTrace) {
                const trace = document.createElement("details");
                trace.className = "trace-disclosure top-gap";
                const toolCalls = retrievalTrace.tool_calls || [];
                trace.innerHTML = `
                    <summary class="trace-preview-line">${escapeHtml(buildRetrievalTracePreview(retrievalTrace))}</summary>
                    <pre class="trace-box trace-box--expanded top-gap">${escapeHtml(JSON.stringify(retrievalTrace, null, 2))}</pre>
                `;
                card.appendChild(trace);

                if (toolCalls.length) {
                    const toolTrace = document.createElement("details");
                    toolTrace.className = "trace-disclosure top-gap";
                    toolTrace.innerHTML = `
                        <summary class="trace-preview-line">${escapeHtml(`Tool Calls · ${toolCalls.length}`)}</summary>
                        <pre class="trace-box trace-box--expanded top-gap">${escapeHtml(JSON.stringify(toolCalls, null, 2))}</pre>
                    `;
                    card.appendChild(toolTrace);
                }
            }

            if (facet.conflicts?.length) {
                const conflicts = document.createElement("div");
                conflicts.className = "top-gap";
                conflicts.innerHTML = `<strong>${escapeHtml(ui.notes || "备注与冲突")}</strong>`;
                const list = document.createElement("ul");
                facet.conflicts.slice(0, 4).forEach((item) => {
                    const li = document.createElement("li");
                    li.textContent = typeof item === "string" ? item : JSON.stringify(item, null, 2);
                    list.appendChild(li);
                });
                conflicts.appendChild(list);
                card.appendChild(conflicts);
            }

            if (facet.findings?.llm_response_text || facet.findings?.llm_live_text || facet.findings?.retrieval_trace?.tool_calls?.length) {
                const trace = document.createElement("details");
                trace.className = "top-gap";
                trace.innerHTML = `<summary>${escapeHtml(ui.trace || "LLM 跟踪")}</summary>`;

                const toolCallsHtml = (facet.findings?.retrieval_trace?.tool_calls || []).map(call => `
                    <details class="tool-call">
                        <summary class="tool-header">
                            <span class="tool-icon">⚙️</span>
                            <span>calling ${escapeHtml(call.tool)}(...)</span>
                        </summary>
                        <div class="tool-body">
                            <p>Arguments:</p>
                            <pre><code>${escapeHtml(typeof call.arguments === 'string' ? call.arguments : JSON.stringify(call.arguments, null, 2))}</code></pre>
                            ${call.result ? `<p style="margin-top: 8px;">Result:</p><pre><code>${escapeHtml(typeof call.result === 'string' ? call.result : JSON.stringify(call.result, null, 2))}</code></pre>` : ''}
                            ${call.error ? `<p style="margin-top: 8px; color: var(--danger);">Error:</p><pre><code>${escapeHtml(call.error)}</code></pre>` : ''}
                        </div>
                    </details>
                `).join("");

                const textOutput = facet.findings?.llm_response_text || facet.findings?.llm_live_text || "";
                const textOutputHtml = textOutput ? `
                    <div class="msg-assistant">
                        <div class="code-block-wrapper">
                            <div class="code-block-header">
                                <span>output</span>
                            </div>
                            <pre><code>${escapeHtml(textOutput)}</code></pre>
                        </div>
                    </div>
                ` : "";

                const container = document.createElement("div");
                container.className = "msg-assistant";
                container.innerHTML = toolCallsHtml + textOutputHtml;

                trace.appendChild(container);
                card.appendChild(trace);
            }

            const actions = document.createElement("div");
            actions.className = "button-row top-gap";
            const rerunButton = document.createElement("button");
            rerunButton.type = "button";
            rerunButton.className = "ghost-button";
            rerunButton.textContent = ui.rerun || "重跑维度";
            rerunButton.addEventListener("click", () => rerunFacet(facet.facet_key, rerunButton));
            actions.appendChild(rerunButton);
            card.appendChild(actions);

            elements.resultList.appendChild(card);
        });
    }

    function renderTraceList() {
        if (!elements.traceList) {
            return;
        }
        elements.traceList.innerHTML = "";
        const events = state.traceEvents.slice(-80);
        if (!events.length) {
            elements.traceList.innerHTML = `<div class="empty-panel"><strong>等待 Agent trace…</strong></div>`;
            return;
        }
        events.forEach((event) => {
            const card = document.createElement("article");
            card.className = "event-card compact-card";
            const meta = [
                event.agent || "",
                event.facet_key || "",
                event.round_index ? `round ${event.round_index}` : "",
                event.tool_name ? `tool ${event.tool_name}` : "",
            ]
                .filter(Boolean)
                .join(" · ");
            const detailText = event.output_preview || event.response_text_preview || event.arguments_preview || event.prompt_preview || "";
            card.innerHTML = `
                <div class="project-card__title-row">
                    <strong>${escapeHtml(summarizeTraceEvent(event))}</strong>
                    <span class="status-chip tone-${traceTone(event.kind)}">${escapeHtml(event.kind || "trace")}</span>
                </div>
                <p class="helper-text">${escapeHtml(meta || "--")}</p>
                <div class="event-card__meta top-gap">${escapeHtml(formatDateTime(event.timestamp))}</div>
                ${detailText ? `
                    <details class="trace-disclosure top-gap">
                        <summary class="trace-preview-line">${escapeHtml(collapseToSingleLine(detailText))}</summary>
                        <pre class="trace-box trace-box--expanded top-gap">${escapeHtml(detailText)}</pre>
                    </details>
                ` : ""}
            `;
            elements.traceList.appendChild(card);
        });
    }

    function renderLiveOutput(event = null) {
        if (!elements.liveTitle || !elements.liveOutput) {
            return;
        }
        const requestKey = event?.request_key || state.activeRequestKey;
        if (!requestKey) {
            updateText(elements.liveTitle, "等待新的 Agent 请求");
            updateText(elements.liveOutput, "");
            return;
        }
        updateText(elements.liveTitle, summarizeLiveTitle(event, requestKey));
        updateText(elements.liveOutput, state.liveOutputByRequest[requestKey] || "");
    }

    async function rerunFacet(facetKey, button) {
        button.disabled = true;
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/analysis/${facetKey}/rerun`, { method: "POST" });
            state.payload = payload;
            render(payload);
            stopStream();
            connectStream();
        } finally {
            button.disabled = false;
        }
    }

    function buildRetrievalTracePreview(trace) {
        const topicCount = Number(trace.topic_count_used || trace.topic_ids?.length || 0);
        const weekCount = Number(trace.topic_weeks_used?.length || 0);
        const toolCount = Number(trace.tool_calls?.length || 0);
        return `Topics ${topicCount} · Weeks ${weekCount} · Tools ${toolCount}`;
    }

    function summarizeTraceEvent(event) {
        if (event.kind === "agent_started") {
            return `${event.label || event.facet_key || "Agent"} started`;
        }
        if (event.kind === "agent_completed") {
            return `${event.label || event.facet_key || "Agent"} completed`;
        }
        if (event.kind === "llm_request_started") {
            return "LLM 请求开始";
        }
        if (event.kind === "llm_request_completed") {
            return "LLM 请求完成";
        }
        if (event.kind === "tool_call") {
            return `Tool 调用 · ${event.tool_name || "unknown"}`;
        }
        if (event.kind === "tool_result") {
            return `Tool 返回 · ${event.tool_name || "unknown"}`;
        }
        return event.kind || "trace";
    }

    function summarizeLiveTitle(event, requestKey) {
        const trace = event || state.traceEvents.slice().reverse().find((item) => item.request_key === requestKey) || {};
        const parts = [trace.agent || "agent", trace.facet_key || "", trace.tool_name ? `tool ${trace.tool_name}` : ""].filter(Boolean);
        return parts.join(" · ") || requestKey;
    }

    function traceTone(kind) {
        if (kind === "tool_call") {
            return "queued";
        }
        if (kind === "tool_result" || kind === "llm_request_completed" || kind === "agent_completed") {
            return "ready";
        }
        if (kind === "llm_request_started" || kind === "agent_started" || kind === "llm_delta") {
            return "processing";
        }
        return "warning";
    }

    function collapseToSingleLine(text, limit = 220) {
        const compact = String(text || "").replace(/\s+/g, " ").trim();
        if (compact.length <= limit) {
            return compact;
        }
        return `${compact.slice(0, limit)}...`;
    }

    function normalizeTone(status) {
        if (status === "completed") {
            return "ready";
        }
        if (status === "failed") {
            return "failed";
        }
        if (status === "queued") {
            return "queued";
        }
        return "processing";
    }

    function isRunning(status) {
        return ["queued", "running"].includes(status);
    }
}
