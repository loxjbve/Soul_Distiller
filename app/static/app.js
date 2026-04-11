document.addEventListener("DOMContentLoaded", () => {
    setupDropUploads();
    setupAnalysisMonitor();
});

function setupDropUploads() {
    document.querySelectorAll("[data-drop-upload]").forEach((form) => {
        const dropzone = form.querySelector("[data-dropzone]");
        const input = form.querySelector("[data-file-input]");
        const list = form.querySelector("[data-file-list]");
        const count = form.querySelector("[data-file-count]");
        if (!dropzone || !input || !list || !count) {
            return;
        }

        const renderFiles = (files) => {
            list.innerHTML = "";
            if (!files.length) {
                count.textContent = "No files selected";
                return;
            }
            count.textContent = `${files.length} file(s) selected`;
            Array.from(files).forEach((file) => {
                const pill = document.createElement("span");
                pill.textContent = file.name;
                list.appendChild(pill);
            });
        };

        input.addEventListener("change", () => renderFiles(input.files || []));

        ["dragenter", "dragover"].forEach((eventName) => {
            dropzone.addEventListener(eventName, (event) => {
                event.preventDefault();
                dropzone.classList.add("drag-active");
            });
        });

        ["dragleave", "drop"].forEach((eventName) => {
            dropzone.addEventListener(eventName, (event) => {
                event.preventDefault();
                dropzone.classList.remove("drag-active");
            });
        });

        dropzone.addEventListener("drop", (event) => {
            const files = event.dataTransfer?.files;
            if (!files || !files.length) {
                return;
            }
            if (window.DataTransfer) {
                const transfer = new DataTransfer();
                Array.from(files).forEach((file) => transfer.items.add(file));
                input.files = transfer.files;
                renderFiles(transfer.files);
                return;
            }
            renderFiles(files);
        });
    });
}

function setupAnalysisMonitor() {
    const monitor = document.querySelector("[data-analysis-monitor]");
    if (!monitor) {
        return;
    }

    const bootstrap = document.getElementById("analysis-bootstrap");
    const projectId = monitor.dataset.projectId;
    const runId = monitor.dataset.runId;
    if (!bootstrap || !projectId || !runId) {
        return;
    }

    let payload = null;
    try {
        payload = JSON.parse(bootstrap.textContent || "null");
    } catch {
        payload = null;
    }

    if (payload) {
        renderAnalysis(payload, projectId);
    }

    const stream = new EventSource(`/api/projects/${projectId}/analysis/stream?run_id=${encodeURIComponent(runId)}`);
    let pollId = null;

    stream.addEventListener("snapshot", (event) => {
        const nextPayload = JSON.parse(event.data);
        renderAnalysis(nextPayload, projectId);
        if (!["queued", "running"].includes(nextPayload.status)) {
            stream.close();
            stopPolling();
        }
    });

    stream.addEventListener("done", () => {
        stream.close();
        stopPolling();
    });

    stream.onerror = () => {
        if (pollId) {
            return;
        }
        pollId = window.setInterval(async () => {
            const response = await fetch(`/api/projects/${projectId}/analysis?run_id=${encodeURIComponent(runId)}`);
            if (!response.ok) {
                return;
            }
            const nextPayload = await response.json();
            renderAnalysis(nextPayload, projectId);
            if (!["queued", "running"].includes(nextPayload.status)) {
                stopPolling();
            }
        }, 1500);
    };

    function stopPolling() {
        if (pollId) {
            clearInterval(pollId);
            pollId = null;
        }
    }
}

function renderAnalysis(payload, projectId) {
    updateText("analysis-run-title", `Analysis Run ${payload.id.slice(0, 8)}`);
    updateText("analysis-status-chip", payload.status);
    updateText("analysis-role-chip", payload.summary?.target_role || "Not set");
    updateText("analysis-stage", payload.summary?.current_stage || "Queued");
    updateText("analysis-percent", `${payload.summary?.progress_percent || 0}%`);
    updateText("metric-completed", payload.summary?.completed_facets || 0);
    updateText("metric-failed", payload.summary?.failed_facets || 0);
    updateText("metric-llm-success", payload.summary?.llm_successes || 0);
    updateText("metric-llm-failure", payload.summary?.llm_failures || 0);
    updateText("metric-total-tokens", payload.summary?.total_tokens || 0);
    updateText("metric-current-facet", payload.summary?.current_facet || "-");

    const progressFill = document.getElementById("analysis-progress-fill");
    if (progressFill) {
        progressFill.style.width = `${payload.summary?.progress_percent || 0}%`;
    }

    const facetStatusList = document.getElementById("facet-status-list");
    if (facetStatusList) {
        facetStatusList.innerHTML = payload.facets.map((facet) => renderFacetStatusCard(facet, payload.status)).join("");
    }

    const eventList = document.getElementById("analysis-events");
    if (eventList) {
        eventList.innerHTML = payload.events.length
            ? payload.events.map(renderEventItem).join("")
            : `<div class="event-item"><strong>No Events Yet</strong><p>Run logs will appear here when analysis starts.</p></div>`;
    }

    const resultList = document.getElementById("analysis-result-list");
    if (resultList) {
        resultList.innerHTML = payload.facets.map((facet) => renderFacetResult(facet, payload.status)).join("");
    }

    document.querySelectorAll("[data-facet-rerun]").forEach((button) => {
        button.onclick = async () => {
            button.disabled = true;
            try {
                const response = await fetch(
                    `/api/projects/${projectId}/analysis/${encodeURIComponent(button.dataset.facetRerun)}/rerun`,
                    {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                    }
                );
                if (!response.ok) {
                    const errorPayload = await response.json().catch(() => ({}));
                    throw new Error(errorPayload.detail || "Rerun failed");
                }
            } catch (error) {
                window.alert(error instanceof Error ? error.message : "Rerun failed");
                button.disabled = false;
            }
        };
    });
}

function renderFacetStatusCard(facet, runStatus) {
    const findings = facet.findings || {};
    const llmLine = findings.llm_called
        ? `LLM ${findings.llm_success ? "ok" : "failed"} · ${findings.total_tokens || 0} tokens`
        : "LLM not used";
    const rerunDisabled = ["queued", "running"].includes(runStatus) ? "disabled" : "";
    return `
        <article class="facet-status-card status-${escapeHtml(facet.status || "pending")}">
            <div class="facet-status-head">
                <strong>${escapeHtml(findings.label || facet.facet_key)}</strong>
                <span class="status-pill">${escapeHtml(facet.status || "pending")}</span>
            </div>
            <p>${escapeHtml(findings.summary || "Waiting for result...")}</p>
            <small>${escapeHtml(llmLine)}</small>
            <div class="inline-actions top-gap">
                <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${rerunDisabled}>Rerun</button>
            </div>
        </article>
    `;
}

function renderEventItem(event) {
    const payload = event.payload || {};
    let payloadHtml = "";
    if (event.event_type === "llm_delta") {
        payloadHtml = `<pre class="trace-box">${escapeHtml(payload.text || payload.delta || "")}</pre>`;
    } else if (event.event_type === "llm_response" && payload.response_text) {
        payloadHtml = `<pre class="trace-box">${escapeHtml(payload.response_text)}</pre>`;
    } else if (event.event_type === "retrieval") {
        const trace = payload.retrieval_trace || {};
        const summary = [
            payload.retrieval_mode ? `mode=${payload.retrieval_mode}` : "",
            typeof payload.hit_count === "number" ? `hits=${payload.hit_count}` : "",
            trace.embedding_url ? `embedding=${trace.embedding_url}` : "",
            trace.embedding_skip_reason ? `skip=${trace.embedding_skip_reason}` : "",
            trace.fallback_reason ? `fallback=${trace.fallback_reason}` : "",
            trace.embedding_error ? `embedding_error=${trace.embedding_error}` : "",
            trace.error ? `error=${trace.error}` : "",
        ]
            .filter(Boolean)
            .join("\n");
        payloadHtml = summary
            ? `<pre class="trace-box">${escapeHtml(summary)}</pre>`
            : `<pre class="trace-box">${escapeHtml(JSON.stringify(payload, null, 2))}</pre>`;
    } else if (Object.keys(payload).length) {
        payloadHtml = `<pre class="trace-box">${escapeHtml(JSON.stringify(payload, null, 2))}</pre>`;
    }
    return `
        <article class="event-item level-${escapeHtml(event.level || "info")}">
            <div class="event-item-head">
                <strong>${escapeHtml(event.event_type)}</strong>
                <span>${formatTime(event.created_at)}</span>
            </div>
            <p>${escapeHtml(event.message || "")}</p>
            ${payloadHtml}
        </article>
    `;
}

function renderFacetResult(facet, runStatus) {
    const findings = facet.findings || {};
    const rerunDisabled = ["queued", "running"].includes(runStatus) ? "disabled" : "";
    const bullets = (findings.bullets || [])
        .map((item) => `<li>${escapeHtml(item)}</li>`)
        .join("");
    const evidence = (facet.evidence || [])
        .map(
            (item) => `
                <div class="evidence-block">
                    <strong>${escapeHtml(item.filename || item.document_title || "Evidence")}</strong>
                    <p class="muted">${escapeHtml(item.reason || "")}</p>
                    <blockquote>${escapeHtml(item.quote || "")}</blockquote>
                </div>
            `
        )
        .join("");
    const conflicts = (facet.conflicts || [])
        .map(
            (item) => `
                <div class="evidence-block">
                    <strong>${escapeHtml(item.title || "Conflict")}</strong>
                    <p>${escapeHtml(item.detail || "")}</p>
                </div>
            `
        )
        .join("");
    const llmTrace = (findings.llm_request_url || findings.llm_error)
        ? `
            <div class="top-gap">
                <h3>LLM Trace</h3>
                ${findings.llm_request_url ? `<p class="muted">${escapeHtml(findings.llm_request_url)}</p>` : ""}
                ${findings.llm_error ? `<p class="danger">${escapeHtml(findings.llm_error)}</p>` : ""}
            </div>
        `
        : "";
    const liveTrace = findings.llm_live_text
        ? `
            <div class="top-gap">
                <h3>LLM Live Text</h3>
                <pre class="trace-box">${escapeHtml(findings.llm_live_text)}</pre>
            </div>
        `
        : "";

    return `
        <article class="facet-panel">
            <div class="section-head facet-head">
                <div>
                    <p class="eyebrow">${escapeHtml(facet.facet_key)}</p>
                    <h2>${escapeHtml(findings.label || facet.facet_key)}</h2>
                </div>
                <div class="facet-meta">
                    <span>Status ${escapeHtml(facet.status || "pending")}</span>
                    <span>Confidence ${(facet.confidence || 0).toFixed(2)}</span>
                    <span>${facet.accepted ? "Accepted" : "Pending"}</span>
                </div>
            </div>
            <p class="facet-summary">${escapeHtml(findings.summary || "")}</p>
            <div class="inline-actions">
                <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${rerunDisabled}>Rerun This Facet</button>
            </div>
            ${bullets ? `<ul class="facet-bullets">${bullets}</ul>` : ""}
            <div class="facet-grid">
                <div>
                    <h3>Evidence</h3>
                    ${evidence || `<p class="muted">No evidence captured yet.</p>`}
                </div>
                <div>
                    <h3>Notes</h3>
                    ${conflicts || `<p class="muted">No conflicts recorded.</p>`}
                    ${facet.error_message ? `<p class="danger">${escapeHtml(facet.error_message)}</p>` : ""}
                    ${findings.notes ? `<p class="muted">${escapeHtml(findings.notes)}</p>` : ""}
                    ${llmTrace}
                </div>
            </div>
            ${liveTrace}
        </article>
    `;
}

function updateText(id, value) {
    const node = document.getElementById(id);
    if (node) {
        node.textContent = String(value);
    }
}

function formatTime(value) {
    try {
        return new Intl.DateTimeFormat("zh-CN", {
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
        }).format(new Date(value));
    } catch {
        return value;
    }
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}
