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

const analysisUiState = {
    detailOpen: new Map(),
};

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
    captureDetailStates();

    updateText("analysis-run-title", `Analysis Run ${payload.id.slice(0, 8)}`);
    updateText("analysis-status-chip", payload.status);
    updateText("analysis-role-chip", payload.summary?.target_role || "Not set");
    updateText("analysis-stage", payload.summary?.current_stage || "Queued");
    updateText("analysis-percent", `${payload.summary?.progress_percent || 0}%`);
    updateText("metric-completed", payload.summary?.completed_facets || 0);
    updateText("metric-failed", payload.summary?.failed_facets || 0);
    updateText("metric-llm-success", payload.summary?.llm_successes || 0);
    updateText("metric-llm-failure", payload.summary?.llm_failures || 0);
    updateText("metric-input-tokens", payload.summary?.prompt_tokens || 0);
    updateText("metric-output-tokens", payload.summary?.completion_tokens || 0);
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
            : `<details class="event-item" open><summary class="event-item-summary"><strong>No Events Yet</strong></summary><div class="event-item-body"><p>Run logs will appear here when analysis starts.</p></div></details>`;
    }

    const resultList = document.getElementById("analysis-result-list");
    if (resultList) {
        resultList.innerHTML = payload.facets.map((facet) => renderFacetResult(facet, payload.status)).join("");
    }

    bindFacetRerunActions(projectId);
    bindDetailToggleState();
    scrollLiveTextToBottom();
}

function bindFacetRerunActions(projectId) {
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
                // Reload page to start streaming the rerun
                window.location.reload();
            } catch (error) {
                window.alert(error instanceof Error ? error.message : "Rerun failed");
                button.disabled = false;
            }
        };
    });
}

function renderFacetStatusCard(facet, runStatus) {
    const findings = facet.findings || {};
    const promptTokens = Number(findings.prompt_tokens || 0);
    const completionTokens = Number(findings.completion_tokens || 0);
    const totalTokens = Number(findings.total_tokens || 0);
    const llmLine = findings.llm_called
        ? `LLM ${findings.llm_success ? "ok" : "failed"} · in ${promptTokens} / out ${completionTokens} / total ${totalTokens}`
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
    const level = (event.level || "info").toLowerCase();
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
            typeof trace.candidate_chunks === "number" ? `candidate_chunks=${trace.candidate_chunks}` : "",
            typeof trace.candidate_documents === "number" ? `candidate_documents=${trace.candidate_documents}` : "",
            typeof trace.selected_documents === "number" ? `selected_documents=${trace.selected_documents}` : "",
            typeof trace.per_document_cap_applied === "boolean"
                ? `per_document_cap_applied=${trace.per_document_cap_applied}`
                : "",
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

    const openByDefault = level === "warning" || level === "error";
    const detailKey = `event:${event.id || `${event.event_type}:${event.created_at}`}`;
    return `
        <details class="event-item level-${escapeHtml(level)}" data-detail-key="${escapeHtml(detailKey)}" ${detailOpenAttr(detailKey, openByDefault)}>
            <summary class="event-item-summary">
                <span class="event-main">
                    <strong>${escapeHtml(event.event_type)}</strong>
                    <small>${escapeHtml(event.message || "")}</small>
                </span>
                <span class="event-time">${formatTime(event.created_at)}</span>
            </summary>
            <div class="event-item-body">
                ${payloadHtml || `<p class="muted">No payload.</p>`}
            </div>
        </details>
    `;
}

function renderFacetResult(facet, runStatus) {
    const findings = facet.findings || {};
    const facetKey = facet.facet_key || "unknown";
    const rerunDisabled = ["queued", "running"].includes(runStatus) ? "disabled" : "";
    const facetOpenDefault = ["running", "failed"].includes(facet.status || "");
    const hasError = Boolean(facet.error_message || findings.llm_error);

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

    const notesBody = `
        ${conflicts || `<p class="muted">No conflicts recorded.</p>`}
        ${facet.error_message ? `<p class="danger">${escapeHtml(facet.error_message)}</p>` : ""}
        ${findings.notes ? `<p class="muted">${escapeHtml(findings.notes)}</p>` : ""}
    `;
    const traceBody = `
        ${findings.llm_request_url ? `<p class="muted">${escapeHtml(findings.llm_request_url)}</p>` : ""}
        ${findings.llm_error ? `<p class="danger">${escapeHtml(findings.llm_error)}</p>` : ""}
    `;
    const liveTextBody = findings.llm_live_text
        ? `<pre class="trace-box live-trace-box" data-live-text-scroll>${escapeHtml(findings.llm_live_text)}</pre>`
        : `<p class="muted">No live text yet.</p>`;

    // LLM Live Text should default open if the facet is running
    const isRunning = (facet.status || "") === "running";
    // We want LLM Live Text to be opened by default ALWAYS, not just when running, 
    // to give user a better observation of the streaming
    const defaultOpenLive = true;
    
    return `
        <details class="facet-panel facet-panel-details status-${escapeHtml(facet.status || "pending")}" data-detail-key="facet:${escapeHtml(facetKey)}" ${detailOpenAttr(`facet:${facetKey}`, facetOpenDefault)}>
            <summary class="facet-panel-summary">
                <div>
                    <p class="eyebrow">${escapeHtml(facetKey)}</p>
                    <h2>${escapeHtml(findings.label || facetKey)}</h2>
                    <p class="facet-summary">${escapeHtml(findings.summary || "Waiting for analysis output...")}</p>
                </div>
                <div class="facet-meta">
                    <span>Status ${escapeHtml(facet.status || "pending")}</span>
                    <span>Confidence ${(facet.confidence || 0).toFixed(2)}</span>
                    <span>${facet.accepted ? "Accepted" : "Pending"}</span>
                </div>
            </summary>
            <div class="facet-panel-body">
                <div class="inline-actions">
                    <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facetKey)}" ${rerunDisabled}>Rerun This Facet</button>
                </div>
                ${bullets ? `<ul class="facet-bullets">${bullets}</ul>` : ""}
                ${renderSubDetails(`facet:${facetKey}:evidence`, "Evidence", evidence || `<p class="muted">No evidence captured yet.</p>`, false)}
                ${renderSubDetails(`facet:${facetKey}:notes`, "Notes", notesBody, hasError)}
                ${(findings.llm_request_url || findings.llm_error)
                    ? renderSubDetails(`facet:${facetKey}:trace`, "LLM Trace", traceBody, hasError)
                    : ""}
                ${renderSubDetails(`facet:${facetKey}:live`, "LLM Live Text", liveTextBody, defaultOpenLive)}
            </div>
        </details>
    `;
}

function renderSubDetails(key, title, body, defaultOpen) {
    return `
        <details class="analysis-subsection" data-detail-key="${escapeHtml(key)}" ${detailOpenAttr(key, defaultOpen)}>
            <summary>${escapeHtml(title)}</summary>
            <div class="analysis-subsection-body">
                ${body}
            </div>
        </details>
    `;
}

function captureDetailStates() {
    document.querySelectorAll("details[data-detail-key]").forEach((node) => {
        const key = node.getAttribute("data-detail-key");
        if (!key) {
            return;
        }
        analysisUiState.detailOpen.set(key, node.open);
    });
}

function bindDetailToggleState() {
    document.querySelectorAll("details[data-detail-key]").forEach((node) => {
        if (node.dataset.toggleBound === "1") {
            return;
        }
        node.dataset.toggleBound = "1";
        const key = node.getAttribute("data-detail-key");
        if (!key) {
            return;
        }
        node.addEventListener("toggle", () => {
            analysisUiState.detailOpen.set(key, node.open);
        });
    });
}

function detailOpenAttr(key, defaultOpen) {
    if (analysisUiState.detailOpen.has(key)) {
        return analysisUiState.detailOpen.get(key) ? "open" : "";
    }
    return defaultOpen ? "open" : "";
}

function scrollLiveTextToBottom() {
    document.querySelectorAll("[data-live-text-scroll]").forEach((node) => {
        node.scrollTop = node.scrollHeight;
    });
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
