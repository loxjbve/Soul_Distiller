document.addEventListener("DOMContentLoaded", () => {
    setupDropUploads();
    setupAnalysisMonitor();
    setupAssetGenerator();
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
    renderCache: {
        runId: null,
        statusSignature: "",
        eventSignature: "",
        facetSignatures: new Map(),
    },
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

    let latestPayload = payload;
    let renderQueued = false;
    let pollId = null;

    const scheduleRender = (nextPayload) => {
        latestPayload = nextPayload;
        if (renderQueued || !latestPayload) {
            return;
        }
        renderQueued = true;
        window.requestAnimationFrame(() => {
            renderQueued = false;
            renderAnalysis(latestPayload, projectId);
        });
    };

    if (payload) {
        scheduleRender(payload);
    }

    const stream = new EventSource(`/api/projects/${projectId}/analysis/stream?run_id=${encodeURIComponent(runId)}`);

    stream.addEventListener("snapshot", (event) => {
        const nextPayload = safeParseJson(event.data);
        if (!nextPayload) {
            return;
        }
        scheduleRender(nextPayload);
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
            try {
                const response = await fetch(`/api/projects/${projectId}/analysis?run_id=${encodeURIComponent(runId)}`);
                if (!response.ok) {
                    return;
                }
                const nextPayload = await response.json();
                scheduleRender(nextPayload);
                if (!["queued", "running"].includes(nextPayload.status)) {
                    stopPolling();
                }
            } catch {
                // Ignore transient polling failures.
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
    return renderAnalysisV2(payload, projectId);
    captureDetailStates();
    resetAnalysisRenderCacheIfNeeded(payload.id);

    const summary = payload.summary || {};
    const facets = payload.facets || [];
    const events = payload.events || [];
    const totalFacets = Number(summary.total_facets || facets.length || 0);
    const completedFacets = Number(summary.completed_facets || 0);
    const failedFacets = Number(summary.failed_facets || 0);
    const runningCount = facets.filter((facet) => (facet.status || "") === "running").length;
    const latestEvent = events[0] || null;
    const percent = clampPercent(summary.progress_percent || 0);

    updateText("analysis-run-title", `分析运行 ${payload.id.slice(0, 8)}`);
    updateText("analysis-role-chip", summary.target_role || "未指定角色");
    updateText("analysis-stage", summary.current_stage || "排队中");
    updateText("analysis-percent", `${percent}%`);
    updateText("analysis-progress-caption", `${completedFacets + failedFacets} / ${totalFacets} 维`);
    updateText(
        "analysis-active-message",
        runningCount
            ? `${summary.current_facet || "当前维度"} 正在生成中`
            : (summary.current_stage || "等待任务开始")
    );
    updateText("analysis-progress-detail", buildAnalysisDetail(summary, facets, events));
    updateText(
        "analysis-latest-event",
        latestEvent ? trimText(`${latestEvent.event_type} · ${latestEvent.message || "状态更新"}`, 80) : "暂无事件"
    );
    updateText("analysis-running-count", runningCount || 0);
    updateText("analysis-last-updated", formatTime(payload.finished_at || latestEvent?.created_at || payload.started_at));
    updateText("metric-completed", completedFacets);
    updateText("metric-failed", failedFacets);
    updateText("metric-llm-success", summary.llm_successes || 0);
    updateText("metric-llm-failure", summary.llm_failures || 0);
    updateText("metric-input-tokens", summary.prompt_tokens || 0);
    updateText("metric-output-tokens", summary.completion_tokens || 0);
    updateText("metric-total-tokens", summary.total_tokens || 0);
    updateText("metric-current-facet", summary.current_facet || "-");

    const progressFill = document.getElementById("analysis-progress-fill");
    if (progressFill) {
        progressFill.style.width = `${percent}%`;
    }

    setStatusToken("analysis-status-chip", payload.status, payload.status);
    renderFacetStatusList(facets, payload.status);
    renderEventList(events);
    renderFacetResultList(facets, payload.status);

    bindFacetRerunActions(projectId);
    bindDetailToggleState();
    scrollLiveTextToBottom();
}

function renderFacetStatusList(facets, runStatus) {
    const facetStatusList = document.getElementById("facet-status-list");
    if (!facetStatusList) {
        return;
    }

    const signature = facets
        .map((facet) => {
            const findings = facet.findings || {};
            return [
                facet.facet_key,
                facet.status,
                findings.summary || "",
                findings.total_tokens || 0,
                findings.llm_success,
                runStatus,
            ].join("|");
        })
        .join("::");

    if (analysisUiState.renderCache.statusSignature === signature) {
        return;
    }
    analysisUiState.renderCache.statusSignature = signature;
    facetStatusList.innerHTML = facets.map((facet) => renderFacetStatusCard(facet, runStatus)).join("");
}

function renderEventList(events) {
    const eventList = document.getElementById("analysis-events");
    if (!eventList) {
        return;
    }

    const signature = events.map((event) => `${event.id}:${event.event_type}:${event.level}:${event.created_at}`).join("|");
    if (analysisUiState.renderCache.eventSignature === signature) {
        return;
    }
    analysisUiState.renderCache.eventSignature = signature;

    eventList.innerHTML = events.length
        ? events.map(renderEventItem).join("")
        : `<details class="event-item" open><summary class="event-item-summary"><strong>暂无事件</strong></summary><div class="event-item-body"><p>分析开始后，这里会持续写入运行日志。</p></div></details>`;
}

function renderFacetResultList(facets, runStatus) {
    const resultList = document.getElementById("analysis-result-list");
    if (!resultList) {
        return;
    }

    const existingChildren = Array.from(resultList.children);
    const needsFullRender =
        existingChildren.length !== facets.length ||
        existingChildren.some((node, index) => node.getAttribute("data-facet-key") !== (facets[index]?.facet_key || ""));

    if (needsFullRender) {
        resultList.innerHTML = facets.map((facet) => renderFacetResult(facet, runStatus)).join("");
        analysisUiState.renderCache.facetSignatures.clear();
        facets.forEach((facet) => {
            analysisUiState.renderCache.facetSignatures.set(
                facet.facet_key,
                buildFacetRenderSignature(facet, runStatus)
            );
        });
        return;
    }

    facets.forEach((facet, index) => {
        const signature = buildFacetRenderSignature(facet, runStatus);
        const previousSignature = analysisUiState.renderCache.facetSignatures.get(facet.facet_key);
        if (previousSignature === signature) {
            return;
        }
        const nextNode = createNodeFromHtml(renderFacetResult(facet, runStatus));
        existingChildren[index].replaceWith(nextNode);
        analysisUiState.renderCache.facetSignatures.set(facet.facet_key, signature);
    });
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
    const hitCount = Number(findings.hit_count || 0);
    const llmLine = findings.llm_called
        ? `LLM ${findings.llm_success ? "成功" : "降级"} · in ${promptTokens} / out ${completionTokens} / total ${totalTokens}`
        : "尚未调用 LLM";
    const rerunDisabled = ["queued", "running"].includes(runStatus) ? "disabled" : "";
    const preview = trimText(findings.summary || "等待结果返回…", 110);

    return `
        <article class="facet-status-card status-${escapeHtml(normalizeStatus(facet.status || "pending"))}">
            <div class="facet-status-head">
                <strong>${escapeHtml(findings.label || facet.facet_key)}</strong>
                <span class="status-pill">${escapeHtml(facet.status || "pending")}</span>
            </div>
            <p>${escapeHtml(preview)}</p>
            <small>${escapeHtml(`${llmLine} · 命中 ${hitCount} 条证据`)}</small>
            <div class="inline-actions top-gap">
                <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${rerunDisabled}>重新跑这一维</button>
            </div>
        </article>
    `;
}

function renderEventItem(event) {
    const payload = event.payload || {};
    const level = (event.level || "info").toLowerCase();
    let payloadHtml = "";

    if (event.event_type === "llm_response" && payload.response_text) {
        payloadHtml = `<pre class="trace-box">${escapeHtml(payload.response_text)}</pre>`;
        if (payload.response_text_truncated) {
            payloadHtml += `<p class="muted">响应文本过长，当前只展示预览；更完整内容可在对应维度的实时输出里继续查看。</p>`;
        }
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
                ${payloadHtml || `<p class="muted">没有附带 payload。</p>`}
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
    const summaryText = findings.summary || "等待分析输出…";

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
        ${conflicts || `<p class="muted">没有记录到冲突项。</p>`}
        ${facet.error_message ? `<p class="danger">${escapeHtml(facet.error_message)}</p>` : ""}
        ${findings.notes ? `<p class="muted">${escapeHtml(findings.notes)}</p>` : ""}
    `;
    const traceBody = `
        ${findings.llm_request_url ? `<p class="muted">${escapeHtml(findings.llm_request_url)}</p>` : ""}
        ${findings.llm_error ? `<p class="danger">${escapeHtml(findings.llm_error)}</p>` : ""}
    `;
    const liveTextBody = findings.llm_live_text
        ? `
            ${findings.llm_live_text_truncated ? `<p class="muted">流式文本过长，当前展示最近一段预览。</p>` : ""}
            <pre class="trace-box live-trace-box" data-live-text-scroll>${escapeHtml(findings.llm_live_text)}</pre>
        `
        : `<p class="muted">还没有实时文本。</p>`;

    return `
        <details
            class="facet-panel facet-panel-details status-${escapeHtml(normalizeStatus(facet.status || "pending"))}"
            data-detail-key="facet:${escapeHtml(facetKey)}"
            data-facet-key="${escapeHtml(facetKey)}"
            ${detailOpenAttr(`facet:${facetKey}`, facetOpenDefault)}
        >
            <summary class="facet-panel-summary">
                <div class="facet-panel-copy">
                    <p class="eyebrow">${escapeHtml(facetKey)}</p>
                    <h2>${escapeHtml(findings.label || facetKey)}</h2>
                    <p class="facet-summary">${escapeHtml(summaryText)}</p>
                    ${
                        findings.summary_truncated
                            ? `<p class="summary-clamp-note">顶部摘要已自动截断，展开卡片可继续查看流式文本与细节。</p>`
                            : ""
                    }
                </div>
                <div class="facet-meta">
                    <span>Status ${escapeHtml(facet.status || "pending")}</span>
                    <span>Confidence ${Number(facet.confidence || 0).toFixed(2)}</span>
                    <span>${facet.accepted ? "Accepted" : "Pending"}</span>
                </div>
            </summary>
            <div class="facet-panel-body">
                <div class="inline-actions">
                    <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facetKey)}" ${rerunDisabled}>重新运行这一维</button>
                </div>
                ${bullets ? `<ul class="facet-bullets">${bullets}</ul>` : ""}
                ${renderSubDetails(`facet:${facetKey}:evidence`, "Evidence", evidence || `<p class="muted">还没有证据。</p>`, false)}
                ${renderSubDetails(`facet:${facetKey}:notes`, "Notes", notesBody, hasError)}
                ${(findings.llm_request_url || findings.llm_error)
                    ? renderSubDetails(`facet:${facetKey}:trace`, "LLM Trace", traceBody, hasError)
                    : ""}
                ${renderSubDetails(`facet:${facetKey}:live`, "LLM Live Text", liveTextBody, true)}
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

function resetAnalysisRenderCacheIfNeeded(runId) {
    if (analysisUiState.renderCache.runId === runId) {
        return;
    }
    analysisUiState.renderCache.runId = runId;
    analysisUiState.renderCache.statusSignature = "";
    analysisUiState.renderCache.eventSignature = "";
    analysisUiState.renderCache.facetSignatures.clear();
}

function buildFacetRenderSignature(facet, runStatus) {
    return JSON.stringify({
        runStatus,
        facetKey: facet.facet_key || "",
        status: facet.status || "",
        accepted: Boolean(facet.accepted),
        confidence: Number(facet.confidence || 0).toFixed(2),
        errorMessage: facet.error_message || "",
        findings: facet.findings || {},
        evidence: facet.evidence || [],
        conflicts: facet.conflicts || [],
    });
}

function buildAnalysisDetail(summary, facets, events) {
    const totalFacets = Number(summary.total_facets || facets.length || 0);
    const completedFacets = Number(summary.completed_facets || 0);
    const failedFacets = Number(summary.failed_facets || 0);
    const latestEvent = events[0];
    const latestHint = latestEvent ? `最近事件：${latestEvent.message || latestEvent.event_type}` : "事件流会在分析开始后出现。";
    return `${summary.current_stage || "排队中"} · 已完成 ${completedFacets}/${totalFacets}，失败 ${failedFacets}。${latestHint}`;
}

function renderAnalysisV2(payload, projectId) {
    captureDetailStates();
    resetAnalysisRenderCacheIfNeeded(payload.id);

    const summary = payload.summary || {};
    const facets = sortAnalysisFacetsV2(payload.facets || []);
    const events = payload.events || [];
    const totalFacets = Number(summary.total_facets || facets.length || 0);
    const completedFacets = Number(summary.completed_facets || 0);
    const failedFacets = Number(summary.failed_facets || 0);
    const concurrency = Math.max(1, Number(summary.concurrency || 1));
    const activeFacets = Number(summary.active_facets || facets.filter(isFacetActiveV2).length);
    const queuedFacets = Number(summary.queued_facets || facets.filter((facet) => normalizeStatus(facet.status) === "queued").length);
    const currentFacet = findFacetByKeyV2(facets, summary.current_facet) || facets.find(isFacetActiveV2) || null;
    const currentFacetName = currentFacet ? facetLabelV2(currentFacet) : "-";
    const currentPhase = currentFacet
        ? phaseLabelV2(currentFacet.findings?.phase || summary.current_phase || currentFacet.status)
        : phaseLabelV2(summary.current_phase || (queuedFacets ? "queued" : payload.status));
    const latestEvent = selectHeadlineEventV2(events, currentFacet?.facet_key || null);
    const percent = clampPercent(summary.progress_percent || 0);

    updateText("analysis-run-title", `Analysis Run ${payload.id.slice(0, 8)}`);
    updateText("analysis-role-chip", summary.target_role || "Unspecified");
    updateText("analysis-stage", summary.current_stage || "Queued");
    updateText("analysis-percent", `${percent}%`);
    updateText("analysis-progress-caption", `${completedFacets + failedFacets} / ${totalFacets} facets`);
    updateText("analysis-actual-concurrency", concurrency);
    updateText("analysis-slot-usage", `${activeFacets} / ${concurrency}`);
    updateText("analysis-running-count", activeFacets);
    updateText("analysis-active-message", currentFacetName);
    updateText("analysis-current-phase", currentPhase);
    updateText("analysis-queued-count", queuedFacets);
    updateText("analysis-queue-note", buildQueueNoteV2(activeFacets, queuedFacets, concurrency));
    updateText("analysis-progress-detail", buildAnalysisDetailV2(summary, facets, latestEvent));
    updateText("analysis-latest-event", latestEvent ? trimTextV2(latestEvent.message || latestEvent.event_type, 88) : "No events yet");
    updateText(
        "analysis-latest-event-detail",
        latestEvent ? `${latestEvent.event_type} · ${formatTime(latestEvent.created_at)}` : "Live run events will appear here."
    );
    updateText("analysis-last-updated", formatTime(payload.finished_at || latestEvent?.created_at || payload.started_at));
    updateText("metric-completed", completedFacets);
    updateText("metric-failed", failedFacets);
    updateText("metric-llm-success", summary.llm_successes || 0);
    updateText("metric-llm-failure", summary.llm_failures || 0);
    updateText("metric-input-tokens", summary.prompt_tokens || 0);
    updateText("metric-output-tokens", summary.completion_tokens || 0);
    updateText("metric-total-tokens", summary.total_tokens || 0);
    updateText("metric-current-facet", currentFacetName === "-" ? "-" : currentFacetName);

    const progressFill = document.getElementById("analysis-progress-fill");
    if (progressFill) {
        progressFill.style.width = `${percent}%`;
    }

    setStatusToken("analysis-status-chip", statusLabelV2(payload.status), payload.status);
    renderFacetStatusListV2(facets, payload.status);
    renderEventListV2(events);
    renderFacetResultListV2(facets, payload.status);

    bindFacetRerunActions(projectId);
    bindDetailToggleState();
    scrollLiveTextToBottom();
}

function renderFacetStatusListV2(facets, runStatus) {
    const facetStatusList = document.getElementById("facet-status-list");
    if (!facetStatusList) {
        return;
    }

    const signature = facets
        .map((facet) => {
            const findings = facet.findings || {};
            return [
                facet.facet_key,
                facet.status,
                findings.phase || "",
                findings.queue_position ?? "",
                findings.summary || "",
                findings.hit_count || 0,
                findings.total_tokens || 0,
                runStatus,
            ].join("|");
        })
        .join("::");

    if (analysisUiState.renderCache.statusSignature === signature) {
        return;
    }
    analysisUiState.renderCache.statusSignature = signature;
    facetStatusList.innerHTML = facets.map((facet) => renderFacetStatusCardV2(facet, runStatus)).join("");
}

function renderEventListV2(events) {
    const eventList = document.getElementById("analysis-events");
    if (!eventList) {
        return;
    }

    const signature = events.map((event) => `${event.id}:${event.event_type}:${event.level}:${event.created_at}`).join("|");
    if (analysisUiState.renderCache.eventSignature === signature) {
        return;
    }
    analysisUiState.renderCache.eventSignature = signature;

    eventList.innerHTML = events.length
        ? events.map(renderEventItemV2).join("")
        : `<details class="event-item" open><summary class="event-item-summary"><strong>No events yet</strong></summary><div class="event-item-body"><p>Run events will appear here as facets move through the queue.</p></div></details>`;
}

function renderFacetResultListV2(facets, runStatus) {
    const resultList = document.getElementById("analysis-result-list");
    if (!resultList) {
        return;
    }

    const existingChildren = Array.from(resultList.children);
    const needsFullRender =
        existingChildren.length !== facets.length ||
        existingChildren.some((node, index) => node.getAttribute("data-facet-key") !== (facets[index]?.facet_key || ""));

    if (needsFullRender) {
        resultList.innerHTML = facets.map((facet) => renderFacetResultV2(facet, runStatus)).join("");
        analysisUiState.renderCache.facetSignatures.clear();
        facets.forEach((facet) => {
            analysisUiState.renderCache.facetSignatures.set(
                facet.facet_key,
                buildFacetRenderSignature(facet, runStatus)
            );
        });
        return;
    }

    facets.forEach((facet, index) => {
        const signature = buildFacetRenderSignature(facet, runStatus);
        const previousSignature = analysisUiState.renderCache.facetSignatures.get(facet.facet_key);
        if (previousSignature === signature) {
            return;
        }
        const nextNode = createNodeFromHtml(renderFacetResultV2(facet, runStatus));
        existingChildren[index].replaceWith(nextNode);
        analysisUiState.renderCache.facetSignatures.set(facet.facet_key, signature);
    });
}

function renderFacetStatusCardV2(facet, runStatus) {
    const findings = facet.findings || {};
    const hitCount = Number(findings.hit_count || 0);
    const promptTokens = Number(findings.prompt_tokens || 0);
    const completionTokens = Number(findings.completion_tokens || 0);
    const totalTokens = Number(findings.total_tokens || 0);
    const rerunDisabled = isRunBusyV2(runStatus) ? "disabled" : "";
    const status = normalizeStatus(facet.status || "queued");
    const phase = phaseLabelV2(findings.phase || status);
    const preview = buildFacetLeadV2(facet);
    const llmLine = findings.llm_called
        ? `LLM ${findings.llm_success ? "succeeded" : "fallback"} · in ${promptTokens} / out ${completionTokens} / total ${totalTokens}`
        : "No completed LLM call yet";
    const tags = [];

    if (status === "queued" && findings.queue_position) {
        tags.push(renderFacetTagV2(`Queue #${findings.queue_position}`, "warning"));
    }
    if (status === "preparing" || status === "running") {
        tags.push(renderFacetTagV2(phase, "active"));
    }
    if (hitCount) {
        tags.push(renderFacetTagV2(`${hitCount} hits`, ""));
    }
    if (findings.started_at) {
        tags.push(renderFacetTagV2(`Started ${formatTime(findings.started_at)}`, ""));
    }

    return `
        <article class="facet-status-card status-${escapeHtml(status)}">
            <div class="facet-status-head">
                <div class="facet-status-title">
                    <strong>${escapeHtml(facetLabelV2(facet))}</strong>
                    <span class="facet-status-key">${escapeHtml(facet.facet_key || "")}</span>
                </div>
                <span class="status-pill">${escapeHtml(statusLabelV2(status))}</span>
            </div>
            <div class="facet-status-flags">${tags.join("")}</div>
            <p class="facet-status-preview">${escapeHtml(preview)}</p>
            <small>${escapeHtml(`${llmLine} · evidence ${hitCount}`)}</small>
            <div class="inline-actions top-gap">
                <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${rerunDisabled}>Rerun Facet</button>
            </div>
        </article>
    `;
}

function renderEventItemV2(event) {
    const payload = event.payload || {};
    const level = (event.level || "info").toLowerCase();
    let payloadHtml = "";

    if (event.event_type === "llm_response" && payload.response_text) {
        payloadHtml = `<pre class="trace-box">${escapeHtml(payload.response_text)}</pre>`;
        if (payload.response_text_truncated) {
            payloadHtml += `<p class="muted">The response preview was truncated to keep the event panel compact.</p>`;
        }
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
                ${payloadHtml || `<p class="muted">No payload was attached to this event.</p>`}
            </div>
        </details>
    `;
}

function renderFacetResultV2(facet, runStatus) {
    const findings = facet.findings || {};
    const facetKey = facet.facet_key || "unknown";
    const status = normalizeStatus(facet.status || "queued");
    const rerunDisabled = isRunBusyV2(runStatus) ? "disabled" : "";
    const facetOpenDefault = ["preparing", "running", "failed"].includes(status);
    const hasError = Boolean(facet.error_message || findings.llm_error);
    const summaryText = findings.summary || buildFacetLeadV2(facet);
    const metaTags = [
        renderFacetTagV2(statusLabelV2(status), "status"),
        renderFacetTagV2(`Phase: ${phaseLabelV2(findings.phase || status)}`, isFacetActiveV2(facet) ? "active" : ""),
        findings.queue_position ? renderFacetTagV2(`Queue #${findings.queue_position}`, "warning") : "",
        renderFacetTagV2(`Confidence ${Number(facet.confidence || 0).toFixed(2)}`, ""),
        renderFacetTagV2(facet.accepted ? "Accepted" : "Pending", ""),
    ]
        .filter(Boolean)
        .join("");

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
        ${conflicts || `<p class="muted">No conflicts were recorded for this facet.</p>`}
        ${facet.error_message ? `<p class="danger">${escapeHtml(facet.error_message)}</p>` : ""}
        ${findings.notes ? `<p class="muted">${escapeHtml(findings.notes)}</p>` : ""}
    `;
    const traceBody = `
        ${findings.llm_request_url ? `<p class="muted">${escapeHtml(findings.llm_request_url)}</p>` : ""}
        ${findings.llm_error ? `<p class="danger">${escapeHtml(findings.llm_error)}</p>` : ""}
        ${findings.llm_request_payload_preview ? `<pre class="trace-box">${escapeHtml(findings.llm_request_payload_preview)}</pre>` : ""}
    `;
    const liveTextBody = findings.llm_live_text
        ? `
            ${findings.llm_live_text_truncated ? `<p class="muted">The live text preview was truncated to keep the card height bounded.</p>` : ""}
            <pre class="trace-box live-trace-box" data-live-text-scroll>${escapeHtml(findings.llm_live_text)}</pre>
        `
        : `<p class="muted">No live text has been stored for this facet yet.</p>`;

    return `
        <details
            class="facet-panel facet-panel-details status-${escapeHtml(status)}"
            data-detail-key="facet:${escapeHtml(facetKey)}"
            data-facet-key="${escapeHtml(facetKey)}"
            ${detailOpenAttr(`facet:${facetKey}`, facetOpenDefault)}
        >
            <summary class="facet-panel-summary">
                <div class="facet-panel-copy">
                    <p class="eyebrow">${escapeHtml(facetKey)}</p>
                    <h2>${escapeHtml(facetLabelV2(facet))}</h2>
                    <p class="facet-summary">${escapeHtml(summaryText)}</p>
                    ${
                        findings.summary_truncated
                            ? `<p class="summary-clamp-note">The card preview was truncated automatically to keep queue items compact.</p>`
                            : ""
                    }
                </div>
                <div class="facet-meta facet-result-meta">${metaTags}</div>
            </summary>
            <div class="facet-panel-body">
                <div class="inline-actions">
                    <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facetKey)}" ${rerunDisabled}>Rerun Facet</button>
                </div>
                ${bullets ? `<ul class="facet-bullets">${bullets}</ul>` : ""}
                ${renderSubDetails(`facet:${facetKey}:evidence`, "Evidence", evidence || `<p class="muted">No evidence has been attached yet.</p>`, false)}
                ${renderSubDetails(`facet:${facetKey}:notes`, "Notes", notesBody, hasError)}
                ${(findings.llm_request_url || findings.llm_error || findings.llm_request_payload_preview)
                    ? renderSubDetails(`facet:${facetKey}:trace`, "LLM Trace", traceBody, hasError)
                    : ""}
                ${renderSubDetails(`facet:${facetKey}:live`, "LLM Live Text", liveTextBody, status === "running")}
            </div>
        </details>
    `;
}

function buildAnalysisDetailV2(summary, facets, latestEvent) {
    const totalFacets = Number(summary.total_facets || facets.length || 0);
    const completedFacets = Number(summary.completed_facets || 0);
    const failedFacets = Number(summary.failed_facets || 0);
    const activeFacets = Number(summary.active_facets || 0);
    const queuedFacets = Number(summary.queued_facets || 0);
    const concurrency = Math.max(1, Number(summary.concurrency || 1));
    const latestHint = latestEvent ? `Latest: ${latestEvent.message || latestEvent.event_type}` : "Waiting for fresh events.";
    return `${summary.current_stage || "Queued"} · active ${activeFacets}/${concurrency}, queued ${queuedFacets}, completed ${completedFacets}/${totalFacets}, failed ${failedFacets}. ${latestHint}`;
}

function sortAnalysisFacetsV2(facets) {
    const priority = {
        running: 0,
        preparing: 1,
        queued: 2,
        failed: 3,
        completed: 4,
    };
    return facets
        .map((facet, index) => ({ facet, index }))
        .sort((left, right) => {
            const leftStatus = normalizeStatus(left.facet.status || "queued");
            const rightStatus = normalizeStatus(right.facet.status || "queued");
            const leftPriority = priority[leftStatus] ?? 99;
            const rightPriority = priority[rightStatus] ?? 99;
            if (leftPriority !== rightPriority) {
                return leftPriority - rightPriority;
            }

            const leftQueue = Number(left.facet.findings?.queue_position || 0);
            const rightQueue = Number(right.facet.findings?.queue_position || 0);
            if (leftStatus === "queued" && rightStatus === "queued" && leftQueue !== rightQueue) {
                return leftQueue - rightQueue;
            }

            return left.index - right.index;
        })
        .map((entry) => entry.facet);
}

function isFacetActiveV2(facet) {
    const status = normalizeStatus(facet?.status || "queued");
    return status === "preparing" || status === "running";
}

function isRunBusyV2(runStatus) {
    return ["queued", "running"].includes(normalizeStatus(runStatus || ""));
}

function findFacetByKeyV2(facets, facetKey) {
    if (!facetKey) {
        return null;
    }
    return facets.find((facet) => facet.facet_key === facetKey) || null;
}

function selectHeadlineEventV2(events, facetKey) {
    if (facetKey) {
        const matching = events.find((event) => event?.payload?.facet_key === facetKey);
        if (matching) {
            return matching;
        }
    }
    return events[0] || null;
}

function facetLabelV2(facet) {
    return facet?.findings?.label || facet?.facet_key || "Unknown Facet";
}

function buildFacetLeadV2(facet) {
    const findings = facet.findings || {};
    if (findings.summary) {
        return trimTextV2(findings.summary, 220);
    }
    const status = normalizeStatus(facet.status || "queued");
    if (status === "queued") {
        const queuePosition = findings.queue_position ? `Queue #${findings.queue_position}` : "Queued";
        return `${queuePosition} and waiting for a free slot.`;
    }
    if (status === "preparing") {
        return "Retrieving evidence and preparing the facet payload.";
    }
    if (status === "running") {
        return `Active phase: ${phaseLabelV2(findings.phase || "running")}.`;
    }
    if (status === "failed") {
        return trimTextV2(
            findings.notes || facet.error_message || "The facet failed before a structured summary was produced.",
            220,
        );
    }
    return "No summary was returned for this facet.";
}

function buildQueueNoteV2(activeFacets, queuedFacets, concurrency) {
    if (queuedFacets > 0) {
        return `${queuedFacets} facet(s) are waiting while ${activeFacets}/${concurrency} slot(s) are in use.`;
    }
    if (activeFacets > 0) {
        return "The queue is empty. Active slots are working on the remaining facets.";
    }
    return "The queue is empty.";
}

function renderFacetTagV2(text, tone) {
    return `<span class="facet-inline-tag ${tone ? `tag-${escapeHtml(tone)}` : ""}">${escapeHtml(text)}</span>`;
}

function statusLabelV2(status) {
    switch (normalizeStatus(status)) {
        case "queued":
            return "Queued";
        case "preparing":
            return "Preparing";
        case "running":
            return "Running";
        case "completed":
            return "Completed";
        case "failed":
            return "Failed";
        default:
            return String(status || "Queued");
    }
}

function phaseLabelV2(phase) {
    switch (String(phase || "").toLowerCase()) {
        case "queued":
            return "Queued";
        case "retrieving":
            return "Retrieving evidence";
        case "llm":
            return "Generating with LLM";
        case "analyzing":
            return "Analyzing";
        case "persisting":
            return "Finalizing";
        case "completed":
            return "Completed";
        case "failed":
            return "Failed";
        default:
            return String(phase || "Queued");
    }
}

function trimTextV2(value, limit) {
    const text = String(value || "");
    if (text.length <= limit) {
        return text;
    }
    return `${text.slice(0, Math.max(0, limit - 3))}...`;
}

function setupAssetGenerator() {
    const root = document.querySelector("[data-asset-generator]");
    if (!root) {
        return;
    }

    const form = root.querySelector("[data-generate-form]");
    const button = document.getElementById("generate-btn");
    const shell = document.getElementById("asset-generation-shell");
    const output = document.getElementById("generation-output");
    const projectId = root.dataset.projectId;
    const assetKind = root.dataset.assetKind;
    const assetLabel = root.dataset.assetLabel || assetKind || "asset";
    if (!form || !button || !shell || !output || !projectId || !assetKind) {
        return;
    }

    const state = {
        active: false,
        status: "idle",
        phase: "idle",
        stage: "等待开始",
        message: "点击开始生成。",
        percent: 0,
        chunkCount: 0,
        charCount: 0,
        outputBuffer: "",
        renderQueued: false,
        redirectTimer: null,
    };

    const scheduleRender = () => {
        if (state.renderQueued) {
            return;
        }
        state.renderQueued = true;
        window.requestAnimationFrame(() => {
            state.renderQueued = false;
            renderAssetGenerationState(state, assetLabel, output);
        });
    };

    const resetState = () => {
        state.active = true;
        state.status = "running";
        state.phase = "prepare";
        state.stage = `开始生成 ${assetLabel}`;
        state.message = "正在建立流式连接。";
        state.percent = 2;
        state.chunkCount = 0;
        state.charCount = 0;
        state.outputBuffer = "";
        if (state.redirectTimer) {
            clearTimeout(state.redirectTimer);
            state.redirectTimer = null;
        }
        output.textContent = "";
    };

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (state.active) {
            return;
        }

        resetState();
        shell.hidden = false;
        button.disabled = true;
        scheduleRender();

        try {
            const response = await fetch(`/api/projects/${projectId}/assets/generate/stream`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ asset_kind: assetKind }),
            });

            if (!response.ok || !response.body) {
                throw new Error(`Failed to start ${assetLabel} generation.`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder("utf-8");
            let done = false;
            let buffer = "";

            while (!done) {
                const { value, done: readerDone } = await reader.read();
                done = readerDone;
                if (!value) {
                    continue;
                }
                buffer += decoder.decode(value, { stream: !done });
                const blocks = buffer.split("\n\n");
                buffer = blocks.pop() || "";

                blocks.forEach((block) => {
                    const parsed = parseSseBlock(block);
                    if (!parsed) {
                        return;
                    }
                    handleAssetStreamEvent(parsed.eventType, parsed.data);
                });
            }
        } catch (error) {
            state.active = false;
            state.status = "failed";
            state.phase = "failed";
            state.stage = "生成失败";
            state.message = error instanceof Error ? error.message : `无法启动 ${assetLabel} 生成`;
            button.disabled = false;
            state.outputBuffer += `\n\n[error] ${state.message}`;
            scheduleRender();
        }
    });

    function handleAssetStreamEvent(eventType, data) {
        if (eventType === "status") {
            state.active = true;
            state.status = data.status || "running";
            state.phase = data.phase || state.phase;
            state.stage = data.message || phaseLabelFromState(data.phase) || state.stage;
            state.message = data.message || state.message;
            if (typeof data.progress_percent === "number") {
                state.percent = Math.max(state.percent, clampPercent(data.progress_percent));
            }
        } else if (eventType === "delta") {
            const chunk = String(data.chunk || "");
            if (!chunk) {
                return;
            }
            state.active = true;
            state.status = "running";
            state.phase = "streaming";
            state.stage = "正在接收模型输出";
            state.message = "模型正在持续返回内容。";
            state.chunkCount += 1;
            state.charCount += chunk.length;
            state.outputBuffer += chunk;
            state.percent = Math.max(state.percent, 56);
            if (state.chunkCount % 4 === 0) {
                state.percent = Math.min(88, state.percent + 1);
            }
        } else if (eventType === "done") {
            state.active = false;
            state.status = "completed";
            state.phase = "done";
            state.stage = data.message || "草稿生成完成";
            state.message = "草稿已生成完成，正在跳转到编辑页。";
            state.percent = 100;
            button.disabled = false;
            if (data.draft_id) {
                state.redirectTimer = window.setTimeout(() => {
                    window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}&draft=${encodeURIComponent(data.draft_id)}`;
                }, 900);
            }
        } else if (eventType === "error") {
            state.active = false;
            state.status = "failed";
            state.phase = "failed";
            state.stage = "生成失败";
            state.message = data.message || `无法生成 ${assetLabel}`;
            state.outputBuffer += `\n\n[error] ${state.message}`;
            button.disabled = false;
        }

        scheduleRender();
    }
}

function renderAssetGenerationState(state, assetLabel, output) {
    updateText("asset-current-stage", state.stage);
    updateText("asset-current-percent", `${state.percent}%`);
    updateText("asset-progress-percent", `${state.percent}%`);
    updateText("asset-generation-state", assetStatusLabel(state.status));
    updateText("asset-generation-message", state.message);
    updateText("asset-chunk-count", state.chunkCount);
    updateText("asset-char-count", state.charCount);
    updateText(
        "asset-generation-hint",
        state.status === "completed"
            ? `${assetLabel} 草稿已经生成完成。`
            : state.status === "failed"
                ? state.message
                : `${assetLabel} 正在生成中，实时输出会持续追加。`
    );
    updateText("asset-output-status", assetOutputStatus(state.status));
    setStatusToken("asset-stage-chip", assetStatusLabel(state.status), state.status);

    const progressFill = document.getElementById("asset-progress-fill");
    if (progressFill) {
        progressFill.style.width = `${state.percent}%`;
    }

    if (state.outputBuffer) {
        const shouldStick = shouldAutoScroll(output);
        output.textContent += state.outputBuffer;
        state.outputBuffer = "";
        if (shouldStick) {
            output.scrollTop = output.scrollHeight;
        }
    }
}

function parseSseBlock(block) {
    const lines = String(block || "")
        .split("\n")
        .map((line) => line.trimEnd())
        .filter(Boolean);
    if (!lines.length) {
        return null;
    }

    const eventLine = lines.find((line) => line.startsWith("event:"));
    const dataLines = lines.filter((line) => line.startsWith("data:"));
    if (!eventLine || !dataLines.length) {
        return null;
    }

    const eventType = eventLine.replace("event:", "").trim();
    const rawData = dataLines.map((line) => line.replace("data:", "").trim()).join("\n");
    return {
        eventType,
        data: safeParseJson(rawData) || {},
    };
}

function createNodeFromHtml(html) {
    const template = document.createElement("template");
    template.innerHTML = html.trim();
    return template.content.firstElementChild;
}

function setStatusToken(id, text, status) {
    const node = document.getElementById(id);
    if (!node) {
        return;
    }
    node.textContent = String(text);
    ["status-pending", "status-queued", "status-preparing", "status-running", "status-completed", "status-failed", "status-partial_failed"]
        .forEach((className) => node.classList.remove(className));
    node.classList.add(`status-${normalizeStatus(status || "pending")}`);
}

function normalizeStatus(status) {
    const normalized = String(status || "pending").toLowerCase().replaceAll(" ", "_");
    if (normalized === "partial_failed") {
        return "failed";
    }
    if (normalized === "pending") {
        return "queued";
    }
    return normalized;
}

function assetStatusLabel(status) {
    switch (normalizeStatus(status)) {
        case "running":
            return "生成中";
        case "completed":
            return "已完成";
        case "failed":
            return "失败";
        case "queued":
            return "排队中";
        default:
            return "待命";
    }
}

function assetOutputStatus(status) {
    switch (normalizeStatus(status)) {
        case "running":
            return "流式输出中";
        case "completed":
            return "输出完成";
        case "failed":
            return "输出中断";
        default:
            return "等待数据";
    }
}

function phaseLabelFromState(phase) {
    switch (String(phase || "")) {
        case "prepare":
            return "准备生成";
        case "load":
            return "读取分析结果";
        case "personality_context":
            return "补充人格证据";
        case "memory_context":
            return "补充经历证据";
        case "synthesis":
            return "生成结构化草稿";
        case "normalize":
            return "规范化字段";
        case "render":
            return "整理结构";
        case "bundle":
            return "渲染 Markdown 与 Prompt";
        case "persist":
            return "保存草稿";
        case "done":
            return "生成完成";
        default:
            return "";
    }
}

function shouldAutoScroll(node) {
    const remaining = node.scrollHeight - node.scrollTop - node.clientHeight;
    return remaining < 48;
}

function clampPercent(value) {
    const number = Number(value || 0);
    return Math.max(0, Math.min(100, Math.round(number)));
}

function trimText(value, limit) {
    const text = String(value || "");
    if (text.length <= limit) {
        return text;
    }
    return `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function updateText(id, value) {
    const node = document.getElementById(id);
    if (node) {
        node.textContent = String(value);
    }
}

function formatTime(value) {
    if (!value) {
        return "--";
    }
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

function safeParseJson(value) {
    try {
        return JSON.parse(value || "null");
    } catch {
        return null;
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
