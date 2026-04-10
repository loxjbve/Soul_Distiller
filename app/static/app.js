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
                count.textContent = "未选择文件";
                return;
            }
            count.textContent = `已选择 ${files.length} 个文件`;
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
    if (!projectId || !runId || !bootstrap) {
        return;
    }

    let payload = null;
    try {
        payload = JSON.parse(bootstrap.textContent || "null");
    } catch {
        payload = null;
    }
    if (payload) {
        renderAnalysis(payload);
    }

    const poll = async () => {
        const response = await fetch(`/api/projects/${projectId}/analysis?run_id=${encodeURIComponent(runId)}`);
        if (!response.ok) {
            return;
        }
        const nextPayload = await response.json();
        renderAnalysis(nextPayload);
        if (!["queued", "running"].includes(nextPayload.status)) {
            clearInterval(intervalId);
        }
    };

    const intervalId = window.setInterval(poll, 2000);
}

function renderAnalysis(payload) {
    updateText("analysis-run-title", `分析运行 ${payload.id.slice(0, 8)}`);
    updateText("analysis-status-chip", payload.status);
    updateText("analysis-role-chip", payload.summary.target_role || "未指定角色");
    updateText("analysis-stage", payload.summary.current_stage || "排队中");
    updateText("analysis-percent", `${payload.summary.progress_percent || 0}%`);
    updateText("metric-completed", payload.summary.completed_facets || 0);
    updateText("metric-failed", payload.summary.failed_facets || 0);
    updateText("metric-llm-success", payload.summary.llm_successes || 0);
    updateText("metric-llm-failure", payload.summary.llm_failures || 0);
    updateText("metric-total-tokens", payload.summary.total_tokens || 0);
    updateText("metric-current-facet", payload.summary.current_facet || "-");

    const progressFill = document.getElementById("analysis-progress-fill");
    if (progressFill) {
        progressFill.style.width = `${payload.summary.progress_percent || 0}%`;
    }

    const facetStatusList = document.getElementById("facet-status-list");
    if (facetStatusList) {
        facetStatusList.innerHTML = payload.facets.map(renderFacetStatusCard).join("");
    }

    const eventList = document.getElementById("analysis-events");
    if (eventList) {
        eventList.innerHTML = payload.events.length
            ? payload.events.map(renderEventItem).join("")
            : `<div class="event-item"><strong>暂无事件</strong><p>任务启动后会在这里持续追加日志。</p></div>`;
    }

    const resultList = document.getElementById("analysis-result-list");
    if (resultList) {
        resultList.innerHTML = payload.facets.map(renderFacetResult).join("");
    }
}

function renderFacetStatusCard(facet) {
    const findings = facet.findings || {};
    const llmLine = findings.llm_called
        ? `LLM ${findings.llm_success ? "成功" : "失败"} · ${findings.total_tokens || 0} tokens`
        : "未调用 LLM";
    return `
        <article class="facet-status-card status-${facet.status}">
            <div class="facet-status-head">
                <strong>${escapeHtml(findings.label || facet.facet_key)}</strong>
                <span class="status-pill">${escapeHtml(facet.status)}</span>
            </div>
            <p>${escapeHtml(findings.summary || "等待结果...")}</p>
            <small>${escapeHtml(llmLine)}</small>
        </article>
    `;
}

function renderEventItem(event) {
    return `
        <article class="event-item level-${escapeHtml(event.level || "info")}">
            <div class="event-item-head">
                <strong>${escapeHtml(event.event_type)}</strong>
                <span>${formatTime(event.created_at)}</span>
            </div>
            <p>${escapeHtml(event.message)}</p>
        </article>
    `;
}

function renderFacetResult(facet) {
    const findings = facet.findings || {};
    const bullets = (findings.bullets || [])
        .map((item) => `<li>${escapeHtml(item)}</li>`)
        .join("");
    const evidence = (facet.evidence || [])
        .map(
            (item) => `
                <div class="evidence-block">
                    <strong>${escapeHtml(item.filename || item.document_title || "证据")}</strong>
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
                    <strong>${escapeHtml(item.title || "冲突")}</strong>
                    <p>${escapeHtml(item.detail || "")}</p>
                </div>
            `
        )
        .join("");
    return `
        <article class="facet-panel">
            <div class="section-head facet-head">
                <div>
                    <p class="eyebrow">${escapeHtml(facet.facet_key)}</p>
                    <h2>${escapeHtml(findings.label || facet.facet_key)}</h2>
                </div>
                <div class="facet-meta">
                    <span>状态 ${escapeHtml(facet.status)}</span>
                    <span>置信度 ${(facet.confidence || 0).toFixed(2)}</span>
                    <span>${facet.accepted ? "已接受" : "待确认"}</span>
                </div>
            </div>
            <p class="facet-summary">${escapeHtml(findings.summary || "")}</p>
            ${bullets ? `<ul class="facet-bullets">${bullets}</ul>` : ""}
            <div class="facet-grid">
                <div>
                    <h3>证据片段</h3>
                    ${evidence || `<p class="muted">暂无证据。</p>`}
                </div>
                <div>
                    <h3>冲突与备注</h3>
                    ${conflicts || `<p class="muted">暂无冲突记录。</p>`}
                    ${facet.error_message ? `<p class="danger">${escapeHtml(facet.error_message)}</p>` : ""}
                    ${findings.notes ? `<p class="muted">${escapeHtml(findings.notes)}</p>` : ""}
                </div>
            </div>
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
        return new Intl.DateTimeFormat("zh-Hant", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            month: "2-digit",
            day: "2-digit",
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
