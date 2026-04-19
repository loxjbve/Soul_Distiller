import {
    clampPercent,
    escapeHtml,
    fetchJson,
    parseSseBlock,
    safeParseJson,
    setButtonBusy,
    showNotice,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("assets-page-bootstrap")?.textContent, {});

if (bootstrap?.project_id) {
    const ui = bootstrap.ui_strings || {};
    const projectId = bootstrap.project_id;
    const assetKind = bootstrap.asset_kind || "skill";

    const elements = {
        form: document.getElementById("generate-form"),
        button: document.getElementById("generate-btn"),
        shell: document.getElementById("asset-generation-shell"),
        stageChip: document.getElementById("asset-stage-chip"),
        stage: document.getElementById("asset-current-stage"),
        percent: document.getElementById("asset-current-percent"),
        fill: document.getElementById("asset-progress-fill"),
        state: document.getElementById("asset-generation-state"),
        message: document.getElementById("asset-generation-message"),
        output: document.getElementById("generation-output"),
        chunkCount: document.getElementById("asset-chunk-count"),
        charCount: document.getElementById("asset-char-count"),
        docStatus: document.getElementById("asset-document-status"),
        jsonPayload: document.getElementById("asset-json-payload"),
    };

    let chunkCount = 0;
    let charCount = 0;

    renderDocumentStatus();
    elements.jsonPayload?.addEventListener("input", () => renderDocumentStatus());

    elements.form?.addEventListener("submit", async (event) => {
        event.preventDefault();
        setButtonBusy(elements.button, true, ui.status_running || "Generating...");
        if (elements.shell) {
            elements.shell.hidden = false;
        }
        if (elements.output) {
            elements.output.textContent = "";
        }
        chunkCount = 0;
        charCount = 0;
        updateCounts();
        try {
            await streamGenerate();
        } catch (error) {
            await fallbackGenerate(error);
        } finally {
            setButtonBusy(elements.button, false);
        }
    });

    async function streamGenerate() {
        const response = await fetch(`/api/projects/${projectId}/assets/generate/stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ asset_kind: assetKind }),
        });
        if (!response.ok || !response.body) {
            throw new Error("Streaming asset generation is not available.");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            buffer += decoder.decode(value || new Uint8Array(), { stream: !done });

            let boundary = buffer.indexOf("\n\n");
            while (boundary >= 0) {
                const block = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);
                handleEventBlock(block);
                boundary = buffer.indexOf("\n\n");
            }

            if (done) {
                break;
            }
        }
    }

    function handleEventBlock(block) {
        const parsed = parseSseBlock(block);
        if (!parsed) {
            return;
        }
        const { eventType, data } = parsed;
        if (eventType === "status") {
            renderStatus(data);
            return;
        }
        if (eventType === "delta") {
            const chunk = data.chunk || "";
            if (elements.output) {
                elements.output.textContent += chunk;
            }
            chunkCount += 1;
            charCount += chunk.length;
            updateCounts();
            return;
        }
        if (eventType === "done") {
            renderStatus({ status: "completed", progress_percent: 100, message: data.message || ui.status_completed || "Completed" });
            window.setTimeout(() => {
                window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}`;
            }, 700);
            return;
        }
        if (eventType === "error") {
            throw new Error(data.message || ui.status_failed || "Asset generation failed.");
        }
    }

    function renderStatus(payload) {
        const percent = clampPercent(payload.progress_percent || 0);
        updateText(elements.stageChip, payload.status || ui.status_running || "Generating");
        updateText(elements.stage, payload.phase || payload.status || ui.status_running || "Generating");
        updateText(elements.percent, `${percent}%`);
        updateText(elements.state, payload.status || ui.status_running || "Generating");
        updateText(elements.message, payload.message || "");
        if (elements.fill) {
            elements.fill.style.width = `${percent}%`;
        }
    }

    function updateCounts() {
        updateText(elements.chunkCount, chunkCount);
        updateText(elements.charCount, charCount);
    }

    async function fallbackGenerate(error) {
        renderStatus({
            status: "failed",
            progress_percent: 0,
            message: `${error.message} Switching to non-streaming generation.`,
        });
        const payload = await fetchJson(`/api/projects/${projectId}/assets/generate`, {
            method: "POST",
            body: JSON.stringify({ asset_kind: assetKind }),
        });
        showNotice(elements.message, payload.message || "Draft generated.", "success");
        window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}&draft=${encodeURIComponent(payload.id || "")}`;
    }

    function renderDocumentStatus() {
        if (!elements.docStatus) {
            return;
        }
        const payload = safeParseJson(elements.jsonPayload?.value || "{}", {});
        const documents = payload?.documents && typeof payload.documents === "object" ? payload.documents : {};

        elements.docStatus.innerHTML = "";
        const keys = assetKind === "skill"
            ? ["skill", "personality", "memories", "merge"]
            : assetKind === "cc_skill"
                ? ["skill", "personality", "memories"]
                : ["skill"];

        if (!keys.length) {
            elements.docStatus.innerHTML = `<div class="empty-panel"><strong>No split documents for this asset kind.</strong></div>`;
            return;
        }

        keys.forEach((key) => {
            const documentPayload = documents?.[key] && typeof documents[key] === "object" ? documents[key] : {};
            const markdown = String(documentPayload.markdown || "").trim();
            const title = String(documentPayload.title || key);
            const exists = Boolean(markdown);
            const card = document.createElement("article");
            card.className = `document-card compact-card asset-doc-card ${exists ? "is-ready" : "is-missing"}`;
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>${escapeHtml(title)}</strong>
                    <span class="status-chip ${exists ? "tone-ready" : "tone-warning"}">${exists ? "ready" : "missing"}</span>
                </div>
                <p class="helper-text">${escapeHtml(`${markdown.length} chars`)}</p>
                <p class="helper-text">${escapeHtml(markdown ? markdown.slice(0, 120) : "This document is empty in the current draft payload.")}</p>
                ${bootstrap.draft_id && exists ? `
                    <div class="button-row top-gap">
                        <a class="ghost-button" href="/api/projects/${projectId}/assets/${bootstrap.draft_id}/exports/${encodeURIComponent(key)}">Export</a>
                    </div>
                ` : ""}
            `;
            elements.docStatus.appendChild(card);
        });
    }
}
