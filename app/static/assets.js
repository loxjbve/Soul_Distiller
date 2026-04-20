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
    const defaultStreamKey = assetKind === "profile_report" ? "asset" : "skill";
    const splitDocumentKeys = assetKind === "skill"
        ? ["skill", "personality", "memories", "merge"]
        : assetKind === "cc_skill"
            ? ["skill", "personality", "memories"]
            : [];

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
        chunkCount: document.getElementById("asset-chunk-count"),
        charCount: document.getElementById("asset-char-count"),
        docStatus: document.getElementById("asset-document-status"),
        jsonPayload: document.getElementById("asset-json-payload"),
        promptText: document.getElementById("asset-prompt-text"),
        markdownText: document.getElementById("asset-markdown-text"),
        draftForm: document.getElementById("asset-draft-form"),
    };

    const streamOutputs = Object.fromEntries(
        Array.from(document.querySelectorAll("[data-stream-output]")).map((element) => [element.dataset.streamOutput, element]),
    );
    const streamStates = Object.fromEntries(
        Object.keys(streamOutputs).map((key) => [key, { chunks: 0, chars: 0, status: "waiting", text: "" }]),
    );
    const documentEditors = Object.fromEntries(
        Array.from(document.querySelectorAll("[data-document-editor]")).map((element) => [element.dataset.documentEditor, element]),
    );
    const editorTabs = Array.from(document.querySelectorAll("[data-editor-tab-trigger]"));
    const editorPages = Array.from(document.querySelectorAll("[data-editor-page]"));

    let chunkCount = 0;
    let charCount = 0;

    if (splitDocumentKeys.length) {
        hydrateEditorsFromPayload(readDraftPayload());
        bindEditorTabs();
        bindDraftEditorSync();
    }

    renderDocumentStatus();

    elements.form?.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (splitDocumentKeys.length) {
            syncPayloadFromEditors();
        }
        setButtonBusy(elements.button, true, ui.status_running || "Generating...");
        if (elements.shell) {
            elements.shell.hidden = false;
        }
        resetStreamingState();
        try {
            await streamGenerate();
        } catch (error) {
            await fallbackGenerate(error);
        } finally {
            setButtonBusy(elements.button, false);
        }
    });

    elements.draftForm?.addEventListener("submit", () => {
        if (splitDocumentKeys.length) {
            syncPayloadFromEditors();
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
            appendStreamChunk(data.document_key || defaultStreamKey, data.chunk || "");
            return;
        }
        if (eventType === "done") {
            renderStatus({
                status: "completed",
                progress_percent: 100,
                message: data.message || ui.status_completed || "Completed",
                document_key: data.document_key || defaultStreamKey,
            });
            Object.keys(streamStates).forEach((key) => {
                if (streamStates[key].text.trim()) {
                    setStreamPanelStatus(key, "ready");
                }
            });
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

        const documentKey = normalizeStreamKey(payload.document_key || defaultStreamKey);
        if (streamStates[documentKey]) {
            const statusLabel = normalizeStreamStatus(payload.status || payload.phase || "running");
            setStreamPanelStatus(documentKey, statusLabel);
        }
    }

    function appendStreamChunk(documentKey, chunk) {
        const key = normalizeStreamKey(documentKey);
        const output = streamOutputs[key];
        if (!output || !chunk) {
            return;
        }
        output.textContent += chunk;
        streamStates[key].text += chunk;
        streamStates[key].chunks += 1;
        streamStates[key].chars += chunk.length;
        streamStates[key].status = "streaming";
        setStreamPanelStatus(key, "streaming");
        updateText(document.getElementById(`asset-doc-count-${key}`), `${streamStates[key].chars} chars`);
        chunkCount += 1;
        charCount += chunk.length;
        updateCounts();
    }

    function normalizeStreamKey(documentKey) {
        return streamOutputs[documentKey] ? documentKey : defaultStreamKey;
    }

    function normalizeStreamStatus(value) {
        const normalized = String(value || "").toLowerCase();
        if (!normalized) {
            return "waiting";
        }
        if (normalized.includes("fail") || normalized.includes("error")) {
            return "failed";
        }
        if (normalized.includes("done") || normalized.includes("complete") || normalized === "ready") {
            return "ready";
        }
        if (normalized.includes("merge")) {
            return "merging";
        }
        if (normalized.includes("context") || normalized.includes("synthesis") || normalized.includes("render") || normalized.includes("bundle")) {
            return "streaming";
        }
        if (normalized.includes("load") || normalized.includes("prepare")) {
            return "preparing";
        }
        return "streaming";
    }

    function setStreamPanelStatus(documentKey, status) {
        const chip = document.getElementById(`asset-doc-status-${documentKey}`);
        if (!chip) {
            return;
        }
        streamStates[documentKey].status = status;
        const tone = status === "ready" ? "tone-ready" : status === "failed" ? "tone-warning" : "tone-queued";
        chip.className = `status-chip ${tone}`;
        chip.textContent = status;
    }

    function resetStreamingState() {
        chunkCount = 0;
        charCount = 0;
        updateCounts();
        Object.entries(streamOutputs).forEach(([key, output]) => {
            output.textContent = "";
            streamStates[key] = { chunks: 0, chars: 0, status: "waiting", text: "" };
            setStreamPanelStatus(key, "waiting");
            updateText(document.getElementById(`asset-doc-count-${key}`), "0 chars");
        });
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
            document_key: defaultStreamKey,
        });
        const payload = await fetchJson(`/api/projects/${projectId}/assets/generate`, {
            method: "POST",
            body: JSON.stringify({ asset_kind: assetKind }),
        });
        showNotice(elements.message, payload.message || "Draft generated.", "success");
        window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}&draft=${encodeURIComponent(payload.id || "")}`;
    }

    function bindEditorTabs() {
        editorTabs.forEach((button) => {
            button.addEventListener("click", () => activateEditorPage(button.dataset.editorTabTrigger || ""));
        });
    }

    function activateEditorPage(pageKey) {
        editorTabs.forEach((button) => {
            const isActive = button.dataset.editorTabTrigger === pageKey;
            button.classList.toggle("is-active", isActive);
            button.setAttribute("aria-selected", isActive ? "true" : "false");
        });
        editorPages.forEach((page) => {
            page.classList.toggle("is-active", page.dataset.editorPage === pageKey);
        });
    }

    function bindDraftEditorSync() {
        Object.entries(documentEditors).forEach(([key, editor]) => {
            if (key === "merge") {
                return;
            }
            editor.addEventListener("input", () => syncPayloadFromEditors());
        });
        elements.jsonPayload?.addEventListener("input", () => {
            const payload = safeParseJson(elements.jsonPayload.value, null);
            if (payload && typeof payload === "object") {
                hydrateEditorsFromPayload(payload);
            }
            renderDocumentStatus();
        });
    }

    function readDraftPayload() {
        return safeParseJson(elements.jsonPayload?.value || "{}", {});
    }

    function syncPayloadFromEditors() {
        if (!splitDocumentKeys.length || !elements.jsonPayload) {
            renderDocumentStatus();
            return;
        }
        const payload = readDraftPayload();
        const nextPayload = payload && typeof payload === "object" ? payload : {};
        const documents = nextPayload.documents && typeof nextPayload.documents === "object" ? nextPayload.documents : {};

        splitDocumentKeys.forEach((key) => {
            if (key === "merge") {
                return;
            }
            const fallbackFilename = getDocumentFilename(key);
            const markdown = String(documentEditors[key]?.value || "");
            documents[key] = {
                ...(documents[key] && typeof documents[key] === "object" ? documents[key] : {}),
                filename: String(documents[key]?.filename || fallbackFilename),
                markdown,
            };
        });

        if (assetKind === "skill") {
            const merged = composeSkillMerge(documents);
            documents.merge = {
                ...(documents.merge && typeof documents.merge === "object" ? documents.merge : {}),
                filename: String(documents.merge?.filename || getDocumentFilename("merge")),
                markdown: merged,
            };
            if (documentEditors.merge) {
                documentEditors.merge.value = merged;
            }
            if (elements.markdownText) {
                elements.markdownText.value = merged;
            }
            if (elements.promptText) {
                elements.promptText.value = merged;
            }
        } else if (assetKind === "cc_skill") {
            const skillMarkdown = String(documents.skill?.markdown || "");
            if (elements.markdownText) {
                elements.markdownText.value = skillMarkdown;
            }
            if (elements.promptText) {
                elements.promptText.value = skillMarkdown;
            }
        }

        nextPayload.documents = documents;
        elements.jsonPayload.value = JSON.stringify(nextPayload, null, 2);
        renderDocumentStatusFromPayload(nextPayload);
    }

    function hydrateEditorsFromPayload(payload) {
        if (!splitDocumentKeys.length || !payload || typeof payload !== "object") {
            return;
        }
        const documents = payload.documents && typeof payload.documents === "object" ? payload.documents : {};
        splitDocumentKeys.forEach((key) => {
            const editor = documentEditors[key];
            if (!editor) {
                return;
            }
            if (key === "merge" && assetKind === "skill") {
                const merged = String(documents.merge?.markdown || composeSkillMerge(documents));
                editor.value = merged;
                if (elements.markdownText) {
                    elements.markdownText.value = merged;
                }
                if (elements.promptText) {
                    elements.promptText.value = merged;
                }
                return;
            }
            editor.value = String(documents[key]?.markdown || "");
        });
        if (assetKind === "cc_skill") {
            const skillMarkdown = String(documents.skill?.markdown || "");
            if (elements.markdownText) {
                elements.markdownText.value = skillMarkdown;
            }
            if (elements.promptText) {
                elements.promptText.value = skillMarkdown;
            }
        }
        renderDocumentStatusFromPayload(payload);
    }

    function composeSkillMerge(documents) {
        return ["skill", "personality", "memories"]
            .map((key) => String(documents[key]?.markdown || "").trim())
            .filter(Boolean)
            .join("\n\n")
            .trim();
    }

    function getDocumentFilename(key) {
        if (assetKind === "cc_skill") {
            return {
                skill: "SKILL.md",
                personality: "references/personality.md",
                memories: "references/memories.md",
            }[key] || key;
        }
        return {
            skill: "Skill.md",
            personality: "personality.md",
            memories: "memories.md",
            merge: "Skill_merge.md",
        }[key] || key;
    }

    function renderDocumentStatus() {
        renderDocumentStatusFromPayload(readDraftPayload());
    }

    function renderDocumentStatusFromPayload(payload) {
        if (!elements.docStatus) {
            return;
        }
        elements.docStatus.innerHTML = "";

        if (!splitDocumentKeys.length) {
            elements.docStatus.innerHTML = `<div class="empty-panel"><strong>No split documents for this asset kind.</strong></div>`;
            return;
        }

        const documents = payload?.documents && typeof payload.documents === "object" ? payload.documents : {};
        splitDocumentKeys.forEach((key) => {
            const documentPayload = documents[key] && typeof documents[key] === "object" ? documents[key] : {};
            const markdown = String(documentPayload.markdown || "").trim();
            const title = String(documentPayload.filename || getDocumentFilename(key));
            const exists = Boolean(markdown);
            const card = document.createElement("article");
            card.className = `document-card compact-card asset-doc-card ${exists ? "is-ready" : "is-missing"}`;
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>${escapeHtml(title)}</strong>
                    <span class="status-chip ${exists ? "tone-ready" : "tone-warning"}">${exists ? "ready" : "missing"}</span>
                </div>
                <p class="helper-text">${escapeHtml(`${markdown.length} chars`)}</p>
                <p class="helper-text">${escapeHtml(markdown ? markdown.slice(0, 160) : "This document is empty in the current draft payload.")}</p>
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
