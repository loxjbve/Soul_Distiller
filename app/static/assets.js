import {
    clampPercent,
    createMiniCardController,
    escapeHtml,
    fetchJson,
    parseSseBlock,
    renderMiniCardDetail,
    safeParseJson,
    setButtonBusy,
    showNotice,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("assets-page-bootstrap")?.textContent, {});

if (bootstrap?.project_id) {
    const ui = bootstrap.ui_strings || {};
    const projectId = bootstrap.project_id;
    const assetKind = bootstrap.asset_kind || "cc_skill";
    const isStoneAsset = [
        "stone_author_model_v3",
        "stone_prototype_index_v3",
    ].includes(assetKind);
    const splitDocumentKeys = assetKind === "cc_skill" ? ["skill", "personality", "memories", "analysis"] : [];

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
        eventCount: document.getElementById("asset-event-count"),
        chunkCount: document.getElementById("asset-chunk-count"),
        charCount: document.getElementById("asset-char-count"),
        streamLog: document.getElementById("asset-stream-log"),
        lockChip: document.getElementById("asset-editor-lock-chip"),
        activeDocument: document.getElementById("asset-active-document"),
        docStatus: document.getElementById("asset-document-status"),
        jsonPayload: document.getElementById("asset-json-payload"),
        promptText: document.getElementById("asset-prompt-text"),
        markdownText: document.getElementById("asset-markdown-text"),
        notes: document.getElementById("asset-notes"),
        draftForm: document.getElementById("asset-draft-form"),
        draftPlaceholder: document.querySelector(".asset-draft-placeholder"),
        saveButton: document.getElementById("asset-save-btn"),
        publishButton: document.getElementById("asset-publish-btn"),
        singleMarkdown: document.getElementById("asset-single-markdown"),
        stonePreview: document.getElementById("stone-asset-preview"),
    };

    const editorTabs = Array.from(document.querySelectorAll("[data-editor-tab-trigger]"));
    const editorPages = Array.from(document.querySelectorAll("[data-editor-page]"));
    const documentEditors = Object.fromEntries(
        Array.from(document.querySelectorAll("[data-document-editor]")).map((element) => [element.dataset.documentEditor, element]),
    );

    const state = {
        draftId: String(bootstrap.draft_id || ""),
        locked: false,
        eventCount: 0,
        chunkCount: 0,
        charCount: 0,
        logs: [],
        logController: null,
        documentController: null,
        versionController: null,
        activePage: isStoneAsset ? "preview" : (splitDocumentKeys[0] || (elements.singleMarkdown ? "markdown" : "")),
        activeStreamDocument: "",
        documents: splitDocumentKeys.reduce((accumulator, key) => {
            accumulator[key] = { markdown: String(documentEditors[key]?.value || "") };
            return accumulator;
        }, {}),
    };

    bindEditorTabs();
    bindDraftSync();
    setEditorsLocked(false);
    renderDocumentStatus();
    syncMarkdownArtifacts();
    refreshDraftState();
    renderStoneAssetPreview();
    renderStreamLog();
    bindVersionCards();

    elements.form?.addEventListener("submit", async (event) => {
        event.preventDefault();
        await streamGenerate();
    });

    elements.saveButton?.addEventListener("click", async (event) => {
        event.preventDefault();
        await saveDraft(event.currentTarget);
    });

    elements.publishButton?.addEventListener("click", async (event) => {
        event.preventDefault();
        await publishDraft(event.currentTarget);
    });

    async function streamGenerate() {
        setButtonBusy(elements.button, true, ui.status_running || "Generating...");
        setEditorsLocked(true);
        showNotice(elements.message, "", "info");
        if (elements.shell) {
            elements.shell.hidden = false;
        }
        resetStreamingState();
        try {
            const response = await fetch(`/api/projects/${projectId}/assets/generate/stream`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ asset_kind: assetKind }),
            });
            if (!response.ok) {
                const responseText = await response.text().catch(() => "");
                throw new Error(extractResponseErrorMessage(responseText, response.status));
            }
            if (!response.body) {
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
        } catch (error) {
            recordStreamLog(error.message || "生成失败。", "warning", { phase: "error" });
            renderStatus({
                status: "failed",
                progress_percent: 0,
                message: error.message || "生成失败。",
            });
            showNotice(elements.message, error.message || "生成失败。", "warning");
            setEditorsLocked(false);
        } finally {
            setButtonBusy(elements.button, false);
        }
    }

    function handleEventBlock(block) {
        const parsed = parseSseBlock(block);
        if (!parsed) {
            return;
        }
        const { eventType, data } = parsed;
        state.eventCount += 1;
        updateCounts();
        if (eventType === "status") {
            recordStreamLog(data.message || data.phase || "status", data.status === "failed" ? "warning" : "info", data);
            renderStatus(data);
            return;
        }
        if (eventType === "delta") {
            appendStreamChunk(data.document_key || (splitDocumentKeys.length ? "skill" : "asset"), data.chunk || "");
            return;
        }
        if (eventType === "done") {
            recordStreamLog(data.message || "Completed", "success", data);
            hydrateFromDraftPayload(data.draft || null, data.draft_id || "");
            renderStatus({
                status: "completed",
                progress_percent: 100,
                message: data.message || ui.status_completed || "Completed",
            });
            setEditorsLocked(false);
            return;
        }
        if (eventType === "error") {
            recordStreamLog(data.message || ui.status_failed || "Generation failed", "warning", data);
            renderStatus({
                status: "failed",
                progress_percent: 0,
                message: data.message || ui.status_failed || "Generation failed",
            });
            showNotice(elements.message, data.message || ui.status_failed || "Generation failed", "warning");
            setEditorsLocked(false);
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

    function appendStreamChunk(documentKey, chunk) {
        if (!chunk) {
            return;
        }
        if (splitDocumentKeys.length) {
            const key = splitDocumentKeys.includes(documentKey) ? documentKey : "skill";
            if (documentEditors[key]) {
                state.activeStreamDocument = key;
                activateEditorPage(key, { scrollTab: true });
                documentEditors[key].value += chunk;
                state.documents[key].markdown = documentEditors[key].value;
            }
        } else if (elements.singleMarkdown) {
            state.activeStreamDocument = "markdown";
            elements.singleMarkdown.value += chunk;
        }
        state.chunkCount += 1;
        state.charCount += chunk.length;
        updateCounts();
        syncMarkdownArtifacts();
        renderStoneAssetPreview();
        renderDocumentStatus();
    }

    function resetStreamingState() {
        state.eventCount = 0;
        state.chunkCount = 0;
        state.charCount = 0;
        state.logs = [];
        state.activeStreamDocument = "";
        updateCounts();
        renderStreamLog();
        if (splitDocumentKeys.length) {
            splitDocumentKeys.forEach((key) => {
                if (documentEditors[key]) {
                    documentEditors[key].value = "";
                }
                state.documents[key] = { markdown: "" };
            });
            activateEditorPage(splitDocumentKeys[0] || "skill");
        } else if (elements.singleMarkdown) {
            state.activePage = "markdown";
            elements.singleMarkdown.value = "";
        }
        if (elements.promptText) {
            elements.promptText.value = "";
        }
        if (elements.jsonPayload) {
            elements.jsonPayload.value = "{}";
        }
        if (elements.draftPlaceholder) {
            elements.draftPlaceholder.hidden = true;
        }
        renderStoneAssetPreview();
        renderDocumentStatus();
    }

    function updateCounts() {
        updateText(elements.eventCount, state.eventCount);
        updateText(elements.chunkCount, state.chunkCount);
        updateText(elements.charCount, state.charCount);
    }

    function recordStreamLog(message, tone = "info", payload = {}) {
        const text = String(message || "").trim();
        if (!text) {
            return;
        }
        const parts = [];
        if (payload?.phase) {
            parts.push(String(payload.phase));
        }
        if (payload?.stage && payload.stage !== payload.phase) {
            parts.push(String(payload.stage));
        }
        if (Number.isFinite(payload?.attempt)) {
            parts.push(`attempt ${payload.attempt}`);
        }
        if (Number.isFinite(payload?.batch_index) && Number.isFinite(payload?.batch_total)) {
            parts.push(`batch ${payload.batch_index}/${payload.batch_total}`);
        }
        state.logs.unshift({
            id: `${Date.now()}-${state.eventCount}-${state.logs.length}`,
            message: text,
            tone,
            meta: parts.join(" | "),
            failureReason: String(payload?.failure_reason || "").trim(),
        });
        state.logs = state.logs.slice(0, 24);
        renderStreamLog();
    }

    function renderStreamLog() {
        if (!elements.streamLog) {
            return;
        }
        state.logController?.destroy?.();
        state.logController = null;
        if (!state.logs.length) {
            elements.streamLog.innerHTML = `<div class="asset-stream-log__empty">No stream activity yet.</div>`;
            return;
        }
        elements.streamLog.classList.add("mini-card-strip");
        elements.streamLog.dataset.miniCardStrip = "";
        elements.streamLog.innerHTML = state.logs.map((entry) => `
            <button
                type="button"
                class="asset-stream-log__entry mini-card"
                data-tone="${escapeHtml(entry.tone || "info")}"
                data-mini-card
                data-mini-card-id="${escapeHtml(entry.id)}"
            >
                <div class="asset-stream-log__meta">
                    <span class="mini-card__title">${escapeHtml(entry.meta || "stream")}</span>
                    <span>${escapeHtml(entry.tone || "info")}</span>
                </div>
                <div class="mini-card__meta">${escapeHtml(entry.message || "")}</div>
            </button>
        `).join("");
        state.logController = createMiniCardController({
            root: elements.streamLog.parentElement || elements.streamLog,
            strip: elements.streamLog,
            detailPanel: ensureAssetDetailPanel(elements.streamLog.parentElement || elements.streamLog, "asset-stream-detail"),
            selectedId: state.logs[0]?.id,
            getItem: (key) => {
                const entry = state.logs.find((item) => String(item.id) === String(key));
                if (!entry) return null;
                return {
                    id: entry.id,
                    title: entry.meta || "stream",
                    status: entry.tone || "info",
                    meta: "Asset generation event",
                    facts: [
                        { label: "类型", value: entry.tone || "info" },
                        { label: "事件数", value: String(state.eventCount) },
                    ],
                    body: [entry.message, entry.failureReason].filter(Boolean).join("\n\n"),
                };
            },
        });
    }

    function bindEditorTabs() {
        editorTabs.forEach((button) => {
            button.addEventListener("click", () => activateEditorPage(button.dataset.editorTabTrigger || ""));
        });
    }

    function activateEditorPage(pageKey, options = {}) {
        if (!pageKey) {
            return;
        }
        state.activePage = pageKey;
        editorPages.forEach((page) => {
            page.classList.toggle("is-active", page.dataset.editorPage === pageKey);
        });
        if (!options.skipStatusRender) {
            renderDocumentStatus();
        }
        updateActiveDocumentLabel();

        if (options.scrollTab && elements.docStatus) {
            const activePill = elements.docStatus.querySelector(`[data-document-jump="${pageKey}"]`);
            if (activePill) {
                activePill.scrollIntoView({ block: "nearest", inline: "nearest", behavior: "smooth" });
            }
        }
    }

    function bindDraftSync() {
        if (splitDocumentKeys.length) {
            splitDocumentKeys.forEach((key) => {
                documentEditors[key]?.addEventListener("input", () => {
                    state.documents[key].markdown = documentEditors[key].value;
                    syncMarkdownArtifacts();
                    renderDocumentStatus();
                });
            });
        } else {
            elements.singleMarkdown?.addEventListener("input", () => syncMarkdownArtifacts());
        }
        elements.jsonPayload?.addEventListener("input", () => renderStoneAssetPreview());
    }

    function syncMarkdownArtifacts() {
        if (splitDocumentKeys.length) {
            const payload = {
                documents: {},
            };
            splitDocumentKeys.forEach((key) => {
                payload.documents[key] = {
                    filename: resolveDocumentFilename(key),
                    markdown: String(documentEditors[key]?.value || ""),
                };
            });
            if (elements.markdownText) {
                elements.markdownText.value = String(documentEditors.skill?.value || "");
            }
            if (elements.promptText) {
                elements.promptText.value = String(documentEditors.skill?.value || "");
            }
            if (elements.jsonPayload) {
                elements.jsonPayload.value = JSON.stringify(payload, null, 2);
            }
            return;
        }

        if (elements.markdownText && elements.singleMarkdown) {
            elements.markdownText.value = elements.singleMarkdown.value;
        }
    }

    function hydrateFromDraftPayload(draft, draftId) {
        if (!draft || typeof draft !== "object") {
            return;
        }
        state.draftId = String(draftId || draft.id || "");
        if (elements.draftForm) {
            elements.draftForm.dataset.draftId = state.draftId;
        }
        if (elements.promptText) {
            elements.promptText.value = String(draft.prompt_text || draft.system_prompt || "");
        }
        if (elements.notes) {
            elements.notes.value = String(draft.notes || "");
        }
        if (splitDocumentKeys.length) {
            const documents = draft.json_payload?.documents || {};
            splitDocumentKeys.forEach((key) => {
                const markdown = String(documents[key]?.markdown || documentEditors[key]?.value || "");
                if (documentEditors[key]) {
                    documentEditors[key].value = markdown;
                }
                state.documents[key] = { markdown };
            });
            syncMarkdownArtifacts();
        } else {
            const markdown = String(draft.markdown_text || "");
            if (elements.singleMarkdown) {
                elements.singleMarkdown.value = markdown;
            }
            if (elements.markdownText) {
                elements.markdownText.value = markdown;
            }
            if (elements.jsonPayload) {
                elements.jsonPayload.value = JSON.stringify(draft.json_payload || {}, null, 2);
            }
        }
        renderStoneAssetPreview();
        if (splitDocumentKeys.length) {
            activateEditorPage(state.activeStreamDocument || state.activePage || splitDocumentKeys[0] || "skill");
        } else {
            updateActiveDocumentLabel();
        }
        if (elements.draftPlaceholder) {
            elements.draftPlaceholder.hidden = true;
        }
        refreshDraftState();
        renderDocumentStatus();
    }

    function setEditorsLocked(locked) {
        state.locked = locked;
        const editableTargets = [
            ...Object.values(documentEditors),
            elements.jsonPayload,
            elements.notes,
            elements.singleMarkdown,
        ].filter(Boolean);
        editableTargets.forEach((element) => {
            element.readOnly = locked;
        });
        if (elements.lockChip) {
            elements.lockChip.className = `status-chip ${locked ? "tone-warning" : "tone-ready"}`;
            elements.lockChip.textContent = locked ? "输出锁定" : "可编辑";
        }
        if (elements.saveButton) {
            elements.saveButton.disabled = locked || !state.draftId;
        }
        if (elements.publishButton) {
            elements.publishButton.disabled = locked || !state.draftId;
        }
    }

    function refreshDraftState() {
        const hasDraft = Boolean(state.draftId);
        if (elements.saveButton) {
            elements.saveButton.disabled = state.locked || !hasDraft;
        }
        if (elements.publishButton) {
            elements.publishButton.disabled = state.locked || !hasDraft;
        }
    }

    async function saveDraft(button) {
        if (!state.draftId) {
            return;
        }
        syncMarkdownArtifacts();
        setButtonBusy(button, true, "保存中...");
        try {
            const payload = buildSavePayload();
            const response = await fetchJson(`/api/projects/${projectId}/assets/${state.draftId}/save`, {
                method: "POST",
                body: JSON.stringify(payload),
            });
            hydrateFromDraftPayload(response, state.draftId);
            showNotice(elements.message, response.message || "草稿已保存。", "success");
        } catch (error) {
            showNotice(elements.message, error.message || "保存失败。", "warning");
        } finally {
            setButtonBusy(button, false);
        }
    }

    async function publishDraft(button) {
        if (!state.draftId) {
            return;
        }
        setButtonBusy(button, true, "发布中...");
        try {
            const response = await fetchJson(`/api/projects/${projectId}/assets/${state.draftId}/publish`, {
                method: "POST",
                body: JSON.stringify({ asset_kind: assetKind }),
            });
            showNotice(elements.message, response.message || "资产版本已发布。", "success");
            window.setTimeout(() => {
                window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}`;
            }, 400);
        } catch (error) {
            showNotice(elements.message, error.message || "发布失败。", "warning");
        } finally {
            setButtonBusy(button, false);
        }
    }

    function buildSavePayload() {
        if (splitDocumentKeys.length) {
            const documents = {};
            splitDocumentKeys.forEach((key) => {
                documents[key] = {
                    filename: resolveDocumentFilename(key),
                    markdown: String(documentEditors[key]?.value || ""),
                };
            });
            return {
                asset_kind: assetKind,
                markdown_text: String(documentEditors.skill?.value || ""),
                json_payload: { documents },
                prompt_text: String(elements.promptText?.value || documentEditors.skill?.value || ""),
                notes: String(elements.notes?.value || ""),
            };
        }
        return {
            asset_kind: assetKind,
            markdown_text: String(elements.singleMarkdown?.value || ""),
            json_payload: safeParseJson(elements.jsonPayload?.value || "{}", {}),
            prompt_text: String(elements.promptText?.value || ""),
            notes: String(elements.notes?.value || ""),
        };
    }

    function renderDocumentStatus() {
        if (!elements.docStatus) {
            return;
        }
        state.documentController?.destroy?.();
        state.documentController = null;
        elements.docStatus.classList.add("mini-card-workbench");
        if (!splitDocumentKeys.length) {
            const hasContent = Boolean(elements.singleMarkdown?.value);
            elements.docStatus.innerHTML = `
                <button
                    type="button"
                    class="asset-track-pill mini-card ${hasContent ? "is-ready" : "is-missing"} ${state.activePage === "markdown" ? "is-active is-selected" : ""}"
                    data-document-jump="markdown"
                    data-mini-card
                    data-mini-card-id="markdown"
                >
                    <div class="asset-track-pill__info">
                        <span class="asset-track-pill__dot"></span>
                        <strong class="asset-track-pill__name">${escapeHtml(assetKind === "profile_report" ? "profile_report.md" : "draft.md")}</strong>
                    </div>
                    <span class="asset-track-pill__meta">${escapeHtml(`${(elements.singleMarkdown?.value || "").length} chars`)}</span>
                </button>
            `;
            bindDocumentStatusActions();
            bindDocumentStatusController(["markdown"]);
            updateActiveDocumentLabel();
            return;
        }

        const documentPills = splitDocumentKeys.map((key) => {
            const markdown = String(documentEditors[key]?.value || "");
            const filename = resolveDocumentFilename(key);
            const isActive = state.activePage === key;
            const isStreaming = state.activeStreamDocument === key && state.locked;
            const statusClass = markdown.trim() ? "is-ready" : "is-missing";

            return `
                <button
                    type="button"
                    class="asset-track-pill mini-card ${statusClass} ${isActive ? "is-active is-selected" : ""} ${isStreaming ? "is-streaming" : ""}"
                    data-document-jump="${escapeHtml(key)}"
                    data-mini-card
                    data-mini-card-id="${escapeHtml(key)}"
                >
                    <div class="asset-track-pill__info">
                        <span class="asset-track-pill__dot"></span>
                        <strong class="asset-track-pill__name">${escapeHtml(filename)}</strong>
                    </div>
                    <span class="asset-track-pill__meta">${escapeHtml(`${markdown.length} chars`)}</span>
                </button>
            `;
        }).join("");

        const extraTabs = [
            { key: "json", label: ui.field_json || "JSON Payload" },
            { key: "prompt", label: ui.field_prompt || "Prompt 文本" },
            { key: "notes", label: ui.field_notes || "备注" }
        ].map(tab => {
            const isActive = state.activePage === tab.key;
            return `
                <button
                    type="button"
                    class="asset-track-pill mini-card is-ready ${isActive ? "is-active is-selected" : ""}"
                    data-document-jump="${escapeHtml(tab.key)}"
                    data-mini-card
                    data-mini-card-id="${escapeHtml(tab.key)}"
                    style="min-width: auto; flex: 0 0 auto;"
                >
                    <div class="asset-track-pill__info">
                        <strong class="asset-track-pill__name">${escapeHtml(tab.label)}</strong>
                    </div>
                </button>
            `;
        }).join("");

        elements.docStatus.innerHTML = documentPills + extraTabs;
        bindDocumentStatusActions();
        bindDocumentStatusController([...splitDocumentKeys, "json", "prompt", "notes"]);
        updateActiveDocumentLabel();
    }

    function bindDocumentStatusController(keys) {
        if (!elements.docStatus) {
            return;
        }
        elements.docStatus.classList.add("mini-card-strip");
        elements.docStatus.dataset.miniCardStrip = "";
        state.documentController = createMiniCardController({
            root: elements.docStatus.parentElement || elements.docStatus,
            strip: elements.docStatus,
            detailPanel: ensureAssetDetailPanel(elements.docStatus.parentElement || elements.docStatus, "asset-document-detail"),
            selectedId: state.activePage,
            getItem: (key) => buildDocumentDetailItem(key, keys),
            onSelect: (key) => {
                if (key) {
                    activateEditorPage(key, { scrollTab: false, skipStatusRender: true });
                }
            },
        });
    }

    function buildDocumentDetailItem(key, keys = []) {
        const pageKey = keys.includes(key) ? key : state.activePage;
        const label = pageKey === "markdown" && !splitDocumentKeys.length
            ? (assetKind === "profile_report" ? "profile_report.md" : "draft.md")
            : splitDocumentKeys.includes(pageKey)
                ? resolveDocumentFilename(pageKey)
                : pageKey;
        const text = getDocumentText(pageKey);
        return {
            id: pageKey,
            title: label,
            status: text.trim() ? "ready" : "empty",
            meta: pageKey,
            facts: [
                { label: "字符", value: String(text.length) },
                { label: "状态", value: state.locked && state.activeStreamDocument === pageKey ? "生成中" : "可编辑" },
            ],
            body: text || "暂无内容。",
        };
    }

    function getDocumentText(pageKey) {
        if (splitDocumentKeys.includes(pageKey)) {
            return String(documentEditors[pageKey]?.value || "");
        }
        if (pageKey === "markdown") {
            return String(elements.singleMarkdown?.value || "");
        }
        if (pageKey === "json") {
            return String(elements.jsonPayload?.value || "");
        }
        if (pageKey === "prompt") {
            return String(elements.promptText?.value || "");
        }
        if (pageKey === "notes") {
            return String(elements.notes?.value || "");
        }
        return "";
    }

    function bindDocumentStatusActions() {
        elements.docStatus?.querySelectorAll("[data-document-jump]").forEach((button) => {
            if (button.dataset.bound === "1") {
                return;
            }
            button.dataset.bound = "1";
            button.addEventListener("click", () => {
                const pageKey = button.dataset.documentJump || "";
                if (pageKey === "markdown" && !splitDocumentKeys.length) {
                    state.activePage = "markdown";
                    renderDocumentStatus();
                    updateActiveDocumentLabel();
                    return;
                }
                activateEditorPage(pageKey);
            });
        });
    }

    function ensureAssetDetailPanel(parent, id) {
        if (!parent) {
            return null;
        }
        let panel = parent.querySelector(`#${CSS.escape(id)}`);
        if (!panel) {
            panel = document.createElement("section");
            panel.id = id;
            panel.className = "detail-panel asset-inline-detail";
            panel.dataset.detailPanel = "";
            parent.appendChild(panel);
        }
        return panel;
    }

    function bindVersionCards() {
        const versionList = document.getElementById("asset-version-list");
        if (!versionList) {
            return;
        }
        state.versionController?.destroy?.();
        state.versionController = createMiniCardController({
            root: versionList.parentElement || versionList,
            strip: versionList,
            detailPanel: ensureAssetDetailPanel(versionList.parentElement || versionList, "asset-version-detail"),
            selectedId: versionList.querySelector("[data-mini-card]")?.dataset.miniCardId || "",
            getItem: (key) => {
                const card = versionList.querySelector(`[data-mini-card-id="${CSS.escape(String(key || ""))}"]`);
                if (!card) return null;
                const links = Array.from(card.querySelectorAll("a"))
                    .map((link) => link.textContent.trim())
                    .filter(Boolean);
                return {
                    id: key,
                    title: card.dataset.versionNumber || card.querySelector("strong")?.textContent || "Version",
                    status: card.dataset.versionKind || assetKind,
                    meta: card.dataset.versionTime || "--",
                    facts: [
                        { label: "类型", value: card.dataset.versionKind || assetKind },
                        { label: "导出", value: links.join(" / ") || "download" },
                    ],
                    body: `发布时间：${card.dataset.versionTime || "--"}\n可用操作：${links.join(" / ") || "Download"}`,
                };
            },
        });
    }

    function updateActiveDocumentLabel() {
        if (!elements.activeDocument) {
            return;
        }
        const currentKey = state.activeStreamDocument || state.activePage;
        if (isStoneAsset && !state.activeStreamDocument) {
            updateText(elements.activeDocument, "当前查看：structured preview");
            return;
        }
        if (!currentKey) {
            updateText(elements.activeDocument, "当前输出文件：等待开始");
            return;
        }
        const label = currentKey === "markdown"
            ? (assetKind === "profile_report" ? "profile_report.md" : "draft.md")
            : resolveDocumentFilename(currentKey);
        const prefix = state.activeStreamDocument ? "当前输出文件" : "当前查看文件";
        updateText(elements.activeDocument, `${prefix}：${label}`);
    }

    function renderStoneAssetPreview() {
        if (!isStoneAsset || !elements.stonePreview) {
            return;
        }
        const payload = safeParseJson(elements.jsonPayload?.value || "{}", {});
        if (assetKind === "stone_author_model_v3") {
            elements.stonePreview.innerHTML = renderStoneAuthorModelV3Preview(payload);
            return;
        }
        elements.stonePreview.innerHTML = renderStonePrototypeIndexV3Preview(payload);
    }

    function renderStoneAuthorModelPreview(payload) {
        const views = payload.views || {};
        const styleInvariants = payload.style_invariants || {};
        const blueprintRules = payload.blueprint_rules || {};
        const prototypeFamilies = Array.isArray(payload.prototype_families) ? payload.prototype_families : [];
        const topicTranslationMap = Array.isArray(payload.topic_translation_map) ? payload.topic_translation_map : [];
        const lengthBehaviors = Array.isArray(payload.length_behaviors) ? payload.length_behaviors : [];
        const evidenceWindows = Array.isArray(payload.evidence_windows) ? payload.evidence_windows : [];
        const antiPatterns = Array.isArray(payload.anti_patterns) ? payload.anti_patterns : [];

        return `
            <div class="stone-asset-metrics">
                ${renderMetric("profiles", payload.profile_count || 0)}
                ${renderMetric("families", prototypeFamilies.length)}
                ${renderMetric("evidence", evidenceWindows.length)}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Voice / Form", renderList(views.voice_form || [], "当前还没有抽出 voice/form 规律。"))}
                ${renderSection("Motif / Worldview", renderList(views.motif_worldview || [], "当前还没有抽出 motif/worldview 规律。"))}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Lexicon / Rhetoric", renderList([...(styleInvariants.lexicon_tics || []), ...(styleInvariants.rhetoric_preferences || [])], "当前还没有词汇和修辞稳定项。"))}
                ${renderSection("Opening / Closure", renderOpeningClosureSignatures(styleInvariants.opening_signatures || [], styleInvariants.closure_signatures || []))}
            </div>
            ${renderSection("Blueprint Rules", renderBlueprintRules(blueprintRules))}
            ${renderSection("Length Behaviors", renderLengthBehaviors(lengthBehaviors))}
            ${renderSection("Topic Translation Map", renderTopicTranslationMap(topicTranslationMap))}
            ${renderSection("Prototype Families", renderPrototypeFamilies(prototypeFamilies.slice(0, 6), false))}
            ${renderSection("Anti-Patterns", renderChips(antiPatterns, "当前没有 anti-pattern。"))}
            ${renderSection("Evidence Windows", renderEvidenceWindows(evidenceWindows.slice(0, 4)))}
        `;
    }

    function renderStonePrototypeIndexPreview(payload) {
        const retrievalPolicy = payload.retrieval_policy || {};
        const termIndex = Array.isArray(payload.retrieval_term_index) ? payload.retrieval_term_index : [];
        const documents = Array.isArray(payload.documents) ? payload.documents : [];
        const familyCounts = new Map();
        documents.forEach((item) => {
            const family = String(item.prototype_family || "").trim();
            if (family) {
                familyCounts.set(family, (familyCounts.get(family) || 0) + 1);
            }
        });
        const familyRows = Array.from(familyCounts.entries())
            .sort((left, right) => right[1] - left[1])
            .slice(0, 8)
            .map(([family, count]) => ({ family, count }));

        return `
            <div class="stone-asset-metrics">
                ${renderMetric("documents", payload.document_count || documents.length)}
                ${renderMetric("families", familyRows.length)}
                ${renderMetric("retrieval terms", termIndex.length)}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Family Distribution", renderFamilyDistribution(familyRows))}
                ${renderSection("Retrieval Policy", renderRetrievalPolicy(retrievalPolicy))}
            </div>
            ${renderSection("Retrieval Term Index", renderRetrievalTermIndex(termIndex))}
            ${renderSection("Document Windows", renderPrototypeDocuments(documents.slice(0, 8)))}
        `;
    }

    function renderStoneAuthorModelV3Preview(payload) {
        const authorCore = payload.author_core || {};
        const translationRules = Array.isArray(payload.translation_rules) ? payload.translation_rules : [];
        const stableMoves = Array.isArray(payload.stable_moves) ? payload.stable_moves : [];
        const forbiddenMoves = Array.isArray(payload.forbidden_moves) ? payload.forbidden_moves : [];
        const familyMap = Array.isArray(payload.family_map) ? payload.family_map : [];
        const globalEvidence = Array.isArray(payload.global_evidence) ? payload.global_evidence : [];

        return `
            <div class="stone-asset-metrics">
                ${renderMetric("profiles", payload.profile_count || 0)}
                ${renderMetric("families", familyMap.length || payload.family_count || 0)}
                ${renderMetric("evidence", globalEvidence.length)}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Author Core", renderAuthorCoreV3(authorCore))}
                ${renderSection("Critic Rubrics", renderCriticRubricsV3(payload.critic_rubrics || {}))}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Stable Moves", renderList(stableMoves, "No stable moves yet."))}
                ${renderSection("Forbidden Moves", renderList(forbiddenMoves, "No forbidden moves yet."))}
            </div>
            ${renderSection("Translation Rules", renderTranslationRulesV3(translationRules))}
            ${renderSection("Family Map", renderFamilyMapV3(familyMap))}
            ${renderSection("Global Evidence", renderGlobalEvidenceV3(globalEvidence))}
        `;
    }

    function renderStonePrototypeIndexV3Preview(payload) {
        const retrievalPolicy = payload.retrieval_policy || {};
        const families = Array.isArray(payload.families) ? payload.families : [];
        const documents = Array.isArray(payload.documents) ? payload.documents : [];
        const anchorRegistry = Array.isArray(payload.anchor_registry) ? payload.anchor_registry : [];

        return `
            <div class="stone-asset-metrics">
                ${renderMetric("documents", payload.document_count || documents.length)}
                ${renderMetric("families", families.length || payload.family_count || 0)}
                ${renderMetric("anchors", anchorRegistry.length)}
            </div>
            <div class="stone-asset-grid">
                ${renderSection("Retrieval Policy", renderRetrievalPolicyV3(retrievalPolicy))}
                ${renderSection("Selection Guides", renderSelectionGuidesV3(payload.selection_guides || {}))}
            </div>
            ${renderSection("Families", renderFamilyMapV3(families))}
            ${renderSection("Prototype Documents", renderPrototypeDocumentsV3(documents.slice(0, 8)))}
            ${renderSection("Anchor Registry", renderAnchorRegistryV3(anchorRegistry.slice(0, 10)))}
        `;
    }

    function renderMetric(label, value) {
        return `
            <article class="stone-asset-metric">
                <span>${escapeHtml(label)}</span>
                <strong>${escapeHtml(String(value))}</strong>
            </article>
        `;
    }

    function renderSection(title, body) {
        return `
            <section class="stone-asset-section">
                <h4>${escapeHtml(title)}</h4>
                ${body}
            </section>
        `;
    }

    function renderList(items, emptyText) {
        const rows = Array.isArray(items) ? items.filter(Boolean) : [];
        if (!rows.length) {
            return `<p class="helper-text">${escapeHtml(emptyText)}</p>`;
        }
        return `<ul class="stone-asset-list">${rows.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`;
    }

    function renderChips(items, emptyText) {
        const rows = Array.isArray(items) ? items.filter(Boolean) : [];
        if (!rows.length) {
            return `<p class="helper-text">${escapeHtml(emptyText)}</p>`;
        }
        return `<div class="stone-asset-chip-row">${rows.map((item) => `<span class="stone-asset-chip">${escapeHtml(item)}</span>`).join("")}</div>`;
    }

    function renderAuthorCoreV3(authorCore) {
        const motifs = Array.isArray(authorCore?.signature_motifs) ? authorCore.signature_motifs : [];
        return `
            <ul class="stone-asset-list">
                <li><strong>voice</strong> ${escapeHtml(String(authorCore?.voice_summary || "--"))}</li>
                <li><strong>worldview</strong> ${escapeHtml(String(authorCore?.worldview_summary || "--"))}</li>
                <li><strong>tone</strong> ${escapeHtml(String(authorCore?.tone_summary || "--"))}</li>
            </ul>
            ${renderChips(motifs, "No signature motifs yet.")}
        `;
    }

    function renderCriticRubricsV3(rubrics) {
        const rows = Object.entries(rubrics || {}).filter(([, items]) => Array.isArray(items) && items.length);
        if (!rows.length) {
            return `<p class="helper-text">No critic rubrics yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map(([key, items]) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(key)}</h4>
                        ${renderList(items, "No rubric items")}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderTranslationRulesV3(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">No translation rules yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.slice(0, 8).map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.value_lens || "--"))}</h4>
                        ${renderChips(item.preferred_motifs || [], "No preferred motifs")}
                        <ul class="stone-asset-list">
                            <li><strong>openings</strong> ${escapeHtml((item.opening_moves || []).join(" / ") || "--")}</li>
                            <li><strong>closures</strong> ${escapeHtml((item.closure_moves || []).join(" / ") || "--")}</li>
                        </ul>
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderFamilyMapV3(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">No family map yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.slice(0, 10).map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.label || item.family_id || "--"))}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`members ${item.member_count || 0}`)}</span>
                        </div>
                        ${item.description ? `<p class="helper-text">${escapeHtml(String(item.description))}</p>` : ""}
                        ${renderChips(item.motif_tags || [], "No motif tags")}
                        ${renderChips(item.selection_cues || [], "No selection cues")}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderGlobalEvidenceV3(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">No global evidence yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.slice(0, 8).map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.title || item.document_id || "--"))}</h4>
                        ${item.summary ? `<div class="stone-asset-window"><strong>summary</strong>${escapeHtml(String(item.summary))}</div>` : ""}
                        ${item.opening ? `<div class="stone-asset-window"><strong>opening</strong>${escapeHtml(String(item.opening))}</div>` : ""}
                        ${item.closing ? `<div class="stone-asset-window"><strong>closing</strong>${escapeHtml(String(item.closing))}</div>` : ""}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderRetrievalPolicyV3(policy) {
        const notes = Array.isArray(policy?.notes) ? policy.notes : [];
        return `
            <ul class="stone-asset-list">
                <li><strong>shortlist</strong> ${escapeHtml(String(policy?.shortlist_formula || "--"))}</li>
                <li><strong>size</strong> ${escapeHtml(String(policy?.target_shortlist_size || "--"))}</li>
                <li><strong>anchor budget</strong> ${escapeHtml(String(policy?.target_anchor_budget || "--"))}</li>
            </ul>
            ${renderList(notes, "No retrieval notes")}
        `;
    }

    function renderSelectionGuidesV3(guides) {
        const rows = [
            { label: "expand", items: Array.isArray(guides?.when_to_expand) ? guides.when_to_expand : [] },
            { label: "prune", items: Array.isArray(guides?.when_to_prune) ? guides.when_to_prune : [] },
            { label: "quality", items: Array.isArray(guides?.quality_checks) ? guides.quality_checks : [] },
        ].filter((item) => item.items.length);
        if (!rows.length) {
            return `<p class="helper-text">No selection guides yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(item.label)}</h4>
                        ${renderList(item.items, "No guide items")}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderPrototypeDocumentsV3(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">No prototype documents yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => {
                    const handles = item.retrieval_handles || {};
                    const anchors = Array.isArray(item.anchor_registry) ? item.anchor_registry : [];
                    const firstAnchor = anchors.find((anchor) => anchor && anchor.quote);
                    return `
                        <article class="stone-asset-item">
                            <h4>${escapeHtml(String(item.title || item.document_id || "--"))}</h4>
                            <div class="stone-asset-item__meta">
                                <span>${escapeHtml(String(item.family_label || item.family_id || "--"))}</span>
                            </div>
                            ${renderChips(handles.keywords || [], "No keywords")}
                            ${firstAnchor ? `<div class="stone-asset-window"><strong>${escapeHtml(String(firstAnchor.role || "anchor"))}</strong>${escapeHtml(String(firstAnchor.quote || ""))}</div>` : ""}
                        </article>
                    `;
                }).join("")}
            </div>
        `;
    }

    function renderAnchorRegistryV3(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">No anchors yet.</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(String(item.document_title || item.document_id || "--"))}</span>
                            <span>${escapeHtml(String(item.role || "anchor"))}</span>
                        </div>
                        <div class="stone-asset-window"><strong>${escapeHtml(String(item.id || "anchor"))}</strong>${escapeHtml(String(item.quote || ""))}</div>
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderLengthBehaviors(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有按长度归纳的行为模式。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.length_band || "--"))}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`count ${item.count || 0}`)}</span>
                            <span>${escapeHtml(`surface ${item.surface_form || "--"}`)}</span>
                        </div>
                        <ul class="stone-asset-list">
                            <li><strong>opening</strong> ${escapeHtml(item.opening_move || "--")}</li>
                            <li><strong>closing</strong> ${escapeHtml(item.closure_move || "--")}</li>
                        </ul>
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderTopicTranslationMap(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有 topic translation map。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.value_lens || "--"))}</h4>
                        ${renderChips(item.motif_tags || [], "暂无 motifs")}
                        <ul class="stone-asset-list">
                            <li><strong>opening</strong> ${escapeHtml((item.opening_moves || []).join(" / ") || "--")}</li>
                            <li><strong>closing</strong> ${escapeHtml((item.closure_moves || []).join(" / ") || "--")}</li>
                        </ul>
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderPrototypeFamilies(rows, showWindows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有 prototype family。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.label || item.family_key || "--"))}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`members ${item.member_count || 0}`)}</span>
                            ${item.family_key ? `<span>${escapeHtml(item.family_key)}</span>` : ""}
                        </div>
                        ${renderChips(item.motif_tags || [], "暂无 motifs")}
                        ${renderList(item.shared_traits || [], "当前没有 shared traits。")}
                        ${showWindows ? renderEvidenceWindows(item.exemplar_windows || []) : ""}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderOpeningClosureSignatures(openingRows, closureRows) {
        const rows = [
            ...(Array.isArray(openingRows) ? openingRows.map((item) => ({ ...item, kind: "opening" })) : []),
            ...(Array.isArray(closureRows) ? closureRows.map((item) => ({ ...item, kind: "closing" })) : []),
        ];
        if (!rows.length) {
            return `<p class="helper-text">当前还没有稳定的起笔或收口签名。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(item.kind)}</span>
                            <span>${escapeHtml(`count ${item.count || 0}`)}</span>
                        </div>
                        <h4>${escapeHtml(String(item.move || "--"))}</h4>
                        ${item.anchor ? `<div class="stone-asset-window"><strong>anchor</strong>${escapeHtml(item.anchor)}</div>` : ""}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderBlueprintRules(rules) {
        const entryRules = Array.isArray(rules?.entry_rules) ? rules.entry_rules : [];
        const developmentRules = Array.isArray(rules?.development_rules) ? rules.development_rules : [];
        const closureRules = Array.isArray(rules?.closure_rules) ? rules.closure_rules : [];
        if (!entryRules.length && !developmentRules.length && !closureRules.length) {
            return `<p class="helper-text">当前还没有蓝图级规则。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                <article class="stone-asset-item">
                    <h4>Entry</h4>
                    ${renderList(entryRules, "暂无 entry rule")}
                </article>
                <article class="stone-asset-item">
                    <h4>Development</h4>
                    ${renderList(developmentRules, "暂无 development rule")}
                </article>
                <article class="stone-asset-item">
                    <h4>Closure</h4>
                    ${renderList(closureRules, "暂无 closure rule")}
                </article>
            </div>
        `;
    }

    function renderEvidenceWindows(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前没有 evidence window。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        ${item.prototype_family ? `<div class="stone-asset-item__meta"><span>${escapeHtml(item.prototype_family)}</span></div>` : ""}
                        ${item.opening ? `<div class="stone-asset-window"><strong>opening</strong>${escapeHtml(item.opening)}</div>` : ""}
                        ${item.closing ? `<div class="stone-asset-window"><strong>closing</strong>${escapeHtml(item.closing)}</div>` : ""}
                        ${Array.isArray(item.signature) && item.signature.length
                            ? item.signature.slice(0, 2).map((line, index) => `<div class="stone-asset-window"><strong>${escapeHtml(`signature ${index + 1}`)}</strong>${escapeHtml(line)}</div>`).join("")
                            : ""}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderFamilyDistribution(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有 family 分布。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(item.family)}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`documents ${item.count}`)}</span>
                        </div>
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderRetrievalPolicy(policy) {
        const weights = policy?.weights || {};
        const fields = Array.isArray(policy?.ranking_fields) ? policy.ranking_fields : [];
        return `
            <div class="stone-asset-rows">
                <article class="stone-asset-item">
                    <h4>${escapeHtml(String(policy?.ranking_formula || "未定义排序公式"))}</h4>
                    ${Object.keys(weights).length ? renderChips(Object.entries(weights).map(([key, value]) => `${key} ${value}%`), "暂无权重") : ""}
                    ${fields.length ? renderList(fields, "暂无排序字段") : `<p class="helper-text">暂无排序字段。</p>`}
                </article>
            </div>
        `;
    }

    function renderRetrievalTermIndex(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有 retrieval term index。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.slice(0, 12).map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.term || "--"))}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`count ${item.count || 0}`)}</span>
                        </div>
                        ${renderChips(item.families || [], "暂无 family")}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function renderPrototypeDocuments(rows) {
        if (!Array.isArray(rows) || !rows.length) {
            return `<p class="helper-text">当前还没有原型文档。</p>`;
        }
        return `
            <div class="stone-asset-rows">
                ${rows.map((item) => `
                    <article class="stone-asset-item">
                        <h4>${escapeHtml(String(item.title || "--"))}</h4>
                        <div class="stone-asset-item__meta">
                            <span>${escapeHtml(`family ${item.prototype_family || "--"}`)}</span>
                            <span>${escapeHtml(`length ${item.length_band || "--"}`)}</span>
                            <span>${escapeHtml(`surface ${item.surface_form || "--"}`)}</span>
                        </div>
                        ${renderChips([
                            item?.retrieval_facets?.judgment,
                            item?.retrieval_facets?.value_lens,
                            item?.retrieval_facets?.distance,
                            ...((item?.retrieval_facets?.motif_tags) || []),
                        ].filter(Boolean), "暂无 retrieval facets")}
                        ${renderChips(item.retrieval_terms || [], "暂无 retrieval terms")}
                        ${(item.windows?.opening) ? `<div class="stone-asset-window"><strong>opening</strong>${escapeHtml(item.windows.opening)}</div>` : ""}
                        ${(item.windows?.closing) ? `<div class="stone-asset-window"><strong>closing</strong>${escapeHtml(item.windows.closing)}</div>` : ""}
                    </article>
                `).join("")}
            </div>
        `;
    }

    function resolveDocumentFilename(key) {
        return {
            skill: "SKILL.md",
            personality: "references/personality.md",
            memories: "references/memories.md",
            analysis: "references/analysis.md",
        }[key] || key;
    }

    function extractResponseErrorMessage(responseText, statusCode) {
        const text = String(responseText || "").trim();
        if (!text) {
            return `Streaming asset generation failed with HTTP ${statusCode}.`;
        }
        try {
            const payload = JSON.parse(text);
            if (typeof payload?.detail === "string" && payload.detail.trim()) {
                return payload.detail.trim();
            }
            if (typeof payload?.message === "string" && payload.message.trim()) {
                return payload.message.trim();
            }
        } catch {}
        return text.slice(0, 240);
    }
}
