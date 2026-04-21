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
    const assetKind = bootstrap.asset_kind || "cc_skill";
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
        chunkCount: document.getElementById("asset-chunk-count"),
        charCount: document.getElementById("asset-char-count"),
        lockChip: document.getElementById("asset-editor-lock-chip"),
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
    };

    const editorTabs = Array.from(document.querySelectorAll("[data-editor-tab-trigger]"));
    const editorPages = Array.from(document.querySelectorAll("[data-editor-page]"));
    const documentEditors = Object.fromEntries(
        Array.from(document.querySelectorAll("[data-document-editor]")).map((element) => [element.dataset.documentEditor, element]),
    );

    const state = {
        draftId: String(bootstrap.draft_id || ""),
        locked: false,
        chunkCount: 0,
        charCount: 0,
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
        } catch (error) {
            renderStatus({
                status: "failed",
                progress_percent: 0,
                message: error.message || "生成失败。",
            });
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
        if (eventType === "status") {
            renderStatus(data);
            return;
        }
        if (eventType === "delta") {
            appendStreamChunk(data.document_key || (splitDocumentKeys.length ? "skill" : "asset"), data.chunk || "");
            return;
        }
        if (eventType === "done") {
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
            renderStatus({
                status: "failed",
                progress_percent: 0,
                message: data.message || ui.status_failed || "Generation failed",
            });
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
                documentEditors[key].value += chunk;
                state.documents[key].markdown = documentEditors[key].value;
            }
        } else if (elements.singleMarkdown) {
            elements.singleMarkdown.value += chunk;
        }
        state.chunkCount += 1;
        state.charCount += chunk.length;
        updateCounts();
        syncMarkdownArtifacts();
        renderDocumentStatus();
    }

    function resetStreamingState() {
        state.chunkCount = 0;
        state.charCount = 0;
        updateCounts();
        if (splitDocumentKeys.length) {
            splitDocumentKeys.forEach((key) => {
                if (documentEditors[key]) {
                    documentEditors[key].value = "";
                }
                state.documents[key] = { markdown: "" };
            });
        } else if (elements.singleMarkdown) {
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
        renderDocumentStatus();
    }

    function updateCounts() {
        updateText(elements.chunkCount, state.chunkCount);
        updateText(elements.charCount, state.charCount);
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
            elements.lockChip.textContent = locked ? "LOCKED" : "READY";
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
        if (!splitDocumentKeys.length) {
            elements.docStatus.innerHTML = `
                <article class="document-card compact-card asset-doc-card ${elements.singleMarkdown?.value ? "is-ready" : "is-missing"}">
                    <div class="document-card__head">
                        <strong>${escapeHtml(assetKind === "profile_report" ? "profile_report.md" : "draft.md")}</strong>
                        <span class="status-chip ${elements.singleMarkdown?.value ? "tone-ready" : "tone-warning"}">${elements.singleMarkdown?.value ? "ready" : "missing"}</span>
                    </div>
                    <p class="helper-text">${escapeHtml(`${(elements.singleMarkdown?.value || "").length} chars`)}</p>
                </article>
            `;
            return;
        }

        elements.docStatus.innerHTML = splitDocumentKeys.map((key) => {
            const markdown = String(documentEditors[key]?.value || "");
            const filename = resolveDocumentFilename(key);
            return `
                <article class="document-card compact-card asset-doc-card ${markdown.trim() ? "is-ready" : "is-missing"}">
                    <div class="document-card__head">
                        <strong>${escapeHtml(filename)}</strong>
                        <span class="status-chip ${markdown.trim() ? "tone-ready" : "tone-warning"}">${markdown.trim() ? "ready" : "missing"}</span>
                    </div>
                    <p class="helper-text">${escapeHtml(`${markdown.length} chars`)}</p>
                    <p class="helper-text">${escapeHtml((markdown || "当前文档为空。").slice(0, 180))}</p>
                    ${state.draftId && markdown.trim() ? `
                        <div class="button-row top-gap">
                            <a class="ghost-button" href="/api/projects/${projectId}/assets/${state.draftId}/exports/${encodeURIComponent(key)}">导出</a>
                        </div>
                    ` : ""}
                </article>
            `;
        }).join("");
    }

    function resolveDocumentFilename(key) {
        return {
            skill: "SKILL.md",
            personality: "references/personality.md",
            memories: "references/memories.md",
            analysis: "references/analysis.md",
        }[key] || key;
    }
}
