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
    const isStoneAsset = assetKind === "stone_author_model_v2" || assetKind === "stone_prototype_index_v2";
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
        chunkCount: 0,
        charCount: 0,
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
        state.chunkCount = 0;
        state.charCount = 0;
        state.activeStreamDocument = "";
        updateCounts();
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
        updateText(elements.chunkCount, state.chunkCount);
        updateText(elements.charCount, state.charCount);
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
        renderDocumentStatus();
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
        if (!splitDocumentKeys.length) {
            const hasContent = Boolean(elements.singleMarkdown?.value);
            elements.docStatus.innerHTML = `
                <button
                    type="button"
                    class="asset-track-pill ${hasContent ? "is-ready" : "is-missing"} ${state.activePage === "markdown" ? "is-active" : ""}"
                    data-document-jump="markdown"
                >
                    <div class="asset-track-pill__info">
                        <span class="asset-track-pill__dot"></span>
                        <strong class="asset-track-pill__name">${escapeHtml(assetKind === "profile_report" ? "profile_report.md" : "draft.md")}</strong>
                    </div>
                    <span class="asset-track-pill__meta">${escapeHtml(`${(elements.singleMarkdown?.value || "").length} chars`)}</span>
                </button>
            `;
            bindDocumentStatusActions();
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
                    class="asset-track-pill ${statusClass} ${isActive ? "is-active" : ""} ${isStreaming ? "is-streaming" : ""}"
                    data-document-jump="${escapeHtml(key)}"
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
                    class="asset-track-pill is-ready ${isActive ? "is-active" : ""}"
                    data-document-jump="${escapeHtml(tab.key)}"
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
        updateActiveDocumentLabel();
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
        elements.stonePreview.innerHTML = assetKind === "stone_author_model_v2"
            ? renderStoneAuthorModelPreview(payload)
            : renderStonePrototypeIndexPreview(payload);
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
}
