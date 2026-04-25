import {
    clampPercent,
    closeModal,
    escapeHtml,
    fetchJson,
    formatDateTime,
    openModal,
    setButtonBusy,
    showNotice,
    debounce,
    throttle
} from "./shared.js";

const bootstrap = JSON.parse(document.getElementById("project-bootstrap")?.textContent || "{}");

if (bootstrap.project?.id) {
    const state = {
        projectId: bootstrap.project.id,
        ui: bootstrap.ui_strings || {},
        telegram: { ...(bootstrap.telegram || {}) },
        documents: [...(bootstrap.documents || [])],
        pagination: { ...(bootstrap.pagination || { limit: 20, offset: 0, has_more: false }) },
        stats: { ...(bootstrap.stats || {}) },
        filter: "all",
        tasks: new Map(),
        socket: null,
        reconnectTimer: null,
    };

    const elements = {
        grid: document.getElementById("document-grid"),
        empty: document.getElementById("no-docs-message"),
        loadMore: document.getElementById("load-more-docs-btn"),
        refresh: document.getElementById("refresh-docs-btn"),
        processAll: document.getElementById("process-all-btn"),
        retryAll: document.getElementById("retry-all-btn"),
        stopProcessing: document.getElementById("stop-processing-btn"),
        filters: [...document.querySelectorAll("[data-doc-filter]")],
        dropzone: document.getElementById("upload-dropzone"),
        fileInput: document.getElementById("file-input"),
        modal: document.getElementById("document-modal"),
        modalTitle: document.getElementById("document-modal-title"),
        modalBody: document.getElementById("document-modal-body"),
        modalFooter: document.getElementById("document-modal-footer"),
        addTextModal: document.getElementById("add-text-document-modal"),
        addTextTitle: document.getElementById("stone-text-title"),
        addTextSource: document.getElementById("stone-text-source"),
        addTextNote: document.getElementById("stone-text-note"),
        addTextContent: document.getElementById("stone-text-content"),
        addTextSubmit: document.getElementById("add-text-document-submit"),
        feedback: document.getElementById("project-feedback"),
        statsTotal: document.getElementById("status-total"),
        statsReady: document.getElementById("status-ready"),
        statsPending: document.getElementById("status-pending"),
        statsFailed: document.getElementById("status-failed"),
        analyzeSubmit: document.getElementById("analyze-submit-btn"),
        targetPickers: [...document.querySelectorAll("[data-telegram-target-picker]")],
        relationshipPanel: document.getElementById("telegram-relationship-panel"),
        relationshipStatus: document.getElementById("telegram-relationship-status"),
        relationshipEmpty: document.getElementById("telegram-relationship-empty"),
        relationshipShell: document.getElementById("telegram-relationship-shell"),
        relationshipUsers: document.getElementById("telegram-relationship-users"),
        relationshipEdges: document.getElementById("telegram-relationship-edges"),
        relationshipFriendly: document.getElementById("telegram-relationship-friendly"),
        relationshipTense: document.getElementById("telegram-relationship-tense"),
        relationshipFriendlyList: document.getElementById("telegram-relationship-friendly-list"),
        relationshipTenseList: document.getElementById("telegram-relationship-tense-list"),
        relationshipMemberSelect: document.getElementById("telegram-relationship-member-select"),
        relationshipMemberAllies: document.getElementById("telegram-relationship-member-allies"),
        relationshipMemberTense: document.getElementById("telegram-relationship-member-tense"),
        relationshipMemberNeutral: document.getElementById("telegram-relationship-member-neutral"),
        relationshipMemberUnclear: document.getElementById("telegram-relationship-member-unclear"),
    };

    bindEvents();
    render();
    connectSocket();

    function bindEvents() {
        elements.filters.forEach((button) => {
            button.addEventListener("click", () => {
                state.filter = button.dataset.docFilter || "all";
                elements.filters.forEach((item) => item.classList.toggle("is-active", item === button));
                render();
            });
        });

        elements.loadMore?.addEventListener("click", () => loadMoreDocuments());
        elements.refresh?.addEventListener("click", () => refreshDocuments());
        elements.processAll?.addEventListener("click", () => runProjectAction("process-all", elements.processAll, state.ui.process_success));
        elements.retryAll?.addEventListener("click", () => runProjectAction("retry-all", elements.retryAll, state.ui.retry_success));
        elements.stopProcessing?.addEventListener("click", () => runProjectAction("stop-processing", elements.stopProcessing, state.ui.stop_success));
        elements.addTextSubmit?.addEventListener("click", () => createTextDocument());
        initTelegramTargetPickers();
        initPersonaCollapsibles();

        if (elements.dropzone && elements.fileInput) {
            ["dragenter", "dragover"].forEach((eventName) => {
                elements.dropzone.addEventListener(eventName, (event) => {
                    event.preventDefault();
                    elements.dropzone.classList.add("is-dragover");
                });
            });
            ["dragleave", "drop"].forEach((eventName) => {
                elements.dropzone.addEventListener(eventName, (event) => {
                    event.preventDefault();
                    elements.dropzone.classList.remove("is-dragover");
                });
            });
            elements.dropzone.addEventListener("drop", (event) => {
                const files = event.dataTransfer?.files;
                if (files?.length) {
                    void uploadFiles(files);
                }
            });
            elements.fileInput.addEventListener("change", async () => {
                if (elements.fileInput.files?.length) {
                    await uploadFiles(elements.fileInput.files);
                    elements.fileInput.value = "";
                }
            });
        }
    }

    async function uploadFiles(fileList) {
        if (!fileList?.length) {
            showFeedback(state.ui.upload_empty || "请先选择至少一个文件。", "error");
            return;
        }
        const formData = new FormData();
        Array.from(fileList).forEach((file) => formData.append("files", file));
        showFeedback(state.ui.uploading || "正在上传文档…");
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/documents`, {
                method: "POST",
                body: formData,
            });
            (payload.tasks || []).forEach((task) => {
                if (task?.document_id) {
                    state.tasks.set(task.document_id, task);
                }
            });
            await refreshDocuments();
            showFeedback(state.ui.upload_success || "文档上传完成。", "success");
        } catch (error) {
            showFeedback(error.message, "error");
        }
    }

    async function runProjectAction(action, button, successMessage) {
        setButtonBusy(button, true);
        try {
            await fetchJson(`/api/projects/${state.projectId}/${action}`, { method: "POST" });
            await refreshDocuments();
            showFeedback(successMessage || "操作已执行。", "success");
        } catch (error) {
            showFeedback(error.message, "error");
        } finally {
            setButtonBusy(button, false);
        }
    }

    async function refreshDocuments() {
        const payload = await fetchJson(`/api/projects/${state.projectId}/documents?offset=0&limit=${state.pagination.limit || 20}`);
        state.documents = payload.documents || [];
        applyListPayload(payload);
        render();
    }

    async function loadMoreDocuments() {
        if (!state.pagination.has_more) {
            return;
        }
        setButtonBusy(elements.loadMore, true);
        try {
            const nextOffset = state.documents.length;
            const payload = await fetchJson(
                `/api/projects/${state.projectId}/documents?offset=${nextOffset}&limit=${state.pagination.limit || 20}`
            );
            mergeDocuments(payload.documents || []);
            applyListPayload(payload, { preserveOffset: true });
            render();
        } catch (error) {
            showFeedback(error.message, "error");
        } finally {
            setButtonBusy(elements.loadMore, false);
        }
    }

    function applyListPayload(payload, options = {}) {
        state.stats.document_count = payload.total ?? state.stats.document_count ?? state.documents.length;
        state.stats.ready_count = payload.ready ?? state.stats.ready_count ?? 0;
        state.stats.failed_count = payload.failed ?? state.stats.failed_count ?? 0;
        state.stats.queued_count = payload.queued ?? state.stats.queued_count ?? 0;
        state.stats.processing_count = payload.processing ?? state.stats.processing_count ?? 0;
        state.stats.pending_count = payload.pending ?? state.stats.pending_count ?? 0;
        state.pagination = {
            ...state.pagination,
            limit: payload.limit ?? state.pagination.limit,
            offset: options.preserveOffset ? state.pagination.offset : (payload.offset ?? 0),
            has_more: !!payload.has_more,
        };
    }

    function mergeDocuments(nextDocuments) {
        const merged = new Map(state.documents.map((item) => [item.id, item]));
        nextDocuments.forEach((item) => merged.set(item.id, { ...(merged.get(item.id) || {}), ...item }));
        state.documents = [...merged.values()];
    }

    function render() {
        const documents = getFilteredDocuments();
        elements.grid.innerHTML = "";
        const fragment = document.createDocumentFragment();
        documents.forEach((document) => fragment.appendChild(renderDocumentCard(document)));
        elements.grid.appendChild(fragment);
        elements.empty.hidden = documents.length > 0;
        if (elements.loadMore) {
            elements.loadMore.hidden = !state.pagination.has_more;
        }
        updateStats();
        renderTelegramRelationships();
    }

    function getFilteredDocuments() {
        return state.documents.filter((document) => {
            const activeTask = state.tasks.get(document.id);
            const currentStatus = activeTask?.status && !["completed", "failed"].includes(activeTask.status)
                ? "processing"
                : document.ingest_status;
            if (state.filter === "ready") {
                return currentStatus === "ready";
            }
            if (state.filter === "active") {
                return ["queued", "pending", "processing"].includes(currentStatus);
            }
            if (state.filter === "failed") {
                return currentStatus === "failed";
            }
            return true;
        });
    }

    function renderDocumentCard(record) {
        const task = state.tasks.get(record.id);
        const progress = task ? clampPercent(task.progress_percent) : 0;
        const status = record.ingest_status || "pending";
        const statusLabel = statusToLabel(status);
        const actionLabel = status === "failed"
            ? (state.ui.modal_retry_document || "重新处理")
            : (state.ui.modal_process_document || "处理此文档");

        let taskStageLabel = task ? escapeHtml(stageToLabel(task.status)) : "";
        if (task && task.status === "embedding" && task.stages) {
            const processed = task.stages.embedding_processed || 0;
            const total = task.stages.embedding_total || 0;
            if (total > 0) {
                taskStageLabel = `索引中 ${processed}/${total} chunks`;
            }
        }

        const node = document.createElement("article");
        node.className = "document-card";
        node.innerHTML = `
            <div class="project-card__title-row">
                <strong>${escapeHtml(record.title || record.filename)}</strong>
                <span class="status-chip ${statusToTone(status)}">${escapeHtml(statusLabel)}</span>
            </div>
            <div class="document-card__meta">
                <span>${escapeHtml(record.filename || "")}</span>
                <span>${escapeHtml(record.source_type || "未设置来源")}</span>
                <span>${escapeHtml(formatDateTime(record.updated_at || record.created_at))}</span>
            </div>
            ${task ? `
                <div class="progress-shell">
                    <div class="progress-labels">
                        <strong>${taskStageLabel}</strong>
                        <span>${progress}%</span>
                    </div>
                    <div class="progress-track"><div class="progress-fill" style="width:${progress}%"></div></div>
                </div>
            ` : ""}
            ${record.error_message ? `<p class="helper-text">${escapeHtml(record.error_message)}</p>` : ""}
            <div class="document-card__footer top-gap">
                <button type="button" class="ghost-button" data-action="details">${escapeHtml(state.ui.document_open || "查看详情")}</button>
                <button type="button" class="ghost-button" data-action="process">${escapeHtml(actionLabel)}</button>
            </div>
        `;

        node.querySelector('[data-action="details"]')?.addEventListener("click", () => openDocumentModal(record));
        node.querySelector('[data-action="process"]')?.addEventListener("click", () => processDocument(record.id));
        return node;
    }

    function openDocumentModal(documentItem) {
        const current = state.documents.find((item) => item.id === documentItem.id) || documentItem;
        elements.modalTitle.textContent = current.title || current.filename || state.ui.modal_document_title || "文档详情";
        const userNote = current.metadata_json?.user_note || "";
        const stoneProfile = current.metadata_json?.stone_profile_v3 || null;

        elements.modalBody.innerHTML = `
            <label>
                <span>${escapeHtml(state.ui.document_filename || "文件名")}</span>
                <input type="text" value="${escapeHtml(current.filename || "")}" disabled>
            </label>
            <label>
                <span>标题</span>
                <input type="text" id="document-title-input" value="${escapeHtml(current.title || current.filename || "")}">
            </label>
            <label>
                <span>${escapeHtml(state.ui.document_source_type || "来源类型")}</span>
                <input type="text" id="document-source-input" value="${escapeHtml(current.source_type || "")}">
            </label>
            <label>
                <span>${escapeHtml(state.ui.document_note || "备注")}</span>
                <textarea id="document-note-input" rows="4">${escapeHtml(userNote)}</textarea>
            </label>
            <div class="metric-list">
                <div class="metric-row"><span>${escapeHtml(state.ui.document_status || "状态")}</span><strong>${escapeHtml(statusToLabel(current.ingest_status))}</strong></div>
                <div class="metric-row"><span>${escapeHtml(state.ui.document_language || "语言")}</span><strong>${escapeHtml(current.language || "--")}</strong></div>
                <div class="metric-row"><span>${escapeHtml(state.ui.common.updated_at || "更新时间")}</span><strong>${escapeHtml(formatDateTime(current.updated_at || current.created_at))}</strong></div>
            </div>
            ${stoneProfile ? renderStoneProfile(stoneProfile) : ""}
            ${current.error_message ? `<div class="empty-panel"><strong>${escapeHtml(state.ui.document_error || "错误信息")}</strong><p>${escapeHtml(current.error_message)}</p></div>` : ""}
        `;

        elements.modalFooter.innerHTML = "";

        const saveButton = document.createElement("button");
        saveButton.type = "button";
        saveButton.className = "primary-button";
        saveButton.textContent = state.ui.modal_save_document || "保存文档信息";
        saveButton.addEventListener("click", async () => {
            setButtonBusy(saveButton, true);
            try {
                const payload = await fetchJson(`/api/projects/${state.projectId}/documents/${current.id}`, {
                    method: "POST",
                    body: JSON.stringify({
                        title: elements.modalBody.querySelector("#document-title-input")?.value || current.title,
                        source_type: elements.modalBody.querySelector("#document-source-input")?.value || "",
                        user_note: elements.modalBody.querySelector("#document-note-input")?.value || "",
                    }),
                });
                mergeDocuments([payload]);
                render();
                showFeedback(state.ui.document_save_success || "文档信息已保存。", "success");
                closeModal(elements.modal);
            } catch (error) {
                showFeedback(error.message, "error");
            } finally {
                setButtonBusy(saveButton, false);
            }
        });

        const processButton = document.createElement("button");
        processButton.type = "button";
        processButton.className = "ghost-button";
        processButton.textContent = current.ingest_status === "failed"
            ? (state.ui.modal_retry_document || "重新处理")
            : (state.ui.modal_process_document || "处理此文档");
        processButton.addEventListener("click", () => processDocument(current.id));

        const deleteButton = document.createElement("button");
        deleteButton.type = "button";
        deleteButton.className = "ghost-button danger-button";
        deleteButton.textContent = state.ui.document_delete || "删除文档";
        deleteButton.addEventListener("click", async () => {
            if (!window.confirm(state.ui.common?.confirm_delete_document || "确定删除这份文档吗？")) {
                return;
            }
            setButtonBusy(deleteButton, true);
            try {
                await fetchJson(`/api/projects/${state.projectId}/documents/${current.id}/delete`, { method: "POST" });
                state.documents = state.documents.filter((item) => item.id !== current.id);
                state.tasks.delete(current.id);
                await refreshDocuments();
                showFeedback(state.ui.document_delete_success || "文档已删除。", "success");
                closeModal(elements.modal);
            } catch (error) {
                showFeedback(error.message, "error");
            } finally {
                setButtonBusy(deleteButton, false);
            }
        });

        elements.modalFooter.append(processButton, deleteButton, saveButton);
        openModal(elements.modal);
    }

    async function createTextDocument() {
        const title = elements.addTextTitle?.value?.trim() || "";
        const content = elements.addTextContent?.value?.trim() || "";
        const sourceType = elements.addTextSource?.value?.trim() || "";
        const userNote = elements.addTextNote?.value?.trim() || "";
        if (!content) {
            showFeedback("正文不能为空。", "error");
            return;
        }
        setButtonBusy(elements.addTextSubmit, true);
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/documents/text`, {
                method: "POST",
                body: JSON.stringify({
                    title,
                    content,
                    source_type: sourceType,
                    user_note: userNote,
                }),
            });
            mergeDocuments([payload]);
            if (payload.task?.document_id) {
                state.tasks.set(payload.task.document_id, payload.task);
            }
            await refreshDocuments();
            if (elements.addTextTitle) {
                elements.addTextTitle.value = "";
            }
            if (elements.addTextSource) {
                elements.addTextSource.value = "";
            }
            if (elements.addTextNote) {
                elements.addTextNote.value = "";
            }
            if (elements.addTextContent) {
                elements.addTextContent.value = "";
            }
            closeModal(elements.addTextModal);
            showFeedback("文章已加入处理队列。", "success");
        } catch (error) {
            showFeedback(error.message, "error");
        } finally {
            setButtonBusy(elements.addTextSubmit, false);
        }
    }

    async function processDocument(documentId) {
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/documents/${documentId}/process`, {
                method: "POST",
            });
            if (payload.task?.document_id) {
                state.tasks.set(payload.task.document_id, payload.task);
            }
            const documentItem = state.documents.find((item) => item.id === documentId);
            if (documentItem) {
                documentItem.ingest_status = "queued";
            }
            render();
            showFeedback(state.ui.process_success || "任务已加入处理队列。", "success");
        } catch (error) {
            showFeedback(error.message, "error");
        }
    }

    function updateStats() {
        elements.statsTotal.textContent = state.stats.document_count ?? state.documents.length;
        elements.statsReady.textContent = state.stats.ready_count ?? 0;
        elements.statsPending.textContent = (state.stats.pending_count || 0) + (state.stats.queued_count || 0) + (state.stats.processing_count || 0);
        elements.statsFailed.textContent = state.stats.failed_count ?? 0;
        if (elements.analyzeSubmit) {
            elements.analyzeSubmit.disabled = Number(state.stats.ready_count || 0) <= 0;
        }
    }

    function initPersonaCollapsibles() {
        const cards = [...document.querySelectorAll("[data-persona-card]")];
        cards.forEach((card) => {
            const toggle = card.querySelector("[data-persona-toggle]");
            toggle?.addEventListener("click", () => {
                const isExpanded = card.classList.contains("is-expanded");
                // Optional: close others
                // cards.forEach(c => c.classList.remove('is-expanded'));
                card.classList.toggle("is-expanded", !isExpanded);
            });
        });
    }

    const scheduleRender = throttle(() => {
        window.requestAnimationFrame(() => {
            render();
        });
    }, 100);

    function connectSocket() {
        const protocol = window.location.protocol === "https:" ? "wss" : "ws";
        state.socket = new WebSocket(`${protocol}://${window.location.host}/api/projects/${state.projectId}/documents/ws`);

        state.socket.addEventListener("message", (event) => {
            const payload = JSON.parse(event.data || "{}");
            (payload.documents || []).forEach((item) => {
                const current = state.documents.find((documentItem) => documentItem.id === item.id);
                if (current) {
                    current.ingest_status = item.ingest_status || current.ingest_status;
                }
                if (item.task) {
                    state.tasks.set(item.id, item.task);
                } else {
                    state.tasks.delete(item.id);
                }
            });
            refreshStatsFromDocuments();
            scheduleRender();
        });

        state.socket.addEventListener("close", () => {
            state.reconnectTimer = window.setTimeout(connectSocket, 1600);
        });
    }

    function refreshStatsFromDocuments() {
        const totals = { ready: 0, failed: 0, pending: 0, queued: 0, processing: 0 };
        state.documents.forEach((item) => {
            const status = item.ingest_status || "pending";
            if (status === "ready") {
                totals.ready += 1;
            } else if (status === "failed") {
                totals.failed += 1;
            } else if (status === "queued") {
                totals.queued += 1;
            } else if (status === "processing") {
                totals.processing += 1;
            } else {
                totals.pending += 1;
            }
        });
        state.stats.ready_count = totals.ready;
        state.stats.failed_count = totals.failed;
        state.stats.pending_count = totals.pending;
        state.stats.queued_count = totals.queued;
        state.stats.processing_count = totals.processing;
        state.stats.document_count = Math.max(state.stats.document_count || 0, state.documents.length);
    }

    function renderStoneProfile(profile) {
        const voiceMask = profile?.voice_mask || {};
        const stance = profile?.stance_vector || {};
        const anchors = profile?.anchor_spans || {};
        const lexicon = Array.isArray(profile?.lexicon_markers) ? profile.lexicon_markers : [];
        const motifs = Array.isArray(profile?.motif_tags) ? profile.motif_tags : [];
        const rhetoric = Array.isArray(profile?.rhetorical_devices) ? profile.rhetorical_devices : [];
        const signature = Array.isArray(anchors?.signature) ? anchors.signature.filter(Boolean) : [];
        const tagRows = [
            profile?.surface_form,
            profile?.length_band,
            voiceMask?.distance,
            stance?.judgment,
            stance?.value_lens,
        ].filter(Boolean);

        return `
            <div class="empty-panel">
                <strong>Stone v3 文章画像</strong>
                ${tagRows.length ? `<p>${escapeHtml(tagRows.join(" · "))}</p>` : ""}
                <p>语义核：${escapeHtml(profile?.content_kernel || "--")}</p>
                <p>起笔动作：${escapeHtml(profile?.opening_move || "--")}</p>
                <p>收口动作：${escapeHtml(profile?.closure_move || "--")}</p>
                <p>判断镜头：${escapeHtml(stance?.target || "--")} / ${escapeHtml(stance?.judgment || "--")} / ${escapeHtml(stance?.value_lens || "--")}</p>
                ${lexicon.length ? `<p>词汇标记：${escapeHtml(lexicon.join("、"))}</p>` : ""}
                ${motifs.length ? `<p>母题标签：${escapeHtml(motifs.join("、"))}</p>` : ""}
                ${rhetoric.length ? `<p>修辞动作：${escapeHtml(rhetoric.join("、"))}</p>` : ""}
                ${signature.length ? `<p>锚点句：${escapeHtml(signature.join(" / "))}</p>` : ""}
            </div>
        `;
    }

    function initTelegramTargetPickers() {
        elements.targetPickers.forEach((root) => {
            const cardButtons = [...root.querySelectorAll("[data-top-user-card]")];
            const participantInput = root.querySelector("[data-participant-input]");
            const queryInput = root.querySelector("[data-target-query-input]");
            const nameInput = root.querySelector("[data-persona-name-input]");
            const summary = root.querySelector("[data-target-summary]");
            const submit = root.querySelector("[data-target-submit]");
            const lockedDisabled = Boolean(submit?.disabled);
            let autofillValue = nameInput?.value?.trim() || "";
            let nameDirty = false;

            if (!participantInput || !queryInput) {
                return;
            }

            const setSummary = (label, button) => {
                if (!summary) {
                    return;
                }
                const username = button?.dataset.username || "";
                const uid = button?.dataset.uid || "";
                const meta = [username ? `@${username}` : "", uid].filter(Boolean).join(" · ");
                const chipLabel = button
                    ? (state.ui.telegram_picker_selected_chip || "Selected target")
                    : (state.ui.telegram_picker_manual_chip || "Manual search");
                const title = label || state.ui.telegram_picker_select_first_title || "Select a Telegram user first";
                const fallbackNote = button
                    ? (state.ui.telegram_picker_selected_note || "")
                    : (state.ui.telegram_picker_manual_note || "");
                summary.innerHTML = `
                    <span class="status-chip tone-${button ? "ready" : "processing"}">${escapeHtml(chipLabel)}</span>
                    <strong>${escapeHtml(title)}</strong>
                    <p>${escapeHtml(meta || fallbackNote)}</p>
                `;
            };

            const setAutoName = (label) => {
                if (!nameInput) {
                    return;
                }
                const current = nameInput.value.trim();
                if (nameDirty && current && current !== autofillValue) {
                    return;
                }
                autofillValue = label.trim();
                nameInput.value = autofillValue;
                nameInput.dataset.autofillValue = autofillValue;
            };

            const syncSubmit = () => {
                if (!submit) {
                    return;
                }
                const hasTarget = Boolean(participantInput.value.trim() || queryInput.value.trim());
                const hasName = !nameInput || Boolean(nameInput.value.trim());
                submit.disabled = lockedDisabled || !hasTarget || !hasName;
            };

            const clearSelection = () => {
                cardButtons.forEach((button) => button.classList.remove("is-selected"));
                participantInput.value = "";
            };

            const selectCard = (button) => {
                const label = button.dataset.label || "";
                cardButtons.forEach((item) => item.classList.toggle("is-selected", item === button));
                participantInput.value = button.dataset.participantId || "";
                queryInput.value = label;
                setAutoName(label);
                setSummary(label, button);
                syncSubmit();
            };

            cardButtons.forEach((button) => {
                button.addEventListener("click", () => selectCard(button));
            });

            queryInput.addEventListener("input", debounce(() => {
                const value = queryInput.value.trim();
                const selectedCard = cardButtons.find((button) => button.classList.contains("is-selected")) || null;
                if (selectedCard && value !== (selectedCard.dataset.label || "")) {
                    clearSelection();
                }
                if (value) {
                    if (!participantInput.value) {
                        setAutoName(value);
                    }
                    setSummary(value, participantInput.value ? selectedCard : null);
                } else if (!participantInput.value && summary) {
                    const pendingChip = state.ui.telegram_picker_pending_chip || "Pending";
                    const pendingTitle = state.ui.telegram_picker_select_first_title || "Select a Telegram user first";
                    const pendingNote = state.ui.telegram_picker_pending_note || "";
                    summary.innerHTML = `
                        <span class="status-chip tone-processing">${escapeHtml(pendingChip)}</span>
                        <strong>${escapeHtml(pendingTitle)}</strong>
                        <p>${escapeHtml(pendingNote)}</p>
                    `;
                }
                syncSubmit();
            }, 300));

            nameInput?.addEventListener("input", debounce(() => {
                const value = nameInput.value.trim();
                nameDirty = Boolean(value && value !== autofillValue);
                syncSubmit();
            }, 300));

            const initialCard = cardButtons.find(
                (button) => (button.dataset.participantId || "") === participantInput.value
            );
            if (initialCard) {
                selectCard(initialCard);
            } else if (queryInput.value.trim()) {
                setSummary(queryInput.value.trim(), null);
                setAutoName(queryInput.value.trim());
                syncSubmit();
            } else {
                syncSubmit();
            }
        });
    }

    function renderTelegramRelationships() {
        if (!elements.relationshipPanel) {
            return;
        }
        const bundle = state.telegram.relationships || null;
        const snapshot = bundle?.snapshot || null;
        const summary = snapshot?.summary || {};
        const users = Array.isArray(bundle?.users) ? bundle.users : [];
        const edges = Array.isArray(bundle?.edges) ? bundle.edges : [];

        setRelationshipStatus(snapshot?.status || "waiting");

        if (!snapshot) {
            if (elements.relationshipEmpty) {
                elements.relationshipEmpty.hidden = false;
            }
            if (elements.relationshipShell) {
                elements.relationshipShell.hidden = true;
            }
            return;
        }

        if (elements.relationshipEmpty) {
            elements.relationshipEmpty.hidden = true;
        }
        if (elements.relationshipShell) {
            elements.relationshipShell.hidden = false;
        }

        if (elements.relationshipUsers) {
            elements.relationshipUsers.textContent = String(snapshot.analyzed_user_count ?? users.length ?? 0);
        }
        if (elements.relationshipEdges) {
            elements.relationshipEdges.textContent = String(summary.edge_count ?? edges.length ?? 0);
        }
        if (elements.relationshipFriendly) {
            elements.relationshipFriendly.textContent = String(summary.friendly_count ?? 0);
        }
        if (elements.relationshipTense) {
            elements.relationshipTense.textContent = String(summary.tense_count ?? 0);
        }

        const friendlyEdges = edges
            .filter((edge) => edge.relation_label === "friendly")
            .sort(sortRelationshipEdges)
            .slice(0, 8);
        const tenseEdges = edges
            .filter((edge) => edge.relation_label === "tense")
            .sort(sortRelationshipEdges)
            .slice(0, 8);

        renderRelationshipCollection(
            elements.relationshipFriendlyList,
            friendlyEdges,
            {
                emptyText: state.ui.telegram_relationship_no_friendly || "No friendly ties yet.",
            }
        );
        renderRelationshipCollection(
            elements.relationshipTenseList,
            tenseEdges,
            {
                emptyText: state.ui.telegram_relationship_no_tense || "No tense ties yet.",
            }
        );

        if (!elements.relationshipMemberSelect) {
            return;
        }
        const previousSelection = elements.relationshipMemberSelect.value;
        const availableIds = users.map((user) => String(user.participant_id || ""));
        const selectedParticipantId = (
            previousSelection && availableIds.includes(previousSelection)
                ? previousSelection
                : (availableIds[0] || "")
        );

        elements.relationshipMemberSelect.innerHTML = users.map((user) => {
            const participantId = String(user.participant_id || "");
            const label = String(user.label || participantId);
            const messageCount = Number(user.message_count || 0);
            const selected = participantId === selectedParticipantId ? " selected" : "";
            return `<option value="${escapeHtml(participantId)}"${selected}>${escapeHtml(label)} · ${messageCount}</option>`;
        }).join("");

        const selectedUser = users.find((user) => String(user.participant_id || "") === selectedParticipantId) || null;
        renderTelegramRelationshipMember(selectedUser);
    }

    function renderTelegramRelationshipMember(user) {
        const relationGroups = {
            friendly: [],
            tense: [],
            neutral: [],
            unclear: [],
        };
        if (user && Array.isArray(user.relations)) {
            user.relations
                .slice()
                .sort(sortRelationshipEdges)
                .forEach((edge) => {
                    const label = normalizeRelationshipLabel(edge.relation_label);
                    relationGroups[label].push(edge);
                });
        }

        renderRelationshipCollection(
            elements.relationshipMemberAllies,
            relationGroups.friendly,
            {
                participantId: user?.participant_id || "",
                emptyText: state.ui.telegram_relationship_group_empty_allies || "No friendly ties.",
            }
        );
        renderRelationshipCollection(
            elements.relationshipMemberTense,
            relationGroups.tense,
            {
                participantId: user?.participant_id || "",
                emptyText: state.ui.telegram_relationship_group_empty_tense || "No tense ties.",
            }
        );
        renderRelationshipCollection(
            elements.relationshipMemberNeutral,
            relationGroups.neutral,
            {
                participantId: user?.participant_id || "",
                emptyText: state.ui.telegram_relationship_group_empty_neutral || "No neutral ties.",
            }
        );
        renderRelationshipCollection(
            elements.relationshipMemberUnclear,
            relationGroups.unclear,
            {
                participantId: user?.participant_id || "",
                emptyText: state.ui.telegram_relationship_group_empty_unclear || "No unclear ties.",
            }
        );
    }

    function renderRelationshipCollection(container, edges, options = {}) {
        if (!container) {
            return;
        }
        const participantId = String(options.participantId || "");
        if (!Array.isArray(edges) || !edges.length) {
            container.innerHTML = `<p class="telegram-relationship-list__empty">${escapeHtml(options.emptyText || "No relationship data.")}</p>`;
            return;
        }
        container.innerHTML = edges.map((edge) => renderRelationshipItem(edge, { participantId })).join("");
    }

    function renderRelationshipItem(edge, options = {}) {
        const participantId = String(options.participantId || "");
        const label = normalizeRelationshipLabel(edge.relation_label);
        const labelText = relationshipLabelText(label);
        const labelTone = relationshipLabelTone(label);
        const pairLabel = participantId
            ? relationshipCounterpartLabel(edge, participantId)
            : `${edge.participant_a_label || edge.participant_a_id} × ${edge.participant_b_label || edge.participant_b_id}`;
        const summary = String(edge.summary || "").trim() || (state.ui.telegram_relationship_rule_only || "Rule-based evidence only.");
        const metrics = [
            `${state.ui.telegram_relationship_strength || "Strength"} ${formatRelationshipNumber(edge.interaction_strength)}`,
            `${state.ui.telegram_relationship_confidence || "Confidence"} ${formatRelationshipNumber(edge.confidence)}`,
        ];
        const details = renderRelationshipDetails(edge);

        return `
            <article class="telegram-relationship-item">
                <div class="telegram-relationship-item__head">
                    <strong>${escapeHtml(pairLabel)}</strong>
                    <span class="status-chip ${labelTone}">${escapeHtml(labelText)}</span>
                </div>
                <div class="telegram-relationship-item__meta">${escapeHtml(metrics.join(" · "))}</div>
                <p class="telegram-relationship-item__summary">${escapeHtml(summary)}</p>
                ${details}
            </article>
        `;
    }

    function renderRelationshipDetails(edge) {
        const metrics = edge.metrics || {};
        const supportingSignals = Array.isArray(metrics.supporting_signals) ? metrics.supporting_signals : [];
        const counterSignals = Array.isArray(metrics.counter_signals) ? metrics.counter_signals : [];
        const evidence = Array.isArray(edge.evidence) ? edge.evidence : [];
        const counterevidence = Array.isArray(edge.counterevidence) ? edge.counterevidence : [];
        const sections = [];

        if (supportingSignals.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_supporting_signals || "Support")}</span>
                    <div class="telegram-relationship-signal-row">
                        ${supportingSignals.map((item) => `<span class="telegram-relationship-signal">${escapeHtml(String(item || ""))}</span>`).join("")}
                    </div>
                </div>
            `);
        }
        if (counterSignals.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_counter_signals || "Counter-signals")}</span>
                    <div class="telegram-relationship-signal-row">
                        ${counterSignals.map((item) => `<span class="telegram-relationship-signal">${escapeHtml(String(item || ""))}</span>`).join("")}
                    </div>
                </div>
            `);
        }
        if (evidence.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_evidence || "Evidence")}</span>
                    <div class="telegram-relationship-evidence-stack">
                        ${evidence.map((item) => renderRelationshipEvidence(item)).join("")}
                    </div>
                </div>
            `);
        }
        if (counterevidence.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_counterevidence || "Counterevidence")}</span>
                    <div class="telegram-relationship-evidence-stack">
                        ${counterevidence.map((item) => renderRelationshipEvidence(item)).join("")}
                    </div>
                </div>
            `);
        }
        if (!sections.length) {
            return "";
        }
        return `
            <details class="telegram-relationship-item__details">
                <summary>${escapeHtml(state.ui.telegram_relationship_view_evidence || "View evidence")}</summary>
                <div class="telegram-relationship-item__details-body">
                    ${sections.join("")}
                </div>
            </details>
        `;
    }

    function renderRelationshipEvidence(item) {
        if ((item?.kind || "") === "reply_context") {
            const summary = String(item.summary || state.ui.telegram_relationship_reply_chain || "Reply chain");
            const messages = Array.isArray(item.messages) ? item.messages : [];
            return `
                <article class="telegram-relationship-evidence">
                    <strong>${escapeHtml(summary)}</strong>
                    <div class="telegram-relationship-evidence__stack">
                        ${messages.map((message) => {
                            const sender = String(message.sender_name || message.participant_id || "Unknown");
                            const text = String(message.text || "");
                            return `
                                <div class="telegram-relationship-evidence__message">
                                    <span>${escapeHtml(sender)}</span>
                                    <p>${escapeHtml(text)}</p>
                                </div>
                            `;
                        }).join("")}
                    </div>
                </article>
            `;
        }

        const title = String(item?.title || item?.week_key || state.ui.telegram_relationship_shared_topic || "Shared topic");
        const summary = String(item?.summary || "");
        const patterns = Array.isArray(item?.interaction_patterns) ? item.interaction_patterns : [];
        const stanceParts = [
            item?.participant_a_stance ? `${state.ui.telegram_relationship_participant_a || "A"}: ${item.participant_a_stance}` : "",
            item?.participant_b_stance ? `${state.ui.telegram_relationship_participant_b || "B"}: ${item.participant_b_stance}` : "",
        ].filter(Boolean);
        const quotes = Array.isArray(item?.quotes) ? item.quotes : [];

        return `
            <article class="telegram-relationship-evidence">
                <strong>${escapeHtml(title)}</strong>
                ${summary ? `<p>${escapeHtml(summary)}</p>` : ""}
                ${patterns.length ? `<div class="telegram-relationship-evidence__meta">${escapeHtml(patterns.join(" · "))}</div>` : ""}
                ${stanceParts.length ? `<div class="telegram-relationship-evidence__meta">${escapeHtml(stanceParts.join(" · "))}</div>` : ""}
                ${quotes.length ? `
                    <div class="telegram-relationship-evidence__stack">
                        ${quotes.map((quote) => {
                            const label = String(quote.display_name || quote.participant_id || "Member");
                            const text = String(quote.quote || "");
                            return `
                                <div class="telegram-relationship-evidence__message">
                                    <span>${escapeHtml(label)}</span>
                                    <p>${escapeHtml(text)}</p>
                                </div>
                            `;
                        }).join("")}
                    </div>
                ` : ""}
            </article>
        `;
    }

    function relationshipCounterpartLabel(edge, participantId) {
        const isA = String(edge.participant_a_id || "") === String(participantId || "");
        return isA
            ? (edge.participant_b_label || edge.participant_b_id || "")
            : (edge.participant_a_label || edge.participant_a_id || "");
    }

    function setRelationshipStatus(status) {
        if (!elements.relationshipStatus) {
            return;
        }
        const normalizedStatus = String(status || "waiting").trim().toLowerCase();
        elements.relationshipStatus.className = `status-chip ${relationshipStatusTone(normalizedStatus)}`;
        elements.relationshipStatus.textContent = relationshipStatusLabel(normalizedStatus);
    }

    function relationshipStatusTone(status) {
        if (status === "completed") {
            return "tone-ready";
        }
        if (status === "partial") {
            return "tone-warning";
        }
        if (status === "failed") {
            return "tone-failed";
        }
        if (status === "running") {
            return "tone-processing";
        }
        return "tone-queued";
    }

    function relationshipStatusLabel(status) {
        const mapping = {
            waiting: state.ui.telegram_relationship_waiting || "waiting",
            running: state.ui.telegram_relationship_status_running || "running",
            completed: state.ui.telegram_relationship_status_completed || "completed",
            partial: state.ui.telegram_relationship_status_partial || "partial",
            failed: state.ui.telegram_relationship_status_failed || "failed",
        };
        return mapping[status] || status || "--";
    }

    function relationshipLabelTone(label) {
        if (label === "friendly") {
            return "tone-ready";
        }
        if (label === "tense") {
            return "tone-failed";
        }
        if (label === "neutral") {
            return "tone-queued";
        }
        return "tone-warning";
    }

    function relationshipLabelText(label) {
        const mapping = {
            friendly: state.ui.telegram_relationship_label_friendly || "Friendly",
            neutral: state.ui.telegram_relationship_label_neutral || "Neutral",
            tense: state.ui.telegram_relationship_label_tense || "Tense",
            unclear: state.ui.telegram_relationship_label_unclear || "Unclear",
        };
        return mapping[label] || label || "Unclear";
    }

    function normalizeRelationshipLabel(label) {
        const normalized = String(label || "").trim().toLowerCase();
        if (["friendly", "neutral", "tense", "unclear"].includes(normalized)) {
            return normalized;
        }
        return "unclear";
    }

    function sortRelationshipEdges(left, right) {
        const strengthDelta = Number(right?.interaction_strength || 0) - Number(left?.interaction_strength || 0);
        if (strengthDelta !== 0) {
            return strengthDelta;
        }
        return Number(right?.confidence || 0) - Number(left?.confidence || 0);
    }

    function formatRelationshipNumber(value) {
        const normalized = Number(value || 0);
        if (!Number.isFinite(normalized)) {
            return "0.00";
        }
        return normalized.toFixed(2);
    }

    function statusToLabel(status) {
        const mapping = {
            ready: state.ui.ready || "可分析",
            queued: state.ui.queued || "已排队",
            processing: state.ui.processing || "处理中",
            pending: state.ui.unprocessed || "未处理",
            failed: state.ui.failed || "失败",
        };
        return mapping[status] || status || "--";
    }

    function statusToTone(status) {
        if (status === "ready") {
            return "tone-ready";
        }
        if (status === "failed") {
            return "tone-failed";
        }
        if (status === "queued") {
            return "tone-queued";
        }
        return "tone-processing";
    }

    function stageToLabel(stage) {
        const mapping = {
            queued: state.ui.ingest_stage_queued || "Queued",
            parsing: state.ui.ingest_stage_parsing || "Parsing",
            chunking: state.ui.ingest_stage_chunking || "Chunking",
            embedding: state.ui.ingest_stage_embedding || "Embedding",
            storing: state.ui.ingest_stage_storing || "Storing",
            completed: state.ui.ingest_stage_completed || "Completed",
            failed: state.ui.ingest_stage_failed || "Failed",
            retrying: state.ui.common?.retry || "Retrying",
        };
        return mapping[stage] || stage || "--";
    }

    function showFeedback(message, tone = "info") {
        // Show feedback notice
        showNotice(elements.feedback, message, tone);
    }
}
