import {
    clampPercent,
    closeModal,
    escapeHtml,
    fetchJson,
    formatDateTime,
    openModal,
    setButtonBusy,
    showNotice,
} from "./shared.js";

const bootstrap = JSON.parse(document.getElementById("project-bootstrap")?.textContent || "{}");

if (bootstrap.project?.id) {
    const state = {
        projectId: bootstrap.project.id,
        ui: bootstrap.ui_strings || {},
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
        feedback: document.getElementById("project-feedback"),
        statsTotal: document.getElementById("status-total"),
        statsReady: document.getElementById("status-ready"),
        statsPending: document.getElementById("status-pending"),
        statsFailed: document.getElementById("status-failed"),
        analyzeSubmit: document.getElementById("analyze-submit-btn"),
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
            await fetchJson(`/api/projects/${state.projectId}/documents`, {
                method: "POST",
                body: formData,
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
        documents.forEach((document) => elements.grid.appendChild(renderDocumentCard(document)));
        elements.empty.hidden = documents.length > 0;
        if (elements.loadMore) {
            elements.loadMore.hidden = !state.pagination.has_more;
        }
        updateStats();
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
                        <strong>${escapeHtml(stageToLabel(task.status))}</strong>
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
            render();
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
            queued: "排队中",
            parsing: "解析中",
            chunking: "切块中",
            embedding: "生成向量",
            storing: "写入索引",
            completed: "已完成",
            failed: "失败",
            retrying: "重试中",
        };
        return mapping[stage] || stage || "--";
    }

    function showFeedback(message, tone = "info") {
        showNotice(elements.feedback, message, tone);
    }
}
