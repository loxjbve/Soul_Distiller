import {
    clampPercent,
    escapeHtml,
    fetchJson,
    safeParseJson,
    setStatusTone,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("stone-preprocess-bootstrap")?.textContent, {});

if (bootstrap?.project_id) {
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id || bootstrap.initial_run?.id || "",
        bundle: bootstrap.initial_run || null,
        documents: Array.isArray(bootstrap.initial_documents) ? bootstrap.initial_documents : [],
        stream: null,
        pollTimer: null,
        hoverDocId: "",
        hoverPointer: null,
    };

    const elements = {
        bannerStatusText: document.getElementById("stone-banner-status-text"),
        bannerStageText: document.getElementById("stone-banner-stage-text"),
        bannerProgressText: document.getElementById("stone-banner-progress-text"),
        bannerInputTokens: document.getElementById("stone-banner-input-tokens"),
        bannerOutputTokens: document.getElementById("stone-banner-output-tokens"),
        bannerModel: document.getElementById("stone-banner-model"),
        statusChip: document.getElementById("stone-preprocess-status-chip"),
        stageBanner: document.getElementById("stone-preprocess-stage-banner"),
        liveNote: document.getElementById("stone-preprocess-live-note"),
        livePill: document.getElementById("stone-preprocess-live-pill"),
        currentTopicLabel: document.getElementById("stone-preprocess-current-topic-label"),
        currentTopicProgress: document.getElementById("stone-preprocess-current-topic-progress"),
        topicBoardNote: document.getElementById("stone-preprocess-topic-board-note"),
        progressLabel: document.getElementById("stone-preprocess-progress-label"),
        progressFill: document.getElementById("stone-preprocess-progress-fill"),
        inputTokenChip: document.getElementById("stone-preprocess-input-token-chip"),
        outputTokenChip: document.getElementById("stone-preprocess-output-token-chip"),
        totalTokenChip: document.getElementById("stone-preprocess-total-token-chip"),
        docsCompleted: document.getElementById("stone-preprocess-docs-completed"),
        docsRemaining: document.getElementById("stone-preprocess-docs-remaining"),
        concurrency: document.getElementById("stone-preprocess-concurrency"),
        topicLamps: document.getElementById("stone-preprocess-topic-lamps"),
        error: document.getElementById("stone-preprocess-error"),
        hovercard: document.getElementById("stone-preprocess-hovercard"),
        hovercardTitle: document.getElementById("stone-hovercard-title"),
        hovercardStatus: document.getElementById("stone-hovercard-status"),
        hovercardBody: document.getElementById("stone-hovercard-body"),
        startButton: document.querySelector(".stone-preprocess-start-form__submit"),
        concurrencyInput: document.querySelector('.stone-preprocess-start-form input[name="concurrency"]'),
    };

    bindEvents();
    renderBundle(state.bundle);
    setLiveState(state.bundle && isRunning(state.bundle.status) ? "live" : "idle");
    connectStream();

    function bindEvents() {
        elements.topicLamps?.addEventListener("mouseover", (event) => {
            const card = findLampCard(event.target);
            if (!(card instanceof HTMLElement)) {
                return;
            }
            if (card.dataset.docId !== state.hoverDocId) {
                showHovercardForCard(card, event);
                return;
            }
            updateHovercardPosition(event.clientX, event.clientY);
        });

        elements.topicLamps?.addEventListener("mousemove", (event) => {
            const card = findLampCard(event.target);
            if (!(card instanceof HTMLElement)) {
                return;
            }
            if (card.dataset.docId === state.hoverDocId && elements.hovercard && !elements.hovercard.hidden) {
                updateHovercardPosition(event.clientX, event.clientY);
                return;
            }
            showHovercardForCard(card, event);
        });

        elements.topicLamps?.addEventListener("mouseout", (event) => {
            const currentCard = findLampCard(event.target);
            if (!(currentCard instanceof HTMLElement)) {
                return;
            }
            const nextCard = findLampCard(event.relatedTarget);
            if (nextCard === currentCard) {
                return;
            }
            hideHovercard();
        });

        elements.topicLamps?.addEventListener("focusin", (event) => {
            const card = findLampCard(event.target);
            if (!(card instanceof HTMLElement)) {
                return;
            }
            showHovercardForCard(card, null, true);
        });

        elements.topicLamps?.addEventListener("focusout", (event) => {
            const nextCard = findLampCard(event.relatedTarget);
            if (!nextCard) {
                hideHovercard();
            }
        });

        window.addEventListener("resize", () => {
            if (!elements.hovercard?.hidden && state.hoverPointer) {
                updateHovercardPosition(state.hoverPointer.x, state.hoverPointer.y);
            }
        });

        window.addEventListener(
            "scroll",
            () => {
                hideHovercard();
            },
            { passive: true }
        );
    }

    function connectStream() {
        if (!state.runId || (state.bundle && !isRunning(state.bundle.status))) {
            return;
        }
        stopPolling();
        stopStream();
        setLiveState("connecting");
        state.stream = new EventSource(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}/stream`);

        state.stream.addEventListener("snapshot", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            state.runId = payload.id || state.runId;
            state.bundle = payload;
            renderBundle(payload);
            if (isRunning(payload.status)) {
                setLiveState("live");
                return;
            }
            stopStream();
            stopPolling();
            setLiveState("idle");
        });

        state.stream.addEventListener("done", async () => {
            stopStream();
            stopPolling();
            await refreshBundle();
            setLiveState("idle");
        });

        state.stream.onerror = () => {
            stopStream();
            beginPolling();
        };
    }

    function stopStream() {
        if (state.stream) {
            state.stream.close();
            state.stream = null;
        }
    }

    function beginPolling() {
        if (state.pollTimer || !state.runId) {
            return;
        }
        setLiveState("polling");
        state.pollTimer = window.setInterval(async () => {
            await refreshBundle();
            if (!isRunning(state.bundle?.status)) {
                stopPolling();
                setLiveState("idle");
            }
        }, 2000);
    }

    function stopPolling() {
        if (!state.pollTimer) {
            return;
        }
        window.clearInterval(state.pollTimer);
        state.pollTimer = null;
    }

    async function refreshBundle() {
        if (!state.runId) {
            return;
        }
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}`);
            state.bundle = payload;
            renderBundle(payload);
        } catch (error) {
            updateText(elements.liveNote, error.message || "预分析状态刷新失败。");
        }
    }

    function renderBundle(bundle) {
        const documents = Array.isArray(bundle?.documents) ? bundle.documents : state.documents;
        state.documents = documents;

        const completed = Number(bundle?.stone_profile_completed ?? countCompletedDocuments(documents));
        const total = Number(bundle?.stone_profile_total ?? documents.length ?? 0);
        const remaining = Math.max(total - completed, 0);
        const percent = clampPercent(bundle?.progress_percent ?? (total ? (completed / total) * 100 : 0));
        const status = String(bundle?.status || "idle");
        const statusLabel = buildStatusLabel(status);
        const stageText = bundle?.current_stage || "等待运行";

        setStatusTone(elements.statusChip, status, statusLabel);
        updateText(elements.bannerStatusText, statusLabel);
        updateText(elements.bannerStageText, stageText);
        updateText(elements.bannerProgressText, `${completed}/${Math.max(total, documents.length)}`);
        updateText(elements.bannerInputTokens, String(bundle?.prompt_tokens || 0));
        updateText(elements.bannerOutputTokens, `输出 ${bundle?.completion_tokens || 0}`);
        updateText(elements.bannerModel, bundle?.llm_model || "Heuristic / LLM");

        updateText(elements.stageBanner, stageText);
        updateText(elements.liveNote, buildLiveNote(bundle, documents));
        updateText(elements.currentTopicLabel, "文章画像进度");
        updateText(elements.currentTopicProgress, buildProgressText(completed, total, documents.length, status));
        updateText(elements.topicBoardNote, buildBoardNote(completed, total, documents.length, status));
        updateText(elements.progressLabel, `${percent}%`);
        updateText(elements.inputTokenChip, `Input ${bundle?.prompt_tokens || 0}`);
        updateText(elements.outputTokenChip, `Output ${bundle?.completion_tokens || 0}`);
        updateText(elements.totalTokenChip, `Total ${bundle?.total_tokens || 0}`);
        updateText(elements.docsCompleted, String(completed));
        updateText(elements.docsRemaining, String(remaining));
        updateText(elements.concurrency, String(bundle?.concurrency || elements.concurrencyInput?.value || 1));

        if (elements.progressFill) {
            elements.progressFill.style.width = `${percent}%`;
        }

        if (elements.startButton) {
            elements.startButton.disabled = isRunning(status) || documents.length === 0;
            elements.startButton.textContent = isRunning(status) ? "正在预分析..." : "开始预分析";
        }

        if (elements.concurrencyInput && bundle?.concurrency) {
            elements.concurrencyInput.value = String(bundle.concurrency);
        }

        if (elements.error) {
            elements.error.hidden = !bundle?.error_message;
            elements.error.textContent = bundle?.error_message || "";
        }

        renderDocuments(documents);

        if (state.hoverDocId) {
            const doc = getDocumentById(state.hoverDocId);
            if (!doc) {
                hideHovercard();
            } else if (elements.hovercard && !elements.hovercard.hidden) {
                renderHovercard(doc);
            }
        }
    }

    function renderDocuments(documents) {
        if (!elements.topicLamps) {
            return;
        }
        if (!documents.length) {
            elements.topicLamps.innerHTML = `
                <div class="empty-panel stone-empty-panel">
                    <strong>尚未上传文章。</strong>
                    <p>先上传 Stone 文本或 JSON 文章，然后再运行逐篇预分析。</p>
                </div>
            `;
            hideHovercard();
            return;
        }
        elements.topicLamps.innerHTML = documents.map((doc) => renderDocumentCard(doc)).join("");
    }

    function renderDocumentCard(doc) {
        const profile = doc.stone_profile_v3 || {};
        const voiceMask = profile.voice_mask || {};
        const stance = profile.stance_vector || {};
        const status = String(doc.lamp_status || deriveLampStatus(doc));
        const metaTags = [
            profile.surface_form,
            profile.length_band,
            voiceMask.distance,
            stance.judgment,
        ].filter(Boolean);

        return `
            <article
                class="stone-agent-lamp status-${escapeHtml(status)}${status === "running" ? " is-live" : ""}"
                data-doc-id="${escapeHtml(doc.id || "")}"
                data-doc-index="${escapeHtml(String(doc.document_index || 0))}"
                data-has-profile="${doc.has_profile ? "true" : "false"}"
                tabindex="0"
            >
                <span class="stone-agent-lamp__dot" aria-hidden="true"></span>
                <div class="stone-agent-lamp__body">
                    <div class="stone-agent-lamp__head">
                        <strong>${escapeHtml(doc.title || doc.filename || `文章 ${doc.document_index || ""}`)}</strong>
                        <span class="stone-agent-lamp__index">${String(doc.document_index || 0).padStart(2, "0")}</span>
                    </div>
                    <small>${escapeHtml(doc.filename || "")}</small>
                    ${metaTags.length ? `<div class="stone-agent-lamp__meta">${metaTags.map((tag) => `<span class="stone-agent-lamp__tag">${escapeHtml(tag)}</span>`).join("")}</div>` : ""}
                    <p class="stone-agent-lamp__preview">${escapeHtml(buildDocumentPreview(doc))}</p>
                </div>
            </article>
        `;
    }

    function showHovercardForCard(card, pointerEvent = null, fromKeyboard = false) {
        const doc = getDocumentById(card.dataset.docId);
        if (!doc || !elements.hovercard) {
            return;
        }

        state.hoverDocId = String(doc.id || "");
        renderHovercard(doc);
        elements.hovercard.hidden = false;

        if (fromKeyboard) {
            const rect = card.getBoundingClientRect();
            updateHovercardPosition(rect.right - 28, rect.top + Math.min(rect.height, 48));
            return;
        }

        if (pointerEvent) {
            updateHovercardPosition(pointerEvent.clientX, pointerEvent.clientY);
        }
    }

    function renderHovercard(doc) {
        if (!elements.hovercardBody || !elements.hovercardTitle || !elements.hovercardStatus) {
            return;
        }
        const profile = doc.stone_profile_v3 || null;
        elements.hovercardTitle.textContent = doc.title || doc.filename || "文章画像";
        setStatusTone(elements.hovercardStatus, doc.lamp_status || deriveLampStatus(doc), buildStatusLabel(doc.lamp_status || deriveLampStatus(doc)));

        if (!profile) {
            elements.hovercardBody.innerHTML = `
                <div class="stone-hovercard__empty">
                    <strong>${escapeHtml(buildStatusLabel(doc.lamp_status || deriveLampStatus(doc)))}</strong>
                    <p>${escapeHtml(buildDocumentFallback(doc))}</p>
                </div>
            `;
            return;
        }

        const voiceMask = profile.voice_mask || {};
        const stance = profile.stance_vector || {};
        const syntax = profile.syntax_signature || {};
        const anchorSpans = profile.anchor_spans || {};
        elements.hovercardBody.innerHTML = `
            ${renderHovercardSection("语义核", profile.content_kernel || "暂无总结")}
            <div class="stone-hovercard__facts">
                ${renderFactCard("表层形态", profile.surface_form || "未标注")}
                ${renderFactCard("长度", profile.length_band || "short")}
                ${renderFactCard("套路族", profile.prototype_family || "未归类")}
            </div>
            <div class="stone-hovercard__facts">
                ${renderFactCard("距离", voiceMask.distance || "回收")}
                ${renderFactCard("判断", stance.judgment || "悬置")}
                ${renderFactCard("镜头", stance.value_lens || "代价")}
            </div>
            ${renderHovercardSection("起笔 / 收口", `${profile.opening_move || "未标注"}\n${profile.closure_move || "未标注"}`)}
            ${renderHovercardSection("句法签名", [syntax.cadence, syntax.sentence_shape, ...(syntax.punctuation_habits || [])].filter(Boolean).join(" / ") || "暂无")}
            <section class="stone-hovercard__section">
                <span class="stone-hovercard__label">母题 / 修辞</span>
                ${renderTagChips([...(profile.motif_tags || []), ...(profile.rhetorical_devices || [])])}
            </section>
            <section class="stone-hovercard__section">
                <span class="stone-hovercard__label">原文锚点</span>
                ${renderAnchorSpans(anchorSpans)}
            </section>
        `;
    }

    function renderHovercardSection(label, value) {
        return `
            <section class="stone-hovercard__section">
                <span class="stone-hovercard__label">${escapeHtml(label)}</span>
                <p class="stone-hovercard__value">${escapeWithBreaks(value)}</p>
            </section>
        `;
    }

    function renderFactCard(label, value) {
        return `
            <article class="stone-hovercard__fact">
                <span>${escapeHtml(label)}</span>
                <strong>${escapeHtml(value)}</strong>
            </article>
        `;
    }

    function renderPassages(passages) {
        if (!Array.isArray(passages) || !passages.length) {
            return `<div class="stone-hovercard__empty"><p>暂无精选段落。</p></div>`;
        }
        return `
            <div class="stone-hovercard__passages">
                ${passages.map((item) => `<article class="stone-hovercard__passage">${escapeWithBreaks(item)}</article>`).join("")}
            </div>
        `;
    }

    function renderTagChips(items) {
        const rows = Array.isArray(items)
            ? items.filter((item) => String(item || "").trim())
            : [];
        if (!rows.length) {
            return `<div class="stone-hovercard__empty"><p>暂无标签。</p></div>`;
        }
        return `
            <div class="stone-hovercard__facts">
                ${rows.slice(0, 8).map((item) => `<article class="stone-hovercard__fact"><strong>${escapeHtml(String(item))}</strong></article>`).join("")}
            </div>
        `;
    }

    function renderAnchorSpans(anchorSpans) {
        const signature = Array.isArray(anchorSpans?.signature) ? anchorSpans.signature.filter(Boolean) : [];
        const passages = [
            anchorSpans?.opening,
            anchorSpans?.pivot,
            anchorSpans?.closing,
            ...signature,
        ].filter((item, index, array) => {
            const value = String(item || "").trim();
            return value && array.findIndex((candidate) => String(candidate || "").trim() === value) === index;
        });
        return renderPassages(passages.slice(0, 4));
    }

    function updateHovercardPosition(clientX, clientY) {
        if (!elements.hovercard || elements.hovercard.hidden) {
            return;
        }
        state.hoverPointer = { x: clientX, y: clientY };
        const gap = 18;
        const padding = 16;
        const rect = elements.hovercard.getBoundingClientRect();
        const maxLeft = window.innerWidth - rect.width - padding;
        const maxTop = window.innerHeight - rect.height - padding;

        let left = clientX + gap;
        let top = clientY + gap;

        if (left > maxLeft) {
            left = Math.max(padding, clientX - rect.width - gap);
        }
        if (top > maxTop) {
            top = Math.max(padding, clientY - rect.height - gap);
        }

        elements.hovercard.style.left = `${left}px`;
        elements.hovercard.style.top = `${top}px`;
    }

    function hideHovercard() {
        state.hoverDocId = "";
        state.hoverPointer = null;
        if (!elements.hovercard) {
            return;
        }
        elements.hovercard.hidden = true;
    }

    function getDocumentById(documentId) {
        return state.documents.find((item) => String(item.id || "") === String(documentId || "")) || null;
    }

    function findLampCard(target) {
        return target instanceof HTMLElement ? target.closest(".stone-agent-lamp") : null;
    }

    function deriveLampStatus(doc) {
        if (doc.has_profile || doc.stone_profile_v3) {
            return "completed";
        }
        return "queued";
    }

    function buildDocumentPreview(doc) {
        const profile = doc.stone_profile_v3 || {};
        const anchors = profile.anchor_spans || {};
        return (
            doc.profile_preview
            || profile.content_kernel
            || anchors.opening
            || (Array.isArray(anchors.signature) ? anchors.signature[0] : "")
            || buildDocumentFallback(doc)
        );
    }

    function buildDocumentFallback(doc) {
        const status = String(doc.lamp_status || deriveLampStatus(doc));
        if (status === "running") {
            return "正在提取这篇文章的 Stone v3 画像...";
        }
        if (status === "failed") {
            return "当前未完成，可重新运行预分析。";
        }
        if (status === "completed") {
            return "Stone v3 画像已生成，悬浮即可查看。";
        }
        return "等待进入处理队列。";
    }

    function buildLiveNote(bundle, documents) {
        const status = String(bundle?.status || "idle");
        if (status === "running" || status === "queued") {
            return "系统正在逐篇生成 Stone v3 画像，完成后会立即同步到卡片，并支持悬浮预览。";
        }
        if (status === "completed") {
            return `本轮预处理已完成，共生成 ${countCompletedDocuments(documents)} 篇 Stone v3 画像；现在可以悬浮查看每篇文章的结果。`;
        }
        if (status === "failed") {
            return bundle?.error_message || "本轮预分析执行失败，可以修复后重新运行。";
        }
        return documents.length
            ? "运行后会为每篇文章生成 Stone v3 画像，并把结果写回文档元数据。"
            : "先上传文章，然后再启动逐篇预分析。";
    }

    function buildProgressText(completed, total, documentCount, status) {
        if (!total && !documentCount) {
            return "当前没有可处理的文章。";
        }
        if (!total && status === "idle") {
            return `待处理文章 ${documentCount}`;
        }
        return `已处理文章 ${completed}/${Math.max(total, documentCount)}`;
    }

    function buildBoardNote(completed, total, documentCount, status) {
        if (!documentCount) {
            return "等待文章进入工作区";
        }
        if (status === "failed") {
            return `已完成 ${completed}/${Math.max(total, documentCount)}，本轮中途失败`;
        }
        return `已处理文章 ${completed}/${Math.max(total, documentCount)}`;
    }

    function countCompletedDocuments(documents) {
        return documents.filter((item) => item?.has_profile || item?.stone_profile_v3).length;
    }

    function buildStatusLabel(status) {
        switch (String(status || "").toLowerCase()) {
            case "queued":
                return "已排队";
            case "running":
                return "进行中";
            case "completed":
                return "已完成";
            case "failed":
                return "失败";
            default:
                return "等待中";
        }
    }

    function setLiveState(status) {
        if (!elements.livePill) {
            return;
        }
        if (status === "live") {
            elements.livePill.textContent = "LIVE";
            elements.livePill.className = "analysis-live-pill is-live";
            return;
        }
        if (status === "connecting") {
            elements.livePill.textContent = "CONNECTING";
            elements.livePill.className = "analysis-live-pill tone-queued";
            return;
        }
        if (status === "polling") {
            elements.livePill.textContent = "SYNC";
            elements.livePill.className = "analysis-live-pill tone-queued";
            return;
        }
        elements.livePill.textContent = "WAITING";
        elements.livePill.className = "analysis-live-pill";
    }

    function isRunning(status) {
        return status === "queued" || status === "running";
    }

    function escapeWithBreaks(value) {
        return escapeHtml(String(value || "")).replaceAll("\n", "<br>");
    }

    buildLiveNote = function buildLiveNoteV3(bundle, documents) {
        const status = String(bundle?.status || "idle");
        if (status === "running" || status === "queued") {
            return "系统正在逐篇生成 Stone v3 画像，完成后会立即同步到卡片，并支持悬浮预览。";
        }
        if (status === "completed") {
            return `本轮预处理已完成，共生成 ${countCompletedDocuments(documents)} 篇 Stone v3 画像；现在可以悬浮查看每篇文章的结果。`;
        }
        if (status === "partial_failed") {
            return bundle?.error_message || `预处理已以可用状态结束，已有 ${countCompletedDocuments(documents)} 篇文章生成 Stone v3 画像，可以继续进行作者分析。`;
        }
        if (status === "failed") {
            return bundle?.error_message || "本轮预处理执行失败，可以修复后重新运行。";
        }
        return documents.length
            ? "运行后会为每篇文章生成 Stone v3 画像，并把结果写回文档元数据。"
            : "先上传文章，然后再启动逐篇预分析。";
    };

    buildBoardNote = function buildBoardNoteV3(completed, total, documentCount, status) {
        if (!documentCount) {
            return "等待文章进入工作区";
        }
        if (status === "failed") {
            return `已完成 ${completed}/${Math.max(total, documentCount)}，本轮中途失败`;
        }
        if (status === "partial_failed") {
            return `已处理文章 ${completed}/${Math.max(total, documentCount)}，已达到可分析阈值。`;
        }
        return `已处理文章 ${completed}/${Math.max(total, documentCount)}`;
    };

    buildStatusLabel = function buildStatusLabelV3(status) {
        switch (String(status || "").toLowerCase()) {
            case "queued":
                return "已排队";
            case "running":
                return "进行中";
            case "completed":
                return "已完成";
            case "partial_failed":
                return "部分完成";
            case "failed":
                return "失败";
            default:
                return "等待中";
        }
    };

    isRunning = function isRunningStoneV3(status) {
        return status === "queued" || status === "running";
    };
}
