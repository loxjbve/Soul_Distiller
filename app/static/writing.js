import {
    escapeHtml,
    fetchJson,
    createMiniCardController,
    renderMarkdownInto,
    safeParseJson,
    setButtonBusy,
    shouldAutoScroll,
} from "./shared.js";

const shell = document.querySelector("[data-writing-shell]");

if (shell) {
    const bootstrap = safeParseJson(document.getElementById("writing-bootstrap")?.textContent, {});
    const ui = bootstrap.ui_strings || {};

    const state = {
        projectId: shell.dataset.projectId,
        sessions: Array.isArray(bootstrap.sessions) ? bootstrap.sessions : [],
        currentSessionId: bootstrap.selected_session_id || null,
        currentSession: bootstrap.selected_session || { turns: [] },
        baseline: bootstrap.baseline || { status: "missing_analysis" },
        writingSettings: normalizeWritingSettings(bootstrap.writing_settings),
        usageSummary: null,
        settingsStatus: "idle",
        channelTitle: bootstrap.channel_title || "",
        eventSource: null,
        sending: false,
        turnElements: new Map(),
        emptyStateEl: null,
        selectedTurnKey: null,
        timelineTrackEl: null,
        timelineDetailEl: null,
        timelineTooltipEl: null,
        timelineController: null,
        sessionDetails: new Map(),
    };

    if (state.currentSessionId) {
        state.sessionDetails.set(state.currentSessionId, state.currentSession);
    }

    const elements = {
        sessionList: shell.querySelector("[data-session-list]"),
        sessionTitle: shell.querySelector("[data-session-title]"),
        channelTitle: shell.querySelector("[data-channel-title]"),
        baselinePill: shell.querySelector("[data-baseline-pill]"),
        baselineNote: shell.querySelector("[data-baseline-note]"),
        usageInput: shell.querySelector("[data-usage-input]"),
        usageOutput: shell.querySelector("[data-usage-output]"),
        usageTotal: shell.querySelector("[data-usage-total]"),
        concurrencyInput: shell.querySelector("[data-concurrency-input]"),
        concurrencyButtons: shell.querySelectorAll("[data-concurrency-step]"),
        settingsStatus: shell.querySelector("[data-settings-status]"),
        chatList: shell.querySelector("[data-chat-list]"),
        liveFeed: shell.querySelector("[data-live-feed]"),
        messageInput: shell.querySelector("[data-message-input]"),
        composerHint: shell.querySelector("[data-composer-hint]"),
        composerError: shell.querySelector("[data-composer-error]"),
        send: shell.querySelector("[data-send-message]"),
        newSession: shell.querySelector("[data-new-session]"),
        renameSession: shell.querySelector("[data-rename-session]"),
        deleteSession: shell.querySelector("[data-delete-session]"),
        toggleSessions: shell.querySelector("[data-toggle-sessions]"),
    };

    bindEvents();
    renderAll();

    function bindEvents() {
        elements.newSession?.addEventListener("click", async () => {
            const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions`, {
                method: "POST",
                body: JSON.stringify({}),
            });
            syncSessionSummary(payload, { promote: true });
            await loadSession(payload.id);
        });

        elements.renameSession?.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            const nextTitle = window.prompt(
                ui.rename_prompt || "输入新的会话标题",
                state.currentSession?.title || ""
            );
            if (nextTitle === null) {
                return;
            }
            const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}`, {
                method: "PATCH",
                body: JSON.stringify({ title: nextTitle }),
            });
            syncSessionSummary(payload);
            if (state.currentSession) {
                state.currentSession.title = payload.title;
                state.currentSession.has_custom_title = payload.has_custom_title ?? Boolean(payload.title);
            }
            renderSessions();
            renderHeader();
        });

        elements.deleteSession?.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            const confirmed = window.confirm(
                ui.common?.confirm_delete_session || "确定删除这个会话吗？"
            );
            if (!confirmed) {
                return;
            }
            await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}`, {
                method: "DELETE",
            });
            state.sessions = state.sessions.filter((item) => item.id !== state.currentSessionId);
            if (!state.sessions.length) {
                const created = await fetchJson(`/api/projects/${state.projectId}/writing/sessions`, {
                    method: "POST",
                    body: JSON.stringify({}),
                });
                syncSessionSummary(created, { promote: true });
                state.sessions = [created];
            }
            await loadSession(state.sessions[0].id);
        });

        elements.send?.addEventListener("click", () => {
            void sendMessage();
        });

        elements.messageInput?.addEventListener("keydown", (event) => {
            if (event.isComposing) {
                return;
            }
            if (event.key === "Enter") {
                event.preventDefault();
                void sendMessage();
            }
        });

        elements.messageInput?.addEventListener("input", () => {
            hideComposerError();
        });

        elements.toggleSessions?.addEventListener("click", () => {
            const next = shell.dataset.sessionsOpen !== "true";
            shell.dataset.sessionsOpen = String(next);
        });

        elements.concurrencyButtons?.forEach((button) => {
            button.addEventListener("click", () => {
                const delta = Number(button.dataset.concurrencyStep || 0);
                void saveWritingSettings((state.writingSettings?.max_concurrency || 4) + delta);
            });
        });

        elements.concurrencyInput?.addEventListener("change", () => {
            void saveWritingSettings(elements.concurrencyInput?.value);
        });
    }

    async function sendMessage() {
        const message = elements.messageInput?.value?.trim() || "";
        if (!message || !state.currentSessionId || state.sending) {
            return;
        }

        hideComposerError();
        closeStream();
        appendTurn({
            id: `local-user-${Date.now()}`,
            role: "user",
            content: message,
            actor_id: "user",
            actor_name: ui.you_label || "你",
            actor_role: "user",
            message_kind: "request",
            trace: {
                topic: message,
                raw_message: message,
            },
            created_at: new Date().toISOString(),
        });

        state.sending = true;
        setButtonBusy(elements.send, true, ui.sending || "写作中...");
        if (elements.messageInput) {
            elements.messageInput.disabled = true;
        }

        try {
            const payload = await fetchJson(
                `/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}/messages`,
                {
                    method: "POST",
                    body: JSON.stringify({
                        message,
                        max_concurrency: state.writingSettings?.max_concurrency || 4,
                    }),
                }
            );
            if (elements.messageInput) {
                elements.messageInput.value = "";
            }
            if (state.currentSession && payload.session_title) {
                state.currentSession.title = payload.session_title;
                state.currentSession.has_custom_title = payload.has_custom_title ?? true;
            }
            syncSessionSummary({
                id: state.currentSessionId,
                title: payload.session_title || state.currentSession?.title,
                has_custom_title: payload.has_custom_title ?? state.currentSession?.has_custom_title,
                created_at: state.currentSession?.created_at,
                last_active_at: new Date().toISOString(),
                timeline_turn_count: state.currentSession?.turns?.length || 1,
            }, { promote: true });
            renderSessions();
            renderHeader();
            openStream(payload.stream_id);
        } catch (error) {
            appendLocalError(error?.message || String(error));
            restoreComposer();
        }
    }

    function openStream(streamId) {
        const source = new EventSource(
            `/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}/streams/${streamId}`
        );
        state.eventSource = source;

        source.addEventListener("status", (event) => {
            const payload = safeParseJson(event.data, {});
            if (payload.stage !== "generation_packet") {
                return;
            }
            state.baseline.status =
                payload?.baseline_components?.status ||
                (payload?.baseline_components?.rebuild_required ? "requires_rebuild" : "ready");
            if (payload.baseline_components) {
                state.baseline.corpus_ready = Boolean(payload.baseline_components.corpus_ready);
                state.baseline.profile_count = Number(payload.baseline_components.profile_count || 0);
                state.baseline.analysis_ready = Boolean(payload.baseline_components.analysis_ready);
                state.baseline.writing_packet_ready = Boolean(payload.baseline_components.writing_packet_ready);
                state.baseline.author_model_ready = Boolean(payload.baseline_components.author_model_ready);
                state.baseline.prototype_index_ready = Boolean(payload.baseline_components.prototype_index_ready);
                state.baseline.rebuild_required = Boolean(payload.baseline_components.rebuild_required);
                state.baseline.profile_version = payload.baseline_components.profile_version || null;
                state.baseline.baseline_version = payload.baseline_components.baseline_version || null;
                state.baseline.source_anchor_count = Number(payload.baseline_components.source_anchor_count || 0);
            }
            renderBaseline();
        });

        source.addEventListener("stream_update", (event) => {
            const payload = safeParseJson(event.data, {});
            if (payload.usage_summary) {
                updateUsageSummary(payload.usage_summary);
            }
            upsertTurn(normalizeStreamTurn(payload));
        });

        source.addEventListener("stage", (event) => {
            const payload = safeParseJson(event.data, {});
            if (payload.usage_summary) {
                updateUsageSummary(payload.usage_summary);
            }
            upsertTurn(normalizeStreamTurn(payload));
        });

        source.addEventListener("done", (event) => {
            const payload = safeParseJson(event.data, {});
            if (payload.usage_summary) {
                updateUsageSummary(payload.usage_summary);
            }
            upsertTurn(normalizeStreamTurn(payload));
            syncSessionSummary({
                id: state.currentSessionId,
                title: state.currentSession?.title,
                created_at: state.currentSession?.created_at,
                last_active_at: payload.created_at || new Date().toISOString(),
                timeline_turn_count: state.currentSession?.turns?.length || 0,
            }, { promote: true });
            renderSessions();
            restoreComposer();
        });

        source.addEventListener("error", (event) => {
            if (!event.data) {
                return;
            }
            const payload = safeParseJson(event.data, {});
            appendLocalError(payload.message || ui.execution_failed || "写作失败");
            restoreComposer();
        });

        source.onerror = () => {
            if (state.eventSource !== source) {
                return;
            }
            closeStream();
            appendLocalError(ui.connection_interrupted || "连接中断");
            restoreComposer();
        };
    }

    async function loadSession(sessionId) {
        cacheCurrentSession();
        const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${sessionId}`);
        state.currentSessionId = sessionId;
        state.currentSession = mergeSessionDetail(sessionId, payload || { turns: [] });
        state.sessionDetails.set(sessionId, state.currentSession);
        state.selectedTurnKey = null;
        syncCurrentUsageSummary();
        syncSessionSummary(payload);
        shell.dataset.sessionsOpen = "false";
        restoreComposer();
        renderAll();
        scrollToBottom(true);
    }

    function renderAll() {
        syncCurrentUsageSummary();
        renderHeader();
        renderBaseline();
        renderUsageSummary();
        renderWritingSettings();
        renderSessions();
        renderChat();
    }

    function renderHeader() {
        if (elements.channelTitle) {
            elements.channelTitle.textContent = state.channelTitle || "";
        }
        if (elements.sessionTitle) {
            const sessionTitle = state.currentSession?.has_custom_title
                ? (state.currentSession?.title || ui.untitled_session || "未命名会话")
                : (ui.untitled_session || "未命名会话");
            elements.sessionTitle.textContent = sessionTitle;
            elements.sessionTitle.classList.toggle("is-pending", !state.currentSession?.has_custom_title);
        }
    }

    function renderBaseline() {
        const label = resolveBaselineLabel(state.baseline);
        if (elements.baselinePill) {
            elements.baselinePill.textContent = label;
        }
        if (elements.baselineNote) {
            const components = [];
            const profileCount = Number(state.baseline?.profile_count || 0);
            const sourceAnchorCount = Number(state.baseline?.source_anchor_count || 0);
            if (state.baseline?.corpus_ready) {
                components.push(profileCount > 0 ? `逐篇画像 ${profileCount}` : "逐篇画像");
            }
            if (state.baseline?.author_model_ready) {
                components.push("作者模型");
            }
            if (state.baseline?.prototype_index_ready) {
                components.push("原型索引");
            }
            const stats = [];
            if (profileCount > 0) {
                stats.push(`${profileCount} 篇画像`);
            }
            if (sourceAnchorCount > 0) {
                stats.push(`${sourceAnchorCount} 个锚点`);
            }
            const componentLabel = components.length ? ` ${components.join(" + ")}` : "";
            const statsLabel = stats.length ? ` (${stats.join(" / ")})` : "";
            elements.baselineNote.textContent =
                `${ui.hero_note || ""}${componentLabel}${statsLabel}`.trim() || label;
        }
    }

    function normalizeWritingSettings(payload) {
        const raw = payload && typeof payload === "object" ? payload : {};
        const next = Number(raw.max_concurrency || 4);
        return {
            max_concurrency: Math.max(1, Math.min(8, Number.isFinite(next) ? next : 4)),
        };
    }

    function updateUsageSummary(summary) {
        state.usageSummary = summary && typeof summary === "object" ? summary : null;
        renderUsageSummary();
    }

    function syncCurrentUsageSummary() {
        const turns = Array.isArray(state.currentSession?.turns) ? state.currentSession.turns : [];
        for (let index = turns.length - 1; index >= 0; index -= 1) {
            const trace = turns[index]?.trace || {};
            if (trace.usage_summary) {
                state.usageSummary = trace.usage_summary;
                return;
            }
        }
        state.usageSummary = null;
    }

    function formatTokenCount(value) {
        const count = Number(value || 0);
        return Number.isFinite(count) ? count.toLocaleString("en-US") : "0";
    }

    function renderUsageSummary() {
        const billed = state.usageSummary?.billed_total || {};
        if (elements.usageInput) {
            elements.usageInput.textContent = `${ui.token_input_label || "输入"} ${formatTokenCount(billed.prompt_tokens)}`;
        }
        if (elements.usageOutput) {
            elements.usageOutput.textContent = `${ui.token_output_label || "输出"} ${formatTokenCount(billed.completion_tokens)}`;
        }
        if (elements.usageTotal) {
            elements.usageTotal.textContent = `${ui.token_combined_label || "总计"} ${formatTokenCount(billed.total_tokens)}`;
        }
    }

    function renderWritingSettings() {
        if (elements.concurrencyInput) {
            elements.concurrencyInput.value = String(state.writingSettings?.max_concurrency || 4);
        }
        if (elements.settingsStatus) {
            const statusText =
                state.settingsStatus === "saving"
                    ? (ui.settings_saving || "保存中...")
                    : state.settingsStatus === "saved"
                        ? (ui.settings_saved || "已保存")
                        : state.settingsStatus === "error"
                            ? (ui.settings_failed || "保存失败")
                            : (ui.project_scope_hint || "项目级保存");
            elements.settingsStatus.textContent = statusText;
        }
    }

    async function saveWritingSettings(value) {
        const previous = normalizeWritingSettings(state.writingSettings);
        const next = normalizeWritingSettings({ max_concurrency: value });
        state.writingSettings = next;
        state.settingsStatus = "saving";
        renderWritingSettings();
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/writing/settings`, {
                method: "PATCH",
                body: JSON.stringify(next),
            });
            state.writingSettings = normalizeWritingSettings(payload.stone_writing);
            state.settingsStatus = "saved";
        } catch (_error) {
            state.writingSettings = previous;
            state.settingsStatus = "error";
        }
        renderWritingSettings();
    }

    function renderSessions() {
        if (!elements.sessionList) {
            return;
        }
        elements.sessionList.innerHTML = "";
        state.sessions.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `writing-session-item ${item.id === state.currentSessionId ? "is-active" : ""} ${item.has_custom_title ? "" : "is-untitled"}`.trim();
            const displayTitle = item.has_custom_title
                ? (item.title || ui.untitled_session || "未命名会话")
                : (ui.untitled_session || "未命名会话");
            button.title = displayTitle;
            const turnCount = Number(item.turn_count || 0);
            const turnLabel = turnCount > 0
                ? (bootstrap.locale === "en-US" ? `${turnCount} turns` : `${turnCount} 条`)
                : (bootstrap.locale === "en-US" ? "Pending" : "待开始");
            const sessionLabel = item.id === state.currentSessionId
                ? (bootstrap.locale === "en-US" ? "Current" : "当前")
                : (bootstrap.locale === "en-US" ? "Session" : "会话");
            button.innerHTML = `
                <span class="writing-session-item__topline">
                    <span class="writing-session-item__status">${sessionLabel}</span>
                    <span class="writing-session-item__count">${escapeHtml(turnLabel)}</span>
                </span>
                <strong>${escapeHtml(displayTitle)}</strong>
                <small>${escapeHtml(formatSessionTime(item.last_active_at || item.created_at))}</small>
            `;
            button.addEventListener("click", () => {
                void loadSession(item.id);
            });
            elements.sessionList.appendChild(button);
        });
    }

    function renderChat() {
        if (!elements.chatList) {
            return;
        }
        state.turnElements = new Map();
        state.emptyStateEl = null;
        state.timelineController?.destroy?.();
        state.timelineController = null;
        state.timelineTrackEl = null;
        state.timelineDetailEl = null;
        elements.chatList.innerHTML = "";
        const turns = Array.isArray(state.currentSession?.turns) ? state.currentSession.turns : [];
        if (!turns.length) {
            const empty = document.createElement("section");
            empty.className = "writing-empty-state";
            empty.innerHTML = `
                <strong>${escapeHtml(ui.untitled_session || "等待主题")}</strong>
                <p>${escapeHtml(ui.empty_turns || "还没有写作记录，先发第一句要求开始。")}</p>
                <span>${escapeHtml(ui.message_hint || "")}</span>
            `;
            state.emptyStateEl = empty;
            elements.chatList.appendChild(empty);
            return;
        }
        const timeline = createTimelineShell();
        elements.chatList.appendChild(timeline);
        if (!state.selectedTurnKey || !turns.some((turn) => getTurnKey(turn) === state.selectedTurnKey)) {
            const runningTurn = turns.find((turn) => String(turn.stream_state || "") === "streaming");
            state.selectedTurnKey = getTurnKey(runningTurn || turns[turns.length - 1]);
        }
        turns.forEach((turn) => {
            const element = createTurnElement(turn);
            const key = getTurnKey(turn);
            state.turnElements.set(key, element);
            state.timelineTrackEl.appendChild(element);
        });
        bindTimelineController();
        renderSelectedTurnDetails();
    }

    function createTimelineShell() {
        const timeline = document.createElement("section");
        timeline.className = "writing-agent-timeline mini-card-workbench";

        const track = document.createElement("div");
        track.className = "writing-agent-timeline__track mini-card-strip";
        track.setAttribute("role", "list");
        track.dataset.miniCardStrip = "";
        timeline.appendChild(track);

        const detail = document.createElement("article");
        detail.className = "writing-agent-timeline__detail detail-panel";
        detail.dataset.detailPanel = "";
        detail.setAttribute("aria-live", "polite");
        timeline.appendChild(detail);

        state.timelineTrackEl = track;
        state.timelineDetailEl = detail;
        return timeline;
    }

    function createTurnElement(turn) {
        const row = document.createElement("button");
        row.type = "button";
        row.setAttribute("role", "listitem");
        row.dataset.miniCard = "";
        row.dataset.miniCardId = getTurnKey(turn);

        const node = classifyTurn(turn);
        const summary = document.createElement("div");
        summary.className = "group-message__summary";

        const metaLine = document.createElement("div");
        metaLine.className = "group-message__summary-top";

        const headline = document.createElement("div");
        headline.className = "group-message__headline";

        const kindChip = document.createElement("span");
        kindChip.className = "group-message__kind-chip";
        headline.appendChild(kindChip);

        const title = document.createElement("strong");
        title.className = "group-message__title";
        headline.appendChild(title);

        const meta = document.createElement("div");
        meta.className = "group-message__summary-meta";

        const statusChip = document.createElement("span");
        statusChip.className = "group-message__status-chip";
        meta.appendChild(statusChip);

        const tail = document.createElement("span");
        tail.className = "group-message__meta-tail";
        meta.appendChild(tail);

        metaLine.appendChild(headline);
        metaLine.appendChild(meta);
        summary.appendChild(metaLine);

        const metricsWrap = document.createElement("div");
        metricsWrap.className = "group-message__metrics";
        summary.appendChild(metricsWrap);
        row.appendChild(summary);
        row._refs = {
            summary,
            kindChip,
            title,
            statusChip,
            tail,
            metricsWrap,
        };
        updateTurnElement(row, turn);
        return row;
    }

    function renderTurn(turn) {
        return createTurnElement(turn);
    }

    function updateTurnElement(row, turn) {
        const refs = row?._refs;
        if (!refs) {
            return;
        }
        const node = classifyTurn(turn);
        row.className = `group-message group-message--${node.kind} group-message--status-${node.status}`;
        row.classList.add("mini-card");
        row.classList.toggle("is-streaming", node.status === "running");
        row.classList.toggle("is-selected", getTurnKey(turn) === state.selectedTurnKey);
        row._turn = turn;
        row.setAttribute("aria-label", `${resolveTurnLabel(turn, node)} ${node.statusLabel} ${resolveActorName(turn)}`);

        refs.kindChip.textContent = node.kindLabel;
        refs.title.textContent = resolveTurnLabel(turn, node);
        refs.statusChip.textContent = node.statusLabel;
        refs.tail.textContent = `${resolveActorName(turn)} · ${formatMessageTime(turn.created_at)}`;

        const keyFacts = buildKeyFacts(turn, node);
        refs.metricsWrap.innerHTML = keyFacts
            .map((item) => `<span class="group-message__metric">${escapeHtml(item)}</span>`)
            .join("");
        refs.metricsWrap.hidden = !keyFacts.length;
        if (getTurnKey(turn) === state.selectedTurnKey) {
            renderSelectedTurnDetails();
        }
    }

    function selectTurn(key) {
        state.selectedTurnKey = key;
        state.turnElements.forEach((element, itemKey) => {
            element.classList.toggle("is-selected", itemKey === key);
        });
        renderSelectedTurnDetails();
    }

    function centerTimelineCard(element) {
        if (!element || !state.timelineTrackEl) {
            return;
        }
        const track = state.timelineTrackEl;
        const targetLeft = element.offsetLeft - (track.clientWidth - element.offsetWidth) / 2;
        track.scrollTo({
            left: Math.max(0, targetLeft),
            behavior: "smooth",
        });
    }

    function bindTimelineController() {
        state.timelineController?.destroy?.();
        const turnsByKey = new Map((state.currentSession?.turns || []).map((turn) => [getTurnKey(turn), turn]));
        state.timelineController = createMiniCardController({
            root: state.timelineTrackEl?.closest(".writing-agent-timeline"),
            strip: state.timelineTrackEl,
            selectedId: state.selectedTurnKey,
            getItem: (key) => {
                const turn = turnsByKey.get(String(key));
                if (!turn) {
                    return null;
                }
                const node = classifyTurn(turn);
                return {
                    id: key,
                    title: resolveTurnLabel(turn, node),
                    status: node.statusLabel,
                    meta: `${resolveActorName(turn)} · ${formatMessageTime(turn.created_at)}`,
                    facts: buildKeyFacts(turn, node).slice(0, 2),
                };
            },
            onSelect: (key) => {
                selectTurn(key);
            },
        });
    }

    function getSelectedTurn() {
        const turns = Array.isArray(state.currentSession?.turns) ? state.currentSession.turns : [];
        if (!turns.length) {
            return null;
        }
        return turns.find((turn) => getTurnKey(turn) === state.selectedTurnKey) || turns[turns.length - 1];
    }

    function renderSelectedTurnDetails() {
        if (!state.timelineDetailEl) {
            return;
        }
        const turn = getSelectedTurn();
        if (!turn) {
            state.timelineDetailEl.innerHTML = "";
            return;
        }
        const node = classifyTurn(turn);
        const keyFacts = buildKeyFacts(turn, node);
        const detailFacts = buildDetailFacts(turn, node, keyFacts);
        state.timelineDetailEl.className = `writing-agent-timeline__detail group-message--${node.kind} group-message--status-${node.status}`;
        state.timelineDetailEl.innerHTML = `
            <div class="writing-agent-detail__header">
                <div>
                    <span class="group-message__kind-chip">${escapeHtml(node.kindLabel)}</span>
                    <h2>${escapeHtml(resolveTurnLabel(turn, node))}</h2>
                </div>
                <div class="group-message__summary-meta">
                    <span class="group-message__status-chip">${escapeHtml(node.statusLabel)}</span>
                    <span class="group-message__meta-tail">${escapeHtml(`${resolveActorName(turn)} · ${formatMessageTime(turn.created_at)}`)}</span>
                </div>
            </div>
            <div class="writing-agent-detail__summary">
                ${detailFacts.map((item) => `
                    <div class="writing-agent-detail__fact">
                        <span>${escapeHtml(item.label)}</span>
                        <strong>${escapeHtml(item.value)}</strong>
                    </div>
                `).join("")}
            </div>
            <div class="writing-agent-detail__body"></div>
        `;
        const body = state.timelineDetailEl.querySelector(".writing-agent-detail__body");
        if (!body) {
            return;
        }
        if (node.status === "running") {
            const streamPanel = document.createElement("section");
            streamPanel.className = "writing-agent-detail__stream";
            const label = document.createElement("span");
            label.className = "writing-agent-detail__label";
            label.textContent = "流式文本";
            const bubble = document.createElement("div");
            bubble.className = "group-message__bubble group-message__bubble--plain";
            bubble.textContent = turn.content || "等待输出…";
            streamPanel.appendChild(label);
            streamPanel.appendChild(bubble);
            body.appendChild(streamPanel);
            return;
        }

        const outputPanel = document.createElement("section");
        outputPanel.className = "writing-agent-detail__output";
        const outputLabel = document.createElement("span");
        outputLabel.className = "writing-agent-detail__label";
        outputLabel.textContent = "原文";
        const output = document.createElement("div");
        output.className = "group-message__bubble group-message__bubble--plain";
        output.textContent = turn.content || "无";
        outputPanel.appendChild(outputLabel);
        outputPanel.appendChild(output);
        body.appendChild(outputPanel);
    }

    function buildDetailFacts(turn, node, keyFacts) {
        const debug = turn.trace?.debug && typeof turn.trace.debug === "object" ? turn.trace.debug : {};
        const usage = debug.usage && typeof debug.usage === "object" ? debug.usage : null;
        const facts = [
            { label: "状态", value: node.statusLabel },
            { label: "执行者", value: resolveActorName(turn) },
            { label: "阶段", value: normalizeStageLabel("", turn.message_kind, turn.stage || turn.trace?.stage) || turn.stage || turn.message_kind || "步骤" },
        ];
        if (usage) {
            const total = Number(usage.total_tokens || usage.prompt_tokens || 0) + Number(!usage.total_tokens ? usage.completion_tokens || 0 : 0);
            if (total > 0) {
                facts.push({ label: "Token", value: formatTokenCount(total) });
            }
        }
        keyFacts.slice(0, 4).forEach((fact) => {
            const [label, ...rest] = String(fact).split(":");
            facts.push({
                label: rest.length ? label.trim() : "要点",
                value: (rest.length ? rest.join(":") : fact).trim(),
            });
        });
        return facts.filter((item) => item.value).slice(0, 8);
    }

    function getTurnKey(turn) {
        return String(turn.stream_key || turn.id || "");
    }

    function cacheCurrentSession() {
        if (!state.currentSessionId || !state.currentSession) {
            return;
        }
        state.sessionDetails.set(state.currentSessionId, state.currentSession);
    }

    function mergeSessionDetail(sessionId, payload) {
        const incoming = payload && typeof payload === "object" ? payload : { turns: [] };
        const cached = state.sessionDetails.get(sessionId);
        if (!cached || !Array.isArray(cached.turns) || !Array.isArray(incoming.turns)) {
            return incoming;
        }
        const incomingKeys = new Set(incoming.turns.map((turn) => getTurnKey(turn)));
        const cachedHasExtraTurns = cached.turns.some((turn) => !incomingKeys.has(getTurnKey(turn)));
        return {
            ...cached,
            ...incoming,
            turns: cachedHasExtraTurns && cached.turns.length >= incoming.turns.length ? cached.turns : incoming.turns,
        };
    }

    function shouldFollowStreamingTurn(nextKey) {
        if (!state.selectedTurnKey) {
            return true;
        }
        if (state.selectedTurnKey === nextKey) {
            return true;
        }
        const selectedTurn = getSelectedTurn();
        return Boolean(selectedTurn && String(selectedTurn.stream_state || "") === "streaming");
    }

    function clearEmptyState() {
        if (state.emptyStateEl?.parentNode) {
            state.emptyStateEl.parentNode.removeChild(state.emptyStateEl);
        }
        state.emptyStateEl = null;
    }

    function upsertTurn(turn) {
        if (!state.currentSession) {
            state.currentSession = { turns: [] };
        }
        if (!Array.isArray(state.currentSession.turns)) {
            state.currentSession.turns = [];
        }
        const shouldStick = shouldAutoScroll(elements.liveFeed || elements.chatList);
        let inserted = false;
        let currentTurn = turn;

        if (turn.stream_key) {
            const index = state.currentSession.turns.findIndex(
                (item) => item.stream_key && item.stream_key === turn.stream_key
            );
            if (index >= 0) {
                state.currentSession.turns[index] = {
                    ...state.currentSession.turns[index],
                    ...turn,
                    trace: turn.trace || state.currentSession.turns[index].trace || {},
                };
                currentTurn = state.currentSession.turns[index];
            } else {
                state.currentSession.turns.push(turn);
                inserted = true;
            }
        } else {
            state.currentSession.turns.push(turn);
            inserted = true;
        }

        if (inserted && typeof state.currentSession.timeline_turn_count === "number") {
            state.currentSession.timeline_turn_count += 1;
        }

        const key = getTurnKey(currentTurn);
        const followStreaming = currentTurn.stream_state === "streaming" && shouldFollowStreamingTurn(key);
        if (!state.selectedTurnKey || followStreaming) {
            state.selectedTurnKey = key;
        }
        clearEmptyState();
        const existingElement = state.turnElements.get(key);
        if (existingElement) {
            updateTurnElement(existingElement, currentTurn);
        } else if (elements.chatList) {
            if (!state.timelineTrackEl) {
                elements.chatList.innerHTML = "";
                elements.chatList.appendChild(createTimelineShell());
            }
            const element = createTurnElement(currentTurn);
            state.turnElements.set(key, element);
            state.timelineTrackEl.appendChild(element);
            bindTimelineController();
        }
        cacheCurrentSession();
        state.timelineController?.select?.(state.selectedTurnKey, { center: false });
        selectTurn(state.selectedTurnKey);
        if (followStreaming) {
            const selectedElement = state.turnElements.get(state.selectedTurnKey);
            if (selectedElement) {
                centerTimelineCard(selectedElement);
            }
        }
        if (shouldStick) {
            scrollToBottom();
        }
    }

    function appendTurn(turn) {
        upsertTurn(turn);
    }

    function appendLocalError(message) {
        appendTurn({
            id: `error-${Date.now()}`,
            role: "assistant",
            content: `${ui.execution_failed || "写作失败"}\n\n${message}`,
            actor_id: "writer-error",
            actor_name: ui.agent_label || "写作 Agent",
            actor_role: "writer",
            message_kind: "error",
            label: ui.execution_failed || "写作失败",
            render_mode: "plain",
            trace: { debug: { message } },
            created_at: new Date().toISOString(),
        });
    }

    function normalizeStreamTurn(payload) {
        return {
            id:
                payload.stream_key ||
                `${payload.actor_id || "assistant"}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
            role: "assistant",
            content: payload.body || "",
            actor_id: payload.actor_id || "assistant",
            actor_name: payload.actor_name || ui.agent_label || "写作 Agent",
            actor_role: payload.actor_role || "assistant",
            message_kind: payload.message_kind || "update",
            label: payload.label || "",
            stage: payload.stage || payload.message_kind || "",
            stream_key: payload.stream_key || "",
            stream_state: payload.stream_state || "complete",
            render_mode: payload.render_mode || "markdown",
            trace: {
                stage: payload.stage || payload.message_kind || "",
                label: payload.label || "",
                usage_summary: payload.usage_summary || null,
                debug: payload.detail || {},
            },
            created_at: payload.created_at || new Date().toISOString(),
        };
    }

    function syncSessionSummary(detail, { promote = false } = {}) {
        const existing = state.sessions.find((item) => item.id === detail.id) || {};
        const summary = {
            id: detail.id,
            title: detail.title ?? existing.title,
            has_custom_title: detail.has_custom_title ?? existing.has_custom_title ?? Boolean(detail.title),
            session_kind: detail.session_kind ?? existing.session_kind,
            created_at: detail.created_at ?? existing.created_at,
            last_active_at: detail.last_active_at ?? existing.last_active_at,
            turn_count: detail.timeline_turn_count || detail.turn_count || detail.turns?.length || 0,
        };
        if (promote || !existing.id) {
            state.sessions = [summary, ...state.sessions.filter((item) => item.id !== summary.id)];
            return;
        }
        state.sessions = state.sessions.map((item) => (item.id === summary.id ? summary : item));
    }

    function closeStream() {
        if (state.eventSource) {
            state.eventSource.close();
            state.eventSource = null;
        }
    }

    function restoreComposer() {
        closeStream();
        state.sending = false;
        if (elements.messageInput) {
            elements.messageInput.disabled = false;
        }
        setButtonBusy(elements.send, false);
    }

    function showComposerError(message) {
        if (!elements.composerError) {
            return;
        }
        elements.composerError.hidden = false;
        elements.composerError.textContent = message;
    }

    function hideComposerError() {
        if (!elements.composerError) {
            return;
        }
        elements.composerError.hidden = true;
        elements.composerError.textContent = "";
    }

    function resolveBaselineLabel(baseline) {
        if (baseline.status === "ready") {
            return ui.baseline_ready || "当前使用最新 Stone v3 基线。";
        }
        if (baseline.status === "requires_rebuild") {
            return ui.baseline_requires_rebuild || "检测到遗留 Stone v2 数据，请重新运行 Stone 预处理以重建 Stone v3 基线。";
        }
        if (baseline.status === "missing_preprocess") {
            return ui.baseline_missing_preprocess || "请先完成 Stone 预处理。";
        }
        if (baseline.status === "running_preprocess") {
            return ui.baseline_running_preprocess || "Stone 预处理仍在运行中。";
        }
        if (baseline.status === "missing_profiles") {
            return ui.baseline_missing_profiles || "当前还没有 Stone v3 逐篇画像。";
        }
        if (baseline.status === "missing_analysis") {
            return ui.baseline_missing_analysis || "请先完成 Stone 分析，再进入写作。";
        }
        if (baseline.status === "analysis_incomplete") {
            return ui.baseline_analysis_incomplete || "Stone 分析还不完整，当前只具备降级写作条件。";
        }
        if (baseline.status === "incomplete_baseline") {
            return ui.baseline_incomplete_baseline || "Stone v3 基线资产还不完整。";
        }
        return ui.baseline_missing_preprocess || "请先完成 Stone 预处理。";
    }

    function resolveActorName(turn) {
        if (turn.role === "user") {
            return ui.you_label || normalizeActorName(turn.actor_name) || "你";
        }
        return normalizeActorName(turn.actor_name) || ui.agent_label || "写作 Agent";
    }

    function avatarLetter(turn) {
        const name = resolveActorName(turn);
        return Array.from(name)[0] || "S";
    }

    function normalizeActorName(name) {
        const value = String(name || "").trim();
        if (!value) {
            return "";
        }
        if (value === "?? Agent" || value === "？? Agent" || value === "？？Agent" || value === "？？ Agent") {
            return "写作 Agent";
        }
        if (value === "鍐欎綔 Agent" || value === "鍐" || value === "写作 Agent") {
            return "写作 Agent";
        }
        if (value === "浣?" || value === "用户" || value === "你") {
            return "用户";
        }
        if (value.toLowerCase().includes("agent") && (value.includes("鍐") || value.includes("写作"))) {
            return "写作 Agent";
        }
        return value;
    }

    function classifyTurn(turn) {
        const messageKind = String(turn.message_kind || "update");
        const actorRole = String(turn.actor_role || "");
        const status =
            messageKind === "error"
                ? "failed"
                : turn.stream_state === "streaming"
                    ? "running"
                    : "complete";

        if (turn.role === "user") {
            return {
                kind: "command",
                kindLabel: "指令",
                status,
                statusLabel: status === "running" ? "运行中" : "已提交",
                title: "写作指令",
                avatar: ">_",
                defaultOpen: true,
            };
        }

        if (messageKind === "error") {
            return {
                kind: "error",
                kindLabel: "异常",
                status,
                statusLabel: "失败",
                title: "执行异常",
                avatar: "!!",
                defaultOpen: true,
            };
        }

        if (messageKind === "final") {
            return {
                kind: "result",
                kindLabel: "成稿",
                status,
                statusLabel: status === "running" ? "运行中" : "完成",
                title: "最终结果",
                avatar: "OK",
                defaultOpen: true,
            };
        }

        if (
            actorRole === "reviewer" ||
            actorRole === "critic" ||
            messageKind === "critic" ||
            ["feature_density", "cross_domain_generalization", "rhythm_entropy", "extreme_state_handling", "ending_landing"].includes(messageKind)
        ) {
            return {
                kind: "critic",
                kindLabel: "审判",
                status,
                statusLabel: status === "running" ? "运行中" : "完成",
                title: "高仿审判",
                avatar: "CR",
                defaultOpen: status === "running",
            };
        }

        if (["generation_packet", "candidate_shortlist_v3"].includes(messageKind)) {
            return {
                kind: "tool",
                kindLabel: "工具",
                status,
                statusLabel: status === "running" ? "运行中" : "完成",
                title: "工具步骤",
                avatar: "TL",
                defaultOpen: status === "running",
            };
        }

        return {
            kind: "subagent",
            kindLabel: "子代理",
            status,
            statusLabel: status === "running" ? "运行中" : "完成",
            title: "子代理步骤",
            avatar: "AG",
            defaultOpen: status === "running",
        };
    }

    function summarizeTurn(turn) {
        const text = String(turn.content || "")
            .replace(/\s+/g, " ")
            .trim();
        if (!text) {
            return resolveTurnLabel(turn) || turn.message_kind || "等待输出…";
        }
        if (text.length <= 180) {
            return text;
        }
        return `${text.slice(0, 177)}...`;
    }

    function buildNodeMetrics(turn, node) {
        const debug = turn.trace?.debug || {};
        const metrics = [];
        if (node.kind === "subagent" || node.kind === "tool") {
            if (Array.isArray(debug.selected_documents) && debug.selected_documents.length) {
                metrics.push(`${debug.selected_documents.length} 文档`);
            }
            if (Array.isArray(debug.anchor_ids) && debug.anchor_ids.length) {
                metrics.push(`${debug.anchor_ids.length} 锚点`);
            }
            if (Array.isArray(debug.paragraph_map) && debug.paragraph_map.length) {
                metrics.push(`${debug.paragraph_map.length} 段落`);
            }
            if (debug.axis_map && typeof debug.axis_map === "object") {
                metrics.push(`${Object.keys(debug.axis_map).length} 轴`);
            }
            if (debug.axis_source_map && typeof debug.axis_source_map === "object") {
                metrics.push(`${Object.keys(debug.axis_source_map).length} 来源`);
            }
            if (Array.isArray(debug.coverage_warnings) && debug.coverage_warnings.length) {
                metrics.push(`${debug.coverage_warnings.length} 警告`);
            }
        }
        if (typeof debug.word_count === "number" && debug.word_count > 0) {
            metrics.push(`${debug.word_count} 字`);
        }
        if (debug.final_assessment && typeof debug.final_assessment === "object") {
            const reviewCount = Number(debug.final_assessment.critic_total || 0);
            if (reviewCount > 0) {
                metrics.push(`${reviewCount} 审判器`);
            }
        }
        appendUsageFacts(metrics, debug);
        return metrics.slice(0, 6);
    }

    function buildKeyFacts(turn, node) {
        const debug = turn.trace?.debug || {};
        const stage = String(turn.stage || turn.message_kind || "");
        const facts = [];

        const pushFact = (label, value) => {
            const formatted = formatFactValue(value);
            if (!formatted) {
                return;
            }
            facts.push(`${label}: ${formatted}`);
        };

        if (stage === "generation_packet") {
            const baselineDebug = debug.baseline && typeof debug.baseline === "object" ? debug.baseline : debug;
            pushFact("画像", baselineDebug.profile_count);
            pushFact("锚点", baselineDebug.source_anchor_count);
            if (typeof baselineDebug.analysis_ready === "boolean") {
                pushFact("分析", baselineDebug.analysis_ready ? "就绪" : "降级");
            }
        } else if (stage === "request_adapter_v3") {
            pushFact("镜头", debug.value_lens);
            pushFact("距离", debug.distance);
            pushFact("形式", debug.surface_form);
        } else if (stage === "candidate_shortlist_v3") {
            pushFact("候选", debug.shortlist_size);
            pushFact("形式", debug.surface_form);
            pushFact("检索词", summarizeTerms(debug.query_terms, 2));
        } else if (stage === "llm_rerank_v3") {
            pushFact("文档", countValue(debug.selected_documents));
            pushFact("锚点", countValue(debug.anchor_ids));
            pushFact("理由", debug.selection_reason);
        } else if (stage === "writing_packet_v3") {
            pushFact("家族", summarizeTerms(debug.family_labels, 2));
            pushFact("画像切片", countValue(debug.selected_profile_ids));
            pushFact("警告", countValue(debug.coverage_warnings));
        } else if (stage === "blueprint_v3") {
            pushFact("段落", debug.paragraph_count);
            pushFact("形状", debug.shape_note);
            pushFact("轴", countValue(debug.axis_map));
        } else if (node.kind === "critic") {
            pushFact("结论", translateVerdict(debug.verdict));
            pushFact("分数", typeof debug.score === "number" ? `${Math.round(debug.score * 100)}` : "");
            pushFact("锚点", countValue(debug.anchor_ids));
        } else if (isBodyStage(turn)) {
            pushFact("字数", debug.word_count);
            pushFact("审判器", debug.final_assessment?.critic_total);
        }

        appendUsageFacts(facts, debug);

        if (!facts.length) {
            return buildNodeMetrics(turn, node).slice(0, 3);
        }
        return facts.slice(0, 6);
    }

    function shouldKeepPanelOpen(node, turn) {
        if (node.kind === "error") {
            return true;
        }
        if (node.status === "running") {
            return true;
        }
        if (isBodyStage(turn)) {
            return true;
        }
        return false;
    }

    function isBodyStage(turn) {
        const stage = String(turn.stage || turn.message_kind || "");
        return ["draft_v3", "redraft", "line_edit", "final"].includes(stage);
    }

    function countValue(value) {
        if (Array.isArray(value)) {
            return value.length || "";
        }
        if (value && typeof value === "object") {
            return Object.keys(value).length || "";
        }
        return value ?? "";
    }

    function summarizeTerms(value, limit = 2) {
        if (!Array.isArray(value) || !value.length) {
            return "";
        }
        return value
            .map((item) => String(item || "").trim())
            .filter(Boolean)
            .slice(0, limit)
            .join(" / ");
    }

    function formatFactValue(value) {
        if (value === null || value === undefined) {
            return "";
        }
        if (typeof value === "boolean") {
            return value ? "是" : "否";
        }
        const text = String(value).trim();
        if (!text) {
            return "";
        }
        if (text.length <= 18) {
            return text;
        }
        return `${text.slice(0, 18)}...`;
    }

    function appendUsageFacts(target, debug) {
        const usage = debug?.usage && typeof debug.usage === "object" ? debug.usage : null;
        if (!usage) {
            return;
        }
        const promptTokens = Number(usage.prompt_tokens || 0);
        const completionTokens = Number(usage.completion_tokens || 0);
        const totalTokens = Number(usage.total_tokens || promptTokens + completionTokens || 0);
        const retryCount = Number(debug.retry_count || 0);
        if (promptTokens > 0) {
            target.push(`In: ${formatTokenCount(promptTokens)}`);
        }
        if (completionTokens > 0) {
            target.push(`Out: ${formatTokenCount(completionTokens)}`);
        }
        if (totalTokens > 0) {
            target.push(`Total: ${formatTokenCount(totalTokens)}`);
        }
        if (retryCount > 0) {
            target.push(`重试 x${retryCount}`);
        }
    }

    function translateVerdict(value) {
        const verdict = String(value || "").trim();
        if (verdict === "approve") {
            return "通过";
        }
        if (verdict === "line_edit") {
            return "修订";
        }
        if (verdict === "redraft") {
            return "重写";
        }
        return verdict;
    }

    function resolveTurnLabel(turn, node = null) {
        const label = normalizeStageLabel(turn.label || turn.trace?.label || "", turn.message_kind, turn.stage || turn.trace?.stage);
        if (label) {
            return label;
        }
        if (node?.title) {
            return node.title;
        }
        return normalizeStageLabel("", turn.message_kind, turn.stage || turn.trace?.stage) || "执行步骤";
    }

    function normalizeStageLabel(label, messageKind, stage) {
        const value = String(label || "").trim();
        const key = String(stage || messageKind || "").trim();
        const table = {
            generation_packet: "基线装载",
            request_adapter_v3: "请求适配",
            candidate_shortlist_v3: "候选切片",
            llm_rerank_v3: "证据重排",
            writing_packet_v3: "写作包",
            blueprint_v3: "蓝图规划",
            draft_v3: "正文起草",
            redraft: "整篇重写",
            line_edit: "逐句修订",
            critic: "高仿审判",
            final: "最终成稿",
            error: "执行异常",
        };
        if (value) {
            const normalized = {
                "Stone baseline loaded": "基线装载",
                "Request adapter v3": "请求适配",
                "Candidate shortlist v3": "候选切片",
                "LLM rerank v3": "证据重排",
                "Writing packet v3": "写作包",
                "Blueprint v3": "蓝图规划",
                "Draft v3": "正文起草",
                "Redraft": "整篇重写",
                "Line edit": "逐句修订",
                "Final": "最终成稿",
            }[value];
            if (normalized) {
                return normalized;
            }
            return value;
        }
        return table[key] || "";
    }

    function formatMessageTime(value) {
        try {
            return new Intl.DateTimeFormat(document.body.dataset.locale || "zh-CN", {
                hour: "2-digit",
                minute: "2-digit",
            }).format(new Date(value));
        } catch {
            return value || "--";
        }
    }

    function formatSessionTime(value) {
        try {
            return new Intl.DateTimeFormat(document.body.dataset.locale || "zh-CN", {
                month: "2-digit",
                day: "2-digit",
            }).format(new Date(value));
        } catch {
            return value || "--";
        }
    }

    function scrollToBottom(force = false) {
        const scroller = elements.liveFeed;
        if (!scroller) {
            return;
        }
        if (!force && !shouldAutoScroll(scroller)) {
            return;
        }
        scroller.scrollTop = scroller.scrollHeight;
    }
}
