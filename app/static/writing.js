import {
    escapeHtml,
    fetchJson,
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
        sessions: bootstrap.sessions || [],
        currentSessionId: bootstrap.selected_session_id || null,
        currentSession: bootstrap.selected_session || { turns: [] },
        baseline: bootstrap.baseline || { status: "missing_analysis" },
        channelTitle: bootstrap.channel_title || "",
        eventSource: null,
        sending: false,
    };

    const elements = {
        sessionList: shell.querySelector("[data-session-list]"),
        sessionTitle: shell.querySelector("[data-session-title]"),
        channelTitle: shell.querySelector("[data-channel-title]"),
        baselinePill: shell.querySelector("[data-baseline-pill]"),
        baselineNote: shell.querySelector("[data-baseline-note]"),
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
                body: JSON.stringify({ title: ui.new_session || "新建会话" }),
            });
            syncSessionSummary(payload);
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
            }
            renderSessions();
            renderHeader();
        });

        elements.deleteSession?.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            if (!window.confirm(ui.common?.confirm_delete_session || "确定删除这个会话吗？")) {
                return;
            }
            await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}`, {
                method: "DELETE",
            });
            state.sessions = state.sessions.filter((item) => item.id !== state.currentSessionId);
            if (!state.sessions.length) {
                const created = await fetchJson(`/api/projects/${state.projectId}/writing/sessions`, {
                    method: "POST",
                    body: JSON.stringify({ title: ui.new_session || "新建会话" }),
                });
                syncSessionSummary(created);
                state.sessions = [created];
            }
            await loadSession(state.sessions[0].id);
        });

        elements.send?.addEventListener("click", () => sendMessage());

        elements.messageInput?.addEventListener("keydown", (event) => {
            if (event.isComposing) {
                return;
            }
            if (event.key === "Enter") {
                event.preventDefault();
                sendMessage();
            }
        });

        elements.toggleSessions?.addEventListener("click", () => {
            const next = shell.dataset.sessionsOpen !== "true";
            shell.dataset.sessionsOpen = String(next);
        });
    }

    async function sendMessage() {
        const message = elements.messageInput?.value?.trim() || "";
        if (!message || !state.currentSessionId || state.sending) {
            return;
        }

        const parsed = parseWritingMessage(message);
        if (!parsed.ok) {
            showComposerError(parsed.error || ui.message_parse_error || "请补全字数格式。");
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
                topic: parsed.topic,
                target_word_count: parsed.targetWordCount,
                extra_requirements: parsed.extraRequirements,
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
                    body: JSON.stringify({ message }),
                }
            );
            if (elements.messageInput) {
                elements.messageInput.value = "";
            }
            syncSessionSummary({
                id: state.currentSessionId,
                title: state.currentSession?.title,
                created_at: state.currentSession?.created_at,
                last_active_at: new Date().toISOString(),
                timeline_turn_count: state.currentSession?.turns?.length || 1,
            });
            renderSessions();
            openStream(payload.stream_id);
        } catch (error) {
            appendLocalError(error.message);
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
            if (payload.stage === "generation_packet") {
                state.baseline.status = "ready";
                if (payload.baseline_components) {
                    state.baseline.corpus_ready = Boolean(payload.baseline_components.corpus_ready);
                    state.baseline.profile_count = Number(payload.baseline_components.profile_count || 0);
                    state.baseline.author_model_ready = Boolean(payload.baseline_components.author_model_ready);
                    state.baseline.prototype_index_ready = Boolean(payload.baseline_components.prototype_index_ready);
                    state.baseline.source_anchor_count = Number(payload.baseline_components.source_anchor_count || 0);
                }
                renderBaseline();
            }
        });

        source.addEventListener("stream_update", (event) => {
            const payload = safeParseJson(event.data, {});
            upsertTurn(normalizeStreamTurn(payload));
        });

        source.addEventListener("stage", (event) => {
            const payload = safeParseJson(event.data, {});
            upsertTurn(normalizeStreamTurn(payload));
        });

        source.addEventListener("done", (event) => {
            const payload = safeParseJson(event.data, {});
            upsertTurn(normalizeStreamTurn(payload));
            syncSessionSummary({
                id: state.currentSessionId,
                title: state.currentSession?.title,
                created_at: state.currentSession?.created_at,
                last_active_at: payload.created_at || new Date().toISOString(),
                timeline_turn_count: state.currentSession?.turns?.length || 0,
            });
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
        const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${sessionId}`);
        state.currentSessionId = sessionId;
        state.currentSession = payload;
        syncSessionSummary(payload);
        shell.dataset.sessionsOpen = "false";
        restoreComposer();
        renderAll();
        scrollToBottom(true);
    }

    function renderAll() {
        renderHeader();
        renderBaseline();
        renderSessions();
        renderChat();
    }

    function renderHeader() {
        if (elements.channelTitle) {
            elements.channelTitle.textContent = state.channelTitle || "";
        }
        if (elements.sessionTitle) {
            elements.sessionTitle.textContent = state.currentSession?.title || ui.untitled_session || "未命名会话";
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
                components.push(`profiles ${profileCount || ""}`.trim());
            }
            if (state.baseline?.author_model_ready) {
                components.push("author_model");
            }
            if (state.baseline?.prototype_index_ready) {
                components.push("prototype_index");
            }
            const stats = [];
            if (profileCount > 0) {
                stats.push(`${profileCount} profiles`);
            }
            if (sourceAnchorCount > 0) {
                stats.push(`${sourceAnchorCount} anchors`);
            }
            const componentLabel = components.length ? ` ${components.join(" + ")}` : "";
            const statsLabel = stats.length ? ` (${stats.join(" / ")})` : "";
            elements.baselineNote.textContent = `${ui.hero_note || ""}${componentLabel}${statsLabel}`.trim() || label;
        }
    }

    function renderSessions() {
        if (!elements.sessionList) {
            return;
        }
        elements.sessionList.innerHTML = "";
        state.sessions.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `writing-session-item ${item.id === state.currentSessionId ? "is-active" : ""}`;
            button.innerHTML = `
                <strong>${escapeHtml(item.title || ui.untitled_session || "未命名会话")}</strong>
                <small>${escapeHtml(String(item.turn_count || 0))} · ${escapeHtml(formatSessionTime(item.last_active_at || item.created_at))}</small>
            `;
            button.addEventListener("click", () => loadSession(item.id));
            elements.sessionList.appendChild(button);
        });
    }

    function renderChat() {
        if (!elements.chatList) {
            return;
        }
        elements.chatList.innerHTML = "";
        const turns = state.currentSession?.turns || [];
        if (!turns.length) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = ui.empty_turns || "还没有写作消息，先发一条命令开始。";
            elements.chatList.appendChild(empty);
            return;
        }
        turns.forEach((turn) => {
            elements.chatList.appendChild(renderTurn(turn));
        });
    }

    function renderTurn(turn) {
        const row = document.createElement("article");
        const role = turn.role === "user" ? "user" : "assistant";
        const kind = turn.message_kind || "update";
        const liveStateClass = turn.stream_state === "streaming" ? " is-streaming" : "";
        row.className = `group-message group-message--${role} group-message--${kind}${liveStateClass}`;

        const avatar = document.createElement("div");
        avatar.className = "group-message__avatar";
        avatar.textContent = avatarLetter(turn);
        row.appendChild(avatar);

        const inner = document.createElement("div");
        inner.className = "group-message__inner";

        const meta = document.createElement("div");
        meta.className = "group-message__meta";
        meta.innerHTML = `
            <strong>${escapeHtml(resolveActorName(turn))}</strong>
            <span>${escapeHtml(formatMessageTime(turn.created_at))}</span>
            ${turn.label ? `<span class="group-message__meta-state">${escapeHtml(turn.label)}</span>` : ""}
            ${turn.stream_state === "streaming" ? `<span class="group-message__meta-live">${escapeHtml(ui.streaming || "流式输出中")}</span>` : ""}
        `;
        inner.appendChild(meta);

        const bubble = document.createElement("div");
        bubble.className = "group-message__bubble";
        if (turn.role === "user" || turn.render_mode === "plain") {
            bubble.classList.add("group-message__bubble--plain");
            bubble.textContent = turn.content || "";
        } else {
            renderMarkdownInto(bubble, turn.content || "");
        }
        inner.appendChild(bubble);

        const debugPayload = turn.trace?.debug;
        if (debugPayload && Object.keys(debugPayload).length) {
            const details = document.createElement("details");
            details.className = "group-message__details";
            details.innerHTML = `
                <summary>展开详情</summary>
                <pre>${escapeHtml(JSON.stringify(debugPayload, null, 2))}</pre>
            `;
            inner.appendChild(details);
        }

        row.appendChild(inner);
        return row;
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
        if (turn.stream_key) {
            const index = state.currentSession.turns.findIndex((item) => item.stream_key && item.stream_key === turn.stream_key);
            if (index >= 0) {
                state.currentSession.turns[index] = {
                    ...state.currentSession.turns[index],
                    ...turn,
                    trace: turn.trace || state.currentSession.turns[index].trace || {},
                };
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
        renderChat();
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
            id: payload.stream_key || `${payload.actor_id || "assistant"}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
            role: "assistant",
            content: payload.body || "",
            actor_id: payload.actor_id || "assistant",
            actor_name: payload.actor_name || ui.agent_label || "写作 Agent",
            actor_role: payload.actor_role || "assistant",
            message_kind: payload.message_kind || "update",
            label: payload.label || "",
            stream_key: payload.stream_key || "",
            stream_state: payload.stream_state || "complete",
            render_mode: payload.render_mode || "markdown",
            trace: {
                debug: payload.detail || {},
            },
            created_at: payload.created_at || new Date().toISOString(),
        };
    }

    function syncSessionSummary(detail) {
        const summary = {
            id: detail.id,
            title: detail.title,
            session_kind: detail.session_kind,
            created_at: detail.created_at,
            last_active_at: detail.last_active_at,
            turn_count: detail.timeline_turn_count || detail.turn_count || detail.turns?.length || 0,
        };
        const index = state.sessions.findIndex((item) => item.id === summary.id);
        if (index >= 0) {
            state.sessions[index] = summary;
        } else {
            state.sessions.unshift(summary);
        }
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
            return ui.baseline_ready || "当前使用最新 Stone v2 基线。";
        }
        if (baseline.status === "missing_preprocess") {
            return ui.baseline_missing_preprocess || "请先完成 Stone 预分析。";
        }
        if (baseline.status === "running_preprocess") {
            return ui.baseline_running_preprocess || "Stone 预分析仍在运行中。";
        }
        if (baseline.status === "missing_profiles") {
            return ui.baseline_missing_profiles || "当前还没有 Stone v2 逐篇画像。";
        }
        if (baseline.status === "incomplete_baseline") {
            return ui.baseline_incomplete_baseline || "Stone v2 基线资产还不完整。";
        }
        return ui.baseline_missing_preprocess || "请先完成 Stone 预分析。";
    }

    function resolveActorName(turn) {
        if (turn.role === "user") {
            return ui.you_label || turn.actor_name || "你";
        }
        return turn.actor_name || ui.agent_label || "写作 Agent";
    }

    function avatarLetter(turn) {
        const name = resolveActorName(turn);
        const first = Array.from(name)[0];
        return first || "S";
    }

    function parseWritingMessage(message) {
        const match = String(message || "").match(/(\d+)\s*(字|words)(?=\s|$|[，,。.;；:：!?！？])/i);
        if (!match) {
            return { ok: false, error: ui.message_parse_error || "请在消息里带上明确字数，例如 800字 或 800 words。" };
        }
        const targetWordCount = Number(match[1]);
        if (!Number.isFinite(targetWordCount) || targetWordCount < 100) {
            return { ok: false, error: ui.message_parse_error || "请在消息里带上明确字数，例如 800字 或 800 words。" };
        }
        const topicText = message.slice(0, match.index).trim().replace(/^[请帮我麻烦\s]*(写(?:一篇|篇|个)?|来(?:一篇|篇|个)?)/, "").trim();
        if (!topicText) {
            return { ok: false, error: ui.message_parse_error || "请在消息里带上明确字数，例如 800字 或 800 words。" };
        }
        const extraRequirements = message.slice((match.index || 0) + match[0].length).trim().replace(/^[，,。.;；:：\s]+/, "");
        return {
            ok: true,
            topic: topicText,
            targetWordCount,
            extraRequirements: extraRequirements || null,
        };
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
