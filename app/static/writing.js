import {
    escapeHtml,
    fetchJson,
    renderMarkdownInto,
    safeParseJson,
    setButtonBusy,
} from "./shared.js";

const shell = document.querySelector("[data-writing-shell]");

if (shell) {
    const bootstrap = safeParseJson(document.getElementById("writing-bootstrap")?.textContent, {});
    const ui = bootstrap.ui_strings || {};

    const state = {
        projectId: shell.dataset.projectId,
        sessions: bootstrap.sessions || [],
        currentSessionId: bootstrap.selected_session_id || null,
        currentSession: bootstrap.selected_session || null,
        documents: bootstrap.documents || [],
        guide: bootstrap.guide || { status: "missing" },
        stageFeed: [],
        eventSource: null,
        sending: false,
    };

    const elements = {
        sessionList: shell.querySelector("[data-session-list]"),
        sessionTitle: shell.querySelector("[data-session-title]"),
        chatList: shell.querySelector("[data-chat-list]"),
        documentList: shell.querySelector("[data-document-list]"),
        stageFeedList: shell.querySelector("[data-stage-feed-list]"),
        guidePill: shell.querySelector("[data-guide-pill]"),
        topic: shell.querySelector("[data-topic-input]"),
        targetWordCount: shell.querySelector("[data-target-word-count]"),
        extraRequirements: shell.querySelector("[data-extra-requirements]"),
        composerHint: shell.querySelector("[data-composer-hint]"),
        send: shell.querySelector("[data-send-message]"),
        newSession: shell.querySelector("[data-new-session]"),
        renameSession: shell.querySelector("[data-rename-session]"),
        deleteSession: shell.querySelector("[data-delete-session]"),
    };

    bindEvents();
    renderAll();

    function bindEvents() {
        elements.newSession?.addEventListener("click", async () => {
            const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions`, {
                method: "POST",
                body: JSON.stringify({ title: ui.new_session || "新建写作会话" }),
            });
            syncSessionSummary(payload);
            await loadSession(payload.id);
        });

        elements.renameSession?.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            const nextTitle = window.prompt(ui.rename_prompt || "输入新的会话标题", state.currentSession?.title || "");
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
            elements.sessionTitle.textContent = payload.title || ui.untitled_session || "未命名会话";
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
                    body: JSON.stringify({ title: ui.new_session || "新建写作会话" }),
                });
                syncSessionSummary(created);
                state.sessions = [created];
            }
            await loadSession(state.sessions[0].id);
        });

        elements.send?.addEventListener("click", () => sendMessage());
    }

    async function sendMessage() {
        const topic = elements.topic?.value?.trim() || "";
        const targetWordCount = Number(elements.targetWordCount?.value || 0);
        const extraRequirements = elements.extraRequirements?.value?.trim() || "";
        if (!topic || !state.currentSessionId || state.sending) {
            return;
        }

        closeStream();
        state.stageFeed = [];
        state.currentSession.turns.push({
            id: `local-${Date.now()}`,
            role: "user",
            content: `Topic: ${topic}\nTarget Word Count: ${targetWordCount}${extraRequirements ? `\nExtra Requirements: ${extraRequirements}` : ""}`,
            trace: {},
            created_at: new Date().toISOString(),
        });
        state.sending = true;
        setButtonBusy(elements.send, true, ui.sending || "写作中...");
        if (elements.topic) {
            elements.topic.disabled = true;
        }
        if (elements.targetWordCount) {
            elements.targetWordCount.disabled = true;
        }
        if (elements.extraRequirements) {
            elements.extraRequirements.disabled = true;
        }
        renderAll();

        try {
            const payload = await fetchJson(
                `/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}/messages`,
                {
                    method: "POST",
                    body: JSON.stringify({
                        topic,
                        target_word_count: targetWordCount,
                        extra_requirements: extraRequirements || null,
                    }),
                }
            );
            openStream(payload.stream_id);
        } catch (error) {
            appendLocalError(error.message);
            restoreComposer();
            renderAll();
        }
    }

    function openStream(streamId) {
        const source = new EventSource(
            `/api/projects/${state.projectId}/writing/sessions/${state.currentSessionId}/streams/${streamId}`
        );
        state.eventSource = source;

        source.addEventListener("stage", (event) => {
            const payload = safeParseJson(event.data, {});
            state.stageFeed.push(payload);
            renderStageFeed();
        });

        source.addEventListener("done", async (event) => {
            const payload = safeParseJson(event.data, {});
            state.stageFeed.push({ stage: "done", label: "Final text ready", result: payload });
            renderStageFeed();
            closeStream();
            await loadSession(state.currentSessionId);
        });

        source.addEventListener("status", (event) => {
            const payload = safeParseJson(event.data, {});
            state.stageFeed.push(payload);
            renderStageFeed();
        });

        source.addEventListener("error", async (event) => {
            if (!event.data) {
                return;
            }
            const payload = safeParseJson(event.data, {});
            closeStream();
            appendLocalError(payload.message || ui.execution_failed || "写作失败");
            await loadSession(state.currentSessionId);
        });

        source.onerror = async () => {
            if (state.eventSource !== source) {
                return;
            }
            closeStream();
            appendLocalError(ui.connection_interrupted || "连接中断");
            await loadSession(state.currentSessionId);
        };
    }

    async function loadSession(sessionId) {
        const payload = await fetchJson(`/api/projects/${state.projectId}/writing/sessions/${sessionId}`);
        state.currentSessionId = sessionId;
        state.currentSession = payload;
        syncSessionSummary(payload);
        restoreComposer();
        renderAll();
    }

    function renderAll() {
        renderGuide();
        renderSessions();
        renderChat();
        renderDocuments();
        renderStageFeed();
        elements.sessionTitle.textContent = state.currentSession?.title || ui.untitled_session || "未命名会话";
    }

    function renderGuide() {
        if (!elements.guidePill) {
            return;
        }
        if (state.guide.status === "published") {
            elements.guidePill.textContent = ui.guide_published || "当前使用已发布指南。";
            return;
        }
        if (state.guide.status === "draft") {
            elements.guidePill.textContent = ui.guide_unpublished || "当前使用未发布指南。";
            return;
        }
        elements.guidePill.textContent = ui.guide_missing || "还没有 writing_guide。";
    }

    function renderSessions() {
        elements.sessionList.innerHTML = "";
        state.sessions.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `session-item ${item.id === state.currentSessionId ? "is-active" : ""}`;
            button.innerHTML = `<strong>${escapeHtml(item.title || ui.untitled_session || "未命名会话")}</strong><small>${item.turn_count || 0} 轮消息</small>`;
            button.addEventListener("click", () => loadSession(item.id));
            elements.sessionList.appendChild(button);
        });
    }

    function renderChat() {
        elements.chatList.innerHTML = "";
        const turns = state.currentSession?.turns || [];
        if (!turns.length) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = ui.empty_turns || "还没有写作任务。";
            elements.chatList.appendChild(empty);
            return;
        }
        turns.forEach((turn) => {
            elements.chatList.appendChild(renderTurn(turn));
        });
    }

    function renderTurn(turn) {
        const row = document.createElement("article");
        row.className = `chat-row chat-row--${turn.role === "user" ? "user" : "assistant"}`;

        if (turn.role === "assistant") {
            normalizeTraceBlocks(turn.trace || {}).forEach((block) => {
                row.appendChild(renderTraceBlock(block));
            });
        }

        const bubble = document.createElement("div");
        bubble.className = `chat-bubble chat-bubble--${turn.role === "user" ? "user" : "assistant"}`;
        if (turn.role === "assistant") {
            renderMarkdownInto(bubble, turn.content || "");
        } else {
            bubble.textContent = turn.content || "";
        }
        row.appendChild(bubble);

        const meta = document.createElement("div");
        meta.className = "chat-meta";
        meta.textContent = formatTime(turn.created_at);
        row.appendChild(meta);
        return row;
    }

    function renderTraceBlock(block) {
        if (block.type === "stage") {
            const pill = document.createElement("div");
            pill.className = "turn-pill";
            pill.textContent = block.label || block.stage || ui.working || "执行中...";
            return pill;
        }
        if (block.type === "review") {
            const details = document.createElement("details");
            details.className = "tool-call";
            details.innerHTML = `
                <summary><span>${escapeHtml(block.dimension || "review")}</span><span>score ${escapeHtml(String(block.score ?? ""))}</span></summary>
                <div class="tool-call__body">
                    <pre>${escapeHtml(JSON.stringify({
                        must_fix: block.must_fix || [],
                        keep: block.keep || [],
                    }, null, 2))}</pre>
                </div>
            `;
            return details;
        }
        if (block.type === "judge") {
            const details = document.createElement("details");
            details.className = "tool-call";
            details.innerHTML = `
                <summary><span>judge round ${escapeHtml(String(block.round || ""))}</span><span>${block.result?.pass ? "pass" : "revise"}</span></summary>
                <div class="tool-call__body"><pre>${escapeHtml(JSON.stringify(block.result || {}, null, 2))}</pre></div>
            `;
            return details;
        }
        const fallback = document.createElement("details");
        fallback.className = "tool-call";
        fallback.innerHTML = `
            <summary><span>${escapeHtml(block.type || "trace")}</span><span>detail</span></summary>
            <div class="tool-call__body"><pre>${escapeHtml(JSON.stringify(block, null, 2))}</pre></div>
        `;
        return fallback;
    }

    function renderStageFeed() {
        elements.stageFeedList.innerHTML = "";
        if (!state.stageFeed.length) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = ui.empty_turns || "还没有写作任务。";
            elements.stageFeedList.appendChild(empty);
            return;
        }
        state.stageFeed.forEach((item) => {
            const card = document.createElement("div");
            card.className = "context-card";
            const label = item.label || item.stage || item.message || "stage";
            card.innerHTML = `<strong>${escapeHtml(label)}</strong><small>${escapeHtml(JSON.stringify(item, null, 2))}</small>`;
            elements.stageFeedList.appendChild(card);
        });
    }

    function renderDocuments() {
        elements.documentList.innerHTML = "";
        if (!state.documents.length) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = ui.document_empty || "当前项目还没有可引用文档。";
            elements.documentList.appendChild(empty);
            return;
        }
        state.documents.forEach((item) => {
            const card = document.createElement("div");
            card.className = "context-card";
            card.innerHTML = `
                <strong>${escapeHtml(item.title || item.filename)}</strong>
                <small>${escapeHtml(item.source_type || "text")} · ${escapeHtml(item.ingest_status || "pending")}</small>
            `;
            elements.documentList.appendChild(card);
        });
    }

    function syncSessionSummary(detail) {
        const summary = {
            id: detail.id,
            title: detail.title,
            session_kind: detail.session_kind,
            created_at: detail.created_at,
            last_active_at: detail.last_active_at,
            turn_count: detail.turn_count,
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
        if (elements.topic) {
            elements.topic.disabled = false;
        }
        if (elements.targetWordCount) {
            elements.targetWordCount.disabled = false;
        }
        if (elements.extraRequirements) {
            elements.extraRequirements.disabled = false;
        }
        setButtonBusy(elements.send, false);
    }

    function appendLocalError(message) {
        state.currentSession.turns.push({
            id: `error-${Date.now()}`,
            role: "assistant",
            content: `${ui.execution_failed || "写作失败"}：\n\n${message}`,
            trace: { blocks: [{ type: "stage", label: ui.execution_failed || "写作失败" }] },
            created_at: new Date().toISOString(),
        });
    }

    function formatTime(value) {
        try {
            return new Intl.DateTimeFormat("zh-CN", {
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                minute: "2-digit",
            }).format(new Date(value));
        } catch {
            return value || "--";
        }
    }
}

function normalizeTraceBlocks(trace) {
    return Array.isArray(trace.blocks) ? trace.blocks : [];
}
