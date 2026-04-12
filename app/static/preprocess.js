import {
    escapeHtml,
    fetchJson,
    renderMarkdownInto,
    safeParseJson,
    setButtonBusy,
    shouldAutoScroll,
} from "./shared.js";

const shell = document.querySelector("[data-preprocess-shell]");

if (shell) {
    const bootstrap = JSON.parse(document.getElementById("preprocess-bootstrap")?.textContent || "{}");
    const ui = bootstrap.ui_strings || {};

    const state = {
        projectId: shell.dataset.projectId,
        sessions: bootstrap.sessions || [],
        currentSessionId: bootstrap.selected_session_id || null,
        currentSession: bootstrap.selected_session || null,
        documents: bootstrap.documents || [],
        eventSource: null,
        liveAssistantText: "",
        liveAssistantNode: null,
        liveToolNode: null,
        sending: false,
    };

    const elements = {
        sessionList: shell.querySelector("[data-session-list]"),
        sessionTitle: shell.querySelector("[data-session-title]"),
        chatList: shell.querySelector("[data-chat-list]"),
        documentList: shell.querySelector("[data-document-list]"),
        artifactList: shell.querySelector("[data-artifact-list]"),
        pills: shell.querySelector("[data-context-pills]"),
        composer: shell.querySelector("[data-composer]"),
        composerHint: shell.querySelector("[data-composer-hint]"),
        send: shell.querySelector("[data-send-message]"),
        suggestions: shell.querySelector("[data-mention-suggestions]"),
        newSession: shell.querySelector("[data-new-session]"),
        renameSession: shell.querySelector("[data-rename-session]"),
        deleteSession: shell.querySelector("[data-delete-session]"),
    };

    elements.send.textContent = ui.send || "发送";
    if (bootstrap.initial_mention) {
        elements.composer.value = `${normalizeMention(bootstrap.initial_mention)} `;
    }

    bindEvents();
    renderAll();

    function bindEvents() {
        elements.newSession.addEventListener("click", async () => {
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions`, {
                method: "POST",
                body: JSON.stringify({ title: ui.new_session || "新建会话" }),
            });
            syncSessionSummary(payload);
            await loadSession(payload.id);
        });

        elements.renameSession.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            const nextTitle = window.prompt(ui.rename_prompt || "输入新的会话标题", state.currentSession?.title || "");
            if (nextTitle === null) {
                return;
            }
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}`, {
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

        elements.deleteSession.addEventListener("click", async () => {
            if (!state.currentSessionId) {
                return;
            }
            if (!window.confirm(ui.common?.confirm_delete_session || "确定删除这个会话吗？")) {
                return;
            }
            await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}`, {
                method: "DELETE",
            });
            state.sessions = state.sessions.filter((item) => item.id !== state.currentSessionId);
            if (!state.sessions.length) {
                const created = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions`, {
                    method: "POST",
                    body: JSON.stringify({ title: ui.new_session || "新建会话" }),
                });
                syncSessionSummary(created);
                state.sessions = [created];
            }
            await loadSession(state.sessions[0].id);
        });

        elements.send.addEventListener("click", () => sendMessage());
        elements.composer.addEventListener("keydown", (event) => {
            if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                void sendMessage();
            }
        });
        elements.composer.addEventListener("input", () => {
            void handleMentionAutocomplete();
        });
    }

    async function sendMessage() {
        const message = elements.composer.value.trim();
        if (!message || !state.currentSessionId || state.sending) {
            return;
        }

        closeStream();
        const chatShouldStick = shouldAutoScroll(elements.chatList.parentElement);
        state.currentSession.turns.push({
            id: `local-${Date.now()}`,
            role: "user",
            content: message,
            trace: {},
            created_at: new Date().toISOString(),
        });
        elements.composer.value = "";
        hideSuggestions();
        state.sending = true;
        setButtonBusy(elements.send, true, ui.sending || "发送中…");
        elements.composer.disabled = true;
        renderChat();
        addContextPill(ui.working || "处理中…");
        if (chatShouldStick) {
            scrollChatToBottom();
        }

        try {
            const payload = await fetchJson(
                `/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}/messages`,
                {
                    method: "POST",
                    body: JSON.stringify({ message }),
                }
            );
            openStream(payload.stream_id);
        } catch (error) {
            appendLocalError(error.message);
            restoreComposer();
            renderChat();
        }
    }

    function openStream(streamId) {
        state.liveAssistantText = "";
        state.liveAssistantNode = null;
        state.liveToolNode = null;

        const source = new EventSource(
            `/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}/streams/${streamId}`
        );
        state.eventSource = source;

        source.addEventListener("status", (event) => {
            const payload = safeParseJson(event.data, {});
            addContextPill(payload.label || ui.ready_context || "准备上下文");
            ensureLiveAssistantRow();
        });

        source.addEventListener("tool_call", (event) => {
            const payload = safeParseJson(event.data, {});
            ensureLiveAssistantRow();
            state.liveToolNode = appendToolBlock(payload.name || "tool_call", payload.arguments, false);
            scrollChatToBottom();
        });

        source.addEventListener("tool_result", (event) => {
            const payload = safeParseJson(event.data, {});
            ensureLiveAssistantRow();
            if (state.liveToolNode) {
                const body = state.liveToolNode.querySelector("pre");
                if (body) {
                    body.textContent = JSON.stringify(payload.output, null, 2);
                }
            } else {
                appendToolBlock(payload.name || "tool_result", payload.output, true);
            }
            scrollChatToBottom();
        });

        source.addEventListener("assistant_delta", (event) => {
            const payload = safeParseJson(event.data, {});
            ensureLiveAssistantRow();
            state.liveAssistantText += payload.delta || "";
            renderMarkdownInto(state.liveAssistantNode, state.liveAssistantText);
            scrollChatToBottom();
        });

        source.addEventListener("assistant_done", async () => {
            closeStream();
            await loadSession(state.currentSessionId);
        });

        const handleFailure = async (message) => {
            closeStream();
            appendLocalError(message || ui.execution_failed || "执行失败");
            await loadSession(state.currentSessionId);
        };

        source.addEventListener("stream_error", async (event) => {
            const payload = safeParseJson(event.data, {});
            await handleFailure(payload.message);
        });

        source.onerror = async () => {
            if (state.eventSource !== source) {
                return;
            }
            await handleFailure(ui.connection_interrupted || "连接已中断");
        };
    }

    async function loadSession(sessionId) {
        const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${sessionId}`);
        state.currentSessionId = sessionId;
        state.currentSession = payload;
        syncSessionSummary(payload);
        restoreComposer();
        renderAll();
    }

    function renderAll() {
        renderSessions();
        renderChat();
        renderDocuments();
        renderArtifacts();
        elements.sessionTitle.textContent = state.currentSession?.title || ui.untitled_session || "未命名会话";
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
        clearPills();
        (state.currentSession?.turns || []).forEach((turn) => {
            elements.chatList.appendChild(renderTurn(turn));
        });
        scrollChatToBottom();
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
        if (block.type === "status") {
            const pill = document.createElement("div");
            pill.className = "turn-pill";
            pill.textContent = block.label || block.message || ui.working || "处理中…";
            return pill;
        }
        if (block.type === "artifact") {
            const card = document.createElement("a");
            card.className = "artifact-card";
            card.href = `/api/projects/${state.projectId}/preprocess/artifacts/${block.id}/download`;
            card.innerHTML = `<strong>${escapeHtml(block.filename || "artifact")}</strong><small>${escapeHtml(block.summary || ui.artifacts || "生成文件")}</small>`;
            return card;
        }
        const details = document.createElement("details");
        details.className = "tool-call";
        details.innerHTML = `
            <summary><span>&gt;_ ${escapeHtml(block.name || block.type)}</span><span>${block.type === "tool_call" ? "调用中" : "已完成"}</span></summary>
            <div class="tool-call__body"><pre>${escapeHtml(JSON.stringify(block.output || block.arguments || block, null, 2))}</pre></div>
        `;
        return details;
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
            const mention = normalizeMention(item.filename);
            card.innerHTML = `<strong>${escapeHtml(item.title || item.filename)}</strong><small>${escapeHtml(item.filename)}</small>`;
            const button = document.createElement("button");
            button.type = "button";
            button.className = "ghost-button top-gap";
            button.textContent = `${ui.mention_insert || "插入"} ${mention}`;
            button.addEventListener("click", () => insertMention(mention));
            card.appendChild(button);
            elements.documentList.appendChild(card);
        });
    }

    function renderArtifacts() {
        elements.artifactList.innerHTML = "";
        const artifacts = state.currentSession?.artifacts || [];
        if (!artifacts.length) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = ui.artifact_empty || "当前会话还没有生成文件。";
            elements.artifactList.appendChild(empty);
            return;
        }
        artifacts.forEach((artifact) => {
            const link = document.createElement("a");
            link.className = "artifact-card";
            link.href = artifact.download_url;
            link.innerHTML = `<strong>${escapeHtml(artifact.filename)}</strong><small>${escapeHtml(artifact.summary || "下载文件")}</small>`;
            elements.artifactList.appendChild(link);
        });
    }

    async function handleMentionAutocomplete() {
        const token = getCurrentMentionToken(elements.composer);
        if (!token) {
            hideSuggestions();
            return;
        }
        const payload = await fetchJson(`/api/projects/${state.projectId}/documents/mentions?q=${encodeURIComponent(token.query)}`);
        if (!payload.items?.length) {
            hideSuggestions();
            return;
        }

        elements.suggestions.innerHTML = "";
        payload.items.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "mention-suggestion";
            button.innerHTML = `<strong>${escapeHtml(item.title || item.filename)}</strong><small>${escapeHtml(item.filename)}</small>`;
            button.addEventListener("click", () => {
                replaceCurrentMention(elements.composer, token, normalizeMention(item.filename));
                hideSuggestions();
            });
            elements.suggestions.appendChild(button);
        });
        elements.suggestions.hidden = false;
    }

    function insertMention(mention) {
        const before = elements.composer.value;
        elements.composer.value = `${before}${before.endsWith(" ") || !before ? "" : " "}${mention} `;
        elements.composer.focus();
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

    function ensureLiveAssistantRow() {
        if (state.liveAssistantNode) {
            return;
        }
        const row = document.createElement("article");
        row.className = "chat-row chat-row--assistant";
        const bubble = document.createElement("div");
        bubble.className = "chat-bubble chat-bubble--assistant";
        row.appendChild(bubble);
        elements.chatList.appendChild(row);
        state.liveAssistantNode = bubble;
    }

    function appendToolBlock(name, payload, done) {
        const details = document.createElement("details");
        details.className = "tool-call";
        details.open = !!done;
        details.innerHTML = `
            <summary><span>&gt;_ ${escapeHtml(name || "tool")}</span><span>${done ? "已完成" : "调用中"}</span></summary>
            <div class="tool-call__body"><pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre></div>
        `;
        elements.chatList.appendChild(details);
        return details;
    }

    function addContextPill(label) {
        const pill = document.createElement("div");
        pill.className = "turn-pill";
        pill.textContent = label;
        elements.pills.innerHTML = "";
        elements.pills.appendChild(pill);
    }

    function clearPills() {
        elements.pills.innerHTML = "";
    }

    function closeStream() {
        if (state.eventSource) {
            state.eventSource.close();
            state.eventSource = null;
        }
        state.liveAssistantNode = null;
        state.liveToolNode = null;
        state.liveAssistantText = "";
    }

    function restoreComposer() {
        closeStream();
        state.sending = false;
        elements.composer.disabled = false;
        setButtonBusy(elements.send, false);
    }

    function appendLocalError(message) {
        state.currentSession.turns.push({
            id: `error-${Date.now()}`,
            role: "assistant",
            content: `${ui.execution_failed || "执行失败"}：\n\n${message}`,
            trace: { blocks: [{ type: "status", label: ui.execution_failed || "执行失败" }] },
            created_at: new Date().toISOString(),
        });
    }

    function hideSuggestions() {
        elements.suggestions.hidden = true;
        elements.suggestions.innerHTML = "";
    }

    function scrollChatToBottom() {
        const container = elements.chatList.parentElement;
        container.scrollTop = container.scrollHeight;
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
    const blocks = Array.isArray(trace.blocks) ? trace.blocks : [];
    const normalized = [];
    for (let index = 0; index < blocks.length; index += 1) {
        const current = blocks[index];
        const next = blocks[index + 1];
        if (current?.type === "tool_call" && next?.type === "tool_result" && current.name === next.name) {
            normalized.push({
                type: "tool_result",
                name: current.name,
                arguments: current.arguments,
                output: next.output,
            });
            index += 1;
            continue;
        }
        normalized.push(current);
    }
    return normalized;
}

function normalizeMention(filename) {
    return /\s/.test(filename) ? `@"${filename}"` : `@${filename}`;
}

function getCurrentMentionToken(textarea) {
    const cursor = textarea.selectionStart;
    const before = textarea.value.slice(0, cursor);
    const match = before.match(/(?:^|\s)(@(?:"[^"]*|[^\s@]*))$/);
    if (!match) {
        return null;
    }
    const raw = match[1];
    const query = raw.startsWith('@"') ? raw.slice(2) : raw.slice(1);
    return {
        raw,
        query,
        start: cursor - raw.length,
        end: cursor,
    };
}

function replaceCurrentMention(textarea, token, replacement) {
    textarea.value = `${textarea.value.slice(0, token.start)}${replacement}${textarea.value.slice(token.end)} `;
    textarea.focus();
}
