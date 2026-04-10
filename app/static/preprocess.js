document.addEventListener("DOMContentLoaded", () => {
    const shell = document.querySelector("[data-preprocess-shell]");
    if (!shell) {
        return;
    }

    const bootstrap = JSON.parse(document.getElementById("preprocess-bootstrap").textContent || "{}");
    const state = {
        projectId: shell.dataset.projectId,
        sessions: bootstrap.sessions || [],
        currentSessionId: bootstrap.selected_session_id || null,
        currentSession: bootstrap.selected_session || null,
        documents: bootstrap.documents || [],
        liveAssistantText: "",
        liveAssistantNode: null,
        liveToolDetails: null,
        eventSource: null,
    };

    const elements = {
        sessionList: shell.querySelector("[data-session-list]"),
        sessionTitle: shell.querySelector("[data-session-title]"),
        chatList: shell.querySelector("[data-chat-list]"),
        documentList: shell.querySelector("[data-document-list]"),
        artifactList: shell.querySelector("[data-artifact-list]"),
        contextPills: shell.querySelector("[data-context-pills]"),
        composer: shell.querySelector("[data-composer]"),
        sendButton: shell.querySelector("[data-send-message]"),
        suggestions: shell.querySelector("[data-mention-suggestions]"),
        newSession: shell.querySelector("[data-new-session]"),
        renameSession: shell.querySelector("[data-rename-session]"),
        deleteSession: shell.querySelector("[data-delete-session]"),
    };

    if (bootstrap.initial_mention) {
        elements.composer.value = normalizeMention(bootstrap.initial_mention) + " ";
    }

    renderAll();
    bindEvents();

    function bindEvents() {
        elements.newSession.addEventListener("click", async () => {
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions`, {
                method: "POST",
                body: JSON.stringify({ title: "New Preprocess Session" }),
            });
            state.sessions.unshift(payload);
            await loadSession(payload.id);
        });

        elements.renameSession.addEventListener("click", async () => {
            if (!state.currentSession) {
                return;
            }
            const nextTitle = window.prompt("输入新的会话标题", state.currentSession.title || "");
            if (nextTitle === null) {
                return;
            }
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}`, {
                method: "PATCH",
                body: JSON.stringify({ title: nextTitle }),
            });
            syncSessionSummary(payload);
            state.currentSession.title = payload.title;
            renderAll();
        });

        elements.deleteSession.addEventListener("click", async () => {
            if (!state.currentSessionId || !window.confirm("确定删除这个预分析会话吗？")) {
                return;
            }
            await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}`, {
                method: "DELETE",
            });
            state.sessions = state.sessions.filter((item) => item.id !== state.currentSessionId);
            if (!state.sessions.length) {
                const created = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions`, {
                    method: "POST",
                    body: JSON.stringify({ title: "New Preprocess Session" }),
                });
                state.sessions = [created];
            }
            await loadSession(state.sessions[0].id);
        });

        elements.sendButton.addEventListener("click", sendMessage);
        elements.composer.addEventListener("keydown", (event) => {
            if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                sendMessage();
            }
        });
        elements.composer.addEventListener("input", handleMentionAutocomplete);
    }

    async function sendMessage() {
        const message = elements.composer.value.trim();
        if (!message || !state.currentSessionId) {
            return;
        }
        closeStream();
        elements.composer.value = "";
        hideSuggestions();
        state.currentSession.turns.push({
            id: `local-user-${Date.now()}`,
            role: "user",
            content: message,
            trace: {},
            created_at: new Date().toISOString(),
        });
        renderChat();
        addContextPill("检索中...");
        const payload = await fetchJson(
            `/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}/messages`,
            {
                method: "POST",
                body: JSON.stringify({ message }),
            }
        );
        openStream(payload.stream_id);
    }

    function openStream(streamId) {
        const source = new EventSource(
            `/api/projects/${state.projectId}/preprocess/sessions/${state.currentSessionId}/streams/${streamId}`
        );
        state.eventSource = source;
        state.liveAssistantText = "";
        state.liveAssistantNode = null;
        state.liveToolDetails = null;

        source.addEventListener("status", (event) => {
            const payload = JSON.parse(event.data);
            addContextPill(payload.label);
            ensureLiveAssistantRow();
        });
        source.addEventListener("tool_call", (event) => {
            const payload = JSON.parse(event.data);
            ensureLiveAssistantRow();
            state.liveToolDetails = appendToolCall(elements.chatList, payload.name, payload.arguments);
            scrollChatToBottom();
        });
        source.addEventListener("tool_result", (event) => {
            const payload = JSON.parse(event.data);
            ensureLiveAssistantRow();
            if (state.liveToolDetails) {
                const body = state.liveToolDetails.querySelector("pre");
                body.textContent = JSON.stringify(payload.output, null, 2);
            } else {
                appendToolCall(elements.chatList, payload.name, payload.output, true);
            }
            scrollChatToBottom();
        });
        source.addEventListener("assistant_delta", (event) => {
            const payload = JSON.parse(event.data);
            ensureLiveAssistantRow();
            state.liveAssistantText += payload.delta || "";
            renderMarkdownInto(state.liveAssistantNode, state.liveAssistantText);
            scrollChatToBottom();
        });
        source.addEventListener("assistant_done", async () => {
            closeStream();
            await loadSession(state.currentSessionId);
        });
        source.addEventListener("error", async (event) => {
            closeStream();
            if (event?.data) {
                addContextPill("执行失败");
            }
            await loadSession(state.currentSessionId);
        });
    }

    async function loadSession(sessionId) {
        const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/sessions/${sessionId}`);
        state.currentSessionId = sessionId;
        state.currentSession = payload;
        syncSessionSummary(payload);
        renderAll();
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

    function renderAll() {
        renderSessions();
        renderChat();
        renderDocuments();
        renderArtifacts();
        elements.sessionTitle.textContent = state.currentSession?.title || "Untitled Session";
    }

    function renderSessions() {
        elements.sessionList.innerHTML = "";
        state.sessions.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `session-item ${item.id === state.currentSessionId ? "is-active" : ""}`;
            button.innerHTML = `<strong>${escapeHtml(item.title || "Untitled Session")}</strong><small>${item.turn_count || 0} turns</small>`;
            button.addEventListener("click", () => loadSession(item.id));
            elements.sessionList.appendChild(button);
        });
    }

    function renderChat() {
        elements.chatList.innerHTML = "";
        clearContextPills();
        (state.currentSession?.turns || []).forEach((turn) => {
            elements.chatList.appendChild(renderTurn(turn));
        });
        scrollChatToBottom();
    }

    function renderTurn(turn) {
        const row = document.createElement("article");
        row.className = `chat-row chat-row--${turn.role === "user" ? "user" : "assistant"}`;
        if (turn.role === "assistant") {
            const traceBlocks = normalizeTraceBlocks(turn.trace || {});
            traceBlocks.forEach((block) => row.appendChild(renderTraceBlock(block)));
        }
        const bubble = document.createElement("div");
        bubble.className = `chat-bubble chat-bubble--${turn.role === "user" ? "user" : "assistant"}`;
        if (turn.role === "user") {
            bubble.textContent = turn.content;
        } else {
            renderMarkdownInto(bubble, turn.content || "");
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
            pill.textContent = block.label || block.message || "处理中";
            return pill;
        }
        if (block.type === "artifact") {
            const card = document.createElement("a");
            card.className = "artifact-card";
            card.href = `/api/projects/${state.projectId}/preprocess/artifacts/${block.id}/download`;
            card.innerHTML = `<strong>${escapeHtml(block.filename || "artifact")}</strong><small>${escapeHtml(block.summary || "生成文件")}</small>`;
            return card;
        }
        const details = document.createElement("details");
        details.className = "tool-call";
        const summary = document.createElement("summary");
        summary.innerHTML = `<span>&gt;_ ${escapeHtml(block.name || block.type)}</span><span>${block.type === "tool_call" ? "pending" : "done"}</span>`;
        const body = document.createElement("div");
        body.className = "tool-call__body";
        const pre = document.createElement("pre");
        pre.textContent = JSON.stringify(block.output || block.arguments || block, null, 2);
        body.appendChild(pre);
        details.appendChild(summary);
        details.appendChild(body);
        return details;
    }

    function renderDocuments() {
        elements.documentList.innerHTML = "";
        state.documents.forEach((document) => {
            const card = document.createElement("div");
            card.className = "context-card";
            const mention = normalizeMention(document.filename);
            card.innerHTML = `<strong>${escapeHtml(document.title || document.filename)}</strong><small>${escapeHtml(document.filename)}</small>`;
            const button = document.createElement("button");
            button.type = "button";
            button.className = "secondary-button top-gap";
            button.textContent = `插入 ${mention}`;
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
            empty.textContent = "当前会话还没有生成文件。";
            elements.artifactList.appendChild(empty);
            return;
        }
        artifacts.forEach((artifact) => {
            const link = document.createElement("a");
            link.className = "artifact-card";
            link.href = artifact.download_url;
            link.innerHTML = `<strong>${escapeHtml(artifact.filename)}</strong><small>${escapeHtml(artifact.summary || "下载产物")}</small>`;
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
        const textarea = elements.composer;
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const before = textarea.value.slice(0, start);
        const after = textarea.value.slice(end);
        const spacer = before && !before.endsWith(" ") && !before.endsWith("\n") ? " " : "";
        textarea.value = `${before}${spacer}${mention} ${after}`;
        textarea.focus();
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

    function addContextPill(label) {
        const pill = document.createElement("div");
        pill.className = "turn-pill";
        pill.textContent = label;
        elements.contextPills.innerHTML = "";
        elements.contextPills.appendChild(pill);
    }

    function clearContextPills() {
        elements.contextPills.innerHTML = "";
    }

    function appendToolCall(container, name, payload, isResult = false) {
        const details = document.createElement("details");
        details.className = "tool-call";
        details.open = !!isResult;
        details.innerHTML = `<summary><span>&gt;_ ${escapeHtml(name)}</span><span>${isResult ? "done" : "calling"}</span></summary><div class="tool-call__body"><pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre></div>`;
        container.appendChild(details);
        return details;
    }

    function closeStream() {
        if (state.eventSource) {
            state.eventSource.close();
            state.eventSource = null;
        }
        state.liveAssistantNode = null;
        state.liveAssistantText = "";
        state.liveToolDetails = null;
    }

    function scrollChatToBottom() {
        elements.chatList.parentElement.scrollTop = elements.chatList.parentElement.scrollHeight;
    }
});

async function fetchJson(url, options = {}) {
    const response = await fetch(url, {
        headers: { "Content-Type": "application/json", ...(options.headers || {}) },
        ...options,
    });
    const payload = await response.json();
    if (!response.ok) {
        throw new Error(payload.detail || "Request failed");
    }
    return payload;
}

function normalizeTraceBlocks(trace) {
    const blocks = Array.isArray(trace.blocks) ? trace.blocks : [];
    const normalized = [];
    for (let index = 0; index < blocks.length; index += 1) {
        const current = blocks[index];
        const next = blocks[index + 1];
        if (
            current?.type === "tool_call" &&
            next?.type === "tool_result" &&
            current.name === next.name
        ) {
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

function renderMarkdownInto(node, source) {
    node.innerHTML = markdownToHtml(source || "");
    node.classList.add("markdown-body");
    node.querySelectorAll("[data-copy-code]").forEach((button) => {
        button.addEventListener("click", async () => {
            const pre = button.parentElement.querySelector("pre");
            await navigator.clipboard.writeText(pre.textContent || "");
            button.textContent = "Copied";
            window.setTimeout(() => {
                button.textContent = "Copy";
            }, 1200);
        });
    });
}

function markdownToHtml(source) {
    const codeBlocks = [];
    let text = source.replace(/```([\w-]+)?\n([\s\S]*?)```/g, (_, lang, code) => {
        const placeholder = `__CODE_BLOCK_${codeBlocks.length}__`;
        codeBlocks.push(
            `<div class="md-code"><button type="button" class="secondary-button" data-copy-code>Copy</button><pre><code data-lang="${escapeHtml(lang || "")}">${escapeHtml(code.trimEnd())}</code></pre></div>`
        );
        return placeholder;
    });
    const sections = text.split(/\n{2,}/).filter(Boolean);
    const html = sections
        .map((section) => {
            const trimmed = section.trim();
            if (/^###\s+/.test(trimmed)) {
                return `<h3>${renderInline(trimmed.replace(/^###\s+/, ""))}</h3>`;
            }
            if (/^##\s+/.test(trimmed)) {
                return `<h2>${renderInline(trimmed.replace(/^##\s+/, ""))}</h2>`;
            }
            if (/^#\s+/.test(trimmed)) {
                return `<h1>${renderInline(trimmed.replace(/^#\s+/, ""))}</h1>`;
            }
            if (/^[-*]\s+/m.test(trimmed)) {
                const items = trimmed
                    .split("\n")
                    .filter((line) => /^[-*]\s+/.test(line.trim()))
                    .map((line) => `<li>${renderInline(line.trim().replace(/^[-*]\s+/, ""))}</li>`)
                    .join("");
                return `<ul>${items}</ul>`;
            }
            return `<p>${renderInline(trimmed).replace(/\n/g, "<br>")}</p>`;
        })
        .join("");
    return restoreCodeBlocks(html, codeBlocks);
}

function restoreCodeBlocks(html, codeBlocks) {
    let nextHtml = html;
    codeBlocks.forEach((block, index) => {
        nextHtml = nextHtml.replace(`__CODE_BLOCK_${index}__`, block);
    });
    return nextHtml;
}

function renderInline(text) {
    return escapeHtml(text).replace(/`([^`]+)`/g, "<code>$1</code>");
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
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
        return value;
    }
}

function normalizeMention(filename) {
    if (filename.startsWith("@")) {
        return filename;
    }
    return /\s/.test(filename) ? `@"${filename}"` : `@${filename}`;
}

function getCurrentMentionToken(textarea) {
    const before = textarea.value.slice(0, textarea.selectionStart);
    const match = before.match(/(?:^|\s)@(?:"([^"]*)|([^\s@"]*))$/);
    if (!match) {
        return null;
    }
    return {
        query: match[1] || match[2] || "",
        raw: match[0].trimStart(),
        start: textarea.selectionStart - match[0].trimStart().length,
        end: textarea.selectionStart,
    };
}

function replaceCurrentMention(textarea, token, mention) {
    textarea.value = `${textarea.value.slice(0, token.start)}${mention} ${textarea.value.slice(token.end)}`;
    textarea.focus();
}

function hideSuggestions() {
    const popup = document.querySelector("[data-mention-suggestions]");
    if (!popup) {
        return;
    }
    popup.hidden = true;
    popup.innerHTML = "";
}
