import {
    clampPercent,
    escapeHtml,
    fetchJson,
    formatDateTime,
    safeParseJson,
    setStatusTone,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("telegram-preprocess-bootstrap")?.textContent, {});

if (bootstrap?.project_id) {
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id,
        bundle: bootstrap.bundle || null,
        traceEvents: Array.isArray(bootstrap.bundle?.trace_events) ? [...bootstrap.bundle.trace_events] : [],
        liveOutputByRequest: {},
        activeRequestKey: null,
        stream: null,
        pollTimer: null,
    };

    const elements = {
        statusChip: document.getElementById("telegram-preprocess-status-chip"),
        stage: document.getElementById("telegram-preprocess-stage"),
        progressLabel: document.getElementById("telegram-preprocess-progress-label"),
        progressLabel2: document.querySelector(".progress-labels #telegram-preprocess-progress-label"),
        progressFill: document.getElementById("telegram-preprocess-progress-fill"),
        currentProgressLabel: document.getElementById("telegram-preprocess-current-progress-label"),
        currentProgressFill: document.getElementById("telegram-preprocess-current-progress-fill"),
        weeklyCandidateCount: document.getElementById("telegram-preprocess-weekly-candidate-count"),
        topUserCount: document.getElementById("telegram-preprocess-top-user-count"),
        topicCount: document.getElementById("telegram-preprocess-topic-count"),
        activeUserCount: document.getElementById("telegram-preprocess-active-user-count"),
        totalTokens: document.getElementById("telegram-preprocess-total-tokens"),
        cacheRead: document.getElementById("telegram-preprocess-cache-read"),
        runMeta: document.getElementById("telegram-preprocess-run-meta"),
        error: document.getElementById("telegram-preprocess-error"),
        traceList: document.getElementById("telegram-preprocess-trace-list"),
        liveTitle: document.getElementById("telegram-preprocess-live-title"),
        liveOutput: document.getElementById("telegram-preprocess-live-output"),
        weeklyCandidates: document.getElementById("telegram-preprocess-weekly-candidates"),
        topUsers: document.getElementById("telegram-preprocess-top-users"),
        topics: document.getElementById("telegram-preprocess-topics"),
        activeUsers: document.getElementById("telegram-preprocess-active-users"),
    };

    renderBundle(state.bundle);
    renderTraceList();
    renderLiveOutput();
    connectStream();

    function connectStream() {
        if (!state.runId) {
            return;
        }
        state.stream = new EventSource(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}/stream`);
        state.stream.addEventListener("snapshot", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            state.bundle = payload;
            mergePersistedTrace(payload.trace_events || []);
            renderBundle(payload);
            renderTraceList();
            if (!isRunning(payload.status)) {
                stopStream();
            }
        });
        state.stream.addEventListener("trace", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            handleTraceEvent(payload);
        });
        state.stream.addEventListener("done", async () => {
            stopStream();
            await refreshBundle();
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
        state.pollTimer = window.setInterval(async () => {
            await refreshBundle();
            if (!isRunning(state.bundle?.status)) {
                window.clearInterval(state.pollTimer);
                state.pollTimer = null;
            }
        }, 1800);
    }

    async function refreshBundle() {
        if (!state.runId) {
            return;
        }
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}`);
            state.bundle = payload;
            mergePersistedTrace(payload.trace_events || []);
            renderBundle(payload);
            renderTraceList();
            renderLiveOutput();
        } catch (error) {
            updateText(elements.liveTitle, error.message || "刷新预处理状态失败");
        }
    }

    function mergePersistedTrace(events) {
        const seen = new Set(state.traceEvents.map((item) => String(item.seq ?? `${item.timestamp}:${item.kind}:${item.request_key || ""}`)));
        (events || []).forEach((item) => {
            const key = String(item.seq ?? `${item.timestamp}:${item.kind}:${item.request_key || ""}`);
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            state.traceEvents.push(item);
        });
        state.traceEvents.sort((left, right) => {
            const leftSeq = Number(left.seq || 0);
            const rightSeq = Number(right.seq || 0);
            if (leftSeq && rightSeq) {
                return leftSeq - rightSeq;
            }
            return String(left.timestamp || "").localeCompare(String(right.timestamp || ""));
        });
    }

    function handleTraceEvent(event) {
        if (event.kind === "llm_delta") {
            if (event.request_key) {
                state.activeRequestKey = event.request_key;
                state.liveOutputByRequest[event.request_key] = String(event.text_preview || "");
            }
            renderLiveOutput(event);
            return;
        }
        mergePersistedTrace([event]);
        if (event.kind === "llm_request_started" && event.request_key) {
            state.activeRequestKey = event.request_key;
            state.liveOutputByRequest[event.request_key] = "";
        }
        if (event.kind === "llm_request_completed" && event.request_key) {
            state.activeRequestKey = event.request_key;
            state.liveOutputByRequest[event.request_key] = String(
                event.response_text_preview || state.liveOutputByRequest[event.request_key] || ""
            );
        }
        renderTraceList();
        renderLiveOutput(event);
    }

    function renderBundle(bundle) {
        if (!bundle) {
            setStatusTone(elements.statusChip, "idle", "idle");
            updateText(elements.stage, "等待开始");
            updateText(elements.progressLabel, "0%");
            if (elements.progressLabel2) updateText(elements.progressLabel2, "0%");
            if (elements.currentProgressLabel) updateText(elements.currentProgressLabel, "0%");
            updateText(elements.weeklyCandidateCount, "0");
            updateText(elements.topUserCount, "0");
            updateText(elements.topicCount, "0");
            updateText(elements.activeUserCount, "0");
            updateText(elements.totalTokens, "0");
            updateText(elements.cacheRead, "0");
            updateText(elements.runMeta, "暂无预处理 run");
            if (elements.progressFill) {
                elements.progressFill.style.width = "0%";
            }
            if (elements.currentProgressFill) {
                elements.currentProgressFill.style.width = "0%";
            }
            renderWeeklyCandidates([]);
            renderTopUsers([]);
            renderTopics([]);
            renderActiveUsers([]);
            return;
        }

        const percent = clampPercent(bundle.progress_percent || 0);
        setStatusTone(elements.statusChip, bundle.status, bundle.status);
        updateText(elements.stage, bundle.current_stage || "等待开始");
        updateText(elements.progressLabel, `${percent}%`);
        if (elements.progressLabel2) updateText(elements.progressLabel2, `${percent}%`);
        
        const candidatesCount = bundle.weekly_candidate_count || bundle.window_count || 0;
        const topicCount = bundle.topic_count || 0;
        updateText(elements.weeklyCandidateCount, candidatesCount);
        updateText(elements.topUserCount, bundle.top_user_count || 0);
        updateText(elements.topicCount, topicCount);

        let currentPercent = 0;
        if (candidatesCount > 0) {
            currentPercent = clampPercent(Math.floor((topicCount / candidatesCount) * 100));
        } else if (topicCount > 0) {
            currentPercent = 100;
        }
        if (elements.currentProgressLabel) updateText(elements.currentProgressLabel, `${currentPercent}%`);
        if (elements.currentProgressFill) elements.currentProgressFill.style.width = `${currentPercent}%`;
        updateText(elements.activeUserCount, bundle.active_user_count || 0);
        updateText(elements.totalTokens, bundle.total_tokens || 0);
        updateText(elements.cacheRead, bundle.cache_read_tokens || 0);
        updateText(
            elements.runMeta,
            `${formatDateTime(bundle.started_at || bundle.created_at)} -> ${formatDateTime(bundle.finished_at)}`
        );
        if (elements.progressFill) {
            elements.progressFill.style.width = `${percent}%`;
        }
        if (elements.error) {
            elements.error.hidden = !bundle.error_message;
            elements.error.textContent = bundle.error_message || "";
        }
        renderWeeklyCandidates(bundle.weekly_candidates || []);
        renderTopUsers(bundle.top_users || []);
        renderTopics(bundle.topics || []);
        renderActiveUsers(bundle.active_users || []);
    }

    function renderTraceList() {
        elements.traceList.innerHTML = "";
        const events = state.traceEvents.slice(-80);
        if (!events.length) {
            elements.traceList.innerHTML = `<div class="empty-panel"><strong>等待任务事件。</strong></div>`;
            return;
        }
        events.forEach((event) => {
            const card = document.createElement("article");
            card.className = "event-card compact-card";
            const title = summarizeTraceEvent(event);
            const meta = [
                event.stage || "",
                event.agent || "",
                event.week_key ? `week ${event.week_key}` : "",
                event.round_index ? `round ${event.round_index}` : "",
                event.tool_name ? `tool ${event.tool_name}` : "",
            ]
                .filter(Boolean)
                .join(" · ");
            const detailText = event.response_text_preview || event.output_preview || event.arguments_preview || event.prompt_preview || "";
            card.innerHTML = `
                <div class="project-card__title-row">
                    <strong>${escapeHtml(title)}</strong>
                    <span class="status-chip tone-${traceTone(event.kind)}">${escapeHtml(event.kind || "trace")}</span>
                </div>
                <p class="helper-text">${escapeHtml(meta || "--")}</p>
                <div class="event-card__meta top-gap">${escapeHtml(formatDateTime(event.timestamp))}</div>
                ${detailText ? `<pre class="trace-box top-gap">${escapeHtml(detailText)}</pre>` : ""}
            `;
            elements.traceList.appendChild(card);
        });
    }

    function renderLiveOutput(event = null) {
        const requestKey = event?.request_key || state.activeRequestKey;
        if (!requestKey) {
            updateText(elements.liveTitle, "等待新的 LLM 请求开始。");
            updateText(elements.liveOutput, "");
            return;
        }
        const text = state.liveOutputByRequest[requestKey] || "";
        updateText(elements.liveTitle, summarizeLiveTitle(event, requestKey));
        updateText(elements.liveOutput, text);
    }

    function renderWeeklyCandidates(candidates) {
        elements.weeklyCandidates.innerHTML = "";
        if (!candidates.length) {
            elements.weeklyCandidates.innerHTML = `<div class="empty-panel"><strong>当前 Run 还没有周话题候选结果。</strong></div>`;
            return;
        }
        candidates.forEach((candidate) => {
            const card = document.createElement("article");
            card.className = "document-card compact-card";
            const participants = (candidate.top_participants || [])
                .slice(0, 6)
                .map((item) => item.display_name || item.username || item.participant_id)
                .filter(Boolean)
                .join(" / ");
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>${escapeHtml(candidate.week_key || candidate.id)}</strong>
                    <span class="status-chip tone-processing">${escapeHtml(String(candidate.message_count || 0))} 条</span>
                </div>
                <p class="helper-text">${escapeHtml(formatDateTime(candidate.start_at))} -> ${escapeHtml(formatDateTime(candidate.end_at))}</p>
                <p class="helper-text">消息锚点: #${escapeHtml(String(candidate.start_message_id || "--"))} -> #${escapeHtml(String(candidate.end_message_id || "--"))}</p>
                <p class="helper-text">参与者: ${escapeHtml(String(candidate.participant_count || 0))}</p>
                ${participants ? `<p class="helper-text">${escapeHtml(participants)}</p>` : ""}
            `;
            elements.weeklyCandidates.appendChild(card);
        });
    }

    function renderTopUsers(users) {
        elements.topUsers.innerHTML = "";
        if (!users.length) {
            elements.topUsers.innerHTML = `<div class="empty-panel"><strong>当前 Run 还没有 SQL Top Users 结果。</strong></div>`;
            return;
        }
        users.forEach((user) => {
            const card = document.createElement("article");
            card.className = "document-card compact-card";
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>#${escapeHtml(String(user.rank || "--"))} · ${escapeHtml(user.display_name || user.username || user.uid || user.participant_id)}</strong>
                    <span class="status-chip tone-processing">${escapeHtml(String(user.message_count || 0))} 条</span>
                </div>
                <p class="helper-text">UID: ${escapeHtml(user.uid || "--")} · username: ${escapeHtml(user.username || "--")}</p>
                <p class="helper-text">${escapeHtml(formatDateTime(user.first_seen_at))} -> ${escapeHtml(formatDateTime(user.last_seen_at))}</p>
            `;
            elements.topUsers.appendChild(card);
        });
    }

    function renderTopics(topics) {
        elements.topics.innerHTML = "";
        if (!topics.length) {
            elements.topics.innerHTML = `<div class="empty-panel"><strong>当前 Run 还没有最终话题结果。</strong></div>`;
            return;
        }
        topics.forEach((topic) => {
            const card = document.createElement("article");
            card.className = "document-card";
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>${escapeHtml(topic.title || `Topic ${topic.topic_index || ""}`)}</strong>
                    <span class="status-chip tone-ready">${escapeHtml(String(topic.message_count || 0))} 条消息</span>
                </div>
                <p>${escapeHtml(topic.summary || "")}</p>
                <p class="helper-text">${escapeHtml(formatDateTime(topic.start_at))} -> ${escapeHtml(formatDateTime(topic.end_at))} · 参与者 ${escapeHtml(String(topic.participant_count || 0))}</p>
                <p class="helper-text">消息锚点: #${escapeHtml(String(topic.start_message_id || "--"))} -> #${escapeHtml(String(topic.end_message_id || "--"))}</p>
                ${topic.participants?.length ? `<p class="helper-text">${escapeHtml(topic.participants.slice(0, 8).map((item) => item.display_name || item.username || item.participant_id).join(" / "))}</p>` : ""}
                ${topic.keywords?.length ? `<p class="helper-text">${escapeHtml(topic.keywords.join(" · "))}</p>` : ""}
            `;
            elements.topics.appendChild(card);
        });
    }

    function renderActiveUsers(users) {
        elements.activeUsers.innerHTML = "";
        if (!users.length) {
            elements.activeUsers.innerHTML = `<div class="empty-panel"><strong>当前 Run 还没有活跃用户结果。</strong></div>`;
            return;
        }
        users.forEach((user) => {
            const card = document.createElement("article");
            card.className = "document-card";
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>#${escapeHtml(String(user.rank || "--"))} · ${escapeHtml(user.primary_alias || user.display_name || user.username || user.uid || user.participant_id)}</strong>
                    <span class="status-chip tone-processing">${escapeHtml(String(user.message_count || 0))} 条</span>
                </div>
                <p class="helper-text">UID: ${escapeHtml(user.uid || "--")} · username: ${escapeHtml(user.username || "--")}</p>
                <p class="helper-text">display_name: ${escapeHtml(user.display_name || "--")}</p>
                ${user.aliases?.length ? `<p class="helper-text">别名: ${escapeHtml(user.aliases.join(" / "))}</p>` : ""}
                <p class="helper-text">${escapeHtml(formatDateTime(user.first_seen_at))} -> ${escapeHtml(formatDateTime(user.last_seen_at))}</p>
            `;
            elements.activeUsers.appendChild(card);
        });
    }

    function summarizeTraceEvent(event) {
        if (event.message) {
            return event.message;
        }
        const mapping = {
            agent_started: "Agent 开始执行",
            agent_completed: "Agent 完成",
            agent_retry: "Agent 重试",
            stage_progress: "阶段进展",
            llm_request_started: "LLM 请求开始",
            llm_request_completed: "LLM 请求完成",
            tool_call: "Tool 调用",
            tool_result: "Tool 返回",
            run_completed: "预处理完成",
            run_failed: "预处理失败",
        };
        return mapping[event.kind] || event.kind || "trace";
    }

    function summarizeLiveTitle(event, requestKey) {
        if (event?.label) {
            return `${event.label} · ${requestKey}`;
        }
        const latestTrace = [...state.traceEvents].reverse().find((item) => item.request_key === requestKey);
        if (latestTrace?.label) {
            return `${latestTrace.label} · ${requestKey}`;
        }
        return requestKey;
    }

    function traceTone(kind) {
        if (kind === "run_failed") {
            return "failed";
        }
        if (kind === "run_completed" || kind === "agent_completed") {
            return "ready";
        }
        return "processing";
    }

    function isRunning(status) {
        return status === "queued" || status === "running";
    }
}
