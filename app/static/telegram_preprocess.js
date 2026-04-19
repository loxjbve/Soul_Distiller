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
        snapshotVersion: Number(bootstrap.bundle?.snapshot_version || 0),
        updatedAt: bootstrap.bundle?.updated_at || "",
    };

    const elements = {
        statusChip: document.getElementById("telegram-preprocess-status-chip"),
        stage: document.getElementById("telegram-preprocess-stage"),
        progressLabel: document.getElementById("telegram-preprocess-progress-label"),
        progressLabelDuplicate: document.getElementById("telegram-preprocess-progress-label-duplicate"),
        progressFill: document.getElementById("telegram-preprocess-progress-fill"),
        currentProgressLabel: document.getElementById("telegram-preprocess-current-progress-label"),
        currentProgressFill: document.getElementById("telegram-preprocess-current-progress-fill"),
        weeklyCandidateCount: document.getElementById("telegram-preprocess-weekly-candidate-count"),
        topUserCount: document.getElementById("telegram-preprocess-top-user-count"),
        topicCount: document.getElementById("telegram-preprocess-topic-count"),
        activeUserCount: document.getElementById("telegram-preprocess-active-user-count"),
        weeklyConcurrency: document.getElementById("telegram-preprocess-weekly-concurrency"),
        activeAgents: document.getElementById("telegram-preprocess-active-agents"),
        completedWeeks: document.getElementById("telegram-preprocess-completed-weeks"),
        remainingWeeks: document.getElementById("telegram-preprocess-remaining-weeks"),
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
        stageBanner: document.getElementById("telegram-preprocess-stage-banner"),
        liveNote: document.getElementById("telegram-preprocess-live-note"),
        livePill: document.getElementById("telegram-preprocess-live-pill"),
        coverageChip: document.getElementById("telegram-preprocess-coverage-chip"),
        weeklyConcurrencyChip: document.getElementById("telegram-preprocess-weekly-concurrency-chip"),
        activeAgentChip: document.getElementById("telegram-preprocess-active-agent-chip"),
        tokenChip: document.getElementById("telegram-preprocess-token-chip"),
        snapshotChip: document.getElementById("telegram-preprocess-snapshot-chip"),
        laneStrip: document.getElementById("telegram-preprocess-agent-lanes"),
    };

    renderBundle(state.bundle);
    renderTraceList();
    renderLiveOutput();
    renderAgentLanes();
    setLiveState(state.bundle?.status && isRunning(state.bundle.status) ? "live" : "idle");
    connectStream();

    function connectStream() {
        if (!state.runId) {
            return;
        }
        stopStream();
        setLiveState("connecting");
        state.stream = new EventSource(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}/stream`);

        state.stream.addEventListener("snapshot", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload || !acceptSnapshot(payload)) {
                return;
            }
            state.bundle = payload;
            mergePersistedTrace(payload.trace_events || []);
            renderBundle(payload);
            renderTraceList();
            renderLiveOutput();
            renderAgentLanes();
            setLiveState(isRunning(payload.status) ? "live" : "idle");
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
            setLiveState("idle");
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
        setLiveState("polling");
        state.pollTimer = window.setInterval(async () => {
            await refreshBundle();
            if (!isRunning(state.bundle?.status)) {
                window.clearInterval(state.pollTimer);
                state.pollTimer = null;
                setLiveState("idle");
            }
        }, 1800);
    }

    async function refreshBundle() {
        if (!state.runId) {
            return;
        }
        try {
            const payload = await fetchJson(`/api/projects/${state.projectId}/preprocess/runs/${encodeURIComponent(state.runId)}`);
            if (!acceptSnapshot(payload)) {
                return;
            }
            state.bundle = payload;
            mergePersistedTrace(payload.trace_events || []);
            renderBundle(payload);
            renderTraceList();
            renderLiveOutput();
            renderAgentLanes();
            setLiveState(isRunning(payload.status) ? "live" : "idle");
        } catch (error) {
            updateText(elements.liveTitle, error.message || "Failed to refresh preprocess status.");
            setLiveState("polling");
        }
    }

    function acceptSnapshot(payload) {
        const nextVersion = Number(payload.snapshot_version || 0);
        const nextUpdatedAt = String(payload.updated_at || "");
        if (nextVersion < state.snapshotVersion) {
            return false;
        }
        if (nextVersion === state.snapshotVersion && state.updatedAt && nextUpdatedAt && nextUpdatedAt < state.updatedAt) {
            return false;
        }
        state.snapshotVersion = nextVersion;
        state.updatedAt = nextUpdatedAt || state.updatedAt;
        return true;
    }

    function mergePersistedTrace(events) {
        const seen = new Set(state.traceEvents.map((item) => buildTraceKey(item)));
        (events || []).forEach((item) => {
            const key = buildTraceKey(item);
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            state.traceEvents.push(item);
        });
        state.traceEvents.sort((left, right) => {
            const leftSeq = Number(left.seq || 0);
            const rightSeq = Number(right.seq || 0);
            if (leftSeq && rightSeq && leftSeq !== rightSeq) {
                return leftSeq - rightSeq;
            }
            return String(left.timestamp || "").localeCompare(String(right.timestamp || ""));
        });
    }

    function buildTraceKey(event) {
        return String(
            event?.seq
            ?? `${event?.timestamp || ""}:${event?.kind || ""}:${event?.request_key || ""}:${event?.tool_name || ""}:${event?.week_key || ""}`
        );
    }

    function handleTraceEvent(event) {
        const requestKey = String(event.request_key || "").trim();
        if (event.kind === "llm_request_started" && requestKey) {
            state.activeRequestKey = requestKey;
            state.liveOutputByRequest[requestKey] = state.liveOutputByRequest[requestKey] || "";
        }
        if (event.kind === "llm_delta" && requestKey) {
            state.activeRequestKey = requestKey;
            state.liveOutputByRequest[requestKey] = String(event.text_preview || "");
        }
        if (event.kind === "llm_request_completed" && requestKey) {
            state.activeRequestKey = requestKey;
            state.liveOutputByRequest[requestKey] = String(
                event.response_text_preview || state.liveOutputByRequest[requestKey] || ""
            );
        }
        mergePersistedTrace([event]);
        renderTraceList();
        renderLiveOutput(event);
        renderAgentLanes();
    }

    function renderBundle(bundle) {
        if (!bundle) {
            setStatusTone(elements.statusChip, "idle", "idle");
            updateText(elements.stage, "Waiting");
            updateText(elements.stageBanner, "Waiting");
            updateText(elements.progressLabel, "0%");
            updateText(elements.progressLabelDuplicate, "0%");
            updateText(elements.currentProgressLabel, "0%");
            updateText(elements.weeklyCandidateCount, "0");
            updateText(elements.topUserCount, "0");
            updateText(elements.topicCount, "0");
            updateText(elements.activeUserCount, "0");
            updateText(elements.weeklyConcurrency, "1");
            updateText(elements.activeAgents, "0");
            updateText(elements.completedWeeks, "0");
            updateText(elements.remainingWeeks, "0");
            updateText(elements.totalTokens, "0");
            updateText(elements.cacheRead, "0");
            updateText(elements.runMeta, "No preprocess run yet.");
            updateText(elements.liveNote, "Waiting for a new preprocess run.");
            updateText(elements.coverageChip, "Topics 0 / 0");
            updateText(elements.weeklyConcurrencyChip, "Concurrency 1");
            updateText(elements.activeAgentChip, "Active 0");
            updateText(elements.tokenChip, "Tokens 0");
            updateText(elements.snapshotChip, "Snapshot 0");
            setWidth(elements.progressFill, 0);
            setWidth(elements.currentProgressFill, 0);
            renderWeeklyCandidates([]);
            renderTopUsers([]);
            renderTopics([]);
            renderActiveUsers([]);
            return;
        }

        const percent = clampPercent(bundle.progress_percent || 0);
        const completedWeeks = Number(bundle.completed_week_count || 0);
        const remainingWeeks = Number(bundle.remaining_week_count || 0);
        const totalWeeks = completedWeeks + remainingWeeks || Number(bundle.weekly_candidate_count || bundle.window_count || 0);
        const currentPercent = totalWeeks > 0
            ? clampPercent(Math.floor((completedWeeks / totalWeeks) * 100))
            : 0;
        const weeklyConcurrency = Number(bundle.requested_weekly_concurrency || bundle.weekly_summary_concurrency || 1);
        const activeAgents = Number(bundle.active_agents || 0);
        const snapshotVersion = Number(bundle.snapshot_version || 0);

        setStatusTone(elements.statusChip, bundle.status, bundle.status);
        updateText(elements.stage, bundle.current_stage || "Waiting");
        updateText(elements.stageBanner, bundle.current_stage || "Waiting");
        updateText(elements.progressLabel, `${percent}%`);
        updateText(elements.progressLabelDuplicate, `${percent}%`);
        updateText(elements.currentProgressLabel, `${currentPercent}%`);
        updateText(elements.weeklyCandidateCount, bundle.weekly_candidate_count || bundle.window_count || 0);
        updateText(elements.topUserCount, bundle.top_user_count || 0);
        updateText(elements.topicCount, bundle.topic_count || 0);
        updateText(elements.activeUserCount, bundle.active_user_count || 0);
        updateText(elements.weeklyConcurrency, weeklyConcurrency);
        updateText(elements.activeAgents, activeAgents);
        updateText(elements.completedWeeks, completedWeeks);
        updateText(elements.remainingWeeks, remainingWeeks);
        updateText(elements.totalTokens, bundle.total_tokens || 0);
        updateText(elements.cacheRead, bundle.cache_read_tokens || 0);
        updateText(
            elements.runMeta,
            `${formatDateTime(bundle.started_at || bundle.created_at)} → ${formatDateTime(bundle.finished_at || bundle.updated_at)}`
        );
        updateText(elements.liveNote, buildProgressNote(bundle, totalWeeks, completedWeeks, remainingWeeks));
        updateText(elements.coverageChip, `Topics ${bundle.topic_count || 0} / ${bundle.weekly_candidate_count || bundle.window_count || 0}`);
        updateText(elements.weeklyConcurrencyChip, `Concurrency ${weeklyConcurrency}`);
        updateText(elements.activeAgentChip, `Active ${activeAgents}`);
        updateText(elements.tokenChip, `Tokens ${bundle.total_tokens || 0}`);
        updateText(elements.snapshotChip, `Snapshot ${snapshotVersion}`);
        setWidth(elements.progressFill, percent);
        setWidth(elements.currentProgressFill, currentPercent);

        if (elements.error) {
            elements.error.hidden = !bundle.error_message;
            elements.error.textContent = bundle.error_message || "";
        }

        renderWeeklyCandidates(bundle.weekly_candidates || []);
        renderTopUsers(bundle.top_users || []);
        renderTopics(bundle.topics || []);
        renderActiveUsers(bundle.active_users || []);
    }

    function setWidth(element, percent) {
        if (element) {
            element.style.width = `${percent}%`;
        }
    }

    function buildProgressNote(bundle, totalWeeks, completedWeeks, remainingWeeks) {
        return [
            bundle.current_stage || bundle.status || "idle",
            totalWeeks ? `${completedWeeks}/${totalWeeks} weeks complete` : "",
            remainingWeeks ? `${remainingWeeks} weeks pending` : "",
            bundle.active_agents ? `${bundle.active_agents} active workers` : "",
            bundle.updated_at ? `Updated ${formatDateTime(bundle.updated_at)}` : "",
        ].filter(Boolean).join(" · ");
    }

    function renderAgentLanes() {
        if (!elements.laneStrip) {
            return;
        }
        elements.laneStrip.innerHTML = "";

        const activeTracks = deriveWorkerTracks();
        const expected = Number(state.bundle?.active_agents || 0);

        if (!activeTracks.length && !expected) {
            const placeholder = document.createElement("article");
            placeholder.className = "agent-lane-card agent-lane-card--empty";
            placeholder.innerHTML = "<strong>No active workers</strong><p>The command center will light up when weekly workers start.</p>";
            elements.laneStrip.appendChild(placeholder);
            return;
        }

        activeTracks.forEach((track) => {
            const card = document.createElement("article");
            card.className = `agent-lane-card status-${escapeHtml(track.status)}`;
            card.innerHTML = `
                <div class="agent-lane-card__head">
                    <div>
                        <strong>${escapeHtml(track.label)}</strong>
                        <span>${escapeHtml(track.agent)}</span>
                    </div>
                    <span class="status-pill">${escapeHtml(track.statusLabel)}</span>
                </div>
                <div class="agent-lane-card__meta">
                    <span>${escapeHtml(track.stage)}</span>
                    <span>${escapeHtml(track.updatedAt)}</span>
                </div>
                <p class="agent-lane-card__requests">${escapeHtml(track.detail)}</p>
            `;
            elements.laneStrip.appendChild(card);
        });

        if (activeTracks.length < expected) {
            for (let index = activeTracks.length; index < expected; index += 1) {
                const placeholder = document.createElement("article");
                placeholder.className = "agent-lane-card status-running";
                placeholder.innerHTML = `
                    <div class="agent-lane-card__head">
                        <div>
                            <strong>Worker Slot ${index + 1}</strong>
                            <span>weekly_topic_agent</span>
                        </div>
                        <span class="status-pill">Running</span>
                    </div>
                    <div class="agent-lane-card__meta">
                        <span>Waiting for next trace</span>
                        <span>${escapeHtml(formatDateTime(state.bundle?.updated_at || state.bundle?.started_at))}</span>
                    </div>
                    <p class="agent-lane-card__requests">This slot is active but has not emitted a detailed trace event yet.</p>
                `;
                elements.laneStrip.appendChild(placeholder);
            }
        }
    }

    function deriveWorkerTracks() {
        const openTracks = new Map();
        orderedTraceEvents().forEach((event) => {
            const key = workerTrackKey(event);
            if (!key) {
                return;
            }
            const label = event.week_key ? `Week ${event.week_key}` : (event.label || event.request_key || "Worker");
            const existing = openTracks.get(key) || {
                key,
                label,
                agent: event.agent || "worker",
                stage: event.stage || "working",
                detail: event.request_key || event.message || "Live trace",
                updatedAt: formatDateTime(event.timestamp),
                status: "running",
                statusLabel: "Running",
            };

            existing.label = label;
            existing.agent = event.agent || existing.agent;
            existing.stage = event.stage || existing.stage;
            existing.detail = event.request_key || event.tool_name || event.message || existing.detail;
            existing.updatedAt = formatDateTime(event.timestamp);

            if (event.kind === "agent_completed") {
                existing.status = "completed";
                existing.statusLabel = "Completed";
            } else if (event.kind === "agent_retry") {
                existing.status = "failed";
                existing.statusLabel = "Retrying";
            } else {
                existing.status = "running";
                existing.statusLabel = "Running";
            }

            openTracks.set(key, existing);
        });

        return [...openTracks.values()]
            .filter((track) => track.status !== "completed" || isRunning(state.bundle?.status))
            .slice(-12);
    }

    function workerTrackKey(event) {
        if (!event || !event.kind) {
            return "";
        }
        if (event.week_key) {
            return `${event.agent || "worker"}:${event.week_key}`;
        }
        if (event.request_key) {
            return `${event.agent || "worker"}:${event.request_key}`;
        }
        if (event.kind === "agent_started" || event.kind === "agent_completed" || event.kind === "agent_retry") {
            return `${event.agent || "worker"}:${event.stage || "stage"}`;
        }
        return "";
    }

    function orderedTraceEvents() {
        return [...state.traceEvents].sort((left, right) => {
            const leftSeq = Number(left.seq || 0);
            const rightSeq = Number(right.seq || 0);
            if (leftSeq && rightSeq && leftSeq !== rightSeq) {
                return leftSeq - rightSeq;
            }
            return String(left.timestamp || "").localeCompare(String(right.timestamp || ""));
        });
    }

    function renderTraceList() {
        if (!elements.traceList) {
            return;
        }
        elements.traceList.innerHTML = "";

        const events = orderedTraceEvents().slice(-120);
        if (!events.length) {
            elements.traceList.appendChild(renderEmptyPanel("Waiting for preprocess events."));
            return;
        }

        events.forEach((event) => {
            const meta = [
                event.stage || "",
                event.agent || "",
                event.week_key ? `week ${event.week_key}` : "",
                event.round_index ? `round ${event.round_index}` : "",
                event.tool_name ? `tool ${event.tool_name}` : "",
            ].filter(Boolean).join(" · ");

            if (event.kind === "llm_request_started" || event.kind === "llm_request_completed") {
                const bubble = document.createElement("article");
                bubble.className = "bubble bubble--assistant preprocess-trace-bubble";
                bubble.innerHTML = `
                    <div class="bubble__head bubble__head--assistant">
                        <span class="bubble__badge">LLM</span>
                        <span class="bubble__meta">${escapeHtml(meta || "--")}</span>
                    </div>
                `;
                const content = document.createElement("div");
                content.className = "bubble__content";
                const detailText = event.response_text_preview || event.prompt_preview || "";
                if (detailText) {
                    content.appendChild(createCodeBlock(
                        event.kind === "llm_request_started" ? "request" : "response",
                        detailText
                    ));
                } else {
                    content.innerHTML = `<p class="muted">${escapeHtml(event.kind === "llm_request_started" ? "Waiting for the model to answer..." : "No displayable text was returned.")}</p>`;
                }
                bubble.appendChild(content);
                elements.traceList.appendChild(bubble);
                return;
            }

            if (event.kind === "tool_call" || event.kind === "tool_result") {
                const details = document.createElement("details");
                details.className = "bubble bubble--tool preprocess-trace-bubble";
                details.open = event.kind === "tool_call";
                details.innerHTML = `
                    <summary class="bubble__tool-summary">
                        <span class="bubble__tool-label">&gt;_ ${escapeHtml(event.tool_name || "tool")}</span>
                        <span class="bubble__meta">${escapeHtml(meta || summarizeTraceEvent(event))}</span>
                    </summary>
                    <div class="bubble__tool-body"></div>
                `;
                const body = details.querySelector(".bubble__tool-body");
                if (event.arguments_preview) {
                    body.appendChild(createCodeBlock("arguments", event.arguments_preview));
                }
                if (event.output_preview) {
                    body.appendChild(createCodeBlock("result", event.output_preview));
                }
                if (event.error) {
                    body.appendChild(createCodeBlock("error", event.error, { tone: "error" }));
                }
                if (!event.arguments_preview && !event.output_preview && !event.error) {
                    body.innerHTML = "<p class=\"muted\">This tool event did not include extra content.</p>";
                }
                elements.traceList.appendChild(details);
                return;
            }

            elements.traceList.appendChild(buildContextBubble({
                text: summarizeTraceEvent(event),
                meta,
                active: ["agent_started", "stage_progress"].includes(event.kind),
                tone: traceTone(event.kind),
            }));
        });
    }

    function buildContextBubble(item) {
        const row = document.createElement("div");
        row.className = "bubble-context-row";
        row.innerHTML = `
            <div class="bubble bubble--context bubble--context-${escapeHtml(item.tone || "processing")}">
                ${item.active ? '<span class="bubble__spinner" aria-hidden="true"></span>' : '<span class="bubble__dot" aria-hidden="true"></span>'}
                <span>${escapeHtml(item.text || "Context update")}</span>
                ${item.meta ? `<small>${escapeHtml(item.meta)}</small>` : ""}
            </div>
        `;
        return row;
    }

    function createCodeBlock(label, text, options = {}) {
        const wrapper = document.createElement("div");
        wrapper.className = `code-block ${options.tone === "error" ? "code-block--error" : ""}`;
        const value = String(text || "").trim();
        wrapper.innerHTML = `
            <div class="code-block__header">
                <span>${escapeHtml(label)}</span>
                <button type="button" class="code-block__copy">Copy</button>
            </div>
            <pre><code>${escapeHtml(value)}</code></pre>
        `;
        wrapper.querySelector(".code-block__copy")?.addEventListener("click", async (event) => {
            try {
                await navigator.clipboard.writeText(value);
                const button = event.currentTarget;
                button.textContent = "Copied";
                window.setTimeout(() => {
                    button.textContent = "Copy";
                }, 1200);
            } catch (error) {
                console.error(error);
            }
        });
        return wrapper;
    }

    function renderLiveOutput(event = null) {
        const requestKey = event?.request_key || state.activeRequestKey;
        if (!requestKey) {
            updateText(elements.liveTitle, "Waiting for the next LLM request.");
            updateText(elements.liveOutput, "");
            return;
        }
        const text = state.liveOutputByRequest[requestKey] || "";
        updateText(elements.liveTitle, summarizeLiveTitle(event, requestKey));
        updateText(elements.liveOutput, text);
    }

    function summarizeLiveTitle(event, requestKey) {
        let traceEvent = event || null;
        if (!traceEvent) {
            const events = orderedTraceEvents();
            for (let index = events.length - 1; index >= 0; index -= 1) {
                if (events[index]?.request_key === requestKey) {
                    traceEvent = events[index];
                    break;
                }
            }
        }
        if (!traceEvent) {
            return `Live output · ${requestKey}`;
        }
        return [
            traceEvent.label || traceEvent.week_key || requestKey,
            traceEvent.stage || "",
            traceEvent.agent || "",
        ].filter(Boolean).join(" · ");
    }

    function renderWeeklyCandidates(candidates) {
        if (!elements.weeklyCandidates) {
            return;
        }
        elements.weeklyCandidates.innerHTML = "";
        if (!candidates.length) {
            elements.weeklyCandidates.appendChild(renderEmptyPanel("No weekly candidates have been materialized yet."));
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
                    <span class="status-chip tone-processing">${escapeHtml(String(candidate.message_count || 0))} msgs</span>
                </div>
                <p class="helper-text">${escapeHtml(formatDateTime(candidate.start_at))} → ${escapeHtml(formatDateTime(candidate.end_at))}</p>
                <p class="helper-text">Message window: #${escapeHtml(String(candidate.start_message_id || "--"))} → #${escapeHtml(String(candidate.end_message_id || "--"))}</p>
                <p class="helper-text">Participants ${escapeHtml(String(candidate.participant_count || 0))}</p>
                ${participants ? `<p class="helper-text">${escapeHtml(participants)}</p>` : ""}
            `;
            elements.weeklyCandidates.appendChild(card);
        });
    }

    function renderTopUsers(users) {
        if (!elements.topUsers) {
            return;
        }
        elements.topUsers.innerHTML = "";
        if (!users.length) {
            elements.topUsers.appendChild(renderEmptyPanel("Top users are not ready yet."));
            return;
        }
        users.forEach((user) => {
            const card = document.createElement("article");
            card.className = "document-card compact-card";
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>#${escapeHtml(String(user.rank || "--"))} · ${escapeHtml(user.display_name || user.username || user.uid || user.participant_id)}</strong>
                    <span class="status-chip tone-processing">${escapeHtml(String(user.message_count || 0))} msgs</span>
                </div>
                <p class="helper-text">UID ${escapeHtml(user.uid || "--")} · @${escapeHtml(user.username || "--")}</p>
                <p class="helper-text">${escapeHtml(formatDateTime(user.first_seen_at))} → ${escapeHtml(formatDateTime(user.last_seen_at))}</p>
            `;
            elements.topUsers.appendChild(card);
        });
    }

    function renderActiveUsers(users) {
        if (!elements.activeUsers) {
            return;
        }
        elements.activeUsers.innerHTML = "";
        if (!users.length) {
            elements.activeUsers.appendChild(renderEmptyPanel("No active user summary yet."));
            return;
        }
        users.forEach((user) => {
            const card = document.createElement("article");
            card.className = "document-card compact-card";
            const aliases = (user.aliases || []).filter(Boolean).join(" / ");
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>#${escapeHtml(String(user.rank || "--"))} · ${escapeHtml(user.display_name || user.username || user.primary_alias || user.participant_id)}</strong>
                    <span class="status-chip tone-ready">${escapeHtml(String(user.message_count || 0))} msgs</span>
                </div>
                <p class="helper-text">Primary alias: ${escapeHtml(user.primary_alias || "--")}</p>
                ${aliases ? `<p class="helper-text">${escapeHtml(aliases)}</p>` : ""}
            `;
            elements.activeUsers.appendChild(card);
        });
    }

    function renderTopics(topics) {
        if (!elements.topics) {
            return;
        }
        elements.topics.innerHTML = "";
        if (!topics.length) {
            elements.topics.appendChild(renderEmptyPanel("Final weekly topics are not ready yet."));
            return;
        }
        topics.forEach((topic) => {
            const card = document.createElement("article");
            card.className = "document-card";
            const keywords = (topic.keywords || []).filter(Boolean).join(" · ");
            const participants = (topic.participants || [])
                .map((item) => item.display_name || item.username || item.participant_id)
                .filter(Boolean)
                .join(" / ");
            card.innerHTML = `
                <div class="document-card__head">
                    <strong>${escapeHtml(topic.title || `Topic ${topic.topic_index || ""}`)}</strong>
                    <span class="status-chip tone-ready">${escapeHtml(String(topic.message_count || 0))} msgs</span>
                </div>
                <p>${escapeHtml(topic.summary || "")}</p>
                ${keywords ? `<p class="helper-text">${escapeHtml(keywords)}</p>` : ""}
                ${participants ? `<p class="helper-text">${escapeHtml(participants)}</p>` : ""}
            `;
            elements.topics.appendChild(card);
        });
    }

    function renderEmptyPanel(text) {
        const panel = document.createElement("div");
        panel.className = "empty-panel";
        panel.innerHTML = `<strong>${escapeHtml(text)}</strong>`;
        return panel;
    }

    function summarizeTraceEvent(event) {
        if (event.kind === "agent_started") {
            return `${event.label || event.week_key || event.agent || "Agent"} started`;
        }
        if (event.kind === "agent_completed") {
            return `${event.label || event.week_key || event.agent || "Agent"} completed`;
        }
        if (event.kind === "agent_retry") {
            return `${event.label || event.week_key || event.agent || "Agent"} retrying`;
        }
        if (event.kind === "llm_request_started") {
            return `${event.label || event.week_key || "LLM"} request started`;
        }
        if (event.kind === "llm_request_completed") {
            return `${event.label || event.week_key || "LLM"} request completed`;
        }
        if (event.kind === "tool_call") {
            return `Calling ${event.tool_name || "tool"}`;
        }
        if (event.kind === "tool_result") {
            return `${event.tool_name || "tool"} returned`;
        }
        return event.message || event.kind || "trace";
    }

    function traceTone(kind) {
        switch (kind) {
            case "agent_completed":
                return "ready";
            case "agent_retry":
            case "error":
                return "failed";
            default:
                return "processing";
        }
    }

    function isRunning(status) {
        return String(status || "").toLowerCase() === "running";
    }

    function setLiveState(mode) {
        if (!elements.livePill) {
            return;
        }
        elements.livePill.classList.remove("is-connecting", "is-live", "is-idle", "is-polling");
        switch (mode) {
            case "connecting":
                elements.livePill.classList.add("is-connecting");
                elements.livePill.textContent = "Connecting";
                break;
            case "live":
                elements.livePill.classList.add("is-live");
                elements.livePill.textContent = "Live";
                break;
            case "polling":
                elements.livePill.classList.add("is-polling");
                elements.livePill.textContent = "Polling";
                break;
            default:
                elements.livePill.classList.add("is-idle");
                elements.livePill.textContent = "Idle";
        }
    }
}
