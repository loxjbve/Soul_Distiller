import {
    clampPercent,
    fetchJson,
    formatDateTime,
    openModal,
    safeParseJson,
    setStatusTone,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("telegram-preprocess-bootstrap")?.textContent, {});
const TRACE_HISTORY_LIMIT = 720;
const ACTIVE_TRACK_STALE_MS = 45_000;

if (bootstrap?.project_id) {
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id,
        bundle: bootstrap.bundle || null,
        stream: null,
        pollTimer: null,
        snapshotVersion: Number(bootstrap.bundle?.snapshot_version || 0),
        updatedAt: bootstrap.bundle?.updated_at || "",
        traceEvents: [],
        traceEventKeys: new Set(),
        traceTracks: [],
        selectedTrackId: "",
    };

    const elements = {
        statusChip: document.getElementById("telegram-preprocess-status-chip"),
        stageBanner: document.getElementById("telegram-preprocess-stage-banner"),
        liveNote: document.getElementById("telegram-preprocess-live-note"),
        livePill: document.getElementById("telegram-preprocess-live-pill"),
        currentTopicLabel: document.getElementById("telegram-preprocess-current-topic-label"),
        currentTopicProgress: document.getElementById("telegram-preprocess-current-topic-progress"),
        topicBoardNote: document.getElementById("telegram-preprocess-topic-board-note"),
        progressLabel: document.getElementById("telegram-preprocess-progress-label"),
        progressFill: document.getElementById("telegram-preprocess-progress-fill"),
        inputTokenChip: document.getElementById("telegram-preprocess-input-token-chip"),
        outputTokenChip: document.getElementById("telegram-preprocess-output-token-chip"),
        topicLamps: document.getElementById("telegram-preprocess-topic-lamps"),
        error: document.getElementById("telegram-preprocess-error"),
        agentNote: document.getElementById("telegram-preprocess-agent-note"),
        agentList: document.getElementById("telegram-preprocess-agent-list"),
        agentModal: document.getElementById("telegram-preprocess-agent-modal"),
        agentModalTitle: document.getElementById("telegram-preprocess-agent-modal-title"),
        agentModalNote: document.getElementById("telegram-preprocess-agent-modal-note"),
        agentModalBody: document.getElementById("telegram-preprocess-agent-modal-body"),
    };

    bindEvents();
    mergeTraceEvents(state.bundle?.trace_events || []);
    renderBundle(state.bundle);
    setLiveState(state.bundle?.status && isRunning(state.bundle.status) ? "live" : "idle");
    connectStream();

    function bindEvents() {
        elements.agentList?.addEventListener("click", (event) => {
            const trigger = event.target instanceof HTMLElement
                ? event.target.closest("[data-trace-track]")
                : null;
            if (!(trigger instanceof HTMLElement)) {
                return;
            }
            const trackId = String(trigger.dataset.traceTrack || "");
            if (!trackId) {
                return;
            }
            state.selectedTrackId = trackId;
            renderTraceModal();
            openModal(elements.agentModal);
        });
    }

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
            renderBundle(payload);
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
            mergeTraceEvents([payload]);
            renderTraceMonitor();
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
            renderBundle(payload);
            setLiveState(isRunning(payload.status) ? "live" : "idle");
        } catch (error) {
            updateText(elements.liveNote, error.message || "Failed to refresh preprocess status.");
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

    function renderBundle(bundle) {
        mergeTraceEvents(bundle?.trace_events || []);
        if (!bundle) {
            setStatusTone(elements.statusChip, "idle", "idle");
            updateText(elements.stageBanner, "Waiting");
            updateText(elements.liveNote, "Waiting for a new preprocess run.");
            updateText(elements.currentTopicLabel, "Waiting");
            updateText(elements.currentTopicProgress, "Processing Topic 0/0");
            
            
            updateText(elements.topicBoardNote, "Processing Topic 0/0");
            updateText(elements.progressLabel, "0%");
            
            
            updateText(elements.inputTokenChip, "In 0");
            updateText(elements.outputTokenChip, "Out 0");
            setWidth(elements.progressFill, 0);
            renderTopicLamps([]);
            renderTraceMonitor();
            return;
        }

        const percent = clampPercent(bundle.progress_percent || 0);
        const progressText = buildTopicProgressText(bundle);

        setStatusTone(elements.statusChip, bundle.status, bundle.status);
        updateText(elements.stageBanner, bundle.current_stage || "Waiting");
        updateText(elements.liveNote, buildLiveNote(bundle));
        updateText(elements.currentTopicLabel, bundle.current_topic_label || bundle.current_stage || "Waiting");
        updateText(elements.currentTopicProgress, progressText);
        
        
        updateText(elements.topicBoardNote, progressText);
        updateText(elements.progressLabel, `${percent}%`);
        
        
        updateText(elements.inputTokenChip, `In ${bundle.prompt_tokens || 0}`);
        updateText(elements.outputTokenChip, `Out ${bundle.completion_tokens || 0}`);
        setWidth(elements.progressFill, percent);
        renderTopicLamps(buildTopicLampModels(bundle));
        renderTraceMonitor();

        if (elements.error) {
            elements.error.hidden = !bundle.error_message;
            elements.error.textContent = bundle.error_message || "";
        }
    }

    function renderTopicLamps(items) {
        if (!elements.topicLamps) {
            return;
        }
        elements.topicLamps.innerHTML = "";
        if (!items.length) {
            const empty = document.createElement("div");
            empty.className = "empty-panel";
            empty.innerHTML = "<strong>No topics yet.</strong>";
            elements.topicLamps.appendChild(empty);
            return;
        }
        items.forEach((item) => {
            const lamp = document.createElement("article");
            lamp.className = `telegram-topic-lamp status-${item.status}${item.current ? " is-current" : ""}`;
            lamp.innerHTML = `
                <span class="telegram-topic-lamp__dot" aria-hidden="true"></span>
                <div class="telegram-topic-lamp__body">
                    <strong>${escapeHtml(item.label)}</strong>
                    <small>${escapeHtml(item.meta)}</small>
                </div>
                <span class="telegram-topic-lamp__index">${String(item.index).padStart(2, "0")}</span>
            `;
            elements.topicLamps.appendChild(lamp);
        });
    }

    function renderTraceMonitor() {
        if (!elements.agentList) {
            return;
        }
        const tracks = buildTraceTracks();
        state.traceTracks = tracks;

        const activeCount = tracks.filter((track) => ["running", "stalled"].includes(track.status)).length;
        const stalledCount = tracks.filter((track) => track.status === "stalled").length;
        const recentCount = tracks.filter((track) => !["running", "stalled"].includes(track.status)).length;

        updateText(
            elements.agentNote,
            tracks.length
                ? `${activeCount} active · ${stalledCount} stalled · ${recentCount} recent`
                : "等待新的 LLM 请求开始。"
        );

        if (!tracks.length) {
            elements.agentList.innerHTML = `
                <div class="empty-panel">
                    <strong>等待新的 LLM 请求开始。</strong>
                </div>
            `;
            if (isModalOpen(elements.agentModal)) {
                renderTraceModal();
            }
            return;
        }

        elements.agentList.innerHTML = tracks.map((track) => renderTraceTrackCard(track)).join("");
        if (isModalOpen(elements.agentModal)) {
            renderTraceModal();
        }
    }

    function renderTraceTrackCard(track) {
        const isActive = ["running", "stalled"].includes(track.status);
        const badgeText = track.kind === "agent" ? "Agent" : track.modeLabel;
        const preview = trimText(track.liveText || track.responsePreview || track.message || track.promptPreview || "Waiting for content.", 180);
        const metaParts = [
            badgeText,
            track.stageLabel,
            track.updatedAt ? formatDateTime(track.updatedAt) : "",
        ].filter(Boolean);
        return `
            <button
                type="button"
                class="telegram-agent-lamp status-${escapeHtml(track.status)}${isActive ? " is-live" : ""}"
                data-trace-track="${escapeHtml(track.id)}"
            >
                <span class="telegram-agent-lamp__dot" aria-hidden="true"></span>
                <div class="telegram-agent-lamp__body">
                    <div class="telegram-agent-lamp__head">
                        <strong>${escapeHtml(track.label)}</strong>
                        <span class="status-chip ${traceStatusTone(track.status)}">${escapeHtml(traceStatusLabel(track.status))}</span>
                    </div>
                    <small>${escapeHtml(metaParts.join(" · "))}</small>
                    <p>${escapeHtml(preview)}</p>
                </div>
            </button>
        `;
    }

    function renderTraceModal() {
        if (!elements.agentModalBody || !elements.agentModalTitle || !elements.agentModalNote) {
            return;
        }
        const track = state.traceTracks.find((item) => item.id === state.selectedTrackId) || null;
        if (!track) {
            elements.agentModalTitle.textContent = "Agent Detail";
            elements.agentModalNote.textContent = "未找到该 agent 的实时信息。";
            elements.agentModalBody.innerHTML = `
                <div class="empty-panel">
                    <strong>未找到该 agent 的实时信息。</strong>
                </div>
            `;
            return;
        }

        const latestContent = track.liveText || track.responsePreview || track.promptPreview || track.message || "Waiting for streamed content.";
        const stageMeta = [
            track.modeLabel,
            track.stageLabel,
            track.updatedAt ? `Updated ${formatDateTime(track.updatedAt)}` : "",
        ].filter(Boolean).join(" · ");

        elements.agentModalTitle.textContent = track.label;
        elements.agentModalNote.textContent = stageMeta || "实时查看当前 agent 的请求、工具调用和输出。";
        elements.agentModalBody.innerHTML = `
            <section class="telegram-trace-modal__summary">
                <article class="telegram-trace-modal__metric">
                    <span>Status</span>
                    <strong>${escapeHtml(traceStatusLabel(track.status))}</strong>
                </article>
                <article class="telegram-trace-modal__metric">
                    <span>Mode</span>
                    <strong>${escapeHtml(track.modeLabel)}</strong>
                </article>
                <article class="telegram-trace-modal__metric">
                    <span>Events</span>
                    <strong>${escapeHtml(String(track.events.length))}</strong>
                </article>
                <article class="telegram-trace-modal__metric">
                    <span>Request Key</span>
                    <strong>${escapeHtml(track.requestKey || track.agent || "--")}</strong>
                </article>
            </section>

            <section class="telegram-trace-modal__panel">
                <div class="telegram-trace-modal__panel-head">
                    <strong>Current Content</strong>
                    <span>${escapeHtml(track.updatedAt ? formatDateTime(track.updatedAt) : "--")}</span>
                </div>
                <pre class="telegram-trace-modal__stream">${escapeHtml(latestContent)}</pre>
            </section>

            ${track.promptPreview ? `
                <section class="telegram-trace-modal__panel">
                    <div class="telegram-trace-modal__panel-head">
                        <strong>Prompt Preview</strong>
                        <span>${escapeHtml(track.requestKey || track.stageLabel)}</span>
                    </div>
                    <pre class="telegram-trace-modal__stream telegram-trace-modal__stream--prompt">${escapeHtml(track.promptPreview)}</pre>
                </section>
            ` : ""}

            <section class="telegram-trace-modal__panel">
                <div class="telegram-trace-modal__panel-head">
                    <strong>Event Stream</strong>
                    <span>${escapeHtml(`${track.events.length} events`)}</span>
                </div>
                <div class="telegram-trace-modal__timeline">
                    ${track.events.map((event) => renderTraceTimelineItem(event)).join("")}
                </div>
            </section>
        `;
    }

    function renderTraceTimelineItem(event) {
        const preview = traceEventPreview(event);
        return `
            <article class="telegram-trace-event-line kind-${escapeHtml(String(event.kind || "event").toLowerCase())}">
                <div class="telegram-trace-event-line__head">
                    <strong>${escapeHtml(traceEventLabel(event))}</strong>
                    <span>${escapeHtml(event.timestamp ? formatDateTime(event.timestamp) : "--")}</span>
                </div>
                <p>${escapeHtml(preview || "No preview available.")}</p>
            </article>
        `;
    }

    function mergeTraceEvents(events) {
        if (!Array.isArray(events) || !events.length) {
            return;
        }
        events.forEach((event) => {
            if (!event || typeof event !== "object") {
                return;
            }
            const normalized = normalizeTraceEvent(event);
            const eventKey = traceEventKey(normalized);
            if (state.traceEventKeys.has(eventKey)) {
                return;
            }
            state.traceEventKeys.add(eventKey);
            state.traceEvents.push(normalized);
        });
        if (state.traceEvents.length > TRACE_HISTORY_LIMIT) {
            state.traceEvents = state.traceEvents.slice(-TRACE_HISTORY_LIMIT);
            state.traceEventKeys = new Set(state.traceEvents.map((item) => traceEventKey(item)));
        }
    }

    function buildTraceTracks() {
        const tracks = new Map();
        const orderedEvents = [...state.traceEvents].sort(compareTraceEvents);

        orderedEvents.forEach((event) => {
            const trackId = resolveTrackId(event);
            if (!trackId) {
                return;
            }
            const existing = tracks.get(trackId) || createTraceTrack(trackId, event);
            applyTraceEvent(existing, event);
            tracks.set(trackId, existing);
        });

        const runStatus = String(state.bundle?.status || "").toLowerCase();
        const allTracks = [...tracks.values()].map((track) => finalizeTraceTrack(track, runStatus));
        const activeTracks = allTracks
            .filter((track) => ["running", "stalled"].includes(track.status))
            .sort(sortTraceTracks);
        const failedTracks = allTracks
            .filter((track) => track.status === "failed")
            .sort(sortTraceTracks)
            .slice(0, 4);
        const recentRequestTracks = allTracks
            .filter((track) => track.kind === "request" && !["running", "stalled", "failed"].includes(track.status))
            .sort(sortTraceTracks)
            .slice(0, 8);

        return dedupeTracks([...activeTracks, ...failedTracks, ...recentRequestTracks]);
    }

    function createTraceTrack(trackId, event) {
        const hasRequest = Boolean(event.request_key);
        return {
            id: trackId,
            kind: hasRequest ? "request" : "agent",
            stage: String(event.stage || ""),
            stageLabel: traceStageLabel(event.stage),
            agent: String(event.agent || ""),
            label: String(event.label || traceFallbackLabel(event)),
            requestKey: String(event.request_key || ""),
            requestKind: String(event.request_kind || ""),
            modeLabel: traceModeLabel(event),
            status: hasRequest ? "running" : "queued",
            startedAt: String(event.timestamp || ""),
            updatedAt: String(event.timestamp || ""),
            promptPreview: "",
            responsePreview: "",
            liveText: "",
            message: "",
            error: "",
            events: [],
        };
    }

    function applyTraceEvent(track, event) {
        track.updatedAt = String(event.timestamp || track.updatedAt || "");
        track.stage = String(event.stage || track.stage || "");
        track.stageLabel = traceStageLabel(track.stage);
        track.agent = String(event.agent || track.agent || "");
        track.requestKey = String(event.request_key || track.requestKey || "");
        track.requestKind = String(event.request_kind || track.requestKind || "");
        track.modeLabel = track.kind === "request"
            ? traceModeLabel({ request_kind: track.requestKind })
            : "Agent";
        if (event.label) {
            track.label = String(event.label);
        }
        if (event.prompt_preview) {
            track.promptPreview = String(event.prompt_preview);
        }
        if (event.text_preview) {
            track.liveText = String(event.text_preview);
        }
        if (event.response_text_preview) {
            track.responsePreview = String(event.response_text_preview);
        }
        if (event.message) {
            track.message = String(event.message);
        }
        if (event.error) {
            track.error = String(event.error);
        }

        if (event.kind === "agent_started" || event.kind === "llm_request_started") {
            track.status = "running";
            track.startedAt = String(event.timestamp || track.startedAt || "");
        } else if (event.kind === "llm_delta") {
            track.status = "running";
        } else if (event.kind === "agent_completed" || event.kind === "llm_request_completed") {
            track.status = "completed";
        } else if (event.kind === "agent_retry") {
            track.status = track.status === "completed" ? "completed" : "running";
        } else if (event.kind === "run_failed") {
            track.status = "failed";
        } else if (event.kind === "tool_call" || event.kind === "tool_result") {
            track.status = "running";
        }

        track.events.push(event);
        if (track.events.length > 120) {
            track.events = track.events.slice(-120);
        }
    }

    function finalizeTraceTrack(track, runStatus) {
        const normalized = { ...track };
        if (normalized.status === "running") {
            const updatedAtMs = Date.parse(normalized.updatedAt || "");
            if (Number.isFinite(updatedAtMs) && (Date.now() - updatedAtMs) > ACTIVE_TRACK_STALE_MS && isRunning(runStatus)) {
                normalized.status = "stalled";
            }
        }
        if (runStatus === "failed" && ["running", "stalled", "queued"].includes(normalized.status)) {
            normalized.status = "failed";
        }
        if (runStatus === "completed" && ["running", "stalled", "queued"].includes(normalized.status)) {
            normalized.status = normalized.error ? "failed" : "completed";
        }
        return normalized;
    }

    function buildTopicLampModels(bundle) {
        const total = Number(bundle.current_topic_total || bundle.weekly_candidate_count || bundle.window_count || 0);
        const currentIndex = Number(bundle.current_topic_index || 0);
        const currentLabel = String(bundle.current_topic_label || "").trim();
        const topics = Array.isArray(bundle.topics) ? bundle.topics : [];
        const candidates = Array.isArray(bundle.weekly_candidates) ? bundle.weekly_candidates : [];
        const topicByIndex = new Map(topics.map((item) => [Number(item.topic_index || 0), item]));

        const models = [];
        for (let index = 1; index <= total; index += 1) {
            const topic = topicByIndex.get(index);
            const candidate = candidates[index - 1] || null;
            let status = topic ? "completed" : "queued";
            if (bundle.status === "completed") {
                status = "completed";
            } else if (bundle.status === "failed" && currentIndex === index && !topic) {
                status = "failed";
            } else if (isRunning(bundle.status) && currentIndex === index && !topic) {
                status = "running";
            }
            const title = String(topic?.title || candidate?.week_key || `Topic ${index}`).trim();
            const weekKey = String(topic?.metadata?.week_key || candidate?.week_key || "").trim();
            const meta = [weekKey, topic?.summary ? String(topic.summary).trim() : ""].filter(Boolean).join(" 路 ");
            models.push({
                index,
                status,
                current: currentIndex === index || (!!currentLabel && title === currentLabel),
                label: title,
                meta: meta || (status === "queued" ? "Queued" : status === "running" ? "Running" : "Completed"),
            });
        }
        return models;
    }

    function buildTopicProgressText(bundle) {
        const total = Number(bundle.current_topic_total || bundle.weekly_candidate_count || bundle.window_count || 0);
        const index = Number(bundle.current_topic_index || 0);
        if (bundle.status === "completed") {
            return `Completed Topic ${total || 0}/${total || 0}`;
        }
        if (bundle.status === "failed") {
            return `Stopped at Topic ${Math.max(index, 0)}/${total || 0}`;
        }
        if (!total) {
            return "Processing Topic 0/0";
        }
        const current = Math.max(index || 1, 1);
        return `Processing Topic ${current}/${total}`;
    }

    function buildLiveNote(bundle) {
        const noteParts = [
            bundle.current_stage || bundle.status || "idle",
            bundle.current_topic_label ? `Current: ${bundle.current_topic_label}` : "",
            bundle.updated_at ? `Updated ${formatDateTime(bundle.updated_at)}` : "",
        ].filter(Boolean);
        return noteParts.join(" · ");
    }

    function setLiveState(mode) {
        if (!elements.livePill) {
            return;
        }
        const labels = {
            connecting: "CONNECTING",
            live: "LIVE",
            polling: "POLLING",
            idle: state.bundle?.status ? String(state.bundle.status).toUpperCase() : "IDLE",
        };
        elements.livePill.textContent = labels[mode] || labels.idle;
        elements.livePill.className = `analysis-live-pill live-state-${mode}`;
    }

    function isRunning(status) {
        return ["queued", "running"].includes(String(status || "").toLowerCase());
    }

    function setWidth(element, percent) {
        if (element) {
            element.style.width = `${percent}%`;
        }
    }

    function isModalOpen(element) {
        return !!(element && !element.hidden);
    }

    function resolveTrackId(event) {
        if (event.request_key) {
            return `request:${event.request_key}`;
        }
        if (event.agent) {
            return `agent:${event.stage || "stage"}:${event.agent}`;
        }
        return "";
    }

    function normalizeTraceEvent(event) {
        return {
            ...event,
            seq: event.seq == null ? null : Number(event.seq),
            timestamp: String(event.timestamp || ""),
            kind: String(event.kind || ""),
            stage: String(event.stage || ""),
            agent: String(event.agent || ""),
            request_key: String(event.request_key || ""),
            request_kind: String(event.request_kind || ""),
            label: String(event.label || ""),
            message: String(event.message || ""),
            prompt_preview: String(event.prompt_preview || ""),
            text_preview: String(event.text_preview || ""),
            response_text_preview: String(event.response_text_preview || ""),
            arguments_preview: String(event.arguments_preview || ""),
            output_preview: String(event.output_preview || ""),
            error: String(event.error || ""),
            tool_name: String(event.tool_name || ""),
        };
    }

    function traceEventKey(event) {
        if (event.seq != null && Number.isFinite(Number(event.seq))) {
            return `seq:${event.seq}`;
        }
        const preview = traceEventPreview(event);
        return [
            "live",
            event.timestamp || "",
            event.kind || "",
            event.stage || "",
            event.agent || "",
            event.request_key || "",
            event.tool_name || "",
            preview.slice(-48),
        ].join(":");
    }

    function compareTraceEvents(left, right) {
        const leftSeq = Number.isFinite(left.seq) ? Number(left.seq) : Infinity;
        const rightSeq = Number.isFinite(right.seq) ? Number(right.seq) : Infinity;
        if (leftSeq !== rightSeq) {
            return leftSeq - rightSeq;
        }
        const leftTs = Date.parse(left.timestamp || "") || 0;
        const rightTs = Date.parse(right.timestamp || "") || 0;
        return leftTs - rightTs;
    }

    function sortTraceTracks(left, right) {
        const leftActive = ["running", "stalled"].includes(left.status) ? 1 : 0;
        const rightActive = ["running", "stalled"].includes(right.status) ? 1 : 0;
        if (leftActive !== rightActive) {
            return rightActive - leftActive;
        }
        const leftTs = Date.parse(left.updatedAt || "") || 0;
        const rightTs = Date.parse(right.updatedAt || "") || 0;
        return rightTs - leftTs;
    }

    function dedupeTracks(tracks) {
        const seen = new Set();
        return tracks.filter((track) => {
            if (seen.has(track.id)) {
                return false;
            }
            seen.add(track.id);
            return true;
        });
    }

    function traceModeLabel(event) {
        const requestKind = String(event.request_kind || "").toLowerCase();
        if (requestKind === "tool_round") {
            return "Agentic";
        }
        if (requestKind === "chat_completion") {
            return "Single Request";
        }
        return event.request_key ? "Request" : "Agent";
    }

    function traceStageLabel(stage) {
        const normalized = String(stage || "").trim();
        if (!normalized) {
            return "Unknown Stage";
        }
        return normalized.replaceAll("_", " ");
    }

    function traceFallbackLabel(event) {
        if (event.request_key) {
            return event.request_key;
        }
        if (event.agent) {
            return event.agent.replaceAll("_", " ");
        }
        return event.stage || event.kind || "trace";
    }

    function traceStatusLabel(status) {
        const mapping = {
            queued: "Queued",
            running: "Running",
            stalled: "Stalled",
            completed: "Completed",
            failed: "Failed",
        };
        return mapping[String(status || "").toLowerCase()] || "Queued";
    }

    function traceStatusTone(status) {
        const normalized = String(status || "").toLowerCase();
        if (normalized === "completed") {
            return "tone-ready";
        }
        if (normalized === "failed") {
            return "tone-failed";
        }
        if (normalized === "stalled") {
            return "tone-warning";
        }
        if (normalized === "running") {
            return "tone-processing";
        }
        return "tone-queued";
    }

    function traceEventLabel(event) {
        const mapping = {
            agent_started: "Agent Started",
            agent_completed: "Agent Completed",
            agent_retry: "Agent Retry",
            llm_request_started: "Request Started",
            llm_delta: "Streaming Delta",
            llm_request_completed: "Request Completed",
            tool_call: `Tool Call${event.tool_name ? ` · ${event.tool_name}` : ""}`,
            tool_result: `Tool Result${event.tool_name ? ` · ${event.tool_name}` : ""}`,
            run_failed: "Run Failed",
        };
        return mapping[event.kind] || event.kind || "Trace Event";
    }

    function traceEventPreview(event) {
        return String(
            event.text_preview
            || event.response_text_preview
            || event.prompt_preview
            || event.output_preview
            || event.arguments_preview
            || event.error
            || event.message
            || ""
        ).trim();
    }

    function trimText(text, limit) {
        const normalized = String(text || "").trim();
        if (normalized.length <= limit) {
            return normalized;
        }
        return `${normalized.slice(0, Math.max(limit - 1, 0)).trimEnd()}…`;
    }
}

function escapeHtml(value) {
    return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}
