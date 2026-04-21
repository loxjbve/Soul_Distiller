import {
    clampPercent,
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
        stream: null,
        pollTimer: null,
        snapshotVersion: Number(bootstrap.bundle?.snapshot_version || 0),
        updatedAt: bootstrap.bundle?.updated_at || "",
    };

    const elements = {
        statusChip: document.getElementById("telegram-preprocess-status-chip"),
        stageBanner: document.getElementById("telegram-preprocess-stage-banner"),
        liveNote: document.getElementById("telegram-preprocess-live-note"),
        livePill: document.getElementById("telegram-preprocess-live-pill"),
        currentTopicLabel: document.getElementById("telegram-preprocess-current-topic-label"),
        currentTopicProgress: document.getElementById("telegram-preprocess-current-topic-progress"),
        currentProgressValue: document.getElementById("telegram-preprocess-current-progress-value"),
        currentProgressStage: document.getElementById("telegram-preprocess-current-progress-stage"),
        topicBoardNote: document.getElementById("telegram-preprocess-topic-board-note"),
        progressLabel: document.getElementById("telegram-preprocess-progress-label"),
        progressFill: document.getElementById("telegram-preprocess-progress-fill"),
        inputTokens: document.getElementById("telegram-preprocess-input-tokens"),
        outputTokens: document.getElementById("telegram-preprocess-output-tokens"),
        inputTokenChip: document.getElementById("telegram-preprocess-input-token-chip"),
        outputTokenChip: document.getElementById("telegram-preprocess-output-token-chip"),
        topicLamps: document.getElementById("telegram-preprocess-topic-lamps"),
        error: document.getElementById("telegram-preprocess-error"),
    };

    renderBundle(state.bundle);
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
            renderBundle(payload);
            setLiveState(isRunning(payload.status) ? "live" : "idle");
            if (!isRunning(payload.status)) {
                stopStream();
            }
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
        if (!bundle) {
            setStatusTone(elements.statusChip, "idle", "idle");
            updateText(elements.stageBanner, "Waiting");
            updateText(elements.liveNote, "Waiting for a new preprocess run.");
            updateText(elements.currentTopicLabel, "Waiting");
            updateText(elements.currentTopicProgress, "处理中 Topic 0/0");
            updateText(elements.currentProgressValue, "处理中 Topic 0/0");
            updateText(elements.currentProgressStage, "Waiting");
            updateText(elements.topicBoardNote, "处理中 Topic 0/0");
            updateText(elements.progressLabel, "0%");
            updateText(elements.inputTokens, "0");
            updateText(elements.outputTokens, "0");
            updateText(elements.inputTokenChip, "In 0");
            updateText(elements.outputTokenChip, "Out 0");
            setWidth(elements.progressFill, 0);
            renderTopicLamps([]);
            return;
        }

        const percent = clampPercent(bundle.progress_percent || 0);
        const progressText = buildTopicProgressText(bundle);

        setStatusTone(elements.statusChip, bundle.status, bundle.status);
        updateText(elements.stageBanner, bundle.current_stage || "Waiting");
        updateText(elements.liveNote, buildLiveNote(bundle));
        updateText(elements.currentTopicLabel, bundle.current_topic_label || bundle.current_stage || "Waiting");
        updateText(elements.currentTopicProgress, progressText);
        updateText(elements.currentProgressValue, progressText);
        updateText(elements.currentProgressStage, bundle.current_stage || "Waiting");
        updateText(elements.topicBoardNote, progressText);
        updateText(elements.progressLabel, `${percent}%`);
        updateText(elements.inputTokens, bundle.prompt_tokens || 0);
        updateText(elements.outputTokens, bundle.completion_tokens || 0);
        updateText(elements.inputTokenChip, `In ${bundle.prompt_tokens || 0}`);
        updateText(elements.outputTokenChip, `Out ${bundle.completion_tokens || 0}`);
        setWidth(elements.progressFill, percent);
        renderTopicLamps(buildTopicLampModels(bundle));

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
            const meta = [weekKey, topic?.summary ? String(topic.summary).trim() : ""].filter(Boolean).join(" · ");
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
            return `已完成 Topic ${total || 0}/${total || 0}`;
        }
        if (bundle.status === "failed") {
            return `中断于 Topic ${Math.max(index, 0)}/${total || 0}`;
        }
        if (!total) {
            return "处理中 Topic 0/0";
        }
        const current = Math.max(index || 1, 1);
        return `处理中 Topic ${current}/${total}`;
    }

    function buildLiveNote(bundle) {
        const noteParts = [
            bundle.current_stage || bundle.status || "idle",
            bundle.current_topic_label ? `当前：${bundle.current_topic_label}` : "",
            bundle.updated_at ? `更新于 ${formatDateTime(bundle.updated_at)}` : "",
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
}

function escapeHtml(value) {
    return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}
