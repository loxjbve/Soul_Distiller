import {
    clampPercent,
    safeParseJson,
    setStatusTone,
    updateText,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("stone-preprocess-bootstrap")?.textContent, {});

if (bootstrap?.project_id) {
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id,
        stream: null,
    };

    const elements = {
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
        topicLamps: document.getElementById("stone-preprocess-topic-lamps"),
        error: document.getElementById("stone-preprocess-error"),
    };

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
            if (!payload) {
                return;
            }
            renderBundle(payload);
            setLiveState(isRunning(payload.status) ? "live" : "idle");
            if (!isRunning(payload.status)) {
                stopStream();
            }
        });

        state.stream.addEventListener("done", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            setLiveState("idle");
            stopStream();
        });

        state.stream.addEventListener("error", () => {
            setLiveState("disconnected");
            stopStream();
        });
    }

    function stopStream() {
        if (state.stream) {
            state.stream.close();
            state.stream = null;
        }
    }

    function setLiveState(status) {
        if (!elements.livePill) return;
        if (status === "live") {
            elements.livePill.textContent = "LIVE";
            elements.livePill.className = "analysis-live-pill tone-live";
        } else if (status === "connecting") {
            elements.livePill.textContent = "CONNECTING";
            elements.livePill.className = "analysis-live-pill tone-queued";
        } else if (status === "disconnected") {
            elements.livePill.textContent = "DISCONNECTED";
            elements.livePill.className = "analysis-live-pill tone-failed";
        } else {
            elements.livePill.textContent = "WAITING";
            elements.livePill.className = "analysis-live-pill";
        }
    }

    function isRunning(status) {
        return status === "queued" || status === "running";
    }

    function renderBundle(payload) {
        if (!payload) {
            return;
        }

        const isRunActive = isRunning(payload.status);

        if (elements.statusChip) {
            setStatusTone(elements.statusChip, payload.status, payload.status);
        }

        updateText(elements.stageBanner, payload.current_stage || "等待运行");

        const completed = payload.stone_profile_completed || 0;
        const total = payload.stone_profile_total || 0;
        
        updateText(elements.currentTopicLabel, "文档处理进度");
        updateText(elements.currentTopicProgress, `已处理文档 ${completed}/${total}`);
        updateText(elements.topicBoardNote, `已处理文档 ${completed}/${total}`);

        const percent = clampPercent(payload.progress_percent || 0);
        updateText(elements.progressLabel, `${percent}%`);
        if (elements.progressFill) {
            elements.progressFill.style.width = `${percent}%`;
        }

        updateText(elements.inputTokenChip, `Input Tokens ${payload.prompt_tokens || 0}`);
        updateText(elements.outputTokenChip, `Output Tokens ${payload.completion_tokens || 0}`);

        if (payload.error_message && elements.error) {
            updateText(elements.error, payload.error_message);
            elements.error.hidden = false;
        } else if (elements.error) {
            elements.error.hidden = true;
        }

        // Update lamps
        if (elements.topicLamps) {
            const lamps = elements.topicLamps.querySelectorAll(".stone-topic-lamp");
            lamps.forEach((lamp, index) => {
                const docIndex = index + 1;
                // simple heuristic for lamp status
                if (docIndex <= completed) {
                    lamp.className = "stone-topic-lamp status-completed";
                } else if (docIndex === completed + 1 && isRunActive) {
                    lamp.className = "stone-topic-lamp status-running is-current";
                } else {
                    lamp.className = "stone-topic-lamp status-queued";
                }
            });
        }
    }
}
