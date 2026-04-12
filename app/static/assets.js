import {
    clampPercent,
    fetchJson,
    parseSseBlock,
    setButtonBusy,
    showNotice,
    updateText,
} from "./shared.js";

const bootstrap = JSON.parse(document.getElementById("assets-page-bootstrap")?.textContent || "{}");

if (bootstrap?.project_id) {
    const ui = bootstrap.ui_strings || {};
    const projectId = bootstrap.project_id;
    const assetKind = bootstrap.asset_kind || "skill";

    const elements = {
        form: document.getElementById("generate-form"),
        button: document.getElementById("generate-btn"),
        shell: document.getElementById("asset-generation-shell"),
        stageChip: document.getElementById("asset-stage-chip"),
        stage: document.getElementById("asset-current-stage"),
        percent: document.getElementById("asset-current-percent"),
        fill: document.getElementById("asset-progress-fill"),
        state: document.getElementById("asset-generation-state"),
        message: document.getElementById("asset-generation-message"),
        output: document.getElementById("generation-output"),
        chunkCount: document.getElementById("asset-chunk-count"),
        charCount: document.getElementById("asset-char-count"),
    };

    let chunkCount = 0;
    let charCount = 0;

    elements.form?.addEventListener("submit", async (event) => {
        event.preventDefault();
        setButtonBusy(elements.button, true, ui.status_running || "生成中");
        elements.shell.hidden = false;
        elements.output.textContent = "";
        chunkCount = 0;
        charCount = 0;
        updateCounts();
        try {
            await streamGenerate();
        } catch (error) {
            await fallbackGenerate(error);
        } finally {
            setButtonBusy(elements.button, false);
        }
    });

    async function streamGenerate() {
        const response = await fetch(`/api/projects/${projectId}/assets/generate/stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ asset_kind: assetKind }),
        });
        if (!response.ok || !response.body) {
            throw new Error("流式生成不可用");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            buffer += decoder.decode(value || new Uint8Array(), { stream: !done });

            let boundary = buffer.indexOf("\n\n");
            while (boundary >= 0) {
                const block = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);
                handleEventBlock(block);
                boundary = buffer.indexOf("\n\n");
            }

            if (done) {
                break;
            }
        }
    }

    function handleEventBlock(block) {
        const parsed = parseSseBlock(block);
        if (!parsed) {
            return;
        }
        const { eventType, data } = parsed;
        if (eventType === "status") {
            renderStatus(data);
            return;
        }
        if (eventType === "delta") {
            const chunk = data.chunk || "";
            elements.output.textContent += chunk;
            chunkCount += 1;
            charCount += chunk.length;
            updateCounts();
            return;
        }
        if (eventType === "done") {
            renderStatus({ status: "completed", progress_percent: 100, message: data.message || ui.status_completed });
            window.setTimeout(() => {
                window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}`;
            }, 700);
            return;
        }
        if (eventType === "error") {
            throw new Error(data.message || ui.status_failed || "生成失败");
        }
    }

    function renderStatus(payload) {
        const percent = clampPercent(payload.progress_percent || 0);
        updateText(elements.stageChip, payload.status || ui.status_running || "生成中");
        updateText(elements.stage, payload.phase || payload.status || ui.status_running || "生成中");
        updateText(elements.percent, `${percent}%`);
        updateText(elements.state, payload.status || ui.status_running || "生成中");
        updateText(elements.message, payload.message || "");
        if (elements.fill) {
            elements.fill.style.width = `${percent}%`;
        }
    }

    function updateCounts() {
        updateText(elements.chunkCount, chunkCount);
        updateText(elements.charCount, charCount);
    }

    async function fallbackGenerate(error) {
        renderStatus({
            status: "failed",
            progress_percent: 0,
            message: `${error.message}，正在切换到普通生成接口…`,
        });
        const payload = await fetchJson(`/api/projects/${projectId}/assets/generate`, {
            method: "POST",
            body: JSON.stringify({ asset_kind: assetKind }),
        });
        showNotice(elements.message, payload.message || "草稿已生成。", "success");
        window.location.href = `/projects/${projectId}/assets?kind=${encodeURIComponent(assetKind)}&draft=${encodeURIComponent(payload.id || "")}`;
    }
}
