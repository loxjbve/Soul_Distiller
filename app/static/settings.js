import { fetchJson } from "./shared.js";

const bootstrap = JSON.parse(document.getElementById("settings-bootstrap")?.textContent || "{}");
const ui = bootstrap.ui_strings || {};

const PROVIDER_PRESETS = {
    openai: {
        requiresBaseUrl: false,
        hint: ui.hint_openai || "",
        placeholder: ui.placeholder_openai || "",
    },
    xai: {
        requiresBaseUrl: false,
        hint: ui.hint_xai || "",
        placeholder: ui.placeholder_xai || "",
    },
    gemini: {
        requiresBaseUrl: false,
        hint: ui.hint_gemini || "",
        placeholder: ui.placeholder_gemini || "",
    },
    "openai-compatible": {
        requiresBaseUrl: true,
        hint: ui.hint_custom || "",
        placeholder: ui.placeholder_custom || "",
    },
};

document.querySelectorAll("[data-provider-form]").forEach((form) => {
    const providerSelect = form.querySelector("[data-provider-select]");
    const baseUrlInput = form.querySelector("[data-base-url]");
    const baseHint = form.querySelector("[data-base-hint]");
    const discoverButton = form.querySelector("[data-discover-models]");
    const output = form.querySelector("[data-model-output]");
    const service = form.dataset.service;

    providerSelect?.addEventListener("change", () => applyPreset(providerSelect, baseUrlInput, baseHint));
    discoverButton?.addEventListener("click", async () => {
        output.textContent = ui.discover_loading || "正在拉取模型列表…";
        try {
            const payload = await fetchJson(`/api/settings/models?service=${encodeURIComponent(service)}`);
            if (!payload.models?.length) {
                output.textContent = ui.discover_empty || "当前服务没有返回可用模型。";
                return;
            }
            output.textContent = JSON.stringify(payload.models, null, 2);
            const modelInput = form.querySelector("[data-model-input]");
            if (modelInput && !modelInput.value) {
                modelInput.value = payload.models[0];
            }
        } catch (error) {
            output.textContent = `${ui.discover_failed || "模型发现失败"}：${error.message}`;
        }
    });

    applyPreset(providerSelect, baseUrlInput, baseHint);
});

function applyPreset(providerSelect, baseUrlInput, baseHint) {
    const preset = PROVIDER_PRESETS[providerSelect?.value] || PROVIDER_PRESETS["openai-compatible"];
    if (baseUrlInput) {
        baseUrlInput.required = preset.requiresBaseUrl;
        baseUrlInput.placeholder = preset.placeholder;
    }
    if (baseHint) {
        baseHint.textContent = preset.hint;
    }
}
