import { escapeHtml, fetchJson, safeParseJson, setButtonBusy, showNotice } from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("settings-bootstrap")?.textContent, {});
const ui = bootstrap.ui_strings || {};
const providerOptions = bootstrap.provider_options || [];
const apiModeOptions = bootstrap.api_mode_options || [];

const PROVIDER_PRESETS = {
    openai: {
        requiresBaseUrl: false,
        hint: ui.hint_openai || "留空时自动使用 OpenAI 官方端点。",
        placeholder: ui.placeholder_openai || "留空时自动使用官方 OpenAI 端点",
    },
    xai: {
        requiresBaseUrl: false,
        hint: ui.hint_xai || "留空时自动使用 xAI 官方端点。",
        placeholder: ui.placeholder_xai || "留空时自动使用官方 xAI 端点",
    },
    gemini: {
        requiresBaseUrl: false,
        hint: ui.hint_gemini || "留空时自动使用 Gemini 官方兼容端点。",
        placeholder: ui.placeholder_gemini || "留空时自动使用 Gemini 兼容端点",
    },
    "openai-compatible": {
        requiresBaseUrl: true,
        hint: ui.hint_custom || "自定义 OpenAI Compatible 服务必须填写 Base URL。",
        placeholder: ui.placeholder_custom || "例如 https://api.example.com/v1",
    },
};

const serviceStates = new Map();

document.querySelectorAll("[data-settings-service]").forEach((root) => {
    const service = root.dataset.settingsService || "";
    if (!service) {
        return;
    }
    const state = {
        service,
        root,
        bundle: normalizeBundle(bootstrap.services?.[service], service),
        notice: "",
        noticeTone: "info",
    };
    serviceStates.set(service, state);
    renderService(state);
});

function renderService(state) {
    const configs = state.bundle.configs;
    state.root.innerHTML = `
        <div class="settings-service__toolbar">
            <div>
                <strong>${escapeHtml(state.service === "chat" ? "多配置调度" : "多配置回退")}</strong>
                <p class="helper-text">Active 之外，从上到下就是 fallback 顺序。保存某个配置后会立即同步该配置可用模型。</p>
            </div>
            <div class="settings-service__toolbar-actions">
                <button type="button" class="ghost-button" data-save-order>保存排序与切换</button>
                <button type="button" class="ghost-button" data-add-config>添加配置</button>
            </div>
        </div>
        <div class="settings-service__notice" data-service-notice ${state.notice ? "" : "hidden"}></div>
        <div class="settings-config-list">
            ${configs.map((config, index) => renderConfigCard(state, config, index)).join("")}
        </div>
    `;

    showNotice(state.root.querySelector("[data-service-notice]"), state.notice, state.noticeTone);

    state.root.querySelector("[data-add-config]")?.addEventListener("click", async () => {
        state.bundle.configs.push(createBlankConfig(state.service, state.bundle.configs.length + 1));
        syncFallbackOrder(state);
        state.notice = "已新增空配置，填写后保存即可。";
        state.noticeTone = "info";
        renderService(state);
    });

    state.root.querySelector("[data-save-order]")?.addEventListener("click", async (event) => {
        await saveServiceState(state, null, event.currentTarget);
    });

    state.root.querySelectorAll("[data-config-id]").forEach((card) => bindConfigCard(state, card));
}

function renderConfigCard(state, config, index) {
    const isActive = config.id === state.bundle.active_config_id;
    const preset = getProviderPreset(config.provider_kind);
    const modelOptions = buildModelOptions(config);
    const statusLabel = isActive ? "ACTIVE" : `FALLBACK ${resolveFallbackIndex(state, config.id)}`;

    return `
        <article class="settings-config-card" data-config-id="${escapeHtml(config.id)}">
            <div class="settings-config-card__head">
                <label class="settings-config-card__active">
                    <input type="radio" name="${escapeHtml(`${state.service}-active-config`)}" value="${escapeHtml(config.id)}" ${isActive ? "checked" : ""} data-config-active>
                    <span class="status-chip ${isActive ? "tone-ready" : "tone-queued"}">${escapeHtml(statusLabel)}</span>
                </label>
                <div class="settings-config-card__actions">
                    <button type="button" class="ghost-button" data-move-up ${index === 0 ? "disabled" : ""}>上移</button>
                    <button type="button" class="ghost-button" data-move-down ${index === state.bundle.configs.length - 1 ? "disabled" : ""}>下移</button>
                    <button type="button" class="ghost-button" data-remove-config ${state.bundle.configs.length <= 1 ? "disabled" : ""}>删除</button>
                </div>
            </div>

            <div class="settings-config-card__grid">
                <label>
                    <span>配置名称</span>
                    <input type="text" value="${escapeHtml(config.label)}" data-field="label">
                </label>

                <label>
                    <span>${escapeHtml(ui.provider || "Provider")}</span>
                    <select data-field="provider_kind">
                        ${providerOptions.map((option) => `
                            <option value="${escapeHtml(option.value)}" ${option.value === config.provider_kind ? "selected" : ""}>${escapeHtml(option.label)}</option>
                        `).join("")}
                    </select>
                </label>

                ${state.service === "chat" ? `
                    <label>
                        <span>${escapeHtml(ui.api_mode || "API Mode")}</span>
                        <select data-field="api_mode">
                            ${apiModeOptions.map((option) => `
                                <option value="${escapeHtml(option.value)}" ${option.value === config.api_mode ? "selected" : ""}>${escapeHtml(option.label)}</option>
                            `).join("")}
                        </select>
                    </label>
                ` : `
                    <label>
                        <span>回退说明</span>
                        <input type="text" value="${escapeHtml(isActive ? "当前主配置" : "按排序作为回退")}" readonly>
                    </label>
                `}

                <label class="settings-config-card__span-2">
                    <span>${escapeHtml(ui.base_url || "Base URL")}</span>
                    <input type="text" value="${escapeHtml(config.base_url)}" data-field="base_url" placeholder="${escapeHtml(preset.placeholder)}" ${preset.requiresBaseUrl ? "required" : ""}>
                    <small class="helper-text" data-base-hint>${escapeHtml(preset.hint)}</small>
                </label>

                <label class="settings-config-card__span-2">
                    <span>${escapeHtml(ui.api_key || "API Key")}</span>
                    <input type="password" value="${escapeHtml(config.api_key)}" data-field="api_key" placeholder="sk-...">
                </label>

                <label>
                    <span>模型下拉</span>
                    <select data-field="model_select">
                        ${modelOptions}
                    </select>
                </label>

                <label>
                    <span>手动模型覆盖</span>
                    <input type="text" value="${escapeHtml(config.model)}" data-field="model" placeholder="${escapeHtml(ui.model_placeholder || "保存后自动查询模型，也可手动填写")}">
                </label>
            </div>

            <div class="settings-config-card__footer">
                <p class="helper-text">保存后会立刻查询该配置的模型列表，并把结果写回下拉框。</p>
                <button type="button" class="primary-button" data-save-config>保存并同步模型</button>
            </div>
        </article>
    `;
}

function bindConfigCard(state, card) {
    const configId = card.dataset.configId || "";
    const config = findConfig(state, configId);
    if (!config) {
        return;
    }

    card.querySelectorAll("[data-field]").forEach((field) => {
        field.addEventListener("input", () => updateConfigField(state, configId, field));
        field.addEventListener("change", () => updateConfigField(state, configId, field, { rerender: field.dataset.field === "provider_kind" || field.dataset.field === "model_select" }));
    });

    card.querySelector("[data-config-active]")?.addEventListener("change", () => {
        state.bundle.active_config_id = configId;
        syncFallbackOrder(state);
        state.notice = "已切换活跃配置，记得保存需要同步模型的配置。";
        state.noticeTone = "info";
        renderService(state);
    });

    card.querySelector("[data-move-up]")?.addEventListener("click", () => {
        moveConfig(state, configId, -1);
        renderService(state);
    });

    card.querySelector("[data-move-down]")?.addEventListener("click", () => {
        moveConfig(state, configId, 1);
        renderService(state);
    });

    card.querySelector("[data-remove-config]")?.addEventListener("click", () => {
        removeConfig(state, configId);
        renderService(state);
    });

    card.querySelector("[data-save-config]")?.addEventListener("click", async (event) => {
        await saveServiceState(state, configId, event.currentTarget);
    });
}

function updateConfigField(state, configId, field, { rerender = false } = {}) {
    const config = findConfig(state, configId);
    if (!config) {
        return;
    }
    const key = field.dataset.field || "";
    if (key === "model_select") {
        if (field.value) {
            config.model = field.value;
            const modelInput = field.closest("[data-config-id]")?.querySelector('[data-field="model"]');
            if (modelInput) {
                modelInput.value = field.value;
            }
        }
    } else {
        config[key] = field.value;
    }
    if (key === "provider_kind" && !config.base_url) {
        config.base_url = "";
    }
    if (key === "provider_kind" || rerender) {
        renderService(state);
    }
}

async function saveServiceState(state, discoverConfigId, button) {
    const payload = {
        active_config_id: state.bundle.active_config_id,
        discover_config_id: discoverConfigId,
        fallback_order: state.bundle.fallback_order,
        configs: state.bundle.configs.map((config) => ({
            id: config.id,
            label: config.label,
            provider_kind: config.provider_kind,
            base_url: config.base_url,
            api_key: config.api_key,
            model: config.model,
            api_mode: config.api_mode,
            available_models: config.available_models || [],
        })),
    };
    const notice = state.root.querySelector("[data-service-notice]");
    showNotice(notice, discoverConfigId ? "正在保存并同步模型..." : "正在保存设置...", "info");
    setButtonBusy(button, true, discoverConfigId ? "同步中..." : "保存中...");
    try {
        const response = await fetchJson(`/api/settings/${encodeURIComponent(state.service)}`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        state.bundle = normalizeBundle(response.bundle, state.service);
        state.notice = response.discover_error
            ? `已保存，但模型同步失败：${response.discover_error}`
            : (discoverConfigId ? "已保存并同步模型列表。" : "已保存当前顺序与活跃配置。");
        state.noticeTone = response.discover_error ? "warning" : "success";
        renderService(state);
    } catch (error) {
        state.notice = error.message || "保存失败。";
        state.noticeTone = "warning";
        renderService(state);
    } finally {
        setButtonBusy(button, false);
    }
}

function normalizeBundle(bundle, service) {
    const source = bundle && typeof bundle === "object" ? bundle : {};
    const configs = Array.isArray(source.configs) ? source.configs.map((item, index) => normalizeConfig(item, service, index + 1)) : [];
    if (!configs.length) {
        configs.push(createBlankConfig(service, 1));
    }
    const configIds = configs.map((config) => config.id);
    let activeConfigId = String(source.active_config_id || "").trim();
    if (!configIds.includes(activeConfigId)) {
        activeConfigId = configIds[0];
    }
    const fallbackOrder = [];
    const seenIds = new Set([activeConfigId]);
    for (const item of Array.isArray(source.fallback_order) ? source.fallback_order : []) {
        const configId = String(item || "").trim();
        if (configId && configIds.includes(configId) && !seenIds.has(configId)) {
            fallbackOrder.push(configId);
            seenIds.add(configId);
        }
    }
    for (const configId of configIds) {
        if (!seenIds.has(configId)) {
            fallbackOrder.push(configId);
            seenIds.add(configId);
        }
    }
    return {
        active_config_id: activeConfigId,
        fallback_order: fallbackOrder,
        configs,
    };
}

function normalizeConfig(config, service, index) {
    const source = config && typeof config === "object" ? config : {};
    const availableModels = Array.isArray(source.available_models)
        ? [...new Set(source.available_models.map((item) => String(item || "").trim()).filter(Boolean))]
        : [];
    return {
        id: String(source.id || createConfigId(service)).trim() || createConfigId(service),
        label: String(source.label || `${service === "chat" ? "Chat" : "Embedding"} ${index}`).trim() || `${service === "chat" ? "Chat" : "Embedding"} ${index}`,
        provider_kind: String(source.provider_kind || "openai"),
        base_url: String(source.base_url || ""),
        api_key: String(source.api_key || ""),
        model: String(source.model || ""),
        api_mode: service === "chat" ? String(source.api_mode || "responses") : "responses",
        available_models: availableModels,
    };
}

function createBlankConfig(service, index) {
    return {
        id: createConfigId(service),
        label: `${service === "chat" ? "Chat" : "Embedding"} ${index}`,
        provider_kind: "openai",
        base_url: "",
        api_key: "",
        model: "",
        api_mode: "responses",
        available_models: [],
    };
}

function createConfigId(service) {
    if (window.crypto?.randomUUID) {
        return `${service}-${window.crypto.randomUUID()}`;
    }
    return `${service}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function findConfig(state, configId) {
    return state.bundle.configs.find((config) => config.id === configId) || null;
}

function moveConfig(state, configId, offset) {
    const index = state.bundle.configs.findIndex((config) => config.id === configId);
    const targetIndex = index + offset;
    if (index < 0 || targetIndex < 0 || targetIndex >= state.bundle.configs.length) {
        return;
    }
    const [config] = state.bundle.configs.splice(index, 1);
    state.bundle.configs.splice(targetIndex, 0, config);
    syncFallbackOrder(state);
    state.notice = "已调整配置顺序。";
    state.noticeTone = "info";
}

function removeConfig(state, configId) {
    state.bundle.configs = state.bundle.configs.filter((config) => config.id !== configId);
    if (!state.bundle.configs.length) {
        state.bundle.configs = [createBlankConfig(state.service, 1)];
    }
    if (!state.bundle.configs.some((config) => config.id === state.bundle.active_config_id)) {
        state.bundle.active_config_id = state.bundle.configs[0].id;
    }
    syncFallbackOrder(state);
    state.notice = "已移除配置。";
    state.noticeTone = "info";
}

function syncFallbackOrder(state) {
    state.bundle.fallback_order = state.bundle.configs
        .map((config) => config.id)
        .filter((configId) => configId !== state.bundle.active_config_id);
}

function resolveFallbackIndex(state, configId) {
    const index = state.bundle.fallback_order.indexOf(configId);
    return index >= 0 ? index + 1 : 0;
}

function buildModelOptions(config) {
    const values = [...config.available_models];
    if (config.model && !values.includes(config.model)) {
        values.unshift(config.model);
    }
    if (!values.length) {
        return `<option value="">保存后自动查询模型列表</option>`;
    }
    return values.map((modelName) => `
        <option value="${escapeHtml(modelName)}" ${modelName === config.model ? "selected" : ""}>${escapeHtml(modelName)}</option>
    `).join("");
}

function getProviderPreset(provider) {
    return PROVIDER_PRESETS[provider] || PROVIDER_PRESETS["openai-compatible"];
}
