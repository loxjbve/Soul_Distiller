import { escapeHtml, fetchJson, safeParseJson, setButtonBusy, showNotice } from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("settings-bootstrap")?.textContent, {});
const ui = bootstrap.ui_strings || {};
const providerOptions = Array.isArray(bootstrap.provider_options) ? bootstrap.provider_options : [];
const apiModeOptions = Array.isArray(bootstrap.api_mode_options) ? bootstrap.api_mode_options : [];
const providerLabelMap = new Map(providerOptions.map((option) => [String(option.value || ""), String(option.label || option.value || "")]));
const apiModeLabelMap = new Map(apiModeOptions.map((option) => [String(option.value || ""), String(option.label || option.value || "")]));

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

document.querySelectorAll("[data-settings-service]").forEach((root) => {
    const service = root.dataset.settingsService || "";
    if (!service) {
        return;
    }
    const bundle = normalizeBundle(bootstrap.services?.[service], service);
    const state = {
        service,
        root,
        bundle,
        notice: "",
        noticeTone: "info",
        selectedConfigId: bundle.active_config_id,
    };
    renderService(state);
});

function renderService(state) {
    const selectedConfig = ensureSelectedConfig(state);
    const configs = state.bundle.configs;
    const selectedIndex = Math.max(0, configs.findIndex((config) => config.id === selectedConfig.id));
    const activeConfig = findConfig(state, state.bundle.active_config_id) || configs[0];
    const providerCoverage = getProviderCoverage(configs);
    const copy = getServiceCopy(state.service);

    state.root.innerHTML = `
        <div class="settings-service__toolbar">
            <div class="settings-service__toolbar-copy">
                <span class="settings-service__eyebrow">${escapeHtml(copy.eyebrow)}</span>
                <div class="settings-service__toolbar-headline">
                    <strong>${escapeHtml(copy.title)}</strong>
                    <span class="settings-service__toolbar-badge">${escapeHtml(`${configs.length} ${copy.configUnit}`)}</span>
                </div>
                <p class="helper-text">${escapeHtml(copy.summary)}</p>
            </div>
            <div class="settings-service__toolbar-actions">
                <button type="button" class="ghost-button" data-save-order>${escapeHtml(ui.save_order || "保存排序与切换")}</button>
                <button type="button" class="ghost-button" data-add-config>${escapeHtml(ui.add_config || "添加配置")}</button>
            </div>
        </div>
        <div class="settings-service__notice" data-service-notice ${state.notice ? "" : "hidden"}></div>
        <div class="settings-service__overview">
            ${renderMetricCard(
        ui.config_count || "配置数量",
        String(configs.length),
        copy.summary,
    )}
            ${renderMetricCard(
        ui.active_config || "当前 Active",
        activeConfig?.label || "--",
        getStatusLabel(state, activeConfig?.id || ""),
    )}
            ${renderMetricCard(
        ui.fallback_count || "回退数量",
        String(Math.max(configs.length - 1, 0)),
        ui.page_sequence_note || "点击分页卡片切换配置，当前顺序就是回退顺序。",
    )}
            ${renderMetricCard(
        ui.provider_coverage || "Provider 覆盖",
        String(providerCoverage.count),
        providerCoverage.summary,
    )}
        </div>
        <div class="settings-config-deck">
            <div class="settings-config-pagination">
                <div class="settings-config-pagination__head">
                    <div>
                        <span class="settings-config-pagination__eyebrow">${escapeHtml(ui.sequence_title || "配置分页")}</span>
                        <strong>${escapeHtml(ui.page_sequence_note || "点击分页卡片切换配置，当前顺序就是回退顺序。")}</strong>
                    </div>
                    <span class="settings-config-pagination__total">${escapeHtml(formatPageCounter(selectedIndex, configs.length))}</span>
                </div>
                <div class="settings-config-pagination__list">
                    ${configs.map((config, index) => renderPageButton(state, config, index)).join("")}
                </div>
            </div>
            <div class="settings-config-stage">
                <div class="settings-config-stage__head">
                    <div>
                        <span class="settings-config-stage__eyebrow">${escapeHtml(ui.selected_config || "当前编辑")}</span>
                        <h3>${escapeHtml(selectedConfig.label)}</h3>
                        <p class="helper-text">${escapeHtml(copy.pageHint)}</p>
                    </div>
                    <div class="settings-config-stage__pager">
                        <button type="button" class="ghost-button" data-prev-page ${selectedIndex === 0 ? "disabled" : ""}>${escapeHtml(ui.previous || "上一页")}</button>
                        <span class="settings-config-stage__indicator">${escapeHtml(formatPageCounter(selectedIndex, configs.length))}</span>
                        <button type="button" class="ghost-button" data-next-page ${selectedIndex === configs.length - 1 ? "disabled" : ""}>${escapeHtml(ui.next || "下一页")}</button>
                    </div>
                </div>
                ${renderConfigCard(state, selectedConfig, selectedIndex)}
            </div>
        </div>
    `;

    showNotice(state.root.querySelector("[data-service-notice]"), state.notice, state.noticeTone);

    state.root.querySelector("[data-add-config]")?.addEventListener("click", () => {
        const config = createBlankConfig(state.service, state.bundle.configs.length + 1);
        state.bundle.configs.push(config);
        state.selectedConfigId = config.id;
        syncFallbackOrder(state);
        state.notice = ui.notice_added || "已新增空配置，填写后保存即可。";
        state.noticeTone = "info";
        renderService(state);
    });

    state.root.querySelector("[data-save-order]")?.addEventListener("click", async (event) => {
        await saveServiceState(state, null, event.currentTarget);
    });

    state.root.querySelector("[data-prev-page]")?.addEventListener("click", () => {
        selectConfigByOffset(state, -1);
        renderService(state);
    });

    state.root.querySelector("[data-next-page]")?.addEventListener("click", () => {
        selectConfigByOffset(state, 1);
        renderService(state);
    });

    state.root.querySelectorAll("[data-select-config]").forEach((button) => {
        button.addEventListener("click", () => {
            state.selectedConfigId = button.dataset.selectConfig || state.selectedConfigId;
            renderService(state);
        });
    });

    state.root.querySelectorAll("[data-config-id]").forEach((card) => bindConfigCard(state, card));
}

function renderConfigCard(state, config, index) {
    const isActive = config.id === state.bundle.active_config_id;
    const preset = getProviderPreset(config.provider_kind);
    const modelOptions = buildModelOptions(config);
    const statusLabel = getStatusLabel(state, config.id);
    const providerLabel = getOptionLabel(providerLabelMap, config.provider_kind, config.provider_kind);
    const apiModeLabel = getOptionLabel(apiModeLabelMap, config.api_mode, config.api_mode);
    const secondaryMeta = state.service === "chat" ? `${providerLabel} · ${apiModeLabel}` : providerLabel;

    return `
        <article class="settings-config-card" data-config-id="${escapeHtml(config.id)}">
            <div class="settings-config-card__head">
                <div class="settings-config-card__identity">
                    <label class="settings-config-card__active">
                        <input type="radio" name="${escapeHtml(`${state.service}-active-config`)}" value="${escapeHtml(config.id)}" ${isActive ? "checked" : ""} data-config-active>
                        <span class="status-chip ${isActive ? "tone-ready" : "tone-queued"}">${escapeHtml(statusLabel)}</span>
                    </label>
                    <div class="settings-config-card__identity-text">
                        <strong>${escapeHtml(config.label)}</strong>
                        <p>${escapeHtml(secondaryMeta)}</p>
                    </div>
                </div>
                <div class="settings-config-card__actions">
                    <button type="button" class="ghost-button" data-move-up ${index === 0 ? "disabled" : ""}>${escapeHtml(ui.move_up || "上移")}</button>
                    <button type="button" class="ghost-button" data-move-down ${index === state.bundle.configs.length - 1 ? "disabled" : ""}>${escapeHtml(ui.move_down || "下移")}</button>
                    <button type="button" class="ghost-button danger-button" data-remove-config ${state.bundle.configs.length <= 1 ? "disabled" : ""}>${escapeHtml(ui.delete_config || "删除配置")}</button>
                </div>
            </div>

            <div class="settings-config-card__metrics">
                ${renderConfigMetric(
        ui.order_position || "排序位置",
        `#${formatIndex(index + 1)}`,
        statusLabel,
    )}
                ${renderConfigMetric(
        ui.models_discovered || "已发现模型",
        String(config.available_models.length),
        summarizeDiscoveredModels(config),
    )}
                ${renderConfigMetric(
        state.service === "chat" ? (ui.api_mode || "API Mode") : (ui.provider || "Provider"),
        state.service === "chat" ? apiModeLabel : providerLabel,
        state.service === "chat" ? providerLabel : "",
    )}
            </div>

            <div class="settings-config-card__grid">
                <label>
                    <span>${escapeHtml(ui.config_name || "配置名称")}</span>
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
                        <span>${escapeHtml(ui.fallback_note || "回退说明")}</span>
                        <input type="text" value="${escapeHtml(isActive ? (ui.fallback_active || "当前主配置") : (ui.fallback_ordered || "按当前排序参与回退"))}" readonly>
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
                    <span>${escapeHtml(ui.model_select || "模型下拉")}</span>
                    <select data-field="model_select">
                        ${modelOptions}
                    </select>
                </label>

                <label>
                    <span>${escapeHtml(ui.model_override || "手动模型覆盖")}</span>
                    <input type="text" value="${escapeHtml(config.model)}" data-field="model" placeholder="${escapeHtml(ui.model_placeholder || "保存后自动查询模型，也可手动填写")}">
                </label>
            </div>

            <div class="settings-config-card__footer">
                <p class="helper-text">${escapeHtml(ui.save_sync_hint || "保存后会立即拉取模型列表，并刷新该配置的候选模型。")}</p>
                <button type="button" class="primary-button" data-save-config>${escapeHtml(ui.save_and_sync || "保存并同步模型")}</button>
            </div>
        </article>
    `;
}

function renderPageButton(state, config, index) {
    const isSelected = config.id === state.selectedConfigId;
    const isActive = config.id === state.bundle.active_config_id;
    const providerLabel = getOptionLabel(providerLabelMap, config.provider_kind, config.provider_kind);
    const apiModeLabel = getOptionLabel(apiModeLabelMap, config.api_mode, config.api_mode);
    const statusLabel = getStatusLabel(state, config.id);
    const meta = state.service === "chat" ? `${providerLabel} · ${apiModeLabel}` : providerLabel;
    const modelCountLabel = `${config.available_models.length} ${ui.models_discovered || "已发现模型"}`;

    return `
        <button
            type="button"
            class="settings-config-page ${isSelected ? "is-selected" : ""} ${isActive ? "is-active" : ""}"
            data-select-config="${escapeHtml(config.id)}"
            aria-pressed="${isSelected ? "true" : "false"}"
        >
            <span class="settings-config-page__index">${escapeHtml(formatIndex(index + 1))}</span>
            <span class="settings-config-page__body">
                <span class="settings-config-page__headline">
                    <strong>${escapeHtml(config.label)}</strong>
                    <span class="settings-config-page__status">${escapeHtml(statusLabel)}</span>
                </span>
                <span class="settings-config-page__meta">${escapeHtml(meta)}</span>
                <span class="settings-config-page__foot">${escapeHtml(modelCountLabel)}</span>
            </span>
        </button>
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
        field.addEventListener("change", () => updateConfigField(state, configId, field, {
            rerender: ["provider_kind", "model_select", "label", "api_mode"].includes(field.dataset.field || ""),
        }));
    });

    card.querySelector("[data-config-active]")?.addEventListener("change", () => {
        state.bundle.active_config_id = configId;
        syncFallbackOrder(state);
        state.notice = ui.notice_switched || "已切换活跃配置，记得保存需要同步模型的配置。";
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
        config.model = field.value || "";
        const modelInput = field.closest("[data-config-id]")?.querySelector('[data-field="model"]');
        if (modelInput) {
            modelInput.value = config.model;
        }
    } else {
        config[key] = field.value;
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
    const busyLabel = discoverConfigId
        ? (ui.notice_syncing || "正在保存并同步模型...")
        : (ui.notice_saving || "正在保存设置...");
    showNotice(notice, busyLabel, "info");
    setButtonBusy(button, true, busyLabel);

    try {
        const response = await fetchJson(`/api/settings/${encodeURIComponent(state.service)}`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        state.bundle = normalizeBundle(response.bundle, state.service);
        state.notice = response.discover_error
            ? `${ui.notice_sync_failed_prefix || "已保存，但模型同步失败："}${response.discover_error}`
            : (discoverConfigId ? (ui.notice_synced || "已保存并同步模型列表。") : (ui.notice_saved || "已保存当前顺序与活跃配置。"));
        state.noticeTone = response.discover_error ? "warning" : "success";
        renderService(state);
    } catch (error) {
        state.notice = error.message || ui.notice_save_failed || "保存失败。";
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

function ensureSelectedConfig(state) {
    if (!state.bundle.configs.length) {
        const config = createBlankConfig(state.service, 1);
        state.bundle.configs = [config];
        state.bundle.active_config_id = config.id;
        syncFallbackOrder(state);
    }

    if (!state.bundle.configs.some((config) => config.id === state.selectedConfigId)) {
        state.selectedConfigId = state.bundle.active_config_id || state.bundle.configs[0].id;
    }

    return findConfig(state, state.selectedConfigId) || state.bundle.configs[0];
}

function selectConfigByOffset(state, offset) {
    const currentIndex = state.bundle.configs.findIndex((config) => config.id === state.selectedConfigId);
    const targetIndex = currentIndex + offset;
    if (currentIndex < 0 || targetIndex < 0 || targetIndex >= state.bundle.configs.length) {
        return;
    }
    state.selectedConfigId = state.bundle.configs[targetIndex].id;
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
    state.notice = ui.notice_reordered || "已调整配置顺序。";
    state.noticeTone = "info";
}

function removeConfig(state, configId) {
    const removedIndex = state.bundle.configs.findIndex((config) => config.id === configId);
    state.bundle.configs = state.bundle.configs.filter((config) => config.id !== configId);

    if (!state.bundle.configs.length) {
        state.bundle.configs = [createBlankConfig(state.service, 1)];
    }

    if (!state.bundle.configs.some((config) => config.id === state.bundle.active_config_id)) {
        state.bundle.active_config_id = state.bundle.configs[0].id;
    }

    const nextSelection = state.bundle.configs[Math.min(Math.max(removedIndex, 0), state.bundle.configs.length - 1)];
    state.selectedConfigId = nextSelection?.id || state.bundle.active_config_id;
    syncFallbackOrder(state);
    state.notice = ui.notice_removed || "已移除配置。";
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
        return `<option value="">${escapeHtml(ui.model_select_placeholder || "保存后自动查询模型列表")}</option>`;
    }

    return values.map((modelName) => `
        <option value="${escapeHtml(modelName)}" ${modelName === config.model ? "selected" : ""}>${escapeHtml(modelName)}</option>
    `).join("");
}

function getProviderPreset(provider) {
    return PROVIDER_PRESETS[provider] || PROVIDER_PRESETS["openai-compatible"];
}

function renderMetricCard(label, value, detail = "") {
    return `
        <div class="settings-service__metric">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
            <small>${escapeHtml(detail)}</small>
        </div>
    `;
}

function renderConfigMetric(label, value, detail = "") {
    return `
        <div class="settings-config-card__metric">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
            <small>${escapeHtml(detail)}</small>
        </div>
    `;
}

function formatIndex(value) {
    return String(value).padStart(2, "0");
}

function formatPageCounter(index, total) {
    if (!total) {
        return "00 / 00";
    }
    return `${formatIndex(index + 1)} / ${formatIndex(total)}`;
}

function getOptionLabel(map, value, fallback) {
    return map.get(String(value || "")) || fallback || "--";
}

function getStatusLabel(state, configId) {
    if (configId === state.bundle.active_config_id) {
        return ui.active_status || "ACTIVE";
    }
    return `${ui.fallback_status || "FALLBACK"} ${resolveFallbackIndex(state, configId)}`;
}

function summarizeDiscoveredModels(config) {
    if (!config.available_models.length) {
        return ui.discover_empty || "当前服务没有返回可用模型。";
    }
    const preview = config.available_models.slice(0, 2).join(" · ");
    if (config.available_models.length <= 2) {
        return preview;
    }
    return `${preview} +${config.available_models.length - 2}`;
}

function getProviderCoverage(configs) {
    const labels = [...new Set(configs
        .map((config) => getOptionLabel(providerLabelMap, config.provider_kind, config.provider_kind))
        .filter(Boolean))];
    return {
        count: labels.length,
        summary: labels.join(" · ") || "--",
    };
}

function getServiceCopy(service) {
    const isChat = service === "chat";
    return {
        eyebrow: isChat
            ? (ui.service_chat_eyebrow || "Chat Orchestration")
            : (ui.service_embedding_eyebrow || "Embedding Routing"),
        title: isChat
            ? (ui.chat_panel || "Chat LLM")
            : (ui.embedding_panel || "Embedding Model"),
        summary: isChat
            ? (ui.service_chat_summary || "把多个 Chat 入口组织成主备链路，快速切换 Provider 与 API 模式。")
            : (ui.service_embedding_summary || "为 Embedding 服务维护主备链路，确保检索与召回持续可用。"),
        pageHint: ui.page_sequence_note || "点击分页卡片切换配置，当前顺序就是回退顺序。",
        configUnit: ui.config_unit || "项",
    };
}
