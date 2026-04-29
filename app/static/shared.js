const OPEN_CLASS = "is-open";
const FIXED_SHELL_MIN_WIDTH = 1100;
const FIXED_SHELL_MIN_HEIGHT = 780;

let lastFocusedElement = null;
let cursorGlowFrame = null;

export function debounce(func, wait) {
    let timeout;
    return function(...args) {
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(this, args), wait);
    };
}

export function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => (inThrottle = false), limit);
        }
    };
}

export function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

export function safeParseJson(text, fallback = null) {
    try {
        return JSON.parse(text ?? "null");
    } catch {
        return fallback;
    }
}

export async function fetchJson(url, options = {}) {
    const headers = new Headers(options.headers || {});
    if (options.body && !headers.has("Content-Type") && !(options.body instanceof FormData)) {
        headers.set("Content-Type", "application/json");
    }
    const response = await fetch(url, { ...options, headers });
    const payload = safeParseJson(await response.text(), {});
    if (!response.ok) {
        throw new Error(payload.detail || payload.message || "Request failed");
    }
    return payload;
}

export function formatDateTime(value, options = {}) {
    if (!value) {
        return "--";
    }
    try {
        return new Intl.DateTimeFormat(document.body.dataset.locale || "zh-CN", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            ...options,
        }).format(new Date(value));
    } catch {
        return String(value);
    }
}

export function clampPercent(value) {
    const numeric = Number(value || 0);
    if (Number.isNaN(numeric)) {
        return 0;
    }
    return Math.max(0, Math.min(100, Math.round(numeric)));
}

export function normalizeStatus(status) {
    const normalized = String(status || "pending").toLowerCase().replaceAll(" ", "_");
    if (normalized === "partial_failed") {
        return "failed";
    }
    return normalized || "pending";
}

export function updateText(target, value) {
    const node = typeof target === "string" ? document.getElementById(target) : target;
    if (!node) {
        return;
    }
    node.textContent = String(value ?? "");
}

export function setStatusTone(target, status, label) {
    const node = typeof target === "string" ? document.getElementById(target) : target;
    if (!node) {
        return;
    }
    const normalized = normalizeStatus(status);
    node.textContent = label ?? normalized;
    Array.from(node.classList)
        .filter((className) => className.startsWith("tone-"))
        .forEach((className) => node.classList.remove(className));
    node.classList.add(`tone-${normalized}`);
}

export function parseSseBlock(block) {
    const lines = String(block || "")
        .split("\n")
        .map((line) => line.trimEnd())
        .filter(Boolean);
    if (!lines.length) {
        return null;
    }
    const eventLine = lines.find((line) => line.startsWith("event:"));
    const dataLines = lines.filter((line) => line.startsWith("data:"));
    if (!eventLine || !dataLines.length) {
        return null;
    }
    return {
        eventType: eventLine.replace("event:", "").trim(),
        data: safeParseJson(dataLines.map((line) => line.replace("data:", "").trim()).join("\n"), {}),
    };
}

export function createNodeFromHtml(html) {
    const template = document.createElement("template");
    template.innerHTML = html.trim();
    return template.content.firstElementChild;
}

export function shouldAutoScroll(element, threshold = 64) {
    return element.scrollTop + element.clientHeight >= element.scrollHeight - threshold;
}

export function setButtonBusy(button, busy, busyLabel) {
    if (!button) {
        return;
    }
    if (!button.dataset.defaultLabel) {
        button.dataset.defaultLabel = button.textContent || "";
    }
    button.disabled = !!busy;
    button.textContent = busy ? (busyLabel || "Working...") : button.dataset.defaultLabel;
}

export function createMiniCardController({
    root,
    strip,
    detailPanel = null,
    getItem,
    renderDetail,
    onSelect = null,
    selectedId = null,
} = {}) {
    const host = root || strip;
    const track = strip || host?.querySelector?.("[data-mini-card-strip]");
    if (!host || !track || typeof getItem !== "function") {
        return { select: () => {}, destroy: () => {} };
    }

    let currentId = selectedId ? String(selectedId) : "";
    let tooltip = host.querySelector(":scope > .mini-card-preview");
    if (!tooltip) {
        tooltip = document.createElement("div");
        tooltip.className = "mini-card-preview";
        tooltip.hidden = true;
        host.appendChild(tooltip);
    }

    const cardSelector = "[data-mini-card]";

    const cardId = (card) => String(card?.dataset?.miniCardId || "");
    const cards = () => Array.from(track.querySelectorAll(cardSelector));

    const centerCard = (card) => {
        if (!card) {
            return;
        }
        const targetLeft = card.offsetLeft - (track.clientWidth - card.offsetWidth) / 2;
        track.scrollTo({ left: Math.max(0, targetLeft), behavior: "smooth" });
    };

    const renderTooltip = (card, event) => {
        const item = getItem(cardId(card), card);
        if (!item) {
            return;
        }
        const facts = Array.isArray(item.facts) ? item.facts.slice(0, 2) : [];
        tooltip.innerHTML = `
            <strong>${escapeHtml(item.title || "详情")}</strong>
            <span>${escapeHtml([item.status, item.meta].filter(Boolean).join(" · "))}</span>
            ${facts.length ? `<p>${facts.map((fact) => escapeHtml(String(fact))).join(" / ")}</p>` : ""}
        `;
        tooltip.hidden = false;
        positionTooltip(event);
    };

    const positionTooltip = (event) => {
        if (tooltip.hidden) {
            return;
        }
        const margin = 12;
        const x = Math.min(window.innerWidth - tooltip.offsetWidth - margin, Math.max(margin, event.clientX + 14));
        const y = Math.min(window.innerHeight - tooltip.offsetHeight - margin, Math.max(margin, event.clientY + 14));
        tooltip.style.transform = `translate(${Math.round(x)}px, ${Math.round(y)}px)`;
    };

    const hideTooltip = () => {
        tooltip.hidden = true;
    };

    const select = (id, { center = true, notify = true } = {}) => {
        currentId = String(id || "");
        cards().forEach((card) => card.classList.toggle("is-selected", cardId(card) === currentId));
        const item = getItem(currentId);
        if (detailPanel && item) {
            if (typeof renderDetail === "function") {
                renderDetail(detailPanel, item);
            } else {
                renderMiniCardDetail(detailPanel, item);
            }
        }
        if (notify && typeof onSelect === "function") {
            onSelect(currentId, item);
        }
        if (center) {
            centerCard(track.querySelector(`${cardSelector}[data-mini-card-id="${CSS.escape(currentId)}"]`));
        }
    };

    const onClick = (event) => {
        const card = event.target.closest(cardSelector);
        if (!card || !track.contains(card)) {
            return;
        }
        select(cardId(card));
    };

    const onMouseOver = (event) => {
        const card = event.target.closest(cardSelector);
        if (card && track.contains(card)) {
            renderTooltip(card, event);
        }
    };

    const onMouseMove = (event) => {
        if (!tooltip.hidden) {
            positionTooltip(event);
        }
    };

    const onMouseOut = (event) => {
        if (!event.relatedTarget || !event.target.closest(cardSelector)?.contains(event.relatedTarget)) {
            hideTooltip();
        }
    };

    const onWheel = (event) => {
        if (event.target.closest("[data-detail-panel]")) {
            return;
        }
        if (track.scrollWidth <= track.clientWidth) {
            return;
        }
        const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? track.clientWidth : 1;
        const rawDelta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
        const delta = rawDelta * unit;
        if (!delta) {
            return;
        }
        event.preventDefault();
        track.scrollBy({ left: delta, behavior: "auto" });
    };

    track.addEventListener("click", onClick);
    track.addEventListener("mouseover", onMouseOver);
    track.addEventListener("mousemove", onMouseMove);
    track.addEventListener("mouseout", onMouseOut);
    host.addEventListener("wheel", onWheel, { passive: false });
    track.addEventListener("wheel", onWheel, { passive: false });

    if (currentId) {
        select(currentId, { center: false, notify: false });
    }

    return {
        select,
        destroy() {
            track.removeEventListener("click", onClick);
            track.removeEventListener("mouseover", onMouseOver);
            track.removeEventListener("mousemove", onMouseMove);
            track.removeEventListener("mouseout", onMouseOut);
            host.removeEventListener("wheel", onWheel);
            track.removeEventListener("wheel", onWheel);
        },
    };
}

export function renderMiniCardDetail(panel, item = {}) {
    if (!panel) {
        return;
    }
    const facts = Array.isArray(item.facts) ? item.facts : [];
    panel.innerHTML = `
        <div class="detail-panel__head">
            <div>
                <span class="status-chip">${escapeHtml(item.status || "详情")}</span>
                <h3>${escapeHtml(item.title || "详情")}</h3>
            </div>
            ${item.meta ? `<span class="detail-panel__meta">${escapeHtml(item.meta)}</span>` : ""}
        </div>
        <div class="fact-grid">
            ${facts.map((fact) => {
                const label = typeof fact === "object" ? fact.label : "要点";
                const value = typeof fact === "object" ? fact.value : fact;
                return `<article class="fact-card"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`;
            }).join("")}
        </div>
        <div class="source-text-panel">${escapeHtml(item.body || item.raw || item.preview || "暂无详情。")}</div>
    `;
}

export function markdownToHtml(source) {
    const codeBlocks = [];
    let text = String(source || "").replace(/```([\w-]+)?\n([\s\S]*?)```/g, (_, lang, code) => {
        const placeholder = `__CODE_BLOCK_${codeBlocks.length}__`;
        codeBlocks.push(`
            <div class="code-block code-block--markdown">
                <div class="code-block__header">
                    <span>${escapeHtml(lang || "code")}</span>
                    <button type="button" class="code-block__copy" data-copy-code>Copy</button>
                </div>
                <pre><code data-lang="${escapeHtml(lang || "")}">${escapeHtml(code.trimEnd())}</code></pre>
            </div>
        `);
        return placeholder;
    });

    const sections = text.split(/\n{2,}/).filter(Boolean);
    let html = sections
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
            if (/^\d+\.\s+/m.test(trimmed)) {
                const items = trimmed
                    .split("\n")
                    .filter((line) => /^\d+\.\s+/.test(line.trim()))
                    .map((line) => `<li>${renderInline(line.trim().replace(/^\d+\.\s+/, ""))}</li>`)
                    .join("");
                return `<ol>${items}</ol>`;
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

    codeBlocks.forEach((block, index) => {
        html = html.replace(`__CODE_BLOCK_${index}__`, block);
    });
    return html;
}

export function renderMarkdownInto(node, source) {
    node.innerHTML = markdownToHtml(source);
    node.classList.add("markdown-body");
    node.querySelectorAll("[data-copy-code]").forEach((button) => {
        button.addEventListener("click", async () => {
            const code = button.closest(".code-block")?.querySelector("code");
            await navigator.clipboard.writeText(code?.textContent || "");
            button.textContent = "Copied";
            window.setTimeout(() => {
                button.textContent = "Copy";
            }, 1200);
        });
    });
}

export function showNotice(target, message, tone = "info") {
    const node = typeof target === "string" ? document.getElementById(target) : target;
    if (!node) {
        return;
    }
    node.hidden = !message;
    node.textContent = message || "";
    node.dataset.tone = tone;
}

export function openModal(target) {
    const modal = target instanceof HTMLElement ? target : document.getElementById(target);
    if (!modal) {
        return;
    }
    lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    modal.hidden = false;
    modal.classList.add(OPEN_CLASS);
    document.body.classList.add("body-modal-open");
    const autofocusTarget = modal.querySelector("[autofocus], button, input, textarea, select, a");
    if (autofocusTarget instanceof HTMLElement) {
        autofocusTarget.focus();
    }
}

export function closeModal(target) {
    const modal = target instanceof HTMLElement ? target : document.getElementById(target);
    if (!modal) {
        return;
    }
    modal.hidden = true;
    modal.classList.remove(OPEN_CLASS);
    if (!document.querySelector(".modal-overlay.is-open")) {
        document.body.classList.remove("body-modal-open");
        if (lastFocusedElement) {
            lastFocusedElement.focus();
        }
    }
}

function renderInline(text) {
    return escapeHtml(text).replace(/`([^`]+)`/g, "<code>$1</code>");
}

function resolveShellMode() {
    const body = document.body;
    if (body?.classList.contains("theme-ambient--project") && window.innerHeight < 940) {
        return "relaxed";
    }
    if (body?.classList.contains("theme-ambient--telegram") && window.innerHeight < 920) {
        return "relaxed";
    }
    return window.innerWidth >= FIXED_SHELL_MIN_WIDTH && window.innerHeight >= FIXED_SHELL_MIN_HEIGHT
        ? "fixed"
        : "relaxed";
}

function applyShellMode(force = false) {
    const nextMode = resolveShellMode();
    const currentMode = document.documentElement.dataset.shellMode || "";
    if (!force && currentMode === nextMode) {
        return;
    }
    document.documentElement.dataset.shellMode = nextMode;
    if (document.body) {
        document.body.dataset.shellMode = nextMode;
    }
}

function bindShellMode() {
    applyShellMode(true);
    window.addEventListener("resize", () => applyShellMode(), { passive: true });
}

function bindCursorGlow() {
}

function bindModals(root = document) {
    root.querySelectorAll("[data-modal-open]").forEach((button) => {
        button.addEventListener("click", () => openModal(button.dataset.modalOpen));
    });
    root.querySelectorAll("[data-modal-close]").forEach((button) => {
        button.addEventListener("click", () => {
            const modal = button.closest(".modal-overlay");
            if (modal) {
                closeModal(modal);
            }
        });
    });
    root.querySelectorAll(".modal-overlay").forEach((overlay) => {
        overlay.addEventListener("click", (event) => {
            if (event.target === overlay) {
                closeModal(overlay);
            }
        });
    });
    document.addEventListener("keydown", (event) => {
        if (event.key !== "Escape") {
            return;
        }
        document.querySelectorAll(".modal-overlay.is-open").forEach((overlay) => closeModal(overlay));
    });
}

function bindConfirmations(root = document) {
    root.querySelectorAll("[data-confirm]").forEach((element) => {
        element.addEventListener("click", (event) => {
            const message = element.dataset.confirm || "Are you sure?";
            if (!window.confirm(message)) {
                event.preventDefault();
                event.stopPropagation();
            }
        });
    });
}

function initializeShared() {
    bindShellMode();
    bindCursorGlow();
    bindModals();
    bindConfirmations();
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initializeShared);
} else {
    initializeShared();
}
