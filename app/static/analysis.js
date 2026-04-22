import {
    clampPercent,
    escapeHtml,
    fetchJson,
    formatDateTime,
    normalizeStatus,
    renderMarkdownInto,
    safeParseJson,
    setStatusTone,
    shouldAutoScroll,
    updateText,
    debounce,
    throttle,
} from "./shared.js";

const bootstrap = safeParseJson(document.getElementById("analysis-page-bootstrap")?.textContent, {});

if (bootstrap?.project_id && bootstrap?.run_id) {
    const ui = bootstrap.ui_strings || {};
    const state = {
        projectId: bootstrap.project_id,
        runId: bootstrap.run_id,
        payload: bootstrap.initial_run ? safeParseJson(bootstrap.initial_run, null) : null,
        traceEvents: [],
        liveOutputByRequest: {},
        activeRequestKey: null,
        selectedFacetKey: null,
        stream: null,
        pollTimer: null,
    };

    const elements = {
        percent: document.getElementById("analysis-percent"),
        stage: document.getElementById("analysis-stage"),
        requestedConcurrency: document.getElementById("analysis-requested-concurrency"),
        concurrency: document.getElementById("analysis-concurrency"),
        effectiveConcurrency: document.getElementById("analysis-effective-concurrency"),
        effectiveNote: document.getElementById("analysis-effective-note"),
        activeAgents: document.getElementById("analysis-active-agents"),
        effectiveActiveAgents: document.getElementById("analysis-effective-active-agents"),
        slotUsage: document.getElementById("analysis-slot-usage"),
        currentFacet: document.getElementById("analysis-current-facet"),
        lastUpdated: document.getElementById("analysis-last-updated"),
        statusChip: document.getElementById("analysis-status-chip"),
        progressLabel: document.getElementById("analysis-progress-label"),
        progressCaption: document.getElementById("analysis-progress-caption"),
        progressFill: document.getElementById("analysis-progress-fill"),
        diagnosticsList: document.getElementById("analysis-diagnostics-list"),
        resultNav: document.getElementById("analysis-result-nav"),
        resultList: document.getElementById("analysis-result-list"),
        feed: document.getElementById("analysis-feed"),
        heroStage: document.getElementById("analysis-hero-stage"),
        heroNote: document.getElementById("analysis-hero-note"),
        livePill: document.getElementById("analysis-live-pill"),
        percentChip: document.getElementById("analysis-percent-chip"),
        completedCount: document.getElementById("analysis-completed-count"),
        runningCount: document.getElementById("analysis-running-count"),
        queuedCount: document.getElementById("analysis-queued-count"),
        failedCount: document.getElementById("analysis-failed-count"),
        laneStrip: document.getElementById("analysis-agent-lanes"),
    };

    if (state.payload) {
        render(state.payload);
    }

    setLiveState(state.payload?.status && isRunning(state.payload.status) ? "live" : "idle");
    connectStream();

    let renderQueued = false;
    const scheduleRender = throttle((payload) => {
        if (renderQueued) return;
        renderQueued = true;
        window.requestAnimationFrame(() => {
            renderQueued = false;
            if (state.payload === payload) {
                render(payload);
                setLiveState(isRunning(payload.status) ? "live" : "idle");
                if (!isRunning(payload.status)) {
                    stopStream();
                }
            }
        });
    }, 100);

    function connectStream() {
        stopStream();
        setLiveState("connecting");
        state.stream = new EventSource(`/api/projects/${state.projectId}/analysis/stream?run_id=${encodeURIComponent(state.runId)}`);

        state.stream.addEventListener("snapshot", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            state.payload = payload;
            scheduleRender(payload);
        });

        state.stream.addEventListener("trace", (event) => {
            const payload = safeParseJson(event.data, null);
            if (!payload) {
                return;
            }
            handleTraceEvent(payload);
        });

        state.stream.addEventListener("done", () => {
            stopStream();
            setLiveState("idle");
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
        if (state.pollTimer) {
            return;
        }
        setLiveState("polling");
        state.pollTimer = window.setInterval(async () => {
            try {
                const payload = await fetchJson(`/api/projects/${state.projectId}/analysis?run_id=${encodeURIComponent(state.runId)}`);
                state.payload = payload;
                render(payload);
                if (!isRunning(payload.status)) {
                    window.clearInterval(state.pollTimer);
                    state.pollTimer = null;
                    setLiveState("idle");
                }
            } catch (error) {
                console.error(error);
            }
        }, 1800);
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

        pushTraceEvent(event);
        renderAgentCenter(state.payload);
        renderAgentLanes(state.payload);
    }

    function pushTraceEvent(event) {
        const key = buildTraceKey(event);
        if (state.traceEvents.some((item) => item._key === key)) {
            return;
        }
        state.traceEvents.push({ ...event, _key: key });
        state.traceEvents = state.traceEvents.slice(-180);
    }

    function render(payload) {
        if (!payload) {
            return;
        }
        const summary = payload.summary || {};
        const facets = payload.facets || [];
        const events = payload.events || [];
        const counts = countFacetStatuses(facets);
        const percent = clampPercent(summary.progress_percent || 0);
        const requestedConcurrency = Number(summary.requested_concurrency || summary.concurrency || 1);
        const effectiveConcurrency = Number(summary.effective_concurrency || requestedConcurrency || 1);
        const activeAgents = Number(summary.active_agents || 0);
        const effectiveActiveAgents = Number(summary.effective_active_agents || activeAgents || 0);
        const total = Number(summary.total_facets || facets.length || 0);
        const completed = Number(summary.completed_facets || counts.completed || 0);

        updateText(elements.percent, `${percent}%`);
        updateText(elements.stage, summary.current_stage || ui.waiting || "Waiting");
        updateText(elements.requestedConcurrency, requestedConcurrency);
        updateText(elements.concurrency, requestedConcurrency);
        updateText(elements.effectiveConcurrency, effectiveConcurrency);
        updateText(elements.effectiveNote, `Effective ${effectiveConcurrency}`);
        updateText(elements.activeAgents, effectiveActiveAgents);
        updateText(elements.effectiveActiveAgents, effectiveActiveAgents);
        updateText(elements.slotUsage, `${activeAgents} / ${requestedConcurrency}`);
        updateText(elements.currentFacet, summary.current_facet || ui.waiting || "Waiting");
        updateText(
            elements.lastUpdated,
            `${ui.last_updated || "Last updated"} | ${formatDateTime(payload.finished_at || payload.started_at || events[0]?.created_at)}`
        );
        updateText(elements.progressLabel, summary.current_stage || ui.waiting || "Waiting");
        updateText(elements.progressCaption, `${completed} / ${total}`);
        updateText(elements.heroStage, summary.current_stage || ui.waiting || "Waiting");
        updateText(elements.heroNote, buildHeroNote(summary, counts, facets));
        updateText(elements.percentChip, `${percent}%`);
        updateText(elements.completedCount, `Completed ${counts.completed}`);
        updateText(elements.runningCount, `Running ${counts.running}`);
        updateText(elements.queuedCount, `Queued ${counts.queued + counts.preparing}`);
        updateText(elements.failedCount, `Failed ${counts.failed}`);

        if (elements.progressFill) {
            elements.progressFill.style.width = `${percent}%`;
        }
        setStatusTone(elements.statusChip, payload.status, payload.status);

        state.selectedFacetKey = resolveSelectedFacetKey(facets, summary);
        renderDiagnostics(events);
        renderResults(facets, payload.status);
        renderAgentLanes(payload);
        renderAgentCenter(payload);
    }

    function renderAgentLanes(payload) {
        if (!elements.laneStrip) {
            return;
        }
        elements.laneStrip.innerHTML = "";

        const summary = payload?.summary || {};
        const facets = sortFacetsForQueue(payload?.facets || []);
        const requestedConcurrency = Number(summary.requested_concurrency || summary.concurrency || 1);
        const effectiveConcurrency = Number(summary.effective_concurrency || requestedConcurrency || 1);

        if (!facets.length) {
            const placeholder = document.createElement("div");
            placeholder.className = "agent-lamp agent-lamp--empty";
            placeholder.innerHTML = `
                <strong>No active lanes</strong>
                <p>Requested ${escapeHtml(String(requestedConcurrency))} · Effective ${escapeHtml(String(effectiveConcurrency))}</p>
            `;
            const placeholderMeta = document.createElement("span");
            placeholderMeta.textContent = `Requested ${requestedConcurrency} | Effective ${effectiveConcurrency}`;
            placeholder.querySelector("p")?.replaceWith(placeholderMeta);
            elements.laneStrip.appendChild(placeholder);
            return;
        }

        const fragment = document.createDocumentFragment();
        facets.forEach((facet) => {
            const findings = facet.findings || {};
            const status = normalizeStatus(facet.status || "queued");
            const button = document.createElement("button");
            button.type = "button";
            button.className = `agent-lamp status-${escapeHtml(status)}${state.selectedFacetKey === facet.facet_key ? " is-selected" : ""}${summary.current_facet === facet.facet_key ? " is-current" : ""}`;
            button.dataset.facetSelect = facet.facet_key || "";
            button.title = [
                findings.label || facet.facet_key || "Facet",
                statusLabel(status),
                phaseLabel(findings.phase || status),
                buildAgentLampMeta(facet),
            ].filter(Boolean).join(" | ");
            button.setAttribute("aria-pressed", state.selectedFacetKey === facet.facet_key ? "true" : "false");
            button.innerHTML = `
                <span class="agent-lamp__label">${escapeHtml(findings.label || facet.facet_key || "Facet")}</span>
                <span class="agent-lamp__dot" aria-hidden="true"></span>
            `;
            button.addEventListener("click", () => {
                setSelectedFacet(facet.facet_key || null);
            });
            fragment.appendChild(button);
        });
        elements.laneStrip.appendChild(fragment);
    }

    function renderFacetQueue(facets, runStatus) {
        if (!elements.facetList) {
            return;
        }
        elements.facetList.innerHTML = "";

        const fragment = document.createDocumentFragment();
        sortFacetsForQueue(facets).forEach((facet) => {
            const findings = facet.findings || {};
            const status = normalizeStatus(facet.status || "queued");
            const card = document.createElement("article");
            card.className = `facet-status-card status-${escapeHtml(status)}`;
            card.innerHTML = `
                <div class="facet-status-head">
                    <div class="facet-status-title">
                        <strong>${escapeHtml(findings.label || facet.facet_key)}</strong>
                        <span class="facet-status-key">${escapeHtml(facet.facet_key || "")}</span>
                    </div>
                    <span class="status-pill">${escapeHtml(statusLabel(status))}</span>
                </div>
                <div class="facet-status-flags">
                    <span class="facet-inline-tag">${escapeHtml(`Phase ${phaseLabel(findings.phase || status)}`)}</span>
                    ${findings.queue_position ? `<span class="facet-inline-tag tag-warning">${escapeHtml(`Queue #${findings.queue_position}`)}</span>` : ""}
                    <span class="facet-inline-tag">${escapeHtml(`${Number(findings.hit_count || 0)} evidence`)}</span>
                </div>
                <p class="facet-status-preview">${escapeHtml(trimText(findings.summary || buildFacetLead(facet), 120))}</p>
                <div class="inline-actions top-gap">
                    <button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${isRunBusy(runStatus) ? "disabled" : ""}>Rerun Facet</button>
                </div>
            `;
            fragment.appendChild(card);
        });
        elements.facetList.appendChild(fragment);

        bindFacetRerunActions();
    }

    function renderDiagnostics(events) {
        if (!elements.diagnosticsList) {
            return;
        }
        elements.diagnosticsList.innerHTML = "";

        if (!events.length) {
            elements.diagnosticsList.innerHTML = `<div class="empty-panel"><strong>${escapeHtml(ui.no_events || "No events yet.")}</strong></div>`;
            return;
        }

        const fragment = document.createDocumentFragment();
        events.slice(0, 200).forEach((event) => {
            const line = document.createElement("div");
            const text = [
                event.event_type || "event",
                event.message || "",
            ].filter(Boolean).join(" ");
            line.className = `analysis-event-line level-${escapeHtml((event.level || "info").toLowerCase())}`;
            line.title = event.payload && Object.keys(event.payload).length
                ? JSON.stringify(event.payload, null, 2)
                : "";
            line.innerHTML = `
                <span class="analysis-event-line__text">${escapeHtml(text)}</span>
                <span class="analysis-event-line__time">${escapeHtml(formatDateTime(event.created_at))}</span>
            `;
            fragment.appendChild(line);
        });
        elements.diagnosticsList.appendChild(fragment);
    }

    function renderResults(facets, runStatus) {
        if (!elements.resultList) {
            return;
        }
        if (elements.resultNav) {
            elements.resultNav.innerHTML = "";
        }
        elements.resultList.innerHTML = "";

        const orderedFacets = sortFacetsForQueue(facets);
        if (!orderedFacets.length) {
            const empty = `<div class="empty-panel"><strong>${escapeHtml(ui.empty || "No analysis results yet.")}</strong></div>`;
            if (elements.resultNav) {
                elements.resultNav.innerHTML = empty;
            }
            elements.resultList.innerHTML = empty;
            return;
        }

        state.selectedFacetKey = resolveSelectedFacetKey(orderedFacets, state.payload?.summary || {});
        const activeIndex = Math.max(0, orderedFacets.findIndex((facet) => facet.facet_key === state.selectedFacetKey));
        const activeFacet = orderedFacets[activeIndex] || orderedFacets[0];

        if (elements.resultNav) {
            const navFragment = document.createDocumentFragment();
            orderedFacets.forEach((facet, index) => {
                const findings = facet.findings || {};
                const status = normalizeStatus(facet.status || "queued");
                const button = document.createElement("button");
                button.type = "button";
                button.className = `analysis-result-tab status-${escapeHtml(status)}${facet.facet_key === state.selectedFacetKey ? " is-active" : ""}`;
                button.title = trimText(findings.summary || buildFacetLead(facet), 220);
                button.innerHTML = `
                    <span class="analysis-result-tab__head">
                        <span class="analysis-result-tab__index">${escapeHtml(String(index + 1).padStart(2, "0"))}</span>
                        <strong>${escapeHtml(findings.label || facet.facet_key || "Facet")}</strong>
                    </span>
                    <span class="analysis-result-tab__dot" aria-hidden="true"></span>
                `;
                button.addEventListener("click", () => {
                    setSelectedFacet(facet.facet_key || null);
                });
                navFragment.appendChild(button);
            });
            elements.resultNav.appendChild(navFragment);
        }

        elements.resultList.appendChild(buildResultDetail(activeFacet, runStatus, activeIndex, orderedFacets));

        bindFacetRerunActions();
    }

    function buildResultDetail(facet, runStatus, activeIndex, orderedFacets) {
        const findings = facet.findings || {};
        const status = normalizeStatus(facet.status || "queued");
        const total = orderedFacets.length;
        const previousFacet = activeIndex > 0 ? orderedFacets[activeIndex - 1] : null;
        const nextFacet = activeIndex < total - 1 ? orderedFacets[activeIndex + 1] : null;
        const panel = document.createElement("article");
        panel.className = `facet-result-card facet-result-card--detail status-${escapeHtml(status)}`;
        panel.innerHTML = `
            <div class="facet-result-card__summary facet-result-card__summary--detail">
                <div class="facet-result-card__summary-main">
                    <div class="facet-result-card__summary-meta">
                        <p class="eyebrow">${escapeHtml(facet.facet_key || "")}</p>
                        <span class="analysis-result-page">${escapeHtml(`${activeIndex + 1} / ${total}`)}</span>
                    </div>
                    <h3>${escapeHtml(findings.label || facet.facet_key || "Facet")}</h3>
                    <p class="facet-summary">${escapeHtml(trimText(findings.summary || buildFacetLead(facet), 220))}</p>
                </div>
                <div class="facet-result-card__status">
                    <span class="status-chip tone-${escapeHtml(statusTone(status))}">${escapeHtml(statusLabel(status))}</span>
                    <span class="facet-inline-tag">${escapeHtml(phaseLabel(findings.phase || status))}</span>
                </div>
            </div>
            <div class="analysis-result-pager">
                <button type="button" class="ghost-button" data-facet-page="prev" ${previousFacet ? "" : "disabled"}>上一页</button>
                <span class="analysis-result-pager__label">${escapeHtml(`第 ${activeIndex + 1} / ${total} 页`)}</span>
                <button type="button" class="ghost-button" data-facet-page="next" ${nextFacet ? "" : "disabled"}>下一页</button>
            </div>
            <div class="facet-result-card__body facet-result-card__body--detail"></div>
        `;

        panel.querySelector('[data-facet-page="prev"]')?.addEventListener("click", () => {
            if (previousFacet?.facet_key) {
                setSelectedFacet(previousFacet.facet_key);
            }
        });
        panel.querySelector('[data-facet-page="next"]')?.addEventListener("click", () => {
            if (nextFacet?.facet_key) {
                setSelectedFacet(nextFacet.facet_key);
            }
        });

        const body = panel.querySelector(".facet-result-card__body");
        const summaryNode = document.createElement("div");
        summaryNode.className = "markdown-body";
        renderMarkdownInto(summaryNode, findings.summary || buildFacetLead(facet));
        body.appendChild(summaryNode);

        if (Array.isArray(findings.bullets) && findings.bullets.length) {
            const list = document.createElement("ul");
            list.className = "facet-bullets";
            findings.bullets.forEach((item) => {
                const li = document.createElement("li");
                li.textContent = item;
                list.appendChild(li);
            });
            body.appendChild(list);
        }

        if (Array.isArray(facet.evidence) && facet.evidence.length) {
            const evidenceWrap = document.createElement("div");
            evidenceWrap.className = "facet-result-section";
            evidenceWrap.innerHTML = "<strong>Evidence</strong>";
            facet.evidence.slice(0, 10).forEach((item) => {
                const block = document.createElement("div");
                block.className = "evidence-block";
                block.innerHTML = `
                    <strong>${escapeHtml(item.filename || item.document_title || item.sender_name || "Evidence")}</strong>
                    <p class="muted">${escapeHtml(item.reason || "")}</p>
                    <blockquote>${escapeHtml(item.quote || JSON.stringify(item))}</blockquote>
                `;
                evidenceWrap.appendChild(block);
            });
            body.appendChild(evidenceWrap);
        }

        if ((facet.conflicts && facet.conflicts.length) || facet.error_message || findings.notes) {
            const notesWrap = document.createElement("div");
            notesWrap.className = "facet-result-section";
            notesWrap.innerHTML = "<strong>Notes</strong>";
            (facet.conflicts || []).forEach((item) => {
                const block = document.createElement("div");
                block.className = "evidence-block";
                block.innerHTML = `
                    <strong>${escapeHtml(item.title || "Conflict")}</strong>
                    <p>${escapeHtml(item.detail || "")}</p>
                `;
                notesWrap.appendChild(block);
            });
            if (facet.error_message) {
                const error = document.createElement("p");
                error.className = "danger";
                error.textContent = facet.error_message;
                notesWrap.appendChild(error);
            }
            if (findings.notes) {
                const note = document.createElement("p");
                note.className = "muted";
                note.textContent = findings.notes;
                notesWrap.appendChild(note);
            }
            body.appendChild(notesWrap);
        }

        const toolCalls = asToolCalls(findings.retrieval_trace);
        if (toolCalls.length || findings.retrieval_trace) {
            const traceSection = document.createElement("details");
            traceSection.className = "facet-trace-section";
            traceSection.innerHTML = "<summary>Trace Snapshot</summary><div class=\"facet-trace-section__body\"></div>";
            const traceBody = traceSection.querySelector(".facet-trace-section__body");
            toolCalls.forEach((call) => {
                traceBody.appendChild(buildToolBubble({
                    toolName: call.tool || call.name || "tool",
                    arguments: stringifyMaybe(call.arguments),
                    result: stringifyMaybe(call.result_preview || call.result),
                    error: stringifyMaybe(call.error),
                    meta: findings.label || facet.facet_key,
                    open: false,
                }));
            });
            if (findings.retrieval_trace) {
                traceBody.appendChild(createCodeBlock("retrieval_trace", JSON.stringify(findings.retrieval_trace, null, 2)));
            }
            body.appendChild(traceSection);
        }

        const liveText = findings.llm_response_text || findings.llm_live_text || "";
        if (liveText) {
            const liveWrap = document.createElement("details");
            liveWrap.className = "facet-trace-section";
            liveWrap.innerHTML = "<summary>LLM Output</summary><div class=\"facet-trace-section__body\"></div>";
            liveWrap.querySelector(".facet-trace-section__body").appendChild(
                buildAssistantBubble({
                    label: findings.label || facet.facet_key,
                    meta: phaseLabel(findings.phase || status),
                    text: liveText,
                    preferCode: looksStructured(liveText),
                    status,
                })
            );
            body.appendChild(liveWrap);
        }

        const actions = document.createElement("div");
        actions.className = "inline-actions top-gap";
        actions.innerHTML = `<button type="button" class="secondary-button" data-facet-rerun="${escapeHtml(facet.facet_key)}" ${isRunBusy(runStatus) ? "disabled" : ""}>Rerun Facet</button>`;
        body.appendChild(actions);

        return panel;
    }

    function setSelectedFacet(facetKey) {
        if (!facetKey || !state.payload) {
            return;
        }
        state.selectedFacetKey = facetKey;
        renderAgentLanes(state.payload);
        renderResults(state.payload.facets || [], state.payload.status);
    }

    function renderAgentCenter(payload) {
        if (!elements.feed || !payload) {
            return;
        }

        const shouldStick = shouldAutoScroll(elements.feed);
        elements.feed.innerHTML = "";
        
        const fragment = document.createDocumentFragment();
        fragment.appendChild(buildTaskBubble(payload));

        const items = buildLiveFeedItems(payload);
        const normalizedItems = items.length ? items : buildStaticSnapshotItems(payload);

        if (!normalizedItems.length) {
            fragment.appendChild(buildContextBubble({
                text: payload.summary?.current_stage || "Waiting for agent activity",
                meta: payload.status || "queued",
                active: isRunBusy(payload.status),
            }));
        } else {
            normalizedItems.forEach((item) => {
                if (item.type === "assistant") {
                    fragment.appendChild(buildAssistantBubble(item));
                    return;
                }
                if (item.type === "tool") {
                    fragment.appendChild(buildToolBubble(item));
                    return;
                }
                fragment.appendChild(buildContextBubble(item));
            });
        }
        
        elements.feed.appendChild(fragment);

        if (shouldStick) {
            elements.feed.scrollTop = elements.feed.scrollHeight;
        }
    }

    function buildTaskBubble(payload) {
        const summary = payload.summary || {};
        const bubble = document.createElement("article");
        bubble.className = "bubble bubble--user bubble--task";
        const targetLabel = summary.target_role || summary.target_user?.label || summary.target_user_query || "Unspecified target";
        const context = summary.analysis_context || "No extra analysis context was supplied.";
        const requestedConcurrency = Number(summary.requested_concurrency || summary.concurrency || 1);
        const effectiveAgents = Number(summary.effective_active_agents || summary.active_agents || 0);
        bubble.innerHTML = `
            <div class="bubble__head">
                <span class="bubble__badge">Task</span>
                <span class="bubble__meta">${escapeHtml(payload.status || "queued")}</span>
            </div>
            <h3>${escapeHtml(targetLabel)}</h3>
            <p>${escapeHtml(context)}</p>
            <div class="bubble__meta-row">
                <span>${escapeHtml(summary.current_stage || "Queued")}</span>
                <span>${escapeHtml(`Facet ${summary.current_facet || "-"}`)}</span>
                <span>${escapeHtml(`Requested ${requestedConcurrency}`)}</span>
                <span>${escapeHtml(`Live agents ${effectiveAgents}`)}</span>
            </div>
        `;
        return bubble;
    }

    function buildLiveFeedItems(payload) {
        const items = [];
        const assistantThreads = new Map();
        const toolThreads = new Map();

        if (isRunBusy(payload.status)) {
            items.push({
                type: "context",
                text: payload.summary?.current_stage || "Agent is working",
                meta: payload.summary?.current_facet || payload.status,
                active: true,
            });
        }

        orderedTraceEvents().forEach((event) => {
            if (!event?.kind) {
                return;
            }
            if (event.kind === "agent_started" || event.kind === "agent_completed" || event.kind === "stage_progress") {
                items.push({
                    type: "context",
                    text: summarizeTraceEvent(event),
                    meta: traceMeta(event),
                    active: event.kind !== "agent_completed",
                });
                return;
            }

            if (event.kind === "llm_request_started" || event.kind === "llm_delta" || event.kind === "llm_request_completed") {
                const requestKey = resolveRequestKey(event);
                if (!assistantThreads.has(requestKey)) {
                    const entry = {
                        type: "assistant",
                        key: requestKey,
                        label: event.label || event.facet_key || event.agent || "assistant",
                        meta: traceMeta(event),
                        text: "",
                        status: "running",
                        preferCode: false,
                    };
                    assistantThreads.set(requestKey, entry);
                    items.push(entry);
                }
                const entry = assistantThreads.get(requestKey);
                if (event.kind === "llm_request_started") {
                    entry.status = "running";
                }
                if (event.kind === "llm_delta") {
                    entry.text = state.liveOutputByRequest[requestKey] || event.text_preview || entry.text;
                    entry.status = "running";
                    entry.preferCode = looksStructured(entry.text);
                }
                if (event.kind === "llm_request_completed") {
                    entry.text = state.liveOutputByRequest[requestKey] || event.response_text_preview || entry.text;
                    entry.status = "completed";
                    entry.meta = traceMeta(event);
                    entry.preferCode = looksStructured(entry.text);
                }
                return;
            }

            if (event.kind === "tool_call" || event.kind === "tool_result") {
                const toolKey = `${resolveRequestKey(event)}:${event.tool_name || "tool"}:${event.round_index || 0}`;
                if (!toolThreads.has(toolKey)) {
                    const entry = {
                        type: "tool",
                        key: toolKey,
                        toolName: event.tool_name || "tool",
                        meta: traceMeta(event),
                        arguments: "",
                        result: "",
                        error: "",
                        open: false,
                    };
                    toolThreads.set(toolKey, entry);
                    items.push(entry);
                }
                const entry = toolThreads.get(toolKey);
                if (event.kind === "tool_call") {
                    entry.arguments = event.arguments_preview || entry.arguments;
                    entry.open = true;
                }
                if (event.kind === "tool_result") {
                    entry.result = event.output_preview || entry.result;
                    entry.error = event.error || entry.error;
                    entry.meta = traceMeta(event);
                }
                return;
            }

            items.push({
                type: "context",
                text: summarizeTraceEvent(event),
                meta: traceMeta(event),
                active: false,
            });
        });

        return items;
    }

    function buildStaticSnapshotItems(payload) {
        const items = [];
        sortFacetsForQueue(payload.facets || []).forEach((facet) => {
            const findings = facet.findings || {};
            const status = normalizeStatus(facet.status || "queued");
            items.push({
                type: "context",
                text: `${findings.label || facet.facet_key} · ${statusLabel(status)}`,
                meta: phaseLabel(findings.phase || status),
                active: ["preparing", "running"].includes(status),
            });

            asToolCalls(findings.retrieval_trace).forEach((call, index) => {
                items.push({
                    type: "tool",
                    key: `${facet.facet_key}:${call.tool || call.name || "tool"}:${index}`,
                    toolName: call.tool || call.name || "tool",
                    meta: findings.label || facet.facet_key,
                    arguments: stringifyMaybe(call.arguments),
                    result: stringifyMaybe(call.result_preview || call.result),
                    error: stringifyMaybe(call.error),
                    open: false,
                });
            });

            const assistantText = findings.llm_live_text || findings.llm_response_text || "";
            if (assistantText) {
                items.push({
                    type: "assistant",
                    key: `facet:${facet.facet_key}`,
                    label: findings.label || facet.facet_key,
                    meta: phaseLabel(findings.phase || status),
                    text: assistantText,
                    status,
                    preferCode: looksStructured(assistantText),
                });
            } else if (findings.summary) {
                items.push({
                    type: "assistant",
                    key: `facet-summary:${facet.facet_key}`,
                    label: findings.label || facet.facet_key,
                    meta: phaseLabel(findings.phase || status),
                    text: findings.summary,
                    status,
                    preferCode: false,
                });
            }
        });
        return items;
    }

    function buildAssistantBubble(item) {
        const bubble = document.createElement("article");
        bubble.className = "bubble bubble--assistant";
        const head = document.createElement("div");
        head.className = "bubble__head bubble__head--assistant";
        head.innerHTML = `
            <span class="bubble__badge">Assistant</span>
            <span class="bubble__meta">${escapeHtml(item.meta || item.label || "")}</span>
        `;
        bubble.appendChild(head);

        const content = document.createElement("div");
        content.className = "bubble__content markdown-body";
        const text = String(item.text || "").trim();

        if (!text) {
            content.innerHTML = `<p class="muted">${escapeHtml(item.status === "running" ? "Agent is thinking..." : "No response text was captured yet.")}</p>`;
        } else if (item.preferCode || looksStructured(text)) {
            content.appendChild(createCodeBlock("output", text));
        } else {
            renderMarkdownInto(content, text);
        }
        bubble.appendChild(content);
        return bubble;
    }

    function buildToolBubble(item) {
        const details = document.createElement("details");
        details.className = "bubble bubble--tool";
        details.open = Boolean(item.open || item.error);
        details.innerHTML = `
            <summary class="bubble__tool-summary">
                <span class="bubble__tool-label">&gt;_ ${escapeHtml(item.toolName || "tool")}</span>
                <span class="bubble__meta">${escapeHtml(item.meta || "")}</span>
            </summary>
            <div class="bubble__tool-body"></div>
        `;

        const body = details.querySelector(".bubble__tool-body");
        if (item.arguments) {
            body.appendChild(createCodeBlock("arguments", item.arguments));
        }
        if (item.result) {
            body.appendChild(createCodeBlock("result", item.result));
        }
        if (item.error) {
            body.appendChild(createCodeBlock("error", item.error, { tone: "error" }));
        }
        if (!item.arguments && !item.result && !item.error) {
            body.innerHTML = "<p class=\"muted\">Tool body is empty.</p>";
        }
        return details;
    }

    function buildContextBubble(item) {
        const row = document.createElement("div");
        row.className = "bubble-context-row";
        row.innerHTML = `
            <div class="bubble bubble--context">
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

    function bindFacetRerunActions() {
        document.querySelectorAll("[data-facet-rerun]").forEach((button) => {
            button.onclick = async () => {
                button.disabled = true;
                try {
                    const payload = await fetchJson(
                        `/api/projects/${state.projectId}/analysis/${encodeURIComponent(button.dataset.facetRerun)}/rerun`,
                        { method: "POST" }
                    );
                    state.payload = payload;
                    state.traceEvents = [];
                    state.liveOutputByRequest = {};
                    render(payload);
                    connectStream();
                } catch (error) {
                    window.alert(error.message || "Facet rerun failed.");
                    button.disabled = false;
                }
            };
        });
    }

    function buildHeroNote(summary, counts, facets) {
        const activeLabels = facets
            .filter((facet) => ["preparing", "running"].includes(normalizeStatus(facet.status || "")))
            .map((facet) => facet.findings?.label || facet.facet_key)
            .filter(Boolean);
        return [
            summary.current_facet ? `Focus ${summary.current_facet}` : "",
            activeLabels.length ? `Live ${activeLabels.join(", ")}` : "",
            counts.failed ? `Failed ${counts.failed}` : "",
            summary.requested_concurrency ? `Requested ${summary.requested_concurrency}` : "",
        ].filter(Boolean).join(" · ") || "Waiting for the next agent update.";
    }

    function buildAgentLampMeta(facet) {
        const findings = facet.findings || {};
        const status = normalizeStatus(facet.status || "queued");
        if (status === "queued" && findings.queue_position) {
            return `Queue #${findings.queue_position}`;
        }
        if (status === "completed") {
            return `${Number(findings.hit_count || facet.evidence?.length || 0)} evidence`;
        }
        if (status === "running" || status === "preparing") {
            return "Active";
        }
        return phaseLabel(findings.phase || status);
    }

    function resolveSelectedFacetKey(facets, summary) {
        const keys = new Set(
            facets
                .map((facet) => facet.facet_key)
                .filter(Boolean)
        );
        if (state.selectedFacetKey && keys.has(state.selectedFacetKey)) {
            return state.selectedFacetKey;
        }
        if (summary.current_facet && keys.has(summary.current_facet)) {
            return summary.current_facet;
        }
        const liveFacet = facets.find((facet) => ["preparing", "running"].includes(normalizeStatus(facet.status || "")));
        return liveFacet?.facet_key || facets[0]?.facet_key || null;
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

    function buildTraceKey(event) {
        return String(
            event?.seq
            ?? `${event?.timestamp || ""}:${event?.kind || ""}:${event?.request_key || ""}:${event?.tool_name || ""}:${event?.round_index || ""}:${event?.facet_key || ""}`
        );
    }

    function resolveRequestKey(event) {
        return String(
            event.request_key
            || `${event.agent || "agent"}:${event.facet_key || ""}:${event.round_index || 0}`
        );
    }

    function asToolCalls(retrievalTrace) {
        if (!retrievalTrace || typeof retrievalTrace !== "object") {
            return [];
        }
        return Array.isArray(retrievalTrace.tool_calls) ? retrievalTrace.tool_calls.filter((item) => item && typeof item === "object") : [];
    }

    function sortFacetsForQueue(facets) {
        const priority = {
            running: 0,
            preparing: 1,
            queued: 2,
            failed: 3,
            completed: 4,
        };
        return [...facets].sort((left, right) => {
            const leftStatus = normalizeStatus(left.status || "queued");
            const rightStatus = normalizeStatus(right.status || "queued");
            const delta = (priority[leftStatus] ?? 9) - (priority[rightStatus] ?? 9);
            if (delta !== 0) {
                return delta;
            }
            return String(left.facet_key || "").localeCompare(String(right.facet_key || ""));
        });
    }

    function countFacetStatuses(facets) {
        return facets.reduce((accumulator, facet) => {
            const status = normalizeStatus(facet.status || "queued");
            accumulator[status] = (accumulator[status] || 0) + 1;
            return accumulator;
        }, { queued: 0, preparing: 0, running: 0, completed: 0, failed: 0 });
    }

    function buildFacetLead(facet) {
        const findings = facet.findings || {};
        const status = normalizeStatus(facet.status || "queued");
        if (findings.summary) {
            return findings.summary;
        }
        if (status === "queued") {
            return "Waiting for a free execution slot.";
        }
        if (status === "preparing") {
            return "Preparing evidence for the next agent step.";
        }
        if (status === "running") {
            return `Current phase: ${phaseLabel(findings.phase || status)}.`;
        }
        if (status === "failed") {
            return findings.notes || facet.error_message || "This facet failed before a summary was produced.";
        }
        return "No summary was stored for this facet.";
    }

    function looksStructured(value) {
        const text = String(value || "").trim();
        if (!text) {
            return false;
        }
        return text.startsWith("{") || text.startsWith("[") || text.startsWith("```");
    }

    function stringifyMaybe(value) {
        if (value == null || value === "") {
            return "";
        }
        if (typeof value === "string") {
            return value;
        }
        try {
            return JSON.stringify(value, null, 2);
        } catch {
            return String(value);
        }
    }

    function trimText(value, limit = 120) {
        const text = String(value || "").trim();
        if (text.length <= limit) {
            return text;
        }
        return `${text.slice(0, Math.max(0, limit - 3))}...`;
    }

    function isRunBusy(status) {
        return ["queued", "running"].includes(normalizeStatus(status || ""));
    }

    function isRunning(status) {
        return normalizeStatus(status || "") === "running";
    }

    function statusLabel(status) {
        switch (normalizeStatus(status)) {
            case "queued":
                return "Queued";
            case "preparing":
                return "Preparing";
            case "running":
                return "Running";
            case "completed":
                return "Completed";
            case "failed":
                return "Failed";
            default:
                return String(status || "Queued");
        }
    }

    function statusTone(status) {
        switch (normalizeStatus(status)) {
            case "completed":
                return "ready";
            case "failed":
                return "failed";
            case "queued":
                return "queued";
            default:
                return "processing";
        }
    }

    function phaseLabel(phase) {
        switch (String(phase || "").toLowerCase()) {
            case "retrieving":
                return "Retrieving";
            case "llm":
                return "LLM";
            case "analyzing":
                return "Analyzing";
            case "persisting":
                return "Persisting";
            case "completed":
                return "Completed";
            case "failed":
                return "Failed";
            default:
                return String(phase || "Queued");
        }
    }

    function summarizeTraceEvent(event) {
        if (event.kind === "agent_started") {
            return `${event.label || event.facet_key || event.agent || "Agent"} started`;
        }
        if (event.kind === "agent_completed") {
            return `${event.label || event.facet_key || event.agent || "Agent"} completed`;
        }
        if (event.kind === "llm_request_started") {
            return `${event.label || event.facet_key || event.agent || "LLM"} request started`;
        }
        if (event.kind === "llm_request_completed") {
            return `${event.label || event.facet_key || event.agent || "LLM"} request completed`;
        }
        if (event.kind === "tool_call") {
            return `Calling ${event.tool_name || "tool"}`;
        }
        if (event.kind === "tool_result") {
            return `${event.tool_name || "tool"} returned`;
        }
        if (event.kind === "stage_progress") {
            return event.message || event.stage || "Stage progress";
        }
        return event.kind || "trace";
    }

    function traceMeta(event) {
        return [
            event.agent || "",
            event.facet_key || "",
            event.round_index ? `round ${event.round_index}` : "",
            event.tool_name ? `tool ${event.tool_name}` : "",
        ].filter(Boolean).join(" · ");
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
