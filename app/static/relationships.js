import {
    escapeHtml,
} from "./shared.js";

const bootstrap = JSON.parse(document.getElementById("project-bootstrap")?.textContent || "{}");

if (bootstrap.project?.id) {
    const state = {
        projectId: bootstrap.project.id,
        ui: bootstrap.ui_strings || {},
        telegram: { ...(bootstrap.telegram || {}) },
    };

    const elements = {
        relationshipFriendlyList: document.getElementById("telegram-relationship-friendly-list"),
        relationshipTenseList: document.getElementById("telegram-relationship-tense-list"),
        graphContainer: document.getElementById("relationship-graph"),
    };

    renderTelegramRelationships();

    function renderTelegramRelationships() {
        const bundle = state.telegram.relationships || null;
        const snapshot = bundle?.snapshot || null;
        const users = Array.isArray(bundle?.users) ? bundle.users : [];
        const edges = Array.isArray(bundle?.edges) ? bundle.edges : [];

        const friendlyEdges = edges
            .filter((edge) => edge.relation_label === "friendly")
            .sort(sortRelationshipEdges)
            .slice(0, 20);
        const tenseEdges = edges
            .filter((edge) => edge.relation_label === "tense")
            .sort(sortRelationshipEdges)
            .slice(0, 20);

        renderRelationshipCollection(
            elements.relationshipFriendlyList,
            friendlyEdges,
            {
                emptyText: state.ui.telegram_relationship_no_friendly || "No friendly ties yet.",
            }
        );
        renderRelationshipCollection(
            elements.relationshipTenseList,
            tenseEdges,
            {
                emptyText: state.ui.telegram_relationship_no_tense || "No tense ties yet.",
            }
        );

        if (elements.graphContainer && window.echarts) {
            renderGraph(users, edges);
        }
    }

    function renderGraph(users, edges) {
        const chart = echarts.init(elements.graphContainer, 'dark', { backgroundColor: 'transparent' });
        
        const nodes = users.map(user => {
            const msgCount = user.message_count || 1;
            const size = Math.max(10, Math.min(60, Math.log10(msgCount) * 15));
            return {
                id: user.participant_id,
                name: user.label || user.participant_id,
                symbolSize: size,
                itemStyle: {
                    color: '#61a8ff',
                    borderColor: '#fff',
                    borderWidth: 1
                },
                label: {
                    show: size > 20,
                    position: 'bottom',
                    color: '#cbe4ff',
                    fontSize: 10
                },
                value: msgCount
            };
        });

        const links = edges.map(edge => {
            let color = '#555';
            if (edge.relation_label === 'friendly') color = '#42d8a8';
            else if (edge.relation_label === 'tense') color = '#ff718a';
            
            const strength = edge.interaction_strength || 0;
            const width = Math.max(1, strength * 5);
            
            return {
                source: edge.participant_a_id,
                target: edge.participant_b_id,
                value: strength,
                lineStyle: {
                    color: color,
                    width: width,
                    opacity: 0.6,
                    curveness: 0.2
                },
                relation_label: edge.relation_label
            };
        });

        const option = {
            tooltip: {
                trigger: 'item',
                formatter: function (params) {
                    if (params.dataType === 'node') {
                        return `<b>${escapeHtml(params.data.name)}</b><br/>Messages: ${params.data.value}`;
                    } else if (params.dataType === 'edge') {
                        return `<b>${escapeHtml(params.data.relation_label)}</b><br/>Strength: ${params.data.value}`;
                    }
                }
            },
            series: [
                {
                    type: 'graph',
                    layout: 'force',
                    nodes: nodes,
                    links: links,
                    roam: true,
                    label: {
                        position: 'right'
                    },
                    force: {
                        repulsion: 300,
                        edgeLength: 150
                    },
                    lineStyle: {
                        color: 'source',
                        curveness: 0.3
                    },
                    emphasis: {
                        focus: 'adjacency',
                        lineStyle: {
                            width: 5
                        }
                    }
                }
            ]
        };

        chart.setOption(option);
        
        window.addEventListener('resize', () => {
            chart.resize();
        });
    }

    function renderRelationshipCollection(container, edges, options = {}) {
        if (!container) {
            return;
        }
        const participantId = String(options.participantId || "");
        if (!Array.isArray(edges) || !edges.length) {
            container.innerHTML = `<p class="telegram-relationship-list__empty">${escapeHtml(options.emptyText || "No relationship data.")}</p>`;
            return;
        }
        container.innerHTML = edges.map((edge) => renderRelationshipItem(edge, { participantId })).join("");
    }

    function renderRelationshipItem(edge, options = {}) {
        const participantId = String(options.participantId || "");
        const label = normalizeRelationshipLabel(edge.relation_label);
        const labelText = relationshipLabelText(label);
        const labelTone = relationshipLabelTone(label);
        const pairLabel = participantId
            ? relationshipCounterpartLabel(edge, participantId)
            : `${edge.participant_a_label || edge.participant_a_id} × ${edge.participant_b_label || edge.participant_b_id}`;
        const summary = String(edge.summary || "").trim() || (state.ui.telegram_relationship_rule_only || "Rule-based evidence only.");
        const metrics = [
            `${state.ui.telegram_relationship_strength || "Strength"} ${formatRelationshipNumber(edge.interaction_strength)}`,
            `${state.ui.telegram_relationship_confidence || "Confidence"} ${formatRelationshipNumber(edge.confidence)}`,
        ];
        const details = renderRelationshipDetails(edge);

        return `
            <article class="telegram-relationship-item">
                <div class="telegram-relationship-item__head">
                    <strong>${escapeHtml(pairLabel)}</strong>
                    <span class="status-chip ${labelTone}">${escapeHtml(labelText)}</span>
                </div>
                <div class="telegram-relationship-item__meta">${escapeHtml(metrics.join(" · "))}</div>
                <p class="telegram-relationship-item__summary">${escapeHtml(summary)}</p>
                ${details}
            </article>
        `;
    }

    function renderRelationshipDetails(edge) {
        const metrics = edge.metrics || {};
        const supportingSignals = Array.isArray(metrics.supporting_signals) ? metrics.supporting_signals : [];
        const counterSignals = Array.isArray(metrics.counter_signals) ? metrics.counter_signals : [];
        const evidence = Array.isArray(edge.evidence) ? edge.evidence : [];
        const counterevidence = Array.isArray(edge.counterevidence) ? edge.counterevidence : [];
        const sections = [];

        if (supportingSignals.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_supporting_signals || "Support")}</span>
                    <div class="telegram-relationship-signal-row">
                        ${supportingSignals.map((item) => `<span class="telegram-relationship-signal">${escapeHtml(String(item || ""))}</span>`).join("")}
                    </div>
                </div>
            `);
        }
        if (counterSignals.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_counter_signals || "Counter-signals")}</span>
                    <div class="telegram-relationship-signal-row">
                        ${counterSignals.map((item) => `<span class="telegram-relationship-signal">${escapeHtml(String(item || ""))}</span>`).join("")}
                    </div>
                </div>
            `);
        }
        if (evidence.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_evidence || "Evidence")}</span>
                    <div class="telegram-relationship-evidence-stack">
                        ${evidence.map((item) => renderRelationshipEvidence(item)).join("")}
                    </div>
                </div>
            `);
        }
        if (counterevidence.length) {
            sections.push(`
                <div class="telegram-relationship-detail-group">
                    <span>${escapeHtml(state.ui.telegram_relationship_counterevidence || "Counterevidence")}</span>
                    <div class="telegram-relationship-evidence-stack">
                        ${counterevidence.map((item) => renderRelationshipEvidence(item)).join("")}
                    </div>
                </div>
            `);
        }
        if (!sections.length) {
            return "";
        }
        return `
            <details class="telegram-relationship-item__details">
                <summary>${escapeHtml(state.ui.telegram_relationship_view_evidence || "View evidence")}</summary>
                <div class="telegram-relationship-item__details-body">
                    ${sections.join("")}
                </div>
            </details>
        `;
    }

    function renderRelationshipEvidence(item) {
        if ((item?.kind || "") === "reply_context") {
            const summary = String(item.summary || state.ui.telegram_relationship_reply_chain || "Reply chain");
            const messages = Array.isArray(item.messages) ? item.messages : [];
            return `
                <article class="telegram-relationship-evidence">
                    <strong>${escapeHtml(summary)}</strong>
                    <div class="telegram-relationship-evidence__stack">
                        ${messages.map((message) => {
                            const sender = String(message.sender_name || message.participant_id || "Unknown");
                            const text = String(message.text || "");
                            return `
                                <div class="telegram-relationship-evidence__message">
                                    <span>${escapeHtml(sender)}</span>
                                    <p>${escapeHtml(text)}</p>
                                </div>
                            `;
                        }).join("")}
                    </div>
                </article>
            `;
        }

        const title = String(item?.title || item?.week_key || state.ui.telegram_relationship_shared_topic || "Shared topic");
        const summary = String(item?.summary || "");
        const patterns = Array.isArray(item?.interaction_patterns) ? item.interaction_patterns : [];
        const stanceParts = [
            item?.participant_a_stance ? `${state.ui.telegram_relationship_participant_a || "A"}: ${item.participant_a_stance}` : "",
            item?.participant_b_stance ? `${state.ui.telegram_relationship_participant_b || "B"}: ${item.participant_b_stance}` : "",
        ].filter(Boolean);
        const quotes = Array.isArray(item?.quotes) ? item.quotes : [];

        return `
            <article class="telegram-relationship-evidence">
                <strong>${escapeHtml(title)}</strong>
                ${summary ? `<p>${escapeHtml(summary)}</p>` : ""}
                ${patterns.length ? `<div class="telegram-relationship-evidence__meta">${escapeHtml(patterns.join(" · "))}</div>` : ""}
                ${stanceParts.length ? `<div class="telegram-relationship-evidence__meta">${escapeHtml(stanceParts.join(" · "))}</div>` : ""}
                ${quotes.length ? `
                    <div class="telegram-relationship-evidence__stack">
                        ${quotes.map((quote) => {
                            const label = String(quote.display_name || quote.participant_id || "Member");
                            const text = String(quote.quote || "");
                            return `
                                <div class="telegram-relationship-evidence__message">
                                    <span>${escapeHtml(label)}</span>
                                    <p>${escapeHtml(text)}</p>
                                </div>
                            `;
                        }).join("")}
                    </div>
                ` : ""}
            </article>
        `;
    }

    function relationshipCounterpartLabel(edge, participantId) {
        const isA = String(edge.participant_a_id || "") === String(participantId || "");
        return isA
            ? (edge.participant_b_label || edge.participant_b_id || "")
            : (edge.participant_a_label || edge.participant_a_id || "");
    }

    function relationshipLabelTone(label) {
        if (label === "friendly") return "tone-ready";
        if (label === "tense") return "tone-failed";
        if (label === "neutral") return "tone-queued";
        return "tone-warning";
    }

    function relationshipLabelText(label) {
        const mapping = {
            friendly: state.ui.telegram_relationship_label_friendly || "Friendly",
            neutral: state.ui.telegram_relationship_label_neutral || "Neutral",
            tense: state.ui.telegram_relationship_label_tense || "Tense",
            unclear: state.ui.telegram_relationship_label_unclear || "Unclear",
        };
        return mapping[label] || label || "Unclear";
    }

    function normalizeRelationshipLabel(label) {
        const normalized = String(label || "").trim().toLowerCase();
        if (["friendly", "neutral", "tense", "unclear"].includes(normalized)) {
            return normalized;
        }
        return "unclear";
    }

    function sortRelationshipEdges(left, right) {
        const strengthDelta = Number(right?.interaction_strength || 0) - Number(left?.interaction_strength || 0);
        if (strengthDelta !== 0) return strengthDelta;
        return Number(right?.confidence || 0) - Number(left?.confidence || 0);
    }

    function formatRelationshipNumber(value) {
        const normalized = Number(value || 0);
        if (!Number.isFinite(normalized)) return "0.00";
        return normalized.toFixed(2);
    }
}
