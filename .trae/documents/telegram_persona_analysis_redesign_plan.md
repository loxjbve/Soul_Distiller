# Telegram 模式单人多维分析页面 UI 优化方案

## Summary
优化 Telegram 模式下「单人多维分析」页面（即 Persona 分析页）的 UI 布局。移除原有的「事件流」(Diagnostics) 卡片，并将整体网格布局调整为单列，使其在视觉上更接近群组预处理页面的设计，从而更加清晰、聚焦地展示运行中的 Agent 轨道与分析结果。

## Current State Analysis
当前 `app/templates/analysis.html` 在所有模式下都采用了相同的网格布局（包含 `center`、`diagnostics`、`results` 区域），并在右上角 Banner 处显示“Diagnostics”标签。当渲染 Telegram 模式的单人分析页时（条件为 `project.mode == "telegram" and project.parent_id`），页面包含了不必要的事件流卡片，导致信息过载，与群组预处理页面的简洁风格不一致。

## Proposed Changes

1. **修改 `app/templates/analysis.html`**
   - **Banner 标签**: 在渲染右上角 Banner 的 `<span class="page-banner__label">{{ ui.banner_diagnostics_label }}</span>` 时，包裹判断条件 `{% if not (project.mode == "telegram" and project.parent_id) %}`，使其在单人多维分析页隐藏。
   - **Diagnostics 面板**: 在渲染 `<article class="panel-shell analysis-diagnostics-panel">` 时，包裹相同的判断条件，彻底在模板层面移除事件流面板。

2. **修改 `app/static/analysis-page.css`**
   - 针对 `.page-analysis--telegram-persona` 页面容器，重写 `.page-content` 的 Grid 布局。
   - 增加如下样式：
     ```css
     .page-analysis.page-analysis-center.page-analysis--telegram-persona .page-content {
         grid-template-columns: 1fr;
         grid-template-rows: auto;
         grid-template-areas:
             "center"
             "results";
     }
     ```
   - 这样可将原来的多列布局改为单列布局，`center`（运行中的 agent）与 `results`（结果展示）垂直排列，更加清晰。

## Assumptions & Decisions
- **前端安全性**: 决定仅通过模板条件渲染和 CSS 调整实现。`analysis.js` 中包含对 DOM 元素的判空逻辑（如 `if (!elements.diagnosticsList) { return; }`），因此在模板中直接移除面板节点是安全且符合预期的。
- **页面特征匹配**: 在现有系统中，单人多维分析页面的特征为 `project.mode == "telegram"` 且 `project.parent_id` 为真（代表是基于主项目的子 Persona 视图），此条件可准确命中目标页面。

## Verification Steps
- 在模板及 CSS 更新完成后，启动应用并进入 Telegram 模式的单人分析页面，视觉确认「事件流」面板是否已消失，且 Agent 区域与结果区域呈现单列宽度自适应的布局。
- 运行相关的 pytest 测试用例（如 `test_telegram_mode.py` 中的 `test_telegram_analysis_page_renders_agent_center_shell`），确保未引入回归问题。