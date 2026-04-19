# 重构 Agent Trace UI 与恢复周总结并发设置

## 1. 目标与范围 (Summary & Scope)
- **目标 1**: 恢复 `app/telegram_preprocess.py` 中 `_run_weekly_topic_summary` 的周总结并发设置，移除并发上限（即 `max_workers=None`）。
- **目标 2**: 重构多维分析页面（`app/static/app.js` 及相关 CSS）中的 Agent Trace 卡片，采用 Agentic Coding 风格（深色模式、毛玻璃、无边框、折叠的 Tool Calls 气泡、上下文指示器等）。

## 2. 现状分析 (Current State Analysis)
- **周总结并发**: 目前在 `telegram_preprocess.py` 中的 `_run_weekly_topic_summary` 是一个简单的串行 `for` 循环，导致并发上限被实质上限制（或者运行极慢）。
- **多维分析 Agent Trace**: 在 `app/static/app.js` 中，多维分析的 Trace 和实时输出目前仅使用简单的 `<pre class="trace-box">` 渲染纯文本或 JSON，没有展现出 `tool_calls` 的执行层级，且视觉风格较为传统（有边框的灰色背景框），缺乏极客感。

## 3. 具体修改方案 (Proposed Changes)

### 3.1 恢复周总结并发设置
- **文件**: `app/telegram_preprocess.py`
- **修改内容**:
  在 `_run_weekly_topic_summary` 方法中，引入 `concurrent.futures.ThreadPoolExecutor(max_workers=None)`。
  使用多线程并发执行 `_summarize_weekly_candidate_with_retries`，并通过 `as_completed` 收集结果并更新 `progress_callback`。

### 3.2 重构 Agent Trace UI (Agentic Coding 风格)
- **文件**: `app/static/style.css`
  - 引入极客风格的 CSS 变量（深色模式色系、毛玻璃背景色等）。
  - 添加新组件的样式：`.msg-user`, `.msg-assistant`, `.code-block-wrapper`, `.tool-call`, `.context-indicator` 及相关微交互动画和极简滚动条。
- **文件**: `app/static/app.js`
  - 修改 `renderFacetPanel`（或相关 HTML 生成逻辑），检测 `findings.retrieval_trace.tool_calls`。
  - 将原来的 `traceBody` 和 `liveTextBody` 替换为新的 Agentic 交互风格 HTML 结构：
    - 若存在 `tool_calls`，循环生成带有 `⚙️ calling <tool_name>` 的 `<details class="tool-call">`（手风琴组件）。
    - 将 `llm_live_text` 包装在 `.msg-assistant` 的无边框代码块中。
    - 如果当前状态为正在运行（`running`），展示带有 loading 动画的 `.context-indicator-wrapper`。

## 4. 假设与决定 (Assumptions & Decisions)
- **并发无上限**: `ThreadPoolExecutor(max_workers=None)` 意味着线程数由 Python 默认决定（通常为 CPU 核心数 * 5），符合“不设置上限”的诉求。
- **UI 兼容性**: 现有的 `renderSubDetails` 可能仍保留，但内部的 HTML 字符串将根据新设计的 CSS 进行彻底替换。Tool Calls 的 JSON 输出将格式化并放入深色代码块中。

## 5. 验证步骤 (Verification Steps)
1. 检查 Python 代码：确认 `telegram_preprocess.py` 无语法错误，且能正确调度多线程。
2. 检查前端代码：刷新页面，查看多维分析的 Trace 是否呈现深色极简风格的折叠面板和状态指示器。
