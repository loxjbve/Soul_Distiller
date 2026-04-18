# Telegram 预处理页面优化与 Prompt 修改计划

## Summary
本计划旨在简化 Telegram 预处理页面的卡片显示（折叠为单行并支持滚动），增加计算“周话题总结”进度的专属进度条，并修改 LLM Prompt 确保输出为中文。

## Current State Analysis
1. 页面中 Weekly Candidates、Top Users、Subagent 时间线渲染出的卡片目前为多行块级排版，占用大量垂直空间。
2. 页面中目前只有一个表示总体进度的进度条。
3. `app/telegram_preprocess.py` 中 `_run_weekly_topic_agent` 的 Prompt 未明确语言，导致生成内容经常是英文。

## Proposed Changes

### 1. 修改样式使卡片折叠成单行并允许滚动
*   **目标文件**: `app/static/telegram_preprocess.js`
*   **修改方式**: 
    *   在 `renderTraceList`、`renderWeeklyCandidates`、`renderTopUsers` 的卡片元素生成时，给 `article` 额外增加一个类名，例如 `compact-card`。
    *   在 `app/static/style.css` (或直接在 `telegram_preprocess.html` 内嵌样式中) 增加 `.compact-card` 的样式：
        ```css
        .compact-card {
            display: flex;
            align-items: center;
            gap: 12px;
            white-space: nowrap;
            overflow-x: auto;
            padding: 8px 12px;
        }
        .compact-card > * {
            margin: 0 !important;
            flex-shrink: 0;
        }
        .compact-card pre {
            padding: 4px 8px;
        }
        ```

### 2. 增加“当前进度（周话题总结）”进度条
*   **目标文件**: `app/templates/telegram_preprocess.html`
*   **修改方式**:
    *   在总进度条代码块旁，增加一个新的 `progress-shell` 结构，专门用于显示当前进度：
        ```html
        <div class="progress-shell top-gap">
            <div class="progress-labels">
                <strong>当前进度 (周话题)</strong>
                <span id="telegram-preprocess-current-progress-label">0%</span>
            </div>
            <div class="progress-track">
                <div class="progress-fill" id="telegram-preprocess-current-progress-fill" style="width: 0%;"></div>
            </div>
        </div>
        ```
*   **目标文件**: `app/static/telegram_preprocess.js`
*   **修改方式**:
    *   在 `elements` 对象中注册这两个新的 DOM 节点。
    *   在 `renderBundle` 方法中增加计算逻辑：
        `topic_count / weekly_candidate_count`。
        ```javascript
        const topics = bundle.topic_count || 0;
        const candidates = bundle.weekly_candidate_count || bundle.window_count || 0;
        let currentPercent = 0;
        if (candidates > 0) {
            currentPercent = Math.min(100, Math.floor((topics / candidates) * 100));
        } else if (topics > 0) {
            currentPercent = 100;
        }
        updateText(elements.currentProgressLabel, `${currentPercent}%`);
        if (elements.currentProgressFill) {
            elements.currentProgressFill.style.width = `${currentPercent}%`;
        }
        ```

### 3. 强制周话题总结的 LLM 输出为中文
*   **目标文件**: `app/telegram_preprocess.py`
*   **修改方式**:
    *   在 `_run_weekly_topic_agent` 方法的 `system` prompt 字符串末尾追加指令：
        `"IMPORTANT: You must output all textual content (such as title, summary, keywords, role_hint) in Chinese (中文)."`

## Assumptions & Decisions
*   **卡片折叠方案**: 基于用户的要求“直接截断多余文本，仅用最简单的方式表示”，使用 Flex 单行布局并配合 `white-space: nowrap` 与 `overflow-x: auto` 是最简单直接的方式，既限制了卡片高度又允许用户滚动查看超出部分。
*   **当前进度计算**: 当任务刚启动且 `weekly_candidate_count` 未生成时，进度保持 0%。

## Verification steps
1. 访问任意 Telegram 项目的预处理页面。
2. 验证 Weekly Candidates、Top Users 和 Subagent 时间线的卡片均变成了高度为单行且支持横向滚动的紧凑样式。
3. 验证页面出现了“当前进度 (周话题)”的进度条，且其百分比和宽度显示正常。
4. 运行一次预处理，验证生成的话题标题和摘要均为中文输出。