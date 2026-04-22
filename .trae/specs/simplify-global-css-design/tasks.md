# Tasks
- [x] Task 1: 简化基础变量和 tokens
  - [x] SubTask 1.1: 在 `tokens.css` 中移除或简化过于复杂的发光效果、阴影（如 `--shadow-glow`, `--frame-glow` 等）。
  - [x] SubTask 1.2: 调整 `tokens.css` 的 `body` 背景，去除多余的径向渐变，改为简洁的深色渐变或纯色。
- [x] Task 2: 移除 `style.css` 中的花哨装饰元素
  - [x] SubTask 2.1: 移除或隐藏 `.screen-aura`, `.screen-noise`, `.cursor-glow` 相关的样式逻辑。
  - [x] SubTask 2.2: 移除 `Holographic Lab UI Overrides` 中的故障动画（`textGlitch`）等夸张特效。
- [x] Task 3: 优化面板、卡片和按钮的视觉效果
  - [x] SubTask 3.1: 简化 `.surface-card`, `.hero-panel`, `.metric-card` 等容器的背景渐变和边框，降低不必要的 `box-shadow`。
  - [x] SubTask 3.2: 优化按钮（`.primary-button`, `button` 等）的 `hover` 状态，去除不必要的过度形变和多重阴影，保持极简科技感。
  - [x] SubTask 3.3: 统一 `app-body` 等顶级容器的背景，去除复杂的径向渐变叠加。

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3] depends on [Task 1]
