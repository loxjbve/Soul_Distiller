# Tasks
- [x] Task 1: 优化全局 CSS 设计语言（Gemini 风格）：重构 `tokens.css` 和基础样式（`style.css`, `shell.css`, `components.css`），引入 Gemini 风格的配色、阴影、圆角和字体排版。
- [x] Task 2: 排查与优化 JS 性能瓶颈：审查 `app.js`, `project.js`, `analysis.js` 等核心脚本，修复冗余 DOM 操作，添加必要的节流/防抖，优化大列表渲染逻辑。
- [x] Task 3: 优化模板加载与布局不合理之处：检查 `app/templates/` 下的所有 HTML 模板，优化静态资源加载顺序（如 defer/async），修复不合理的嵌套与非响应式布局。
- [x] Task 4: 细化各个页面的样式与交互表现：针对 `project_detail.html`, `analysis.html` 等复杂页面，进行具体的样式打磨，确保整体风格一致且无视觉突兀。

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3] depends on [Task 1]
- [Task 4] depends on [Task 1], [Task 2], [Task 3]
