# 前端 UI 升级计划：全息投影/实验室风格 (Holographic Lab Style)

## 1. 目标与背景
**目标**：将现有项目的界面彻底升级为“全息投影/实验室”风格（Holographic Projection / Laboratory Style），大幅提升科技感、数据感和视觉冲击力，使其看起来像一个未来的高科技控制台（HUD）。
**当前状态**：目前前端基于 Flask Jinja 模板和纯 CSS 构建，已经具备一定的暗黑和赛博朋克基础，但视觉表现较为常规，边框、卡片和动画还有很大的提升空间。
**约束**：保持现有的 Flask 模板结构和类名不变（不破坏后端逻辑），纯粹通过 CSS 的深度重构和少量 JS 动效来实现视觉跃升。

## 2. 具体改造方案

### 2.1 色彩与全局主题重构 (`app/static/style.css`)
- **调色板重写**：
  - 背景：深邃的蓝黑色调（如 `#040B16` 到 `#0B132B`）。
  - 主题色：全息青色（`#00F0FF`）、数据蓝（`#3A86FF`）、警示橙/红用于报错。
  - 文本：高对比度的冷白和科技灰。
- **背景特效 (`.page-aura`)**：
  - 引入动态的扫描线（Scanlines）和全息网格（Holographic Grid）背景。
  - 增加细微的呼吸光晕（Breathing Glow）以模拟全息投影仪的发光效果。

### 2.2 核心组件视觉重塑 (HUD 风格)
- **玻璃拟态与切角设计 (Glassmorphism & Chamfered Corners)**：
  - 将 `.workspace-panel`, `.hero-board`, `.doc-card` 等容器全面升级为玻璃拟态（`backdrop-filter: blur(12px)` + 半透明背景）。
  - 使用 CSS `clip-path: polygon(...)` 替代传统的 `border-radius`，为面板和按钮增加“科幻切角”（Sci-fi Angled Corners）。
- **按钮与交互 (`button`, `.primary-button`)**：
  - 按钮增加全息边框流动动画（Border Marching / Radar Sweep）。
  - 悬停时触发高亮发光和文字毛刺/打字机动效。
- **数据仪表盘 (`.metric-tile`, `.run-metric-grid`)**：
  - 强化数字面板的视觉权重，使用发光的等宽字体（Monospace）。
  - 为进度条（`.progress-fill`）增加光剑或数据流脉冲动画。

### 2.3 模板微调 (`app/templates/base.html` 等)
- 在 `base.html` 中引入一个全局的扫描线覆盖层（Overlay），以增强全息投影的质感。
- 确保所有的 `eyebrow`（小标题）和标记（`span` / `badge`）都采用大写、拉宽字间距的仪表盘排版风格。

### 2.4 交互动效增强 (`app/static/app.js` - 可选/辅助)
- 在现有逻辑中加入微小的鼠标跟随发光效果（Cursor Glow），让用户感觉在操作一个真实的全息面板。

## 3. 假设与决定
- **纯样式驱动**：优先使用 CSS3 特性（如 `clip-path`, `backdrop-filter`, `CSS Variables`, `Keyframes`）完成升级，避免引入笨重的前端框架。
- **兼容性**：切角和发光效果在现代浏览器（Chrome/Edge/Safari）下表现最佳，这是科技感风格的合理权衡。
- **无缝集成**：由于不改变 HTML 的核心 DOM 结构和类名，后端的表单提交、WebSocket 轮询等功能将完全不受影响。

## 4. 验证步骤
1. 应用新 CSS 后，启动本地 Flask 服务器。
2. 访问首页，检查卡片切角、背景网格和发光按钮是否正确渲染。
3. 进入项目详情页，验证模态框（Modal）和拖拽上传区域（Dropzone）在全息风格下的可用性。
4. 进入分析页面，观察进度条和事件流的滚动动画是否具备足够的数据流动感。