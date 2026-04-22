# 首页及预分析页改造计划

## 方案概述 (Summary)
本次修改主要针对首页（`index.html`）的布局精简和 Telegram 项目处理的优化。核心目标是去除首页多余的说明性文本与复杂元素，将状态指标整合至标题区，最大化工作区的展示面积；同时，引入“未命名 Telegram 项目”机制，支持在创建时群组名称为空，并在上传 JSON 后自动解析提取；最后，对预分析页面进行模块化折叠设计，提升重要信息的显示优先级与可读性。

## 当前状态分析 (Current State Analysis)
1. **首页 (`index.html`)**：包含多段说明文本（如“把语料、分析...”）、无用的右上角装饰、占据空间的“创建新项目”表单面板以及“系统项目总览”卡片，导致信息展示不集中。
2. **项目创建 (`routes.py`, `ingest.py`)**：在创建项目时，项目名称为必填项。上传文件时缺乏对 Telegram JSON 文件的解析逻辑以更新默认名称。
3. **预分析页面 (`telegram_preprocess.html`)**：所有 Topics 列表、Agent 状态、Token 消耗均平铺展示，信息密度过大，用户难以快速定位当前进行中的任务和资源消耗。

## 具体修改步骤 (Proposed Changes)

### 1. 首页界面精简与重构 (`app/templates/index.html`)
- **移除冗余文本与装饰**：删除 `.page-banner` 内的 `hero_title`、`hero_note` 相关的段落标签，以及右上角的 `.page-banner__portrait` 等无用元素。
- **重设标题区**：在 `.page-banner__main` 的标题部分，直接集成并重新设计项目数量、ChatLLM 与 EmbeddingLLM 就绪状态的标签（Chips）。
- **移除多余面板**：删除页面中“创建新项目”的独立表单面板（`.panel-shell`），以及包含各个指标卡片的“系统项目总览”面板。
- **全屏工作区**：让保留的“最近项目”面板占满整个页面主体。
- **新增创建项目模态框**：将“创建新项目”功能转换为工作区标题栏右上角的一个主要按钮，点击后弹出模态框（类似现有的编辑模态框），并将表单内的 `name` 输入框的 `required` 属性移除。

### 2. 后端创建项目逻辑优化 (`app/web/routes.py`)
- 修改 `create_project_form` 路由：
  - 放宽 `name` 字段的必填限制。
  - 如果用户提交的 `name` 为空且 `mode == "telegram"`，将其默认设置为“未命名 Telegram 项目”并保存；若为其他模式则抛出 400 错误提示必填。

### 3. Telegram JSON 自动解析 (`app/pipeline/ingest.py`)
- 在 `ingest_bytes` 函数中：
  - 检查当 `project.mode == "telegram"` 且文件扩展名为 `.json` 时，尝试将 `content` 解析为 JSON 对象。
  - 如果提取到了 `"name"` 字段，且当前项目的名称为“未命名 Telegram 项目”（或空），则更新项目的 `name` 属性，将其与文件内容同步。

### 4. 预分析页面的布局与视觉优化 (`app/templates/telegram_preprocess.html` & `app/static/telegram-preprocess-page.css`)
- **强化 Agent 和 Token 显示**：将当前 Agent 监控（`.telegram-preprocess-agent-monitor`）和 Token 消耗统计（Input/Output Tokens）移至更加显眼的位置（如 Spotlight 区域旁边），并调大显示字号，增强实时动态感。
- **信息分级与折叠**：将占用大量空间的 Topics 列表（`.telegram-preprocess-topic-board`）通过 `<details>` 和 `<summary>` 标签进行折叠处理。默认仅展示总结或正在运行的 Topic 详情，次要和已处理的 Topics 被收起以释放空间。
- **模块化样式更新**：同步修改 `app/static/telegram-preprocess-page.css`，为新增的折叠面板添加美观的样式（复用现有的 summary 样式），调整网格布局使首屏只展现核心信息。

## 前提假设与设计决策 (Assumptions & Decisions)
- 假设导出的 Telegram JSON 文件顶层确实包含 `"name"` 字段来表示群名。
- 决定在后端上传接口（`ingest.py`）处静默更新项目名称，以保证整个流程不需要新增额外的前端 API 调用或页面刷新，用户刷新页面即可看到新名字。

## 验证步骤 (Verification Steps)
1. **界面检查**：加载首页，确认说明文字、总览卡片消失；标题栏正确显示项目数及模型状态；工作区占满全屏。
2. **交互检查**：点击“创建新项目”按钮，弹出模态框，在 Telegram 模式下不填写名称提交，能正常创建并跳转。
3. **解析检查**：在刚创建的 Telegram 项目中上传一个包含群名的有效 JSON 文件，刷新后检查项目名是否已自动变更。
4. **预分析页检查**：进入 Telegram 项目的预分析页，确认 Token 计数突出显示，Agent 状态直观可见，同时 Topics 列表已被包裹在折叠面板内。