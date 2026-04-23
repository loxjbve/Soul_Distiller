# 搬石模式逐篇预分析页面方案 (Stone Preprocess Plan)

## Summary
- 响应最新需求：在 `stone` 模式中，将“逐篇文章预分析”（生成 `stone_profile`）从原有的主分析（AnalysisRun）流程中抽离，变成一个独立的“预分析”步骤。
- 增加专门的 `GET /projects/{project_id}/preprocess` 页面（与 telegram 的预分析路由同级，通过项目模式区分展现），让用户能直观看到每一篇文章的预处理进度、消耗 Token 及状态，类似 Telegram 模式的每周话题分析监控台。
- 预分析完成（全部文章提取到 `stone_profile`）后，用户再去执行 `stone` 的作者级多维度分析。

## Current State Analysis
- 目前 `stone` 模式的预分析逻辑嵌套在 `AnalysisRun` 中（`app/analysis/engine.py` 里的 `_prepare_stone_document_profiles`）。
- 这种做法导致没有独立的 UI 页面来专门展示各篇文章的提取进度，不符合用户所期望的类似 Telegram 模式中直观的 Dashboard 体验。
- Telegram 模式拥有独立的 `TelegramPreprocessRun`、专门的 `telegram_preprocess.html` 页面以及 SSE 监控流。

## Proposed Changes

### 1. 数据库模型 (Models)
- 新增 `StonePreprocessRun` 表（继承 Base, TimestampMixin）：
  - `id`: 主键 UUID
  - `project_id`: 外键关联项目
  - `status`: 运行状态 (`queued`, `running`, `completed`, `failed`, `cancelled`)
  - `started_at`, `finished_at`
  - `progress_percent`: 进度百分比
  - `current_stage`: 当前阶段文字
  - `prompt_tokens`, `completion_tokens`, `total_tokens`
  - `error_message`: 失败信息
  - `summary_json`: 用于记录额外统计和中间状态（如已处理文档数、总文档数等）
- 同步更新 `app/storage/repository.py` 以支持 `StonePreprocessRun` 的创建、查询、最新成功运行获取等。
- 更新 `Project` 模型，增加 `stone_preprocess_runs` 关系。

### 2. 后端核心逻辑 (Worker)
- 创建 `app/stone_preprocess.py`，实现 `StonePreprocessWorker`。
  - 从 `app/analysis/engine.py` 中移除 `_prepare_stone_document_profiles` 逻辑。
  - `StonePreprocessWorker` 负责查询项目下所有状态为 `ready` 的 `DocumentRecord`，调用 `build_stone_profile_payload`（或原有提取逻辑）逐篇生成 `stone_profile`。
  - 在处理过程中实时更新 `StonePreprocessRun` 的状态、进度百分比和 Token 消耗，并通过 SSE 事件分发。

### 3. API 路由 (Routes)
- 修改 `app/web/routes.py`，使其在处理 `/projects/{project_id}/preprocess` 及对应的 API 时，根据 `project.mode` 区分 `telegram` 还是 `stone`：
  - **HTML 页面**: `GET /projects/{project_id}/preprocess` 渲染 `stone_preprocess.html`。
  - **发起任务**: `POST /projects/{project_id}/preprocess/run` 创建并启动 `StonePreprocessWorker`。
  - **API**: `/api/projects/{project_id}/preprocess/runs`, `/api/projects/{project_id}/preprocess/runs/{run_id}` 等支持返回 `StonePreprocessRun` 序列化数据。
  - **SSE 流**: `/api/projects/{project_id}/preprocess/runs/{run_id}/stream` 支持推送 `stone` 模式的进度流。

### 4. 前端展示 (UI)
- 新增 `app/templates/stone_preprocess.html`：
  - 页面结构类似 `telegram_preprocess.html`，展示“搬石模式 - 文章预分析”。
  - 左侧/顶部为总览进度条、Token 消耗统计和当前处理文章状态。
  - 主体区域列表展示项目中所有待处理和已处理的文章（Document），使用指示灯卡片标注“已完成”/“排队中”/“进行中”。
- 新增 `app/static/stone_preprocess.js` 和 CSS 样式：
  - 处理前端的 SSE 连接，动态更新进度条、Token 数量和单篇文章的卡片状态。

### 5. 权限与衔接
- 在 `app/analysis/engine.py` 中，当 `stone` 模式进行 `AnalysisRun` 时，必须要求最近一次 `StonePreprocessRun` 成功（或者至少有文档带有 `stone_profile`），从而实现两阶段物理分离。
- 在 `test_stone_mode.py` 中更新测试用例，补齐 Preprocess -> Analysis 两阶段的测试链路。

## Assumptions & Decisions
- **解耦决策**: 将 `stone` 模式下的文档解析和作者分析明确划分为两个任务生命周期（PreprocessRun -> AnalysisRun），这是为了在不破坏现有框架前提下最大化复用 Telegram 的模式交互体验。
- **持久化**: 单篇文章的 `stone_profile` 依然存储在 `DocumentRecord.metadata_json["stone_profile"]` 中，`StonePreprocessRun` 仅负责流程编排与状态追踪，不创建专门的 `StoneProfile` 关系表，保持轻量。

## Verification Steps
1. **数据库迁移**：运行 Alembic revision 确保 `stone_preprocess_runs` 表成功创建。
2. **单元测试**：修改并运行 `pytest tests/test_stone_mode.py`，验证“创建文档 -> 启动预分析 -> 预分析完成 -> 启动主分析”链路。
3. **手动验证**：
   - 启动本地服务器，进入 `stone` 项目。
   - 上传若干 txt 文档。
   - 进入“预分析”页，点击开始，观察 SSE 实时更新状态及 Token 消耗。
   - 预分析完成后，进入“主分析”页发起多维度作者级分析，最终顺利产出 `writing_guide`。