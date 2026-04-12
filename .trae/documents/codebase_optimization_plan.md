# 代码库性能优化与重构计划 (Codebase Performance Optimization Plan)

## 1. 现状分析 (Current State Analysis)
根据排查，前端“跳转页面要等好多秒没反应”以及“页面不流畅”的性能瓶颈主要由以下几个严重的设计缺陷导致：

1. **后端数据库过度 Eager Loading (N+1与数据量过大)**:
   - 在 `app/web/routes.py` 的 `_project_context` 依赖函数中（几乎所有项目相关的页面跳转都会调用），使用了 `repository.get_latest_analysis_run`。
   - 该方法通过 `selectinload(AnalysisRun.facets)` 和 `selectinload(AnalysisRun.events)` **无条件地全量加载**了该项目最新的分析记录的所有维度和事件。
   - `facets` 和 `events` 表中包含海量的 JSON 字段（如 `evidence_json`、`findings_json` 等）。尤其是在 LLM 运行期间，流式输出导致数据库中瞬间累积极大的负载，使得每次页面跳转后端都在反序列化 MB 级别的数据，导致页面阻塞。
2. **LLM 流式输出 (`llm_delta`) 滥用数据库存储**:
   - `app/analysis/engine.py` 在 `_flush_stream_delta` 时，不仅更新了 facet 的 `llm_live_text`，还插入了 `event_type="llm_delta"` 的 `AnalysisEvent`。
   - 每次 LLM 生成都会由于片段更新而在数据库中插入数以百计包含多达 20000 字符文本的事件。这让数据库极速膨胀，并且随着全量查询拖垮后端性能和网络传输（SSE 会推送这些庞大的事件列表）。
3. **前端 WebSocket 滥用与 DOM 频繁重绘**:
   - 文档状态轮询 `/api/projects/{project_id}/documents/ws` 在后端的一个 `while True` 循环中，每 1 秒就会执行一次无限制的 `repository.list_project_documents` 查出全量文档。
   - 前端收到包含全量文档数组的 WebSocket 消息后，调用 `renderDocuments()` 粗暴地执行 `documentGrid.innerHTML = ...`，引发全量 DOM 销毁和重绘。这导致当有几十上百份文档时，浏览器极其卡顿。

## 2. 优化方案 (Proposed Changes)

### 步骤 1: 消除 `llm_delta` 的冗余数据库存储
- **文件**: `app/analysis/engine.py`
- **改动**: 
  - 修改 `_flush_stream_delta`，删除调用 `repository.add_analysis_event(..., event_type="llm_delta", ...)` 的逻辑。
  - 仅保留更新 `facet_record.findings_json["llm_live_text"]` 的部分。
  - 因为前端完全只依赖 `llm_live_text` 渲染打字机效果，这个改动对 UI 表现零影响，但能极大缩减 DB 压力。
- **清理**: 
  - 编写并执行一个脚本 `scripts/cleanup_db.py`，从数据库中删除所有已存的 `event_type = 'llm_delta'` 数据并执行 `VACUUM` 释放空间。

### 步骤 2: 按需延迟加载 (Lazy Loading) 数据库关联
- **文件**: `app/storage/repository.py`
- **改动**: 
  - 修改 `get_latest_analysis_run`、`get_active_analysis_run`、`get_analysis_run` 等方法，增加可选参数 `load_facets: bool = True` 和 `load_events: bool = True`。
  - 当参数为 `False` 时，不附加 `.options(selectinload(...))`。
- **文件**: `app/web/routes.py`
- **改动**: 
  - 修改 `_project_context` 函数，将调用改为 `repository.get_latest_analysis_run(session, project_id, load_facets=False, load_events=False)`。
  - 这样普通的页面跳转（如项目主页、预分析页等）仅拉取几十个字节的基础状态，真正实现毫秒级响应。

### 步骤 3: 优化文档状态 WebSocket 与轻量级查询
- **文件**: `app/web/routes.py`
- **改动**: 
  - 优化 `websocket_document_status`：不再获取完整文档对象。使用轻量级查询 `select(DocumentRecord.id, DocumentRecord.ingest_status)`，并结合 `task_manager.get_by_project` 发送必要状态。
- **文件**: `app/templates/project_detail.html` (以及关联 JS 逻辑)
- **改动**: 
  - 在 `ws.onmessage` 中拦截数据并对 DOM 元素进行**就地更新 (In-place update)**，通过 `document.querySelector` 修改单个文档卡片的进度条、状态图标和文本，避免粗暴地使用 `innerHTML` 覆盖全量列表。

### 步骤 4: 缩减分析监控 SSE 载荷
- **文件**: `app/web/routes.py` 和 `app/static/app.js`
- **改动**: 
  - SSE 中 `stream_analysis_api` 依然每秒推送，但由于第一步移除了几百条 `llm_delta` 事件，负载本身已从数 MB 暴降到数 KB，解析压力被彻底解决，无需对传输层做破坏性修改。

## 3. 假设与决策 (Assumptions & Decisions)
- **决策**: 完全摒弃 `llm_delta` 作为 Event 留存，因为 LLM Live Text 的展示已足以满足用户的“实时反馈”诉求。
- **假设**: 不涉及改变 LLM 实际运算的时间，本次只解决 I/O 阻塞、数据解析阻塞以及渲染卡顿。

## 4. 验证步骤 (Verification Steps)
1. 运行 `scripts/cleanup_db.py` 观察数据库体积是否明显缩减。
2. 在浏览器中打开项目详情页，切换到 Playground 或 Preprocess 页，感受页面跳转速度（应无等待感，瞬间完成渲染）。
3. 上传一份长文档，查看 `project_detail.html` 的进度条动画是否流畅（不再有 1Hz 的全局闪烁）。
4. 运行一次完整的 Analysis，观察浏览器控制台的 Network 标签中 SSE 流，确认每次 payload 的体积保持在合理的低 KB 范围内。