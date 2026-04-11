# RAG 架构增强计划 (Query Rewriting & Hybrid Search)

## 1. 摘要 (Summary)
本项目当前的检索策略为“回退机制”（优先使用向量检索，失败时降级为 BM25 词法检索），且直接使用用户的原始问题进行匹配。
根据对前沿开源 RAG 项目（如 Dify、FastGPT、RAGFlow）的最佳实践调研，我们将对检索架构进行以下核心增强：
1. **Query Rewriting (查询改写)**：在检索前，通过 LLM 独立生成假设性回答 (HyDE) 供向量检索使用，同时生成扩写同义词供 BM25 使用。
2. **混合检索 + RRF 重排 (Hybrid Search + RRF)**：利用线程池并发执行 BM25 与向量检索（双路召回），并在内存中使用倒数秩融合算法 (Reciprocal Rank Fusion, RRF) 合并结果，大幅提升查准率和召回率。

## 2. 当前状态分析 (Current State Analysis)
- `app/retrieval/service.py` 中的 `RetrievalService.search`：仅支持回退逻辑（如果 `embedding_config` 存在且成功，则直接返回向量结果并标记为 `"hybrid"`，实际上并不是真正的混合检索）。
- `app/analysis/engine.py`：在执行分析分面 (Facet) 时，将硬编码的查询词直接传递给检索服务，未进行任何查询优化。
- `app/preprocess/service.py`：同样直接将用户消息传递给检索工具。
- 缺少并发双路召回机制，也缺少结果合并算法（RRF）。

## 3. 提议的变更 (Proposed Changes)

### 3.1 创建查询改写模块 (`app/retrieval/rewrite.py`)
- **What**: 新增 `rewrite_query` 函数。
- **Why**: 使用 LLM 将用户的简短提问扩展为丰富的上下文，以解决词汇不匹配问题并提高向量空间的命中率。
- **How**:
  - 构造 Prompt，要求 LLM 返回 JSON 格式，包含两个字段：`hyde_document` (用于向量检索的假设性回答) 和 `expanded_keywords` (用于 BM25 的同义词/扩展词)。
  - 调用 `OpenAICompatibleClient.chat_completion_result` 获取结果并解析。
  - 提供优雅降级：如果 LLM 调用失败，返回空字符串，不阻塞主流程。

### 3.2 改造检索服务核心 (`app/retrieval/service.py`)
- **What**: 重构 `RetrievalService.search` 方法，并添加 `_rrf_merge` 算法。
- **Why**: 实现真正的并发混合检索和结果融合。
- **How**:
  - 修改 `search` 的签名，增加 `llm_config: ServiceConfig | None = None` 参数。
  - 在方法开头，如果有 `llm_config`，则调用 `rewrite_query`，将原始 `query` 转化为 `lexical_query` 和 `vector_query`。
  - 使用 `concurrent.futures.ThreadPoolExecutor(max_workers=2)` 并发执行 `self.lexical.search` 和 `self.embedding.search`。
  - 收集两路召回的结果，若向量检索成功，调用 `self._rrf_merge` 将两组结果基于 RRF (Reciprocal Rank Fusion) 公式 `1 / (60 + rank)` 计算新得分并重新排序。
  - 完善 `trace` 信息，记录 `query_rewritten` 状态和并发耗时/错误。

### 3.3 适配分析引擎 (`app/analysis/engine.py`)
- **What**: 在分析执行阶段，将 LLM 配置传递给检索服务。
- **Why**: 使得 `AnalysisEngine` 在进行证据召回时也能享受 Query Rewriting 带来的收益。
- **How**:
  - 修改 `_retrieve_hits` 签名，接收 `llm_payload: dict[str, Any] | None` 参数。
  - 在内部将 `llm_payload` 转换为 `ServiceConfig` 并传递给 `self.retrieval.search`。
  - 更新调用 `_retrieve_hits` 的相关代码（如 `_prepare_facet_execution` 及失败重试逻辑），确保正确传入 `llm_payload`。

### 3.4 适配预处理服务 (`app/preprocess/service.py`)
- **What**: 确保预处理智能体使用增强后的检索逻辑。
- **Why**: 预处理 Agent 中的 `search_project_documents` 工具同样需要 Query Rewriting 支持。
- **How**:
  - 修改 `_execute_tool`，在调用 `self.retrieval.search` 时传入 `llm_config=self.config`。

## 4. 假设与决策 (Assumptions & Decisions)
- **并发安全性**：当前的 `Session` (SQLAlchemy) 在 `ThreadPoolExecutor` 中跨线程并发使用时，由于只进行读操作 (`SELECT`)，在多数情况下是安全的，但为了严格保证 SQLAlchemy 的线程安全，我们可能需要使用独立 `Session` 或确信并发读在此 SQLite 配置下不抛出错误。如果 SQLite 并发读受限，我们可以在闭包内直接使用同一个连接执行。因为 SQLite 默认是线程隔离的，最安全的做法是仅使用 `ThreadPoolExecutor` 执行不涉及当前 session 发起新连接的操作，或者我们直接在主线程中串行（但这与用户要求相悖）。*优化决策*：我们将 `ThreadPoolExecutor` 用于包裹 API 网络请求（`self.embedding.search` 主要阻塞在网络 IO 上），而将 `self.lexical.search` 放在主线程，这样既实现了并发，又避免了 SQLAlchemy 同一 `Session` 跨线程使用的潜在风险。
- **降级决策**：Query Rewriting 失败或超时不应导致检索失败，应自动回退至原始 query。
- **Token 消耗**：引入 HyDE 会增加一次 LLM 请求的成本（大约几百 Tokens），这对于获取更好的检索质量是值得的。

## 5. 验证步骤 (Verification steps)
1. **语法检查**：运行 `ruff check app` 确保没有导入错误或语法错误。
2. **测试运行**：执行 `pytest tests/test_chunking_retrieval.py`，验证现有的词法检索测试和向量检索测试没有被破坏。
3. **日志观测**：通过实际触发一次知识库问答或分析任务，查看控制台输出或 `trace` JSON，确认 `query_rewritten: true` 且 `mode: hybrid`，并且包含 `hyde_text`。
4. **合并效果**：检查最终返回给大模型的 `hit_count` 和 `RetrievedChunk` 的 `score`（应表现为 RRF 融合后的小数得分，如 0.033...）。