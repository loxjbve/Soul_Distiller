# 项目全量优化计划 (Codebase Optimization Plan)

## 1. 概述 (Summary)
经过对整个 `app/` 和 `tests/` 目录的深度扫描和代码分析，我们发现了若干影响性能、架构健壮性和代码质量的潜在问题。这份计划旨在解决并发阻塞、内存溢出风险、N+1 查询、静默异常吞没以及不合理的同步阻塞调用，全面提升系统的吞吐量和稳定性。

## 2. 现状分析 (Current State Analysis)

在代码库中发现了以下 4 个维度的核心问题：

1. **性能瓶颈 (Performance Bottlenecks)**
   - `app/web/routes.py` 中的 `stream_analysis_api` 接口使用同步生成器结合 `time.sleep()` 响应 SSE 数据。这会长时间霸占 Starlette 的线程池，导致高并发时整个 Web 服务无响应。
   - `app/llm/client.py` 中使用了全局的 `BoundedSemaphore(5)` 和同步的 `httpx.Client()`，且未开启长连接复用（Keep-Alive），导致并发调用 LLM 时出现严重的阻塞和延迟。
   - `app/pipeline/chunking.py` 的 `chunk_segments` 内部使用 `+=` 进行字符串拼接，处理大段文本时产生 $O(N^2)$ 的时间复杂度。
   - 大文件处理时存在内存溢出 (OOM) 风险（例如 `upload.read()` 全量加载和 `_process_embeddings_concurrent` 一次性查出全量文本）。

2. **代码异味与反模式 (Code Smells & Anti-patterns)**
   - `app/pipeline/rechunk.py` 的 `_run_task` 中存在 N+1 查询问题（列表循环中访问被 defer 的 `clean_text` 字段）。
   - `app/db.py` 中 `Database.session` 上下文管理器存在隐式的 `session.commit()`，可能导致意外的脏数据提交。
   - `app/analysis/engine.py` 的兜底检索逻辑 (`_fallback_hits`) 提取无关联的分块，污染 LLM 上下文。

3. **架构缺陷 (Architectural Issues)**
   - `app/pipeline/ingest_task.py` 中的解析任务仅保存在内存中，一旦服务重启，任务状态会永远丢失，导致数据库里的文档卡在 pending 或 processing 状态。
   - `app/retrieval/vector_store.py` 内部使用粗粒度的实例级 `threading.Lock()` 将写入和检索完全串行化，削弱了并发能力。
   - `app/llm/client.py` 使用同步文件锁 (`_LLM_LOG_LOCK`) 写入日志，在高并发流式响应时会引起磁盘 I/O 竞争并阻塞工作线程。

4. **健壮性问题 (Robustness Issues)**
   - 危险的异常吞噬 (Swallowing Exceptions)：如 `_store_to_vector_db` 写入向量库失败时静默 `pass`；`process_document_api` 读取磁盘文件失败时静默返回空字节。
   - `app/analysis/engine.py` 的 `_flush_stream_delta` 中，如果写入数据库锁冲突报错，会直接被 `except Exception: return False` 拦截，导致流式前端状态被永久中断且无错误日志。

## 3. 优化方案 (Proposed Changes)

### Phase 1: 解决高危性能瓶颈 (High Priority)
- **解绑 Web 线程池**: 将 `stream_analysis_api` 中的 `generate()` 重构为 `async def generate()`，将同步的 `time.sleep` 替换为 `await asyncio.sleep()`。在生成器内部使用 `run_in_threadpool` 执行同步的数据库查询。
- **连接池与异步化 LLM 客户端**: 将 `app/llm/client.py` 改造为使用全局单例的 `httpx.AsyncClient`，开启长连接复用。移除或调大不合理的信号量限制。同步的日志写入通过后台队列 (Queue) 和单独的工作线程异步落盘。
- **修复 OOM 与拼接算法**: 
  - `chunk_segments` 中改用列表收集字符串并最终使用 `"\n\n".join()`。
  - 上传接口改为分块流式写入磁盘，大段文本查询改用生成器/分页（如 SQLAlchemy `yield_per`）进行处理。

### Phase 2: 修复代码质量与数据一致性 (Medium Priority)
- **消除静默异常与错误吞噬**: 
  - `_store_to_vector_db` 等关键方法捕获异常后必须记录 `logger.exception`，并将 Document 和 Task 的状态设为 `failed` 附带错误信息。
  - `_flush_stream_delta` 遇到数据库锁等临时错误应重试，若重试失败则抛出异常并通知前端，不能静默停止更新。
- **修复 N+1 查询**: 在 `rechunk.py` 中移除 `defer(DocumentRecord.clean_text)`，或通过批量查询一次性获取需要的 `clean_text`。
- **调整事务管理策略**: 移除 `app/db.py` 中 `session()` 的自动 `commit()` 行为，将 `commit()` 显式地写在每个需要提交修改的路由或业务方法末尾，避免错误回滚不及时。
- **优化向量库锁机制**: `vector_store.py` 中的 `threading.Lock()` 范围缩小到仅针对修改索引结构的操作，分离读写锁或采用更细粒度的控制。

### Phase 3: 架构健壮性提升 (Long-term / Background)
- **任务状态持久化**: 将 `IngestTaskManager` 的状态追踪与 SQLite 数据库持久化结合，在服务启动时自动扫描并恢复/重置中断的 `processing` 状态任务。
- **移除无效的兜底检索**: 优化 `_fallback_hits`，当没有语义关联度极高的片段时，宁可交给 LLM 一个空上下文，也不要强行塞入前 50 个分块。

## 4. 假设与决策 (Assumptions & Decisions)
- **纯内置优化**: 为保持项目轻量级，暂不引入 Celery、Redis 或外部消息队列。任务状态持久化依然依靠现有的 SQLite 数据库。
- **逐步重构**: 由于涉及底层数据库事务 (`session.commit()`) 和并发客户端 (`httpx.AsyncClient`) 的修改，这些修改会影响几乎所有核心模块。因此，建议在执行阶段拆分为多个小的 PR 逐步推进。

## 5. 验证步骤 (Verification)
1. 运行 `pytest tests/` 确保所有 E2E 测试和单元测试在重构后依旧通过。
2. 上传一个超大文档（如 50MB+），监控内存占用并确认分块时间复杂度是否降低。
3. 在页面上同时发起 5 个以上的并发分析任务，确保 Web 界面响应流畅（验证线程池未被阻塞）。
4. 人为在向量库写入时抛出异常，验证该文档最终状态是否为 `failed`，而非停留在 `ready`。
