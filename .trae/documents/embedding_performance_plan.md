# 向量索引性能优化方案 (Embedding Performance Plan)

## 1. 当前问题分析 (Current State Analysis)
根据你的描述，RTX 4090 处理 13MB 文本应该在 1-2 分钟内完成，但目前 1MB 的文件都需要 1 分钟。经过对代码库的排查，导致索引速度极慢的核心瓶颈如下：

1. **Chunk Size 过大导致 O(N^2) 耗时剧增**
   在 `app/pipeline/chunking.py` 中，默认的 `chunk_size` 高达 `4000` 字符（约 2000 Tokens）。对于基于 Transformer 的 Embedding 模型，Attention 机制的计算复杂度是 $O(N^2)$。将单次序列长度翻倍，耗时会增加 4 倍。过大的 Chunk 尺寸直接拖垮了 4090 的吞吐量。
2. **Ingest 任务并发池未被跑满 (Synchronization Barrier)**
   在 `app/pipeline/ingest_task.py` 中，虽然分配了 `max_workers=16` 的线程池，但系统每次只从数据库中查出 `500` 条记录，将其切分为 8 个 batch（每个 batch 64 块）提交给线程池，并且**必须等待这 8 个任务全部完成**才进入下一次循环。这导致另外 8 个线程始终空闲，且由于木桶效应，系统会被最慢的那个 batch 拖慢。
3. **Rechunk 任务完全串行**
   在 `app/pipeline/rechunk.py` 的重分块逻辑中，Embedding 的获取甚至完全没有使用线程池，而是一个 `while` 循环单线程同步请求，这让 4090 的并发能力（Batching）毫无用武之地。

## 2. 改造方案 (Proposed Changes)

### A. 修正默认分块尺寸 (app/pipeline/chunking.py)
- **修改内容**：将 `chunk_segments` 函数的默认 `chunk_size` 从 `4000` 调整为 `1200`，`overlap` 从 `400` 调整为 `200`。
- **原因**：1200 字符更符合业界 RAG 标准（约 600-800 Tokens），既能保证语义完整，又能让 GPU 推理速度呈指数级提升。

### B. 消除 Ingest 并发瓶颈 (app/pipeline/ingest_task.py)
- **修改内容**：重构 `_process_embeddings_concurrent`。一次性读取文档下的所有 chunk（13MB 文本仅产生约几十 MB 内存占用，完全不会 OOM）。将所有的批次（Batch）一次性提交给 `_embedding_executor`。
- **并发控制**：使用 `concurrent.futures.as_completed`，在任意一个线程完成 API 请求后，立刻将该批次的向量通过 `bulk_update_mappings` 写入 SQLite。这保证 16 个工作线程永远处于 100% 满载状态。
- **批次调优**：将 `EMBEDDING_BATCH_SIZE` 调整为 `32` 或 `64` 保持稳定。

### C. 引入 Rechunk 并发 (app/pipeline/rechunk.py)
- **修改内容**：重构 `_rebuild_embeddings`，引入与 Ingest 相同的 `ThreadPoolExecutor(max_workers=16)`。
- **原因**：使得用户在前端手动点击“重新分块并索引”时，也能享受到多线程并发带来的 4090 推理加速。

## 3. 预期效果 (Expected Outcome)
- **极速响应**：改用 1200 字符切片 + 满载 16 并发后，结合 4090 的算力，13MB 纯文本（约 10,000 个 Chunk）预计能在 1-2 分钟内彻底处理完毕。
- **平滑写入**：SQLite 将在后台通过流式写入（Streaming bulk updates），不会因为锁死导致长时间卡顿。

## 4. 验证步骤 (Verification Steps)
1. 运行 `pytest tests/ -v` 确保现有切片和 Ingest 单元测试通过。
2. 在 UI 界面上传一份 1MB 左右的文件，观察进度条中 Embedding 的耗时，预期时间从原本的 1 分钟缩短至 5-10 秒以内。