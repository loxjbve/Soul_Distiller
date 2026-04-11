# RAG 索引性能极限优化计划 (RAG Indexing Optimization Plan)

## 1. Summary (摘要)
用户希望将项目中的文档索引性能优化至极致（期望达到类似 NotebookLM 的极速处理速度），但在后续沟通中确认了**仍需保留切块（Chunking）与向量化（Embedding）架构**。本计划将通过彻底消除同步的 ORM 单行更新瓶颈、采用向量数据库的安全批量写入机制、以及增大默认的分块容量等手段，大幅提升整个 RAG 索引流水线的吞吐速度与稳定性。

## 2. Current State Analysis (现状分析)
在当前 `app/pipeline/ingest_task.py` 和 `app/pipeline/chunking.py` 索引架构中，主要存在以下性能与架构瓶颈：
1. **ORM 批量更新极慢**：`_process_embeddings_concurrent` 在拿到当前批次的 Embedding 后，通过 `update_session.scalars(...).all()` 将 TextChunk 实体载入内存，并逐一修改属性然后 `commit()`。这种处理方式比原生的 Bulk Update 慢了数十倍，成为关系型数据库写入的巨大阻塞点。
2. **向量库全量写入风险**：`_store_to_vector_db` 会将当前文档的所有有效分块（包含内容和 1024 维的浮点数组）一次性加载并执行 `store.add(...)`。在遇到超大文档（例如包含数万个 Chunk 的电子书）时，极其容易引发瞬时的内存溢出（OOM）或超过 FAISS 的单次计算极限。
3. **分块颗粒度过细**：`chunking.py` 默认的 `chunk_size` 为 1800 字符。在当前大语言模型动辄支持长文本能力的背景下，该设定导致分块数成倍增加，进而拖慢了切块、算 Embedding 以及写入的完整耗时。

## 3. Proposed Changes (拟议变更)

### 3.1 优化数据库更新机制 (ingest_task.py)
- **内容**: 改造 `_process_embeddings_concurrent` 中的写入逻辑，使用 `bulk_update_mappings` 替代原先逐行查询再修改的 ORM 逻辑。
- **原因**: 彻底避开 SQLAlchemy 的对象状态追踪开销，实现毫秒级的极速落盘。
- **方式**: 在 `as_completed` 获取到当前批次的 `chunk_id_to_vector` 后，组装一个包含 `[{"id": id_, "embedding_vector": vector, "embedding_model": resolved_model}, ...]` 的字典列表，传入 `update_session.bulk_update_mappings(TextChunk, mappings)` 中，一次性 Commit。

### 3.2 向量数据库分批安全写入 (ingest_task.py)
- **内容**: 重写 `_store_to_vector_db` 中的 `store.add()` 逻辑，加入 Chunk 分页机制。
- **原因**: 保护应用内存，防范巨型文档造成的处理崩溃。
- **方式**: 设定安全的 `batch_size = 1000`，利用 Python 切片对组装好的 `chunk_ids`, `vectors` 和 `payloads` 进行基于 For 循环的分批 `store.add()` 操作。

### 3.3 调优分块策略 (chunking.py & text.py)
- **内容**: 将 `chunk_segments` 默认的 `chunk_size` 从 `1800` 增大至 `4000`，`overlap` 从 `300` 调整至 `400`。
- **原因**: 贴合当前主流的上下文长度窗口，可以减少约 50% 的分块数量，直接成倍缩减 Embedding API 请求数及后端处理耗时。
- **方式**: 直接修改 `chunking.py` 的参数签名；对于密集调用的 `token_count`，如有性能瓶颈，可引入简易的快速估算函数替换原本的完整 Regex 拆词，提升切分效率。

## 4. Assumptions & Decisions (假设与决策)
- **坚持 RAG 架构**：根据用户的修正指令（“我需要切块”），不将其改为跳过向量化、全文档直接塞给模型的 Full-Context 模式，而是对现有的 RAG 流水线进行极限调优。
- **Bulk Update 的适配性**：由于我们仅需修改现存行的两个字段（`embedding_vector` 和 `embedding_model`），且不需要复杂的对象关联回调，使用 SQLAlchemy 的 `bulk_update_mappings` 是最理想且无副作用的决策。
- **Chunk Size 上限**：增大至 4000 个字符对应英文大约 1000 个 token，纯中文约 4000 个 token，均远低于常见模型（如 `text-embedding-3-small` 支持的 8191 tokens）的单次请求限制，兼顾了安全性与分析连贯性。

## 5. Verification (验证步骤)
1. 运行完整的测试套件（`pytest tests/`），确保 `bulk_update_mappings` 替换后，所有的 ORM 更新与后续检索提取依然符合预期且没有断言失败。
2. 上传一个大尺寸 PDF 或长篇 JSON，观察其在“索引中”阶段的用时（相比于未修改前）是否得到明显缩减。
3. 检查写入后分析引擎检索时的输出结果，确认较大的 `chunk_size` 未影响片段的语境召回质量。