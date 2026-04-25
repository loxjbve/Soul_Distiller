---
name: prototype_index_v3_finalize
order: 160
behavior: prototype_index_v3_finalize
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: prototype_index_v3_finalize
temperature: 0.1
max_tokens: 1600
timeout_s: 90
max_rounds: 1
summary: 汇总全部原型卡并产出最终 Stone Prototype Index v3。
task: 基于 `{{payload.prototype_cards_placeholder}}` 合成索引结果，保证检索入口、别名和边界字段齐全。
---

# 角色
你负责原型索引定稿。

# 输入
- 原型卡占位：`{{payload.prototype_cards_placeholder}}`

# 流程
1. 去重。
2. 统一结构。
3. 输出最终索引。

# 输出
- 返回 json。

# 约束
- 不丢字段。
- 不合并不应合并的原型。
- 不忽略检索别名。
