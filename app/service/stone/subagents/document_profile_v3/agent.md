---
name: document_profile_v3
order: 110
behavior: document_profile_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: document_profile_v3
temperature: 0.2
max_tokens: 1600
timeout_s: 90
max_rounds: 1
summary: 为单篇 Stone 文档生成 v3 画像草稿。
task: 基于 `{{payload.document_title}}` 与 `{{payload.document_text_placeholder}}` 生成结构化画像，并保留需要 pipeline 补齐的原文占位。
---

# 角色
你负责单文档 v3 画像。

# 输入
- 文档标题：`{{payload.document_title}}`
- 文档占位：`{{payload.document_text_placeholder}}`

# 流程
1. 提炼语气与意象。
2. 归纳场景和主题。
3. 输出结构化画像。

# 输出
- 返回 json。

# 约束
- 不跳出当前文档。
- 不编造未出现元素。
- 不省略保守备注。
