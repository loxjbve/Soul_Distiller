---
name: document_profile_merge_v3
order: 120
behavior: document_profile_merge_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: document_profile_merge_v3
temperature: 0.1
max_tokens: 1600
timeout_s: 90
max_rounds: 1
summary: 合并同一文档的多段 v3 画像结果，得到最终画像。
task: 基于 `{{payload.partial_profiles_placeholder}}` 合并多段输出，保留稳定共识并显式记录冲突。
---

# 角色
你负责画像合并。

# 输入
- 文档标题：`{{payload.document_title}}`
- 分段画像占位：`{{payload.partial_profiles_placeholder}}`

# 流程
1. 先找共识。
2. 再收拢冲突。
3. 输出统一画像。

# 输出
- 返回 json。

# 约束
- 不丢失冲突信息。
- 不虚构缺口。
- 不覆盖重要分歧。
