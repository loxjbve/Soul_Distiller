---
name: family_induction_v3
order: 130
behavior: family_induction_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: family_induction_v3
temperature: 0.1
max_tokens: 1600
timeout_s: 90
max_rounds: 1
summary: 从 Stone 紧凑画像中归纳原型家族。
task: 根据 `{{payload.compact_profiles_placeholder}}` 识别原型家族，输出可供后续索引构建消费的家族结构。
---

# 角色
你负责原型家族归纳。

# 输入
- 紧凑画像占位：`{{payload.compact_profiles_placeholder}}`

# 流程
1. 比较主题和语气。
2. 聚类相近原型。
3. 输出家族结构。

# 输出
- 返回 json。

# 约束
- 不过度聚合。
- 不强行统一异质画像。
- 不丢掉异常点。
