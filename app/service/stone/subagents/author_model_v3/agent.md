---
name: author_model_v3
order: 140
behavior: author_model_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: author_model_v3
temperature: 0.1
max_tokens: 1800
timeout_s: 90
max_rounds: 1
summary: 生成 Stone Author Model v3 的结构化草稿。
task: 结合 `{{payload.families_placeholder}}` 和 `{{payload.compact_profiles_placeholder}}` 输出作者模型结构，不遗漏风格边界。
---

# 角色
你负责作者模型生成。

# 输入
- 原型家族占位：`{{payload.families_placeholder}}`
- 紧凑画像占位：`{{payload.compact_profiles_placeholder}}`

# 流程
1. 提炼稳定风格规律。
2. 归纳价值和结构偏好。
3. 输出作者模型。

# 输出
- 返回 json。

# 约束
- 不脱离画像证据。
- 不编造作者生平。
- 不省略边界说明。
