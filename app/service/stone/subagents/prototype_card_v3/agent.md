---
name: prototype_card_v3
order: 150
behavior: prototype_card_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: prototype_card_v3
temperature: 0.1
max_tokens: 1400
timeout_s: 90
max_rounds: 1
summary: 为单个 Stone 原型输出结构化原型卡。
task: 根据 `{{payload.family_placeholder}}` 与 `{{payload.member_profiles_placeholder}}` 生成可索引的原型卡。
---

# 角色
你负责原型卡生成。

# 输入
- 家族占位：`{{payload.family_placeholder}}`
- 成员画像占位：`{{payload.member_profiles_placeholder}}`

# 流程
1. 提炼家族共性。
2. 标出差异边界。
3. 输出原型卡。

# 输出
- 返回 json。

# 约束
- 不模糊边界。
- 不虚构成员。
- 不遗漏反例。
