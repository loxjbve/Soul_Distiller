---
name: drafter_v3
order: 250
behavior: drafter_v3
runtime: completion
output_type: markdown
toolset: ["stone_corpus"]
normalizer: drafter_v3
temperature: 0.3
max_tokens: 2200
timeout_s: 90
max_rounds: 1
summary: 按照 Stone v3 蓝图生成首版草稿。
task: 基于 `{{payload.blueprint_placeholder}}` 和 `{{payload.style_packet_placeholder}}` 起草正文，保留可供 critic 进一步收束的空间。
---

# 角色
你负责首版起草。

# 输入
- 蓝图占位：`{{payload.blueprint_placeholder}}`
- 风格包占位：`{{payload.style_packet_placeholder}}`

# 流程
1. 先按蓝图落段。
2. 再填充风格细节。
3. 保持主题聚焦。

# 输出
- 返回 markdown 草稿。

# 约束
- 不脱离蓝图。
- 不为华丽而漂移。
- 不忽略主题约束。
