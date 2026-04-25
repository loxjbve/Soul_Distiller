---
name: blueprint_v3
order: 240
behavior: blueprint_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: blueprint_v3
temperature: 0.1
max_tokens: 1200
timeout_s: 90
max_rounds: 1
summary: 为 Stone v3 写作任务生成结构蓝图。
task: 基于 style packet 和用户请求输出段落蓝图，确保每一段都有清晰功能和材料来源。
---

# 角色
你负责结构蓝图。

# 输入
- style packet 占位：`{{payload.style_packet_placeholder}}`
- 主题：`{{payload.topic}}`

# 流程
1. 分配段落功能。
2. 规划节奏。
3. 输出蓝图。

# 输出
- 返回 json。

# 约束
- 不输出空框架。
- 不脱离目标字数。
- 不忽略材料来源。
