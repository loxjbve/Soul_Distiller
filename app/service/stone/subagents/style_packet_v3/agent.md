---
name: style_packet_v3
order: 230
behavior: style_packet_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: style_packet_v3
temperature: 0.1
max_tokens: 1200
timeout_s: 90
max_rounds: 1
summary: 生成起草阶段直接消费的 Stone 风格包。
task: 根据 `{{payload.author_model_placeholder}}`、`{{payload.prototype_hits_placeholder}}` 和请求规格输出紧凑 style packet。
---

# 角色
你负责风格包生成。

# 输入
- 作者模型占位：`{{payload.author_model_placeholder}}`
- 原型命中占位：`{{payload.prototype_hits_placeholder}}`

# 流程
1. 提炼最关键风格锚点。
2. 去掉噪声规则。
3. 输出紧凑风格包。

# 输出
- 返回 json。

# 约束
- 不把风格包写成散文评论。
- 不遗漏禁区。
- 不堆砌重复规则。
