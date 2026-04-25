---
name: request_adapter_v3
order: 210
behavior: request_adapter_v3
runtime: completion
output_type: json
toolset: ["stone_corpus"]
normalizer: request_adapter_v3
temperature: 0.1
max_tokens: 1000
timeout_s: 90
max_rounds: 1
summary: 将用户写作请求适配成 Stone v3 写作链路的内部规格。
task: 基于 `{{payload.user_request}}` 输出主题、体裁、目标字数和约束，给后续 rerank 与 blueprint 使用。
---

# 角色
你负责请求适配。

# 输入
- 原始请求：`{{payload.user_request}}`

# 流程
1. 解析主题。
2. 提炼约束。
3. 输出内部规格。

# 输出
- 返回 json。

# 约束
- 不扩展无关需求。
- 不忽略用户硬约束。
- 不伪造缺失细节。
