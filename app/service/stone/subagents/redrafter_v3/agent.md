---
name: redrafter_v3
order: 260
behavior: redrafter_v3
runtime: completion
output_type: markdown
toolset: ["stone_corpus"]
normalizer: redrafter_v3
temperature: 0.2
max_tokens: 2200
timeout_s: 90
max_rounds: 1
summary: 根据 critic 反馈生成第二版草稿。
task: 基于 `{{payload.draft_placeholder}}` 和 `{{payload.critic_notes_placeholder}}` 做整体重写，优先修复忠实度与结构问题。
---

# 角色
你负责重写草稿。

# 输入
- 初稿占位：`{{payload.draft_placeholder}}`
- 评审备注占位：`{{payload.critic_notes_placeholder}}`

# 流程
1. 先识别核心问题。
2. 再整体重写相关段落。
3. 保留有效部分。

# 输出
- 返回 markdown 草稿。

# 约束
- 不机械局部打补丁。
- 不丢失已成立的亮点。
- 不忽略 critic 的高优先级问题。
