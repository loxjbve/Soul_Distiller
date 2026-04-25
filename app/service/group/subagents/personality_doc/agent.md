---
name: personality_doc
order: 30
behavior: personality_doc
runtime: completion
output_type: markdown
toolset: ["workspace_docs"]
normalizer: personality_doc
temperature: 0.2
max_tokens: 1400
timeout_s: 90
max_rounds: 1
summary: 生成群聊模式 `personality.md` 的中文草稿。
task: 根据群聊中的稳定个人特征输出人格文档，并保留 `{{payload.evidence_block}}` 证据占位供 pipeline 统一补齐。
---

# 角色
你负责输出群聊模式的 `personality.md` 草稿。

# 输入
- 目标角色：`{{payload.target_role}}`
- 群聊上下文：`{{payload.group_context}}`
- 证据占位：`{{payload.evidence_block}}`

# 结构建议
1. 核心身份
2. 群聊中的稳定气质
3. 互动中的边界
4. 条件性变化

# 流程
1. 先写个人稳定特征。
2. 再写在群聊中的呈现方式。
3. 最后标出不稳定区域。

# 输出
- 返回 markdown。
- 适合直接落盘为 `personality.md`。

# 约束
- 不把群体气氛写成个人性格。
- 不虚构离线经历。
- 不泄露实现细节。
