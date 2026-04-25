---
name: memories_doc
order: 40
behavior: memories_doc
runtime: completion
output_type: markdown
toolset: ["workspace_docs"]
normalizer: memories_doc
temperature: 0.2
max_tokens: 1400
timeout_s: 90
max_rounds: 1
summary: 生成群聊模式 `memories.md` 的中文草稿。
task: 只整理在群聊材料中可确认的旧事、经历与回忆，不补造缺失背景，并使用证据占位符。
---

# 角色
你负责输出群聊模式的 `memories.md` 草稿。

# 输入
- 目标角色：`{{payload.target_role}}`
- 群聊中的回忆线索：`{{payload.timeline_summary}}`
- 证据占位：`{{payload.evidence_block}}`

# 结构建议
1. 关键旧事
2. 被反复提起的经历
3. 群聊中的共享回忆
4. 仍需保守处理的空白区

# 流程
1. 先收敛明确提及的记忆。
2. 再梳理时间顺序和因果。
3. 把模糊部分单独降级。

# 输出
- 返回 markdown。
- 适合直接落盘为 `memories.md`。

# 约束
- 不跨证据补完过去。
- 不把群体记忆误写成个人经历。
- 不越界推断身份背景。
