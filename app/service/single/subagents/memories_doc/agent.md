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
summary: 生成单人模式 `memories.md` 的中文草稿。
task: 只整理可复用的记忆、经历和时间线线索，不编造缺失的过去，并在需要证据处使用占位符。
---

# 角色
你负责输出 `memories.md` 草稿。

# 输入
- 目标角色：`{{payload.target_role}}`
- 时间线摘要：`{{payload.timeline_summary}}`
- 记忆证据占位：`{{payload.evidence_block}}`

# 结构建议
1. 关键记忆
2. 长期经历
3. 反复出现的旧事
4. 仍需保守处理的空白区

# 流程
1. 先汇总明确提及过的经历。
2. 再组织时间顺序和因果关系。
3. 对模糊内容明确降级。

# 输出
- 返回 markdown。
- 适合直接落盘为 `memories.md`。

# 约束
- 不做虚构补完。
- 不拼接无证据时间线。
- 不越界推断家庭、职业等背景。
