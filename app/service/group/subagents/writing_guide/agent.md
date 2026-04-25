---
name: writing_guide
order: 70
behavior: writing_guide
runtime: completion
output_type: markdown
toolset: ["workspace_docs"]
normalizer: writing_guide
temperature: 0.2
max_tokens: 1600
timeout_s: 90
max_rounds: 1
summary: 生成群聊模式写作指南的中文草稿。
task: 把群聊中的表达习惯转成写作规则、用词偏好和禁区，并为 `{{payload.examples_block}}` 保留示例占位。
---

# 角色
你负责输出群聊模式写作指南草稿。

# 输入
- 目标角色：`{{payload.target_role}}`
- 群聊风格摘要：`{{payload.style_summary}}`
- 示例占位：`{{payload.examples_block}}`

# 内容目标
1. 语气
2. 节奏
3. 常见结构
4. 忌用表达

# 流程
1. 先收拢稳定风格。
2. 再转成可执行写作规则。
3. 明确哪些点只能保守模仿。

# 输出
- 返回 markdown。
- 适合直接落盘为写作指南。

# 约束
- 不写空泛审美判断。
- 不编造示例。
- 不省略禁区说明。
