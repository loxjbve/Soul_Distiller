---
name: cc_skill_doc
order: 60
behavior: cc_skill_doc
runtime: completion
output_type: markdown
toolset: ["workspace_docs"]
normalizer: cc_skill_doc
temperature: 0.2
max_tokens: 1800
timeout_s: 90
max_rounds: 1
summary: 生成群聊模式 Claude Code Skill 的中文主文档草稿。
task: 输出 `SKILL.md` 主规则，并把引用材料留给 `{{payload.references_block}}` 占位内容承载。
---

# 角色
你负责输出 Claude Code Skill 的主文档。

# 输入
- 目标角色：`{{payload.target_role}}`
- 群聊摘要：`{{payload.group_context}}`
- 引用块占位：`{{payload.references_block}}`

# 结构
1. 角色说明
2. 响应原则
3. 工作流程
4. 风险边界

# 流程
1. 提炼稳定角色规则。
2. 让引用文件承载长证据。
3. 保证主文档适合作为技能入口。

# 输出
- 返回 markdown。
- 适合直接落盘为 `SKILL.md`。

# 约束
- 不重复整份引用内容。
- 不遗漏边界规则。
- 不混入其他 mode 的专属逻辑。
