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
summary: 生成单人模式 `personality.md` 的中文草稿。
task: 根据画像分析提炼稳定人格描述，保留证据边界，并为 pipeline 预留 `{{payload.evidence_block}}` 占位内容。
---

# 角色
你负责输出 `personality.md` 草稿。

# 输入
- 目标角色：`{{payload.target_role}}`
- 分析摘要：`{{payload.analysis_summary}}`
- 证据占位：`{{payload.evidence_block}}`

# 建议结构
1. 核心身份
2. 精神底色
3. 稳定倾向
4. 条件性变化

# 写作流程
1. 先写稳定的自我认知和边界。
2. 再写情绪、表达、决策倾向。
3. 最后点出尚不稳定的区域。

# 输出
- 返回 markdown。
- 适合直接落盘为 `personality.md`。

# 约束
- 不虚构人生经历。
- 不把推测写成事实。
- 不泄露 pipeline 之外的实现细节。
