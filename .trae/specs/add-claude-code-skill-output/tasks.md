# Tasks
- [x] Task 1: 增加新的资产类型 cc_skill
  - [x] SubTask 1.1: 扩展资产类型枚举与标签（`ASSET_KINDS`、UI label、normalize 逻辑）
  - [x] SubTask 1.2: 更新资产保存/落盘逻辑，支持为 cc_skill 写入 `SKILL.md` / `references/personality.md` / `references/memories.md`

- [x] Task 2: 设计并实现 cc_skill 的 Prompt 编排
  - [x] SubTask 2.1: 增加 cc_skill 的 prompt 生成函数（system/user messages），确保生成 `SKILL.md` YAML frontmatter + 正文
  - [x] SubTask 2.2: 复用或轻量调整 personality/memories 的生成 prompt，使其适合作为 Claude Code Skill 的附属文档
  - [x] SubTask 2.3: 定义 `name` 生成策略（kebab-case + 中文兜底 slug），并在 prompt 中明确约束与自检步骤

- [x] Task 3: 在 AssetSynthesizer 中加入 cc_skill 的多轮生成流程
  - [x] SubTask 3.1: 参考现有 `skill` 分支，实现 `cc_skill` 的 LLM 多次调用（personality → memories → SKILL）
  - [x] SubTask 3.2: 规范化 cc_skill payload 结构（documents 映射、文件名、merge 策略是否需要）
  - [x] SubTask 3.3: 确保在无检索/无 embedding 时能降级生成（至少 SKILL.md 合规）

- [x] Task 4: Web UI / API 支持 cc_skill
  - [x] SubTask 4.1: 资产生成入口支持选择/切换 `cc_skill`（路由、表单、显示名称）
  - [x] SubTask 4.2: 资产详情页展示 cc_skill 的多文件内容（至少能查看 SKILL.md）

- [x] Task 5: 测试与验证
  - [x] SubTask 5.1: 增加/更新单测：cc_skill 被识别为合法资产类型，且不会影响现有 skill/profile_report
  - [x] SubTask 5.2: 增加/更新单测：`SKILL.md` frontmatter 含 `name` / `description`，并满足字符约束与兜底策略
  - [x] SubTask 5.3: 端到端测试（若已有 web 测试覆盖）：生成 cc_skill draft 并能保存

# Task Dependencies
- Task 3 depends on Task 2
- Task 4 depends on Task 1
- Task 5 depends on Task 1, Task 3, Task 4
