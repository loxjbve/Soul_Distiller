# Claude Code 兼容 Skill 输出模块 Spec

## Why
当前系统的「Skill 输出」面向通用系统 Prompt 文档（Skill.md / Skill_merge.md），不满足 Claude Code 对自定义 Skill 的 SKILL.md 标准格式要求，导致无法直接放入 `.claude/skills/<skill>/SKILL.md` 使用。

## What Changes
- 新增第三种资产输出：`cc_skill`（Claude Code 兼容 Skill）
- `cc_skill` 生成结果改为 Claude Code Skill 目录所需的文件与格式：
  - `SKILL.md`：以 YAML frontmatter 开头，至少包含 `name` 与 `description`，正文为可执行的扮演规则与工作流
  - `references/personality.md`（可选但默认生成）：核心身份与精神底色
  - `references/memories.md`（可选但默认生成）：核心记忆与经历
- `SKILL.md` 正文通过相对路径引用 `references/personality.md` / `references/memories.md`，以支持“按需加载”的使用方式
- 生成流程参考现有 `skill` 输出：允许多次调用 LLM（至少 3 次：personality / memories / SKILL）
- 为 `cc_skill` 增加稳健的 `name` 生成与校验策略，确保即使项目名/角色名为中文也能产出符合约束的 `name`
- Web UI / API / 资产落盘逻辑支持选择并生成 `cc_skill`

## Impact
- Affected specs: 资产合成（AssetSynthesizer）、Prompt 编排（prompts）、资产种类枚举（schemas）、Web routes 与 UI 文案、资产文件落盘命名
- Affected code:
  - [prompts.py](file:///workspace/app/analysis/prompts.py)
  - [synthesizer.py](file:///workspace/app/analysis/synthesizer.py)
  - [schemas.py](file:///workspace/app/schemas.py)
  - [routes.py](file:///workspace/app/web/routes.py)
  - 相关模板与 UI 文案（`app/templates/*`, `app/web/ui_strings.py`）

## ADDED Requirements
### Requirement: Claude Code Skill 资产类型
系统 SHALL 支持一种新的资产类型 `cc_skill`，用于输出 Claude Code 可直接安装的 Skill 文件集合。

#### Scenario: 生成 cc_skill 成功
- **WHEN** 用户在资产生成入口选择 `cc_skill` 并触发生成
- **THEN** 系统返回并落盘以下文件内容：
  - `SKILL.md`（必需）
  - `references/personality.md`（默认生成）
  - `references/memories.md`（默认生成）
- **AND** 资产草稿在列表/详情页可被查看、保存、再次生成

### Requirement: SKILL.md 标准格式兼容
系统 SHALL 生成符合 Claude Code Skill 规范的 `SKILL.md`：
- 必须以 YAML frontmatter 开头，并且 frontmatter 至少包含：
  - `name`: 全小写、仅包含字母数字与短横线（kebab-case），长度 ≤ 64，且不包含保留词（如 “claude”“anthropic”）
  - `description`: 非空，描述“做什么”与“什么时候用”
- frontmatter 与正文之间必须以 `---` 结束分隔

#### Scenario: 前置元数据满足约束
- **WHEN** 项目名/目标角色包含中文或特殊字符
- **THEN** 系统仍能生成稳定且合法的 `name`（例如基于 `project_id` 的可重复 slug），保证 Claude Code 可识别

### Requirement: CC 兼容 Skill 的正文结构与引用
系统 SHALL 在 `SKILL.md` 正文中提供可执行的扮演规则与输出规范，并通过相对路径引用补充文档：
- 在正文中明确：
  - 角色扮演规则（可执行约束）
  - 回答工作流（SOP）
  - 高置信领域与诚实边界（越界处理）
- 在正文中提供“按需阅读”入口：
  - `references/personality.md`
  - `references/memories.md`

#### Scenario: Claude Code 中按需加载
- **WHEN** 用户在 Claude Code 中安装该 Skill 并触发使用
- **THEN** Claude 可先加载 `SKILL.md` 决策是否进一步读取 `references/personality.md` / `references/memories.md`（通过引用引导）

### Requirement: 允许多次调用 LLM（流程对齐 skill 输出）
系统 SHALL 允许 `cc_skill` 的生成过程中进行多次 LLM 调用，并沿用现有 `skill` 的检索增强生成方式：
- personality：结合十维摘要 + 检索片段生成 `personality.md`
- memories：结合十维摘要 + 检索片段生成 `memories.md`
- SKILL：结合十维摘要（并可参考 personality/memories 的摘要要点）生成 `SKILL.md`

#### Scenario: 无检索能力时降级
- **WHEN** 当前会话未提供检索服务或 embedding 配置不可用
- **THEN** `cc_skill` 仍可仅基于十维摘要生成三个文件（或至少生成 `SKILL.md`），并在内容中保持保守、不脑补

## MODIFIED Requirements
### Requirement: 现有 skill 与 profile_report 输出保持不变
系统 SHALL 保持现有 `skill` 与 `profile_report` 的输出格式、文件命名与渲染逻辑不受 `cc_skill` 增量影响。

## REMOVED Requirements
无

