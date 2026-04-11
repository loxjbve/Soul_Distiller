# 项目工作区双模式升级计划 (Dual-Mode Workspace Plan)

## 1. 概述 (Summary)
本次升级旨在将项目工作区设计为“双模式”：支持“基于群聊 (Group)”和“基于单人 (Single)”两种分析模式。同时增加修改项目名称、描述的功能。现有项目将默认平滑迁移为群聊模式。在群聊模式下，系统的提示文案将引导用户描述群聊氛围而非单个人物。

## 2. 现状分析 (Current State Analysis)
- `app/models.py` 中的 `Project` 模型当前仅包含 `name` 和 `description`，无法区分项目模式。
- `app/db.py` 使用 SQLite，存在自动迁移架构 `upgrade_schema` 但未包含 `Project` 的模式字段。
- `app/web/routes.py` 缺少更新项目的路由 (如 `POST /projects/{project_id}/update`)。
- 前端 `index.html` 和 `project_detail.html` 没有工作区模式的选择入口，也没有编辑现有项目信息的入口。此外，目标角色输入框统一要求输入具体的人物，未针对群聊做适配。

## 3. 改动计划 (Proposed Changes)

### 3.1 数据库与模型层 (Database & Models)
- **`app/models.py`**：
  - 在 `Project` 模型中新增 `mode: Mapped[str] = mapped_column(String(32), default="group")` 字段。
- **`app/db.py`**：
  - 在 `upgrade_schema` 中添加自动升级逻辑：检查 `projects` 表是否包含 `mode` 列，若无则执行 `ALTER TABLE projects ADD COLUMN mode VARCHAR(32) DEFAULT 'group'`，并将现有的空值全部更新为 `'group'`。

### 3.2 仓储层与路由层 (Repository & Routes)
- **`app/storage/repository.py`**：
  - 更新 `create_project` 签名，接收 `mode` 参数并存入数据库。
  - 更新 `clone_project` 逻辑，克隆时同步复制 `original_project.mode`。
- **`app/web/routes.py`**：
  - 扩展 `ProjectCreatePayload` 模型，增加 `mode: str = "group"`。
  - 修改 `create_project_form` 和 `create_project_api` 路由，支持接收并传递 `mode` 参数。
  - **新增路由** `POST /projects/{project_id}/update` (对应 `update_project_form` 方法)，接受 `name`、`description` 和 `mode` 参数，保存修改后重定向回项目详情页。

### 3.3 前端视图层 (Frontend Templates)
- **`app/templates/index.html`**：
  - 在“创建人物项目”表单中加入下拉选择框（Select），允许用户在“基于群聊”和“基于单人”之间选择。
- **`app/templates/project_detail.html`**：
  - 在项目头部（`.project-hero-actions`）增加一个“编辑项目”按钮。
  - 增加一个模态框（Modal）包含编辑表单，支持提交项目的新名称、说明和模式到 `/projects/{{ project.id }}/update`。
  - 动态渲染“指定分析目标”区域的文案：
    - 若 `project.mode == 'group'`，Label 显示为“群聊名称或氛围描述”，Placeholder 提示如“例如：一个活跃的吹水群、技术交流群等”。
    - 若 `project.mode == 'single'`，Label 保持为“要分析和模拟的角色”，Placeholder 提示如“例如：作者本人、群聊管理员、小红本人”。

## 4. 假设与决定 (Assumptions & Decisions)
- 历史数据迁移安全：利用 SQLite 的 `ALTER TABLE` 以及 `UPDATE` 语句在服务启动时自动兼容，历史数据无缝升级，不需要额外的独立迁移脚本。
- UI/UX 决策：“编辑项目”功能采用当前风格已有的 Modal 组件形式进行展示（与 `doc-modal` 类似），不破坏现有的页面布局。
- 只有两个固定的模式：`group`（群聊）和 `single`（单人），以简化逻辑，并在后续扩展中留有余地。

## 5. 验证步骤 (Verification Steps)
1. 启动应用，检查控制台无数据库报错，且已有的项目默认表现为“基于群聊”。
2. 访问首页，通过表单创建一个新的“基于单人”模式的项目，确保能成功创建并跳转。
3. 在项目详情页点击“编辑项目”，修改项目名称和描述以及模式，点击保存并观察页面刷新后的信息是否正确更新。
4. 观察“指定分析目标”模块的标题和占位符，确认在不同模式下会切换对应的提示文本。