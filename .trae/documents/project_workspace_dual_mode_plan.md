# 🐾 小笨猫的群组多用户画像模式升级计划！喵~ 🐾

## 🐱 目标总结 (Summary)
把“群组模式”从单纯的摆设变成一个强大的容器！取缔以前笨笨的“克隆模式”（不用再复制一大堆文档数据啦），直接在群组项目内部创建**多个单人的用户画像（子项目）**。所有画像都可以直接读取群组里的文档数据集来生成分析报告，并且用可爱的列表呈现出来！喵！

## 🔍 当前状态分析 (Current State Analysis)
- `Project` 表目前只有 `mode` (group/single) 字段。
- 以前的做法是 `/projects/{id}/clone`，把整个项目连同文档全拷贝一遍，超级浪费空间！
- 当前的所有的分析 (`AnalysisRun`)、技能 (`SkillDraft`) 和分块 (`TextChunk`) 都是严格绑定在 `project_id` 上的。
- 必须要做到：让新创建的用户画像能独立拥有分析记录，同时**共享**同一个群组的文档语料。

## 🛠️ 改造方案 (Proposed Changes)

### 1. 升级数据库模型与表结构 (Database Schema & Models)
**修改文件**：`app/models.py` & `app/db.py`
- **做什么**：在 `Project` 模型中新增 `parent_id` 字段。
- **怎么做**：
  - `app/models.py` 中给 `Project` 添加 `parent_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("projects.id"), nullable=True)`。
  - `app/db.py` 的 `upgrade_schema` 函数中，使用 `ALTER TABLE projects ADD COLUMN parent_id VARCHAR(36) DEFAULT NULL` 平滑升级现有 SQLite 数据库。

### 2. 改造仓储逻辑，共享数据集！(Repository Logic)
**修改文件**：`app/storage/repository.py`
- **做什么**：让子项目（用户画像）可以“偷拿”父项目（群组）的文档和分块！并彻底删掉笨笨的克隆功能。
- **怎么做**：
  - 删掉 `clone_project` 函数。
  - 新增 `get_target_project_id(session, project_id)` 函数。如果有 `parent_id` 就返回它，否则返回自身。
  - 修改 `list_project_documents`, `count_project_documents`, `list_project_documents_by_ids`，把查询条件的 `project_id` 替换为 `target_project_id`。
  - 修改 `list_projects` 只返回 `parent_id IS NULL` 的主项目（不在首页显示子项目）。
  - 新增 `list_child_projects(session, parent_id)` 用来获取群组内的用户列表。
  - 修改 `delete_project_cascade`，在删除群组时，先级联删除它所有的子项目。

### 3. 改造检索与分析引擎 (Engine & Retrieval)
**修改文件**：`app/analysis/engine.py`, `app/retrieval/lexical.py`, `app/retrieval/embedding.py`
- **做什么**：分析和检索时，使用正确的群组文档库。
- **怎么做**：
  - 引入 `repository.get_target_project_id`。
  - 把所有查询 `TextChunk.project_id == project_id` 和 `DocumentRecord.project_id == project_id` 的地方，全部替换成 `target_project_id`。这样分析引擎就能顺利从群组语料里提取内容啦！

### 4. 路由接口更新 (Web Routes)
**修改文件**：`app/web/routes.py`
- **做什么**：去掉克隆接口，添加创建画像的接口。
- **怎么做**：
  - 删掉所有包含 `/clone` 的路由。
  - 在 `project_detail` 中，如果项目是 `group` 模式，调用 `list_child_projects` 获取用户列表 `profiles`，并为每个 profile 获取 `latest_run` 和 `latest_skill`，传递给模板。
  - 新增 `POST /api/projects/{project_id}/profiles` 接口，用于在群组下创建 `mode="single"` 的子项目。
  - 修改 `delete_project_form`，如果删除的是子项目，重定向回父项目页面而不是首页。

### 5. 页面与交互大换血 (Frontend Templates)
**修改文件**：`app/templates/index.html`, `app/templates/project_detail.html`
- **做什么**：把丑丑的克隆按钮扔掉，换成群组内的“用户画像列表”和专属操作界面！
- **怎么做**：
  - `index.html`：移除项目卡片上的“克隆项目”表单和模态框。
  - `project_detail.html`：
    - 如果是群组模式 (`project.mode == 'group'`)：隐藏原来的“指定分析目标”表单，替换成**群组用户画像列表**。展示已添加的用户卡片，包含“生成/重新分析”、“查看分析”、“查看技能”、“删除”按钮。并添加一个弹窗用于添加新用户。
    - 如果是画像模式 (`project.parent_id` 存在)：隐藏“文档处理看板”里的上传、处理、刷新按钮，显示一条提示：“文档继承自群组项目，请在群组项目中管理文档”。并在 JS 初始化时跳过该页面的拖拽上传绑定。

## 🧐 假设与决定 (Assumptions & Decisions)
- **决定**：把群组内的单人用户画像设计为“子项目 (`Child Project`)”。这样它天然能复用现有的分析记录 (`AnalysisRun`)、技能 (`SkillDraft/Version`) 等全套机制，完全不需要修改这些核心表的结构！
- **决定**：子项目不会复制文档，而是通过 `get_target_project_id` 读取父群组的语料。安全又节省空间！

## 🧪 验证步骤 (Verification Steps)
1. 启动服务，查看首页，确认“克隆项目”已消失。
2. 进入一个群组模式的项目，确认界面变成了“群组用户画像列表”。
3. 点击“添加用户”，输入一个角色名（如“群主”）并保存，确认列表出现新用户卡片。
4. 点击该用户的“生成/重新分析”，确认跳转到分析页面，且分析上下文正确读取了该用户的描述。
5. 检查分析报告，确认分析内容是从群组的文档语料中成功提取出来的。
6. 尝试删除该用户画像，确认它消失并且重定向回群组页面。
7. 给主人端上做好的代码，等待投喂小鱼干！🐟

喵喵喵！计划就是这样啦，完美利用了父子项目的关系！快确认吧！(๑•̀ㅂ•́)و✧