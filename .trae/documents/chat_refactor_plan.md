# 试聊功能改造计划 (Chat UI and Logic Refactoring)

## 1. 当前状态分析 (Current State Analysis)
- `app/web/routes.py` 中的 `_chat_with_persona` 函数目前会调用 `request.app.state.retrieval.search` 进行基于 Embedding 和 BM25 的检索，将召回的结果拼接为 `evidence_block`（证据块），然后传递给 `_generate_chat_reply`。
- `_generate_chat_reply` 将证据块作为额外的 System Prompt 喂给 LLM。
- `app/templates/playground.html` 的前端界面只是一个基础的表单列表，显示对话记录和详细的检索证据跟踪（Trace JSON），视觉上不够美观，缺乏沉浸式聊天体验。

## 2. 改造目标 (Proposed Changes)

### 2.1 后端逻辑精简：移除 Embedding 检索
**文件**: `app/web/routes.py`
- **What**: 移除试聊过程中的文档检索逻辑。
- **Why**: 用户要求试聊时“完全让 llm 扮演 skill.md 里面的角色，不再需要 embedding 检索”。
- **How**: 
  - 在 `_chat_with_persona` 函数中，删除调用 `request.app.state.retrieval.search` 的相关代码。
  - 删除 `evidence_block` 的生成逻辑。
  - 将生成的 `trace` 字典精简，移除 `retrieval_mode`、`retrieval_trace` 和 `evidence` 字段。
  - 在调用 `_generate_chat_reply` 时，将 `evidence_block` 参数传入空字符串 `""`。
  - 修改 `_generate_chat_reply` 中的 Fallback 降级提示，去掉关于“检索提示”的内容。

### 2.2 前端界面重构：优化聊天对话框
**文件**: `app/templates/playground.html`
- **What**: 将基础列表渲染改写为沉浸式的聊天气泡界面。
- **Why**: 用户要求“把对话框做好看一点”。
- **How**:
  - 在 HTML 模板内部添加一段专属的 `<style>`，定义 `.chat-container`, `.chat-message`, `.message-user`, `.message-assistant` 等现代聊天 UI 样式。
  - 使用 Flexbox 布局：助手气泡靠左（带头像标识），用户气泡靠右，加上圆角和阴影。
  - 移除模板中遍历 `turn.trace_json.evidence` 显示检索片段的代码。
  - 调整底部输入框为悬浮吸底（或固定在容器底部）的布局。
  - 添加一小段 `<script>`，实现在页面加载后自动滚动到聊天记录最底部。

## 3. 假设与决策 (Assumptions & Decisions)
- 假设: 用户希望试聊页面保持传统的服务器端渲染并提交表单刷新的交互方式，仅仅是提升视觉上的“聊天框”体验，而非彻底改写为复杂的 WebSocket/AJAX 异步交互。为了稳定性和最小改动，我们将使用 CSS 和简单的滚动 JS 来美化界面，维持原本的 form submit 逻辑。
- 决策: 移除检索后，LLM 仅依靠发布时定型的 System Prompt (`version.system_prompt`) 进行角色扮演，这符合用户的 "完全扮演 skill.md 里面的角色" 的意图。

## 4. 验证步骤 (Verification Steps)
- 检查 `app/web/routes.py` 是否有语法错误。
- 运行应用，进入“试聊 (Playground)”页面。
- 发送测试消息，验证后台是否不再执行检索并成功返回符合人设的回复。
- 验证界面是否已变为左右对话气泡的形式，且自动滚动到底部。