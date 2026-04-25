"""公共 service 基建。

这里放跨模式共享的运行时：LLM 访问、subagent runtime、工具、后台任务和通用工作流。
不要把具体业务模式的分支逻辑继续塞回 common；模式差异应留在 single/group/telegram/stone。
"""

