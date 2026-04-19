import json
import re

with open("app/web/ui_strings.py", "r") as f:
    ui_content = f.read()

# Add telegram_preprocess and skill dictionaries to LOCALES
zh_addition = """
            "telegram_preprocess": {
                "title": "Telegram 预处理",
                "hero_title": "Telegram 预处理指挥台",
                "hero_note": "导入结构化 JSON 后，这里会实时显示 SQL 物化、周总结并发、话题沉淀与中间产物状态。",
                "concurrency_label": "周总结并发",
                "btn_resume": "继续预处理",
                "btn_start": "开始预处理",
                "stage_waiting": "等待开始",
                "live_note": "实时监听 SQL 物化、周总结和最终话题写入的进度。",
                "status_waiting": "等待任务",
                "overall_progress": "总体进度",
                "coverage_progress": "周总结覆盖率",
                "current_run": "当前 Run",
                "stage": "阶段",
                "total_progress": "总进度",
                "weekly_candidates": "周候选数",
                "final_topics": "最终话题数",
                "request_concurrency": "请求并发",
                "active_workers": "活跃 Worker",
                "completed_weeks": "已完成周数",
                "remaining_weeks": "剩余周数",
                "no_run": "暂无预处理 run",
                "worker_lanes": "并发轨道",
                "no_workers": "尚无活跃 worker。",
                "trace_timeline": "Subagent 时间线",
                "waiting_events": "等待任务事件。",
                "live_output": "当前 LLM 输出",
                "waiting_llm": "等待新的 LLM 请求开始。",
                "empty_weekly": "当前 Run 还没有周候选结果。",
                "empty_top": "当前 Run 还没有 Top Users 结果。",
                "empty_active": "当前 Run 还没有活跃用户结果。",
                "topic_table": "周话题表",
                "empty_topics": "当前 Run 还没有最终话题结果。",
                "history_run": "历史 Run",
                "empty_history": "暂无历史预处理记录。"
            },
            "skill": {
                "preview": "Skill 预览",
                "draft_title": "的 Skill 草稿",
                "hero_note": "从最新分析生成草稿，编辑后再发布为正式版本。",
                "regenerate": "重新生成草稿",
                "back_to_analysis": "返回分析",
                "draft_eyebrow": "草稿",
                "edit_draft": "编辑草稿",
                "review_notes": "审核备注",
                "save_draft": "保存草稿",
                "publish_version": "发布版本",
                "no_draft": "当前没有草稿。先等分析完成，再生成 Skill。",
                "version_history": "版本记录",
                "published_versions": "已发布版本",
                "published": "已发布",
                "no_versions": "暂无已发布版本。"
            },
"""

en_addition = """
            "telegram_preprocess": {
                "title": "Telegram Preprocess",
                "hero_title": "Telegram Preprocess Console",
                "hero_note": "After importing structured JSON, real-time SQL materialization, weekly summary concurrency, topic precipitation, and intermediate artifact status will be displayed here.",
                "concurrency_label": "Weekly Summary Concurrency",
                "btn_resume": "Resume Preprocess",
                "btn_start": "Start Preprocess",
                "stage_waiting": "Waiting to start",
                "live_note": "Real-time monitoring of SQL materialization, weekly summaries, and final topic writing progress.",
                "status_waiting": "Waiting for tasks",
                "overall_progress": "Overall Progress",
                "coverage_progress": "Weekly Summary Coverage",
                "current_run": "Current Run",
                "stage": "Stage",
                "total_progress": "Total Progress",
                "weekly_candidates": "Weekly Candidates",
                "final_topics": "Final Topics",
                "request_concurrency": "Request Concurrency",
                "active_workers": "Active Workers",
                "completed_weeks": "Completed Weeks",
                "remaining_weeks": "Remaining Weeks",
                "no_run": "No preprocess run yet",
                "worker_lanes": "Worker Lanes",
                "no_workers": "No active workers yet.",
                "trace_timeline": "Subagent Timeline",
                "waiting_events": "Waiting for task events.",
                "live_output": "Current LLM Output",
                "waiting_llm": "Waiting for new LLM request to start.",
                "empty_weekly": "Current Run has no weekly candidate results yet.",
                "empty_top": "Current Run has no Top Users results yet.",
                "empty_active": "Current Run has no active users results yet.",
                "topic_table": "Weekly Topics Table",
                "empty_topics": "Current Run has no final topic results yet.",
                "history_run": "History Runs",
                "empty_history": "No historical preprocess records yet."
            },
            "skill": {
                "preview": "Skill Preview",
                "draft_title": "Skill Draft",
                "hero_note": "Generate draft from latest analysis, edit and publish as a formal version.",
                "regenerate": "Regenerate Draft",
                "back_to_analysis": "Back to Analysis",
                "draft_eyebrow": "Draft",
                "edit_draft": "Edit Draft",
                "review_notes": "Review Notes",
                "save_draft": "Save Draft",
                "publish_version": "Publish Version",
                "no_draft": "No draft available. Wait for analysis to complete before generating Skill.",
                "version_history": "Version History",
                "published_versions": "Published Versions",
                "published": "Published",
                "no_versions": "No published versions yet."
            },
"""

# Insert into zh-CN
ui_content = ui_content.replace('"settings": {', zh_addition + '            "settings": {', 1)
# Insert into en-US (it's the second occurrence of "settings": {)
ui_content = ui_content.replace('"settings": {', en_addition + '            "settings": {', 2)

# Oh wait, replace with count=2 will replace both. So I will find the exact string.
with open("app/web/ui_strings.py", "w") as f:
    f.write(ui_content)
