import re

def update_telegram():
    with open("app/templates/telegram_preprocess.html", "r") as f:
        content = f.read()

    replacements = {
        r"Telegram 预处理指挥台": "{{ ui.hero_title }}",
        r"导入结构化 JSON 后，这里会实时显示 SQL 物化、周总结并发、话题沉淀与中间产物状态。": "{{ ui.hero_note }}",
        r"周总结并发": "{{ ui.concurrency_label }}",
        r'{{ "继续预处理" if selected_run_data and selected_run_data.resume_available else "开始预处理" }}': '{{ ui.btn_resume if selected_run_data and selected_run_data.resume_available else ui.btn_start }}',
        r'{{ selected_run_data.current_stage if selected_run_data else "等待开始" }}': '{{ selected_run_data.current_stage if selected_run_data else ui.stage_waiting }}',
        r"实时监听 SQL 物化、周总结和最终话题写入的进度。": "{{ ui.live_note }}",
        r">等待任务<": ">{{ ui.status_waiting }}<",
        r"总体进度": "{{ ui.overall_progress }}",
        r"周总结覆盖率": "{{ ui.coverage_progress }}",
        r"当前 Run 还没有周候选结果。": "{{ ui.empty_weekly }}",
        r"当前 Run 还没有 Top Users 结果。": "{{ ui.empty_top }}",
        r"当前 Run 还没有活跃用户结果。": "{{ ui.empty_active }}",
        r"当前 Run 还没有最终话题结果。": "{{ ui.empty_topics }}",
        r"当前 Run": "{{ ui.current_run }}",
        r"<span>阶段</span>": "<span>{{ ui.stage }}</span>",
        r"<span>总进度</span>": "<span>{{ ui.total_progress }}</span>",
        r"<span>周候选数</span>": "<span>{{ ui.weekly_candidates }}</span>",
        r"<span>最终话题数</span>": "<span>{{ ui.final_topics }}</span>",
        r"<span>请求并发</span>": "<span>{{ ui.request_concurrency }}</span>",
        r"<span>活跃 Worker</span>": "<span>{{ ui.active_workers }}</span>",
        r"<span>已完成周数</span>": "<span>{{ ui.completed_weeks }}</span>",
        r"<span>剩余周数</span>": "<span>{{ ui.remaining_weeks }}</span>",
        r"暂无预处理 run": "{{ ui.no_run }}",
        r"并发轨道": "{{ ui.worker_lanes }}",
        r"尚无活跃 worker。": "{{ ui.no_workers }}",
        r"Subagent 时间线": "{{ ui.trace_timeline }}",
        r"等待任务事件。": "{{ ui.waiting_events }}",
        r"当前 LLM 输出": "{{ ui.live_output }}",
        r"等待新的 LLM 请求开始。": "{{ ui.waiting_llm }}",
        r"周话题表": "{{ ui.topic_table }}",
        r"暂无历史预处理记录。": "{{ ui.empty_history }}",
        r"历史 Run": "{{ ui.history_run }}",
        r"返回项目": "{{ ui.common.back_to_project }}",
        r"Telegram 预处理": "{{ ui.title }}"
    }
    
    keys = sorted(replacements.keys(), key=len, reverse=True)
    for old in keys:
        new = replacements[old]
        content = content.replace(old, new)
        
    with open("app/templates/telegram_preprocess.html", "w") as f:
        f.write(content)

def update_skill():
    with open("app/templates/skill.html", "r") as f:
        content = f.read()

    replacements = {
        r"从最新分析生成草稿，编辑后再发布为正式版本。": "{{ ui.hero_note }}",
        r"当前没有草稿。先等分析完成，再生成 Skill。": "{{ ui.no_draft }}",
        r"暂无已发布版本。": "{{ ui.no_versions }}",
        r"重新生成草稿": "{{ ui.regenerate }}",
        r"返回分析": "{{ ui.back_to_analysis }}",
        r"编辑草稿": "{{ ui.edit_draft }}",
        r"审核备注": "{{ ui.review_notes }}",
        r"保存草稿": "{{ ui.save_draft }}",
        r"发布版本": "{{ ui.publish_version }}",
        r"版本记录": "{{ ui.version_history }}",
        r"已发布版本": "{{ ui.published_versions }}",
        r"的 Skill 草稿": "{{ ui.draft_title }}",
        r"Skill 预览": "{{ ui.preview }}",
        r">已发布<": ">{{ ui.published }}<",
        r"草稿<": "{{ ui.draft_eyebrow }}<"
    }
    
    keys = sorted(replacements.keys(), key=len, reverse=True)
    for old in keys:
        new = replacements[old]
        content = content.replace(old, new)
        
    with open("app/templates/skill.html", "w") as f:
        f.write(content)

update_telegram()
update_skill()
