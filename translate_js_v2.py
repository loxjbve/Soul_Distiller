import re

with open('app/static/app.js', 'r', encoding='utf-8') as f:
    content = f.read()

replacements = {
    r'"Waiting for live updates."': '"等待实时状态更新…"',
    r'"No events yet"': '"暂无事件"',
    r'"Run events will appear here as facets move through the queue."': '"随着维度在队列中推进，运行事件将在此显示。"',
    r'"No completed LLM call yet"': '"尚无已完成的 LLM 调用"',
    r'"succeeded" : "fallback"': '"成功" : "回退"',
    r'"Rerun Facet"': '"重新运行维度"',
    r'"The response preview was truncated to keep the event panel compact."': '"响应预览已被截断，以保持事件面板紧凑。"',
    r'"No payload was attached to this event."': '"此事件未附带 payload。"',
    r'"No conflicts were recorded for this facet."': '"此维度未记录到冲突项。"',
    r'"The live text preview was truncated to keep the card height bounded."': '"实时流式文本预览已被截断，以限制卡片高度。"',
    r'"No live text has been stored for this facet yet."': '"此维度尚未存储实时流式文本。"',
    r'"Waiting for fresh events."': '"等待最新事件。"',
    r'"Unknown Facet"': '"未知维度"',
    r'"and waiting for a free slot."': '"并等待空闲插槽。"',
    r'"Retrieving evidence and preparing the facet payload."': '"正在检索证据并准备维度 payload。"',
    r'"Active phase: "': '"活动阶段："',
    r'"The facet failed before a structured summary was produced."': '"维度在生成结构化摘要前失败。"',
    r'"No summary was returned for this facet."': '"此维度未返回摘要。"',
    r'"The queue is empty. Active slots are working on the remaining facets."': '"队列已空。活动插槽正在处理剩余维度。"',
    r'"The queue is empty."': '"队列为空。"',
    r'"Queued"': '"排队中"',
    r'"Preparing"': '"准备中"',
    r'"Running"': '"运行中"',
    r'"Completed"': '"已完成"',
    r'"Failed"': '"已失败"',
    r'"Retrieving evidence"': '"正在检索证据"',
    r'"Generating with LLM"': '"LLM 生成中"',
    r'"Analyzing"': '"分析中"',
    r'"Finalizing"': '"处理完成"',
    r'"Evidence"': '"证据"',
    r'"Notes"': '"备注"',
    r'"LLM Trace"': '"LLM 追踪"',
    r'"LLM Live Text"': '"LLM 实时输出"',
    r'"The card preview was truncated automatically to keep queue items compact."': '"卡片预览已自动截断以保持紧凑。"',
    r'"No evidence has been attached yet."': '"尚未附加证据。"',
    r'"Accepted" : "Pending"': '"已采纳" : "处理中"'
}

for pattern, repl in replacements.items():
    content = re.sub(pattern, repl, content)

with open('app/static/app.js', 'w', encoding='utf-8') as f:
    f.write(content)

print("JS V2 translation done.")
