import os
import json
import urllib.request
import urllib.error
import subprocess

# 获取当前分支
branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()

# 从远端 URL 中提取 owner/repo
remote_url = subprocess.check_output(['git', 'remote', 'get-url', 'origin']).decode('utf-8').strip()
repo_path = "loxjbve/--"
if 'github.com/' in remote_url:
    repo_path = remote_url.split('github.com/')[1].replace('.git', '')

# 获取 Token
token = os.environ.get('GITHUB_TOKEN')
if not token:
    if 'x-access-token:' in remote_url:
        token = remote_url.split('x-access-token:')[1].split('@')[0]

if not token:
    print("Error: No GITHUB_TOKEN available")
    exit(1)

url = f"https://api.github.com/repos/{repo_path}/pulls"
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

body = """### 试聊系统全面重构与稳定性修复

本次 PR 主要针对试聊页面体验以及多模型下的稳定性进行了重构，带来如下改进：

1. **沉浸式试聊体验 (Immersive Playground)**
   - **移除前置检索**：根据设计要求，在试聊时（`playground/chat`），大模型将**完全基于已发布 Skill 设定**进行角色扮演，不再动态执行 Embedding 向量与 BM25 的降级词法检索。彻底移除了冗长的 Context Evidence 追踪逻辑。
   - **前端聊天气泡 UI**：在 `playground.html` 引入了美观的 CSS 样式，重写了表单结构。现在的界面是一个真实的**双侧聊天气泡** UI（用户在右，角色在左），包含圆角、阴影、底部悬浮输入框，以及加载完毕自动滚动到底部的体验。

2. **核心 Bug 修复：LLM 置信度解析报错**
   - **问题现象**：执行 10 维分析时，部分不受控的 LLM（如 `x.ai`）未按 JSON schema 返回浮点数的 `confidence`，而是返回了 `"medium"`、`"high"` 等英文单词，导致后端直接抛出 `could not convert string to float` 的致命崩溃。
   - **解决方案**：在 `app/analysis/engine.py` 增加了安全健壮的 `_parse_confidence` 函数，能够兼容并消化 `"high"`(0.8), `"medium"`(0.5), `"low"`(0.2) 等字符串返回值，避免强制类型转换失败。

3. **工程优化与清理**
   - 彻底移除了原先分散的各种陈旧的 `test_*.py` 文件。
   - 更新并重写了 `README.md`，同步了包含**群组双模式架构**、**十维分析与ZIP导出**、以及**高精度提示词工程**的核心特性文档说明。
   - 同步修正了 `tests/test_web_app.py` 中关于移除 `retrieval_mode` 的断言。"""

data = {
    "title": "Feat/Fix: 沉浸式试聊气泡重构与 LLM 字符串转换奔溃修复",
    "head": branch,
    "base": "main",
    "body": body
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")