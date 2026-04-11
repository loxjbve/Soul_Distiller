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

body = """### LLM 异常兼容与 Rerun 分析体验优化

本次 PR 主要针对多维分析 (10-Facet Analysis) 的健壮性以及交互反馈体验进行了全面增强，带来如下改进：

1. **LLM 字符串类型异常的强兼容 (Graceful Fallback)**
   - **问题现象**：执行十维分析时，大模型偶尔无法严格返回规范的 JSON 结构。例如返回了纯文本 `"medium"` 或带有断层的半拉子 JSON，导致引擎在做强制 `float()` 类型转换或 `json.loads()` 时出现致命奔溃。
   - **解决方案**：
     - 在 `app/llm/client.py` 增加了具有弹性的 `parse_json_response(fallback=True)` 解析器。遇到彻底崩坏的 JSON 会把文本兜底包裹成安全的字典结构，交给下游消化。
     - 在 `app/analysis/engine.py` 增强了对 `confidence` 字段的 `_parse_confidence` 兜底解析（可将 `high/medium/low` 软解析成 `0.8/0.5/0.2`）。

2. **Rerun (重新执行) 分析的实时反馈**
   - **重写回调机制**：由于旧版在 `rerun_facet` 时没有为工作线程绑定 SSE Stream 回调，导致页面只会转圈而没有实时日志。现在我补齐了 `stream_callback` 的传递。
   - **前端交互**：点击 "Rerun" 之后自动 `window.location.reload()` 触发浏览器刷新，完美对接 SSE 流实时展示重跑过程中的 LLM 分析文本。

3. **解封长文本限制，优化监控视野**
   - **What**: 之前的 Live Text （实时打印日志）跑到 6000 字符后就不更新了，导致大型角色的分析流经常被截断看不全。
   - **How**: 将引擎全局常量 `RAW_TEXT_PREVIEW_LIMIT` 大幅放宽至 `20000`。
   - 并且修改了前端 `app.js` 的行为，让 `LLM Live Text` 的 `details` 下拉菜单**默认永远展开**，方便用户实时观察。"""

data = {
    "title": "Feat/Fix: LLM 异常包容兜底机制与 Rerun 实时监控流优化",
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