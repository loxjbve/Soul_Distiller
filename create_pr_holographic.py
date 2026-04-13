import os
import json
import urllib.request
import urllib.error
import subprocess

branch = "feat/holographic-ui"
remote_url = subprocess.check_output(['git', 'remote', 'get-url', 'origin']).decode('utf-8').strip()

repo_path = "loxjbve/--"
if 'github.com/' in remote_url:
    repo_path = remote_url.split('github.com/')[1].replace('.git', '')

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

body = """### 前端 UI 升级：全息投影/实验室风格 (Holographic Lab Style)

本次 PR 对前端界面进行了彻底的视觉升级，在不改变原有 Flask 架构和后端逻辑的前提下，通过深度重写 CSS 样式与新增 JavaScript 交互，实现了以下特性：

1. **极简深色全息主题**
   - 采用深蓝黑色作为底色，配合全息青色（Neon Cyan）高亮。
   - 所有卡片、面板 (`.workspace-panel`) 和按钮应用了**玻璃拟态（Glassmorphism）**效果。

2. **硬核科幻切角设计 (Sci-Fi Chamfered Corners)**
   - 使用 CSS `clip-path` 替代了传统的圆角（`border-radius`），为容器和按钮塑造了更具 HUD 特色的多边形切角外观。

3. **光影与动态系统升级**
   - 全局背景增加了缓慢下移的全息扫描线、网格，以及随时间呼吸浮动的大面积光晕 (`.ambient-glow`)。
   - 进度条（如分析任务的加载进度）引入了数据流动感的脉冲动画。
   - 按钮悬停时，新增了高亮发光与文字毛刺/故障动画 (Glitch Effect)。

4. **交互光标跟随 (Cursor Glow)**
   - 在 `app.js` 与 `base.html` 中注入了全局鼠标追踪逻辑，使得光标移动时会有一团微弱的青色全息光晕始终跟随，增强沉浸感。
"""

data = {
    "title": "feat: 前端全息投影/实验室风格升级 (Holographic UI)",
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
