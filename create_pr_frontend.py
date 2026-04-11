import os
import json
import urllib.request
import urllib.error
import subprocess

branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()
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

body = """### 前端视觉全面升级与精简计划

1. **毛玻璃与流光 (Glassmorphism) 全局应用**
   - 增强了深邃的动态光晕背景 (`.page-aura`)，光晕范围扩大且颜色过渡更加柔和。
   - 所有项目面板 (`.workspace-panel`)、顶部信息卡片 (`.hero-board`) 和列表项 (`.project-row`) 均采用了高透度的深色背景配合强力模糊 (`backdrop-filter: blur(24px)`)。
   - 在主要面板顶部增加了渐变发光且带流动的顶部扫描线动画 (`shimmer` 效果)。
   - 提升了所有的交互光效，如按钮和光晕指示灯 (`pulseGlow`) 变得更华丽且呼吸感更强。

2. **彻底精简并重构文档看板**
   - **做减法**：移除了原先占用大量横向空间、视觉繁重的“四列看板”（未处理、队列中、进行中、已完成）。
   - **统一网格**：替换为一个带有精美玻璃悬浮效果的统一文档网格 (`.document-grid`)。
   - **智能排序**：在底层的 JavaScript (`renderDocuments` 方法) 中加入了智能优先级排序逻辑，状态按 **“失败 > 进行中 > 队列中 > 未处理 > 已完成”** 的顺序进行降序排列。让正在处理中和异常的文件永远置顶，同时释放了大量页面空间。

3. **首页信息减负**
   - 删除了首页 Hero 区域中冗长、说明性的次要文字。
   - 将 H1 标题精简为强有力的 `“构建可交互的人物 Skill。”`。
   - 将视觉重心彻底交还给底部的项目列表 (`.project-list`)，且项目列表如今拥有了带玻璃折射和浮动光环的 Hover 交互体验。"""

data = {
    "title": "Feat: 前端全方位视觉升级与流光毛玻璃效果重构",
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
