import os
import json
import urllib.request
import urllib.error
import subprocess

# Get branch
branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()

# Get remote URL
remote_url = subprocess.check_output(['git', 'remote', 'get-url', 'origin']).decode('utf-8').strip()

# Extract repo path
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
data = {
    "title": "feat: 优化文档索引进度条显示",
    "head": branch,
    "base": "main",
    "body": "### 更新内容\n\n1. **后端进度计算平滑化**：在文档解析的索引阶段（70% ~ 90% 区间），修改了批量请求 Embedding 时的进度计算。现在后端会根据实际完成的 chunks 数量动态推算 `progress_percent`，进度条会随着处理逐渐向前推进。\n2. **前端直观展示 chunks 数量**：修改了 Projects 页面中对 `taskStageLabel` 的渲染逻辑。当文档进入 `embedding`（索引）阶段时，卡片上原本的“索引中”文字会被替换为直观的 `索引中 {已处理}/{总数} chunks` 格式。\n\n彻底解决了“卡在70%”以及“进度不直观”的问题。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")
