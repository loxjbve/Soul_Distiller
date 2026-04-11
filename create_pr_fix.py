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
    "title": "Fix: 修复前端灾难级指示灯与 FAISS 隐式写库错误 (UI & DB)",
    "head": branch,
    "base": "main",
    "body": "### 修复项 (Fixes)\\n\\n本次 PR 主要修复了以下问题：\\n\\n1. **修复 FAISS 存库与隐式异常**：在之前针对 OOM 的并发处理优化中，因为批量操作的迭代变量作用域覆盖，导致大部分分块未被写入。写入 FAISS 时由于长度不匹配引发内部异常被吞没，使得任务表面完成但实际失败。已修复数据匹配逻辑。\\n2. **前端看盘指示灯重构**：修复了 Kanban 面板上由于样式阴影挤压导致的大方块以及严重的重叠问题。将指示灯精简为圆点（分离外边框），并将圆形进度条改为了文件上方直观的横向进度条。\\n3. **未处理灯光异常**：修复了“未处理”状态仍具有发光属性导致红灯误亮的问题。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")

