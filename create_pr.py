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
    "title": "feat: 修复 Persona Studio 布局重叠并支持中英文切换",
    "head": branch,
    "base": "main",
    "body": "### 更新内容\n\n1. **修复 Persona Studio 布局**：修复了 `app/static/pages.css` 中 `.persona-stage-stack` 和 `.persona-stage-card` 相关的布局问题，允许内部局部滚动，防止卡片高度被挤压并覆盖底部操作按钮。\n2. **建立多语言支持 (i18n)**：在 `app/web/ui_strings.py` 补充了双语词典，并通过 Cookie (`locale`) 进行服务端动态注入。\n3. **前端全局支持**：将前端 `skill.html`、`telegram_preprocess.html` 以及 `base.html` 中的所有硬编码中文替换为动态变量 `{{ ui.xxx }}`，并在导航栏添加了 `EN | 中文` 的一键切换开关。\n\n彻底解决了分析按钮被遮挡的问题，并实现了中英文语言无缝切换。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")
