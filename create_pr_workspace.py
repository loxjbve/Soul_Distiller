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

body = """### 项目工作区双模式升级 (Dual-Mode Workspace)

本次 PR 将项目工作区升级为“基于群聊”和“基于单人”两种双模式架构，并在前端补全了对现有项目的编辑功能。

1. **数据库与模型层平滑升级**
   - 在 `app/models.py` 中的 `Project` 模型新增 `mode` 字段（`group` 或 `single`）。
   - 在 `app/db.py` 中增加了自动升级逻辑 `upgrade_schema`，自动迁移历史数据，无需繁琐的数据库脚本即可保证历史项目无缝兼容并默认识别为群聊 (`group`) 模式。

2. **仓储与路由层支持**
   - 改造 `app/storage/repository.py` 的 `create_project` 与 `clone_project` 接口，支持传入并复制 `mode` 属性。
   - 新增更新项目的 API 路由 `POST /projects/{project_id}/update`，支持动态更新 `name`、`description` 和 `mode`。

3. **前端页面全面适配**
   - 首页创建项目表单 (`index.html`) 新增了模式的下拉选择框。
   - 项目详情页 (`project_detail.html`) 增加了“编辑项目”按钮与对应的修改表单 Modal 弹窗。
   - 动态适配了语料分析目标的提示文案：
     - 若为 `group` 模式，引导语变为“群聊名称或氛围描述”。
     - 若为 `single` 模式，保留“要分析和模拟的角色”提示。"""

data = {
    "title": "Feat: 项目工作区双模式架构 (Group/Single) 升级与项目信息编辑",
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