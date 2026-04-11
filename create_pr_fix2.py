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
    "title": "Fix: 解决隐藏的 FAISS 维度错误并在前端/后端暴露异常日志",
    "head": branch,
    "base": "main",
    "body": "### 修复隐藏失败与完善日志\\n\\n1. **修复潜在的 FAISS 维度崩溃**：发现如果在 Embedding 时使用了维度不是 1536（比如某些开源模型或 Gemini/Qwen 的 1024 维）的模型，原本写死 1536 维的 FAISS `IndexFlatIP` 会在 `add()` 时直接崩溃。现已在 `add()` 时加入自动识别空索引维度与安全防崩溃校验。\\n2. **前端明确显示失败原因**：由于之前看板的 4 列布局导致 `failed` 状态的文档在 UI 逻辑中退回到了“未处理 (unprocessed)”列，从而给人一种“什么都没发生就被重置”的错觉。现已在未处理列表的卡片中，补充了一行红色的 `doc.error_message` 失败原因。\\n3. **后端详细堆栈日志**：在 `IngestTaskManager` 捕获异常处，增加了 `logger.error` 和 `traceback` 打印，以便终端能清晰输出引发失败的代码行与完整错误栈。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")

