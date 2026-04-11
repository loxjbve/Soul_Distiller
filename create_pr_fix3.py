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
data = {
    "title": "Fix: 解决批量文件摄入时的并发限流雪崩与 ReadTimeout 退回",
    "head": branch,
    "base": "main",
    "body": "### 解决连续报错的核心问题\\n\\n1. **并发雪崩问题**：当您传入大文件时，系统原来采用 `16 线程 * 64 块 * 4000字符` 的恐怖并发打到大模型 API（或本地模型），这不仅容易触发模型的限流，还使得响应极慢并触发 `90s` 的默认超时 (`httpx.ReadTimeout`)。\\n2. **连锁失败效应 (Cascade Failure)**：由于 `_embedding_executor` 是全局共享的线程池，当第一个文件的一个请求超时抛错后，文件被标记为 `failed`。**但池子里排队的成百上千个属于第一个文件的 Embedding 请求并没有被取消**，它们继续占满线程池并向大模型发请求，导致之后的所有文件一进来就在线程池里排队等死，或者被已经“半死不活”的模型 API 直接拒接，最终全员失败退回！\\n\\n### 修复方案\\n- **精细限流与增时**：将 Embedding 并发从 16 降到 4，批次从 64 降为 16。API 请求超时阈值从 90s 延长至 180s。\\n- **添加智能重试 (Retry)**：如果遇到偶发网络超时，现在代码会进行 `3 次` 指数退避重试，而不是直接放弃整个文档。\\n- **加入熔断机制 (Cancellation)**：在提交给线程池的请求闭包里加入 `task.is_cancelled` 检查。一旦某个文档彻底失败或取消，线程池里排队的该文档的所有子任务会立刻执行 `return []`，**瞬间释放线程池资源**，保证后续上传的文件能够被干净的资源处理。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")

