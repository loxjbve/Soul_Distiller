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
    # Try to extract from URL if possible
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
    "title": "Refactor: 全量性能与健壮性优化 (Performance & Stability)",
    "head": branch,
    "base": "main",
    "body": "### 项目全量优化\\n\\n本次 PR 包含以下核心优化，大幅提升了系统的性能与健壮性：\\n\\n1. **解除线程池阻塞**：重构 `stream_analysis_api` 为异步生成器，解决了分析时的并发卡死。\\n2. **重构 LLM 客户端连接池**：改用单例 `httpx.AsyncClient`，开启长连接复用，移除了僵化的信号量。\\n3. **修复 OOM 与拼接算法**：改用列表收集与分段读写，修复了大文件上传及切片时的内存溢出和低效。\\n4. **消灭静默异常**：修复向量库写入失败时的静默跳过，转为正确记录 `failed` 状态。\\n5. **修复 N+1 查询**：优化了 `rechunk.py` 中的批量查表，减轻了数据库 I/O。\\n6. **修复 UI 与交互**：恢复了上传功能的正确可用，并将 Kanban 四列面板切换为匹配深色主题的 CSS 变量。"
}

req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='POST')
try:
    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode('utf-8'))
        print(f"PR created successfully: {res.get('html_url')}")
except urllib.error.HTTPError as e:
    err = e.read().decode('utf-8')
    print(f"Failed to create PR: {e.code} - {err}")

