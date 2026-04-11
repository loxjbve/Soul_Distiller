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

body = """### 修复：文档分块导致块数爆炸与平均切片过小的严重 Bug

**问题描述**：
用户反馈 120MB 的数据被切出了 320,000 个 chunk（平均每个 Chunk 仅约 400 字节，扣除重叠部分甚至只有几十个字符），这会导致文本彻底丧失上下文语义，严重影响后续检索与大模型的理解效果。

**根本原因 (`Root Cause`)**：
在 `app/pipeline/chunking.py` 的 `_find_boundary` 方法中，算法采取了**过度贪婪（Eager match）** 的查找策略。如果当前切片的开头附近（例如 `start + 250` 的位置）存在 `\\n\\n` 等高优先级分隔符，即使后续还有长达数千字的段落，算法也会直接在 `start + 250` 处一刀切断，无视了后面的 `\\n` 或 `。` 等次级分隔符！
更严重的是，当截断位置接近重叠区间（Overlap）时，会导致下一个 Chunk 的起始游标 `start` **每次只前进几个字符甚至 1 个字符**！这就引发了“死循环式”的碎片级推进，生成成百上千个高度重复的微小碎片（Chunk Explosion）。

**修复方案**：
1. **强制最小推进步长 (Min Advance)**：重新设计了 `_find_boundary` 的搜索区间。现在强制要求每一个切分点必须距离起始位置大于 `overlap + min_advance`，确保切片游标每次都能实质性地推进。
2. **两阶段智能查找 (Two-Phase Search)**：
   - **Phase 1**：优先在 `min_index` 到 `hard_end` 的后半段寻找最高级分隔符，使得切出的 Chunk 更接近期望的 `chunk_size`。
   - **Phase 2**：如果在后半段找不到任何分隔符，才回退到 `start + overlap` 到 `min_index` 的区间内寻找，确保文本不会在段落内部生硬截断。
3. **安全回退 (Fallback)**：如果全区间都找不到任何符合条件的自然分隔符，才在 `hard_end` 处做硬截断，确保算法在任何极端语料下都能收敛且输出长短正常的 Chunk。"""

data = {
    "title": "Fix: 修复文档切片算法漏洞，解决 Chunk 数量爆炸与语义碎片化问题",
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
