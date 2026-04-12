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

body = """### Skill 生成逻辑优化与多维拆分

1. **三次独立 LLM 调用**
   - 将原先单次 LLM 生成 `skill.md` 的逻辑重构，现在生成一次 skill 会触发三次独立的 LLM 调用。
   - 分别针对 `personality`（核心性格与精神底色）和 `memories`（核心记忆与过往经历）进行专属生成。

2. **结合向量索引检索 (RAG)**
   - 在生成 `personality` 和 `memories` 前，系统会首先通过 `retrieval_service` 从本地知识库进行向量检索。
   - `personality` 检索关键词：“性格特质 精神状态 自我认知 核心身份”。
   - `memories` 检索关键词：“核心记忆 经历 过往重要事件”。
   - 检索到的专属 Context 将结合多维分析报告一起作为输入，大幅提升生成准确度和事实锚定。

3. **平滑的 Prompt 拼接与回退机制**
   - 提取出的 `core_identity`、`mental_state` 和 `memories` 会在最终的 payload 阶段无缝拼接到 `skill.md`。
   - 新增了 `build_personality_messages` 和 `build_memories_messages`。
   - 保证了即使独立抽取失败，也会回退到原有的 heuristic 提取，保证系统高可用性。
"""

data = {
    "title": "Feat: 结合向量检索独立生成 Personality 与 Memories，升级 Skill 生成质量",
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