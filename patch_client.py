import re
with open('/workspace/app/llm/client.py', 'r') as f:
    content = f.read()

# 1. Imports and global client
new_content = content.replace(
"""OFFICIAL_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
}
MAX_CONCURRENT_LLM_REQUESTS = 5
_LLM_REQUEST_SEMAPHORE = BoundedSemaphore(MAX_CONCURRENT_LLM_REQUESTS)
_LLM_LOG_LOCK = Lock()""",
"""OFFICIAL_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
}
MAX_CONCURRENT_LLM_REQUESTS = 20

# Global httpx client for connection pooling
_HTTP_CLIENT = httpx.Client(limits=httpx.Limits(max_keepalive_connections=20, max_connections=50))

import queue
import threading

_LOG_QUEUE = queue.Queue()

def _log_worker():
    while True:
        log_item = _LOG_QUEUE.get()
        if log_item is None:
            break
        path, record = log_item
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            import json
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\\n")
        except Exception:
            pass
        finally:
            _LOG_QUEUE.task_done()

_log_thread = threading.Thread(target=_log_worker, daemon=True)
_log_thread.start()""")

# 2. _post_stream_text_with_meta
old_stream = """        try:
            with _LLM_REQUEST_SEMAPHORE:
                with httpx.Client(timeout=timeout) as client:
                    with client.stream("POST", url, headers=self._headers(), json=payload) as response:"""

new_stream = """        try:
            with _HTTP_CLIENT.stream("POST", url, headers=self._headers(), json=payload, timeout=timeout) as response:"""

# We must dedent the body of `with client.stream...` by 8 spaces
# The body ends when we hit `        except LLMError:`
stream_start = content.find(old_stream)
if stream_start != -1:
    stream_end = content.find("        except LLMError:", stream_start)
    body = content[stream_start + len(old_stream):stream_end]
    dedented_body = []
    for line in body.split("\\n"):
        if line.startswith("        "):
            dedented_body.append(line[8:])
        else:
            dedented_body.append(line)
    
    new_content = new_content.replace(
        old_stream + body,
        new_stream + "\\n".join(dedented_body)
    )

# 3. _post_json_with_meta
old_post = """        try:
            with _LLM_REQUEST_SEMAPHORE:
                with httpx.Client(timeout=timeout) as client:
                    response = client.post(url, headers=self._headers(), json=payload)
                    response_text = response.text
        except Exception as exc:"""

new_post = """        try:
            response = _HTTP_CLIENT.post(url, headers=self._headers(), json=payload, timeout=timeout)
            response_text = response.text
        except Exception as exc:"""
new_content = new_content.replace(old_post, new_post)

# 4. _get_json_with_meta
old_get = """        try:
            with _LLM_REQUEST_SEMAPHORE:
                with httpx.Client(timeout=timeout) as client:
                    response = client.get(url, headers=self._headers())
                    response_text = response.text
        except Exception as exc:"""
        
new_get = """        try:
            response = _HTTP_CLIENT.get(url, headers=self._headers(), timeout=timeout)
            response_text = response.text
        except Exception as exc:"""
new_content = new_content.replace(old_get, new_get)

# 5. _append_log
old_log = """    def _append_log(self, record: dict[str, Any]) -> None:
        if not self.log_path:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with _LLM_LOG_LOCK:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\\n")"""

new_log = """    def _append_log(self, record: dict[str, Any]) -> None:
        if not self.log_path:
            return
        _LOG_QUEUE.put((self.log_path, record))"""
new_content = new_content.replace(old_log, new_log)

with open('/workspace/app/llm/client.py', 'w') as f:
    f.write(new_content)

