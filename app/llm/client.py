from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore, Lock
from typing import Any
from urllib.parse import urljoin

import httpx

from app.schemas import ChatCompletionResult, LLMToolCall, ServiceConfig, ToolRoundResult
from app.utils.text import token_count

OFFICIAL_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
}
MAX_CONCURRENT_LLM_REQUESTS = 4
_LLM_REQUEST_SEMAPHORE = BoundedSemaphore(MAX_CONCURRENT_LLM_REQUESTS)
_LLM_LOG_LOCK = Lock()


class LLMError(Exception):
    """Raised when the remote LLM service returns an invalid response."""

    def __init__(
        self,
        message: str,
        *,
        raw_text: str | None = None,
        request_url: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.request_url = request_url
        self.status_code = status_code


def normalize_provider_kind(provider_kind: str | None) -> str:
    provider = (provider_kind or "openai-compatible").strip().lower()
    aliases = {
        "openai_compatible": "openai-compatible",
        "custom": "openai-compatible",
    }
    return aliases.get(provider, provider)


def normalize_api_mode(api_mode: str | None) -> str:
    mode = (api_mode or "responses").strip().lower()
    aliases = {
        "response": "responses",
        "response_api": "responses",
        "chat": "chat_completions",
        "chat_completion": "chat_completions",
        "chat-completions": "chat_completions",
    }
    normalized = aliases.get(mode, mode)
    if normalized not in {"responses", "chat_completions"}:
        return "responses"
    return normalized


class OpenAICompatibleClient:
    def __init__(self, config: ServiceConfig, *, log_path: str | None = None) -> None:
        self.config = config
        self.log_path = Path(log_path) if log_path else None

    def list_models(self) -> list[str]:
        payload = self._get_json("/models", timeout=20.0)
        data = payload.get("data", [])
        return [item["id"] for item in data if item.get("id")]

    def resolve_model(self) -> str:
        if self.config.model:
            return self.config.model
        models = self.list_models()
        if not models:
            raise LLMError("No models discovered from the configured service.")
        return models[0]

    def endpoint_url(self, path: str) -> str:
        return self._url(path)

    def chat_completion(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
        max_tokens: int = 1400,
    ) -> str:
        return self.chat_completion_result(
            messages,
            model=model,
            temperature=temperature,
            response_format=response_format,
            max_tokens=max_tokens,
        ).content

    def chat_completion_result(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
        max_tokens: int = 1400,
    ) -> ChatCompletionResult:
        resolved_model = model or self.resolve_model()
        if normalize_api_mode(self.config.api_mode) == "responses":
            payload: dict[str, Any] = {
                "model": resolved_model,
                "input": self._messages_to_responses_input(messages),
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
            if response_format:
                payload["text"] = {"format": response_format}
            data, meta = self._post_json_with_meta("/responses", payload)
            content = self._extract_responses_text(data)
        else:
            payload = {
                "model": resolved_model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if response_format:
                payload["response_format"] = response_format
            data, meta = self._post_json_with_meta("/chat/completions", payload)
            try:
                content = str(data["choices"][0]["message"]["content"] or "")
            except (KeyError, IndexError, TypeError) as exc:
                raise LLMError(
                    "Invalid chat completion response structure.",
                    raw_text=meta["response_text"],
                    request_url=meta["url"],
                    status_code=meta["status_code"],
                ) from exc
        usage = self._extract_usage(data, messages, content)
        return ChatCompletionResult(
            content=content,
            model=str(data.get("model") or resolved_model),
            usage=usage,
            request_url=meta["url"],
            request_payload=payload,
            raw_response_text=meta["response_text"],
            response_id=str(data.get("id") or "") or None,
        )

    def tool_round(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 1400,
    ) -> ToolRoundResult:
        resolved_model = model or self.resolve_model()
        if normalize_api_mode(self.config.api_mode) == "responses":
            payload = {
                "model": resolved_model,
                "input": self._messages_to_responses_input(messages),
                "tools": [self._chat_tool_to_responses_tool(tool) for tool in tools],
                "tool_choice": "auto",
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
            data, _meta = self._post_json_with_meta("/responses", payload)
            tool_calls = self._extract_responses_tool_calls(data)
            content = self._extract_responses_text(data)
            usage = self._extract_usage(data, messages, content)
            return ToolRoundResult(
                content=content,
                model=str(data.get("model") or resolved_model),
                usage=usage,
                tool_calls=tool_calls,
                provider_response_id=data.get("id"),
            )
        payload = {
            "model": resolved_model,
            "messages": self._messages_to_chat_completions(messages),
            "tools": tools,
            "tool_choice": "auto",
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        data, meta = self._post_json_with_meta("/chat/completions", payload)
        try:
            message = data["choices"][0]["message"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError(
                "Invalid tool completion response structure.",
                raw_text=meta["response_text"],
                request_url=meta["url"],
                status_code=meta["status_code"],
            ) from exc
        tool_calls = [
            LLMToolCall(
                id=str(item.get("id") or ""),
                name=str(item.get("function", {}).get("name") or ""),
                arguments_json=str(item.get("function", {}).get("arguments") or "{}"),
                arguments=parse_json_response(str(item.get("function", {}).get("arguments") or "{}")),
            )
            for item in (message.get("tool_calls") or [])
        ]
        content = str(message.get("content") or "")
        usage = self._extract_usage(data, messages, content)
        return ToolRoundResult(
            content=content,
            model=str(data.get("model") or resolved_model),
            usage=usage,
            tool_calls=tool_calls,
            provider_response_id=data.get("id"),
        )

    def embeddings(self, inputs: list[str], *, model: str | None = None) -> list[list[float]]:
        payload = {"model": model or self.resolve_model(), "input": inputs}
        data = self._post_json("/embeddings", payload)
        try:
            ordered = sorted(data["data"], key=lambda item: item["index"])
            return [list(item["embedding"]) for item in ordered]
        except (KeyError, TypeError) as exc:
            raise LLMError("Invalid embeddings response structure.") from exc

    def validate(self) -> dict[str, Any]:
        try:
            models = self.list_models()
            return {"ok": True, "models": models}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _messages_to_responses_input(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for message in messages:
            role = str(message.get("role") or "user")
            tool_calls = list(message.get("tool_calls") or [])
            content = message.get("content")
            if role == "tool":
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": str(message.get("tool_call_id") or ""),
                        "output": str(content or ""),
                    }
                )
                continue
            if content:
                items.append({"role": role, "content": str(content)})
            for tool_call in tool_calls:
                items.append(
                    {
                        "type": "function_call",
                        "call_id": str(tool_call.get("id") or ""),
                        "name": str(tool_call.get("name") or ""),
                        "arguments": str(tool_call.get("arguments_json") or "{}"),
                    }
                )
        return items

    def _messages_to_chat_completions(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for message in messages:
            role = str(message.get("role") or "user")
            if role == "tool":
                normalized.append(
                    {
                        "role": "tool",
                        "tool_call_id": str(message.get("tool_call_id") or ""),
                        "content": str(message.get("content") or ""),
                    }
                )
                continue
            item: dict[str, Any] = {
                "role": role,
                "content": str(message.get("content") or ""),
            }
            tool_calls = list(message.get("tool_calls") or [])
            if tool_calls:
                item["tool_calls"] = [
                    {
                        "id": str(tool_call.get("id") or ""),
                        "type": "function",
                        "function": {
                            "name": str(tool_call.get("name") or ""),
                            "arguments": str(tool_call.get("arguments_json") or "{}"),
                        },
                    }
                    for tool_call in tool_calls
                ]
            normalized.append(item)
        return normalized

    @staticmethod
    def _chat_tool_to_responses_tool(tool: dict[str, Any]) -> dict[str, Any]:
        function = dict(tool.get("function") or {})
        return {
            "type": "function",
            "name": function.get("name"),
            "description": function.get("description"),
            "parameters": function.get("parameters") or {"type": "object", "properties": {}},
        }

    @staticmethod
    def _extract_responses_text(data: dict[str, Any]) -> str:
        parts: list[str] = []
        for item in data.get("output", []):
            if item.get("type") != "message":
                continue
            for content_item in item.get("content", []):
                if content_item.get("type") == "output_text":
                    parts.append(str(content_item.get("text") or ""))
        return "".join(parts).strip()

    @staticmethod
    def _extract_responses_tool_calls(data: dict[str, Any]) -> list[LLMToolCall]:
        tool_calls: list[LLMToolCall] = []
        for item in data.get("output", []):
            if item.get("type") != "function_call":
                continue
            arguments_json = str(item.get("arguments") or "{}")
            tool_calls.append(
                LLMToolCall(
                    id=str(item.get("call_id") or item.get("id") or ""),
                    name=str(item.get("name") or ""),
                    arguments_json=arguments_json,
                    arguments=parse_json_response(arguments_json),
                )
            )
        return tool_calls

    @staticmethod
    def _extract_usage(
        data: dict[str, Any],
        messages: list[dict[str, Any]],
        content: str,
    ) -> dict[str, int]:
        usage = data.get("usage") or {}
        input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
        output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))
        total_tokens = usage.get("total_tokens", 0)
        if input_tokens or output_tokens or total_tokens:
            return {
                "prompt_tokens": int(input_tokens),
                "completion_tokens": int(output_tokens),
                "total_tokens": int(total_tokens or (int(input_tokens) + int(output_tokens))),
            }
        prompt_text = "\n".join(str(message.get("content", "")) for message in messages)
        prompt_tokens = token_count(prompt_text)
        completion_tokens = token_count(content)
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        }

    def _url(self, path: str) -> str:
        base = self._resolve_base_url().rstrip("/") + "/"
        return urljoin(base, path.lstrip("/"))

    def _resolve_base_url(self) -> str:
        if self.config.base_url:
            return self.config.base_url
        provider = normalize_provider_kind(self.config.provider_kind)
        if provider in OFFICIAL_PROVIDER_BASE_URLS:
            return OFFICIAL_PROVIDER_BASE_URLS[provider]
        raise LLMError("Base URL is required for custom OpenAI-compatible providers.")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _post_json(self, path: str, payload: dict[str, Any], *, timeout: float = 90.0) -> dict[str, Any]:
        data, _meta = self._post_json_with_meta(path, payload, timeout=timeout)
        return data

    def _get_json(self, path: str, *, timeout: float = 20.0) -> dict[str, Any]:
        data, _meta = self._get_json_with_meta(path, timeout=timeout)
        return data

    def _post_json_with_meta(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        timeout: float = 90.0,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        url = self._url(path)
        try:
            with _LLM_REQUEST_SEMAPHORE:
                with httpx.Client(timeout=timeout) as client:
                    response = client.post(url, headers=self._headers(), json=payload)
                    response_text = response.text
        except Exception as exc:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "POST",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "request_body": payload,
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise LLMError(f"Request failed: {exc}", request_url=url) from exc

        if response.is_error:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "POST",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "request_body": payload,
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": f"HTTP {response.status_code}",
                }
            )
            raise LLMError(
                f"Remote service returned HTTP {response.status_code}.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            )

        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as exc:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "POST",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "request_body": payload,
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": f"JSON decode failed: {exc}",
                }
            )
            raise LLMError(
                "Invalid JSON response from remote service.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            ) from exc

        if not isinstance(data, dict):
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "POST",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "request_body": payload,
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": "Top-level response JSON was not an object.",
                }
            )
            raise LLMError(
                "Invalid JSON response from remote service.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            )

        self._append_log(
            {
                "timestamp": _utcnow_iso(),
                "method": "POST",
                "url": url,
                "provider_kind": self.config.provider_kind,
                "api_mode": normalize_api_mode(self.config.api_mode),
                "request_body": payload,
                "status_code": response.status_code,
                "response_text": response_text,
                "ok": True,
            }
        )
        return data, {"url": url, "status_code": response.status_code, "response_text": response_text}

    def _get_json_with_meta(self, path: str, *, timeout: float = 20.0) -> tuple[dict[str, Any], dict[str, Any]]:
        url = self._url(path)
        try:
            with _LLM_REQUEST_SEMAPHORE:
                with httpx.Client(timeout=timeout) as client:
                    response = client.get(url, headers=self._headers())
                    response_text = response.text
        except Exception as exc:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "GET",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise LLMError(f"Request failed: {exc}", request_url=url) from exc

        if response.is_error:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "GET",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": f"HTTP {response.status_code}",
                }
            )
            raise LLMError(
                f"Remote service returned HTTP {response.status_code}.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            )

        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as exc:
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "GET",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": f"JSON decode failed: {exc}",
                }
            )
            raise LLMError(
                "Invalid JSON response from remote service.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            ) from exc

        if not isinstance(data, dict):
            self._append_log(
                {
                    "timestamp": _utcnow_iso(),
                    "method": "GET",
                    "url": url,
                    "provider_kind": self.config.provider_kind,
                    "api_mode": normalize_api_mode(self.config.api_mode),
                    "status_code": response.status_code,
                    "response_text": response_text,
                    "ok": False,
                    "error": "Top-level response JSON was not an object.",
                }
            )
            raise LLMError(
                "Invalid JSON response from remote service.",
                raw_text=response_text,
                request_url=url,
                status_code=response.status_code,
            )

        self._append_log(
            {
                "timestamp": _utcnow_iso(),
                "method": "GET",
                "url": url,
                "provider_kind": self.config.provider_kind,
                "api_mode": normalize_api_mode(self.config.api_mode),
                "status_code": response.status_code,
                "response_text": response_text,
                "ok": True,
            }
        )
        return data, {"url": url, "status_code": response.status_code, "response_text": response_text}

    def _append_log(self, record: dict[str, Any]) -> None:
        if not self.log_path:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with _LLM_LOG_LOCK:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def parse_json_response(text: str) -> dict[str, Any]:
    body = text.strip()
    if body.startswith("```"):
        lines = [line for line in body.splitlines() if not line.startswith("```")]
        body = "\n".join(lines)
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        start = body.find("{")
        end = body.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise LLMError("Model did not return valid JSON.", raw_text=body)
        try:
            return json.loads(body[start : end + 1])
        except json.JSONDecodeError as exc:
            raise LLMError("Model did not return valid JSON.", raw_text=body) from exc


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
