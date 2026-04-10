from __future__ import annotations

import json
from typing import Any
from urllib.parse import urljoin

import httpx

from app.schemas import ChatCompletionResult, ServiceConfig
from app.utils.text import token_count

OFFICIAL_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
}


class LLMError(Exception):
    """Raised when the remote LLM service returns an invalid response."""


def normalize_provider_kind(provider_kind: str | None) -> str:
    provider = (provider_kind or "openai-compatible").strip().lower()
    aliases = {
        "openai_compatible": "openai-compatible",
        "custom": "openai-compatible",
    }
    return aliases.get(provider, provider)


class OpenAICompatibleClient:
    def __init__(self, config: ServiceConfig) -> None:
        self.config = config

    def list_models(self) -> list[str]:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(self._url("/models"), headers=self._headers())
            response.raise_for_status()
            payload = response.json()
        data = payload.get("data", [])
        return [item["id"] for item in data if item.get("id")]


    def resolve_model(self) -> str:
        if self.config.model:
            return self.config.model
        models = self.list_models()
        if not models:
            raise LLMError("No models discovered from the configured service.")
        return models[0]


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
        payload: dict[str, Any] = {
            "model": resolved_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format
        with httpx.Client(timeout=90.0) as client:
            response = client.post(
                self._url("/chat/completions"),
                headers=self._headers(),
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError("Invalid chat completion response structure.") from exc
        usage = data.get("usage") or {}
        if not usage:
            prompt_text = "\n".join(str(message.get("content", "")) for message in messages)
            prompt_tokens = token_count(prompt_text)
            completion_tokens = token_count(content)
            usage = {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            }
        return ChatCompletionResult(
            content=content,
            model=str(data.get("model") or resolved_model),
            usage={
                "prompt_tokens": int(usage.get("prompt_tokens", 0)),
                "completion_tokens": int(usage.get("completion_tokens", 0)),
                "total_tokens": int(usage.get("total_tokens", 0)),
            },
        )


    def embeddings(self, inputs: list[str], *, model: str | None = None) -> list[list[float]]:
        payload = {"model": model or self.resolve_model(), "input": inputs}
        with httpx.Client(timeout=90.0) as client:
            response = client.post(
                self._url("/embeddings"),
                headers=self._headers(),
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
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
            raise LLMError("Model did not return valid JSON.")
        return json.loads(body[start : end + 1])
