from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.llm.client_runtime import OpenAICompatibleClient
from app.schemas import ChatCompletionResult, ServiceConfig, ToolRoundResult


class LLMGateway:
    def __init__(self, config: ServiceConfig, *, log_path: str | None = None) -> None:
        self.client = OpenAICompatibleClient(config, log_path=log_path)

    def list_models(self) -> list[str]:
        return self.client.list_models()

    def resolve_model(self) -> str:
        return self.client.resolve_model()

    def endpoint_url(self, path: str) -> str:
        return self.client.endpoint_url(path)

    def chat_completion(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
        max_tokens: int | None = 1400,
    ) -> str:
        return self.client.chat_completion(
            messages,
            model=model,
            temperature=temperature,
            response_format=response_format,
            max_tokens=max_tokens,
        )

    def chat_completion_result(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
        max_tokens: int | None = 1400,
        stream_handler: Callable[[str], None] | None = None,
        timeout: float | None = None,
    ) -> ChatCompletionResult:
        return self.client.chat_completion_result(
            messages,
            model=model,
            temperature=temperature,
            response_format=response_format,
            max_tokens=max_tokens,
            stream_handler=stream_handler,
            timeout=timeout,
        )

    def tool_round(
        self,
        messages: list[dict[str, Any]],
        *,
        tools: list[dict[str, Any]],
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = 1400,
        timeout: float | None = None,
        stream_handler: Callable[[str], None] | None = None,
    ) -> ToolRoundResult:
        return self.client.tool_round(
            messages,
            tools=tools,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            stream_handler=stream_handler,
        )
