"""自主 agent 会话执行器。

这个文件只管一件事：把 LLM 会话、tool call 路由、工具结果回填和进度事件
串成一个可复用的多轮循环。具体工具怎么实现，由 ``tools.py`` 和各模式
自己的工具回调来负责。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.schemas import ChatCompletionResult, LLMToolCall
from app.service.common.llm.client import LLMError, OpenAICompatibleClient

ProgressSink = Callable[[str, dict[str, Any]], None]
ToolExecutor = Callable[[LLMToolCall], Any]


@dataclass(slots=True)
class AgentRunResult:
    content: str
    model: str
    usage: dict[str, int]
    completed: bool
    rounds: int
    provider_response_id: str | None = None
    tool_calls: list[LLMToolCall] = field(default_factory=list)
    tool_results: list[dict[str, Any]] = field(default_factory=list)
    messages: list[dict[str, Any]] = field(default_factory=list)
    trace: list[dict[str, Any]] = field(default_factory=list)
    blocks: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)


class AgentHandler:
    """统一管理需要自主调用工具的 agent。"""

    def __init__(self, client: OpenAICompatibleClient, *, progress_sink: ProgressSink | None = None) -> None:
        self.client = client
        self.progress_sink = progress_sink

    def run_completion(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = 1400,
        timeout: float | None = None,
        agent_name: str = "agent",
        request_key: str | None = None,
        label: str | None = None,
        stream_handler: Callable[[str], None] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> ChatCompletionResult:
        """执行一次纯 completion，并把进度同步到 SSE/trace sink。"""

        request_key = request_key or f"{agent_name}-completion"
        label = label or agent_name
        payload = {
            "agent": agent_name,
            "request_key": request_key,
            "label": label,
            **dict(extra or {}),
        }
        self._emit("llm_request_started", {**payload, "request_kind": "chat_completion"})
        result = self.client.chat_completion_result(
            messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            stream_handler=stream_handler,
        )
        self._emit(
            "llm_request_completed",
            {
                **payload,
                "request_kind": "chat_completion",
                "usage": dict(result.usage or {}),
                "response_text_preview": self._preview_text(result.content),
                "provider_response_id": result.response_id,
            },
        )
        return result

    def run_tool_loop(
        self,
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
        *,
        tool_executor: ToolExecutor,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 1400,
        timeout: float | None = None,
        max_rounds: int = 6,
        agent_name: str = "agent",
        stage: str = "agent",
        request_key_factory: Callable[[int], str] | None = None,
        label_factory: Callable[[int], str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> AgentRunResult:
        """执行一个可自主调用工具的多轮会话。"""

        current_messages = [dict(message) for message in messages]
        usage_totals: dict[str, int] = {}
        provider_response_id: str | None = None
        tool_results: list[dict[str, Any]] = []
        trace: list[dict[str, Any]] = []
        blocks: list[dict[str, Any]] = []
        artifacts: list[str] = []
        latest_model = model or self.client.resolve_model()

        for round_index in range(1, max_rounds + 1):
            request_key = request_key_factory(round_index) if request_key_factory else f"{agent_name}-round-{round_index}"
            label = label_factory(round_index) if label_factory else f"{agent_name} round {round_index}"
            context_payload = {
                "agent": agent_name,
                "stage": stage,
                "request_key": request_key,
                "round_index": round_index,
                "label": label,
                **dict(extra or {}),
            }
            blocks.append({"type": "status", "label": label})
            trace.append({"type": "llm_request_started", "payload": {**context_payload, "request_kind": "tool_round"}})
            self._emit("llm_request_started", {**context_payload, "request_kind": "tool_round", "tool_names": [self._schema_name(schema) for schema in tool_schemas]})

            round_result = self.client.tool_round(
                current_messages,
                tool_schemas,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
            )
            latest_model = round_result.model or latest_model
            provider_response_id = round_result.provider_response_id or provider_response_id
            for key, value in (round_result.usage or {}).items():
                usage_totals[key] = usage_totals.get(key, 0) + int(value or 0)

            round_trace_payload = {
                **context_payload,
                "request_kind": "tool_round",
                "usage": dict(round_result.usage or {}),
                "response_text_preview": self._preview_text(round_result.content),
                "tool_calls": [{"name": call.name, "arguments": call.arguments} for call in round_result.tool_calls],
                "provider_response_id": round_result.provider_response_id,
            }
            trace.append({"type": "llm_request_completed", "payload": round_trace_payload})
            self._emit("llm_request_completed", round_trace_payload)

            assistant_message: dict[str, Any] = {"role": "assistant", "content": round_result.content}
            if round_result.tool_calls:
                assistant_message["tool_calls"] = [
                    {
                        "id": call.id,
                        "name": call.name,
                        "arguments_json": call.arguments_json,
                    }
                    for call in round_result.tool_calls
                ]
            current_messages.append(assistant_message)
            blocks.append(
                {
                    "type": "assistant_message",
                    "content": round_result.content,
                    "tool_calls": [
                        {
                            "id": call.id,
                            "name": call.name,
                            "arguments_json": call.arguments_json,
                        }
                        for call in round_result.tool_calls
                    ],
                }
            )

            if not round_result.tool_calls:
                return AgentRunResult(
                    content=round_result.content,
                    model=latest_model,
                    usage=usage_totals,
                    completed=True,
                    rounds=round_index,
                    provider_response_id=provider_response_id,
                    tool_calls=[],
                    tool_results=tool_results,
                    messages=current_messages,
                    trace=trace,
                    blocks=blocks,
                    artifacts=artifacts,
                )

            for tool_call in round_result.tool_calls:
                tool_call_payload = {
                    **context_payload,
                    "tool_name": tool_call.name,
                    "arguments": dict(tool_call.arguments or {}),
                }
                trace.append({"type": "tool_call", "payload": tool_call_payload})
                blocks.append(
                    {
                        "type": "tool_call",
                        "name": tool_call.name,
                        "arguments": dict(tool_call.arguments or {}),
                    }
                )
                self._emit("tool_call", tool_call_payload)

                try:
                    raw_output = tool_executor(tool_call)
                except Exception as exc:
                    raw_output = {
                        "ok": False,
                        "value": None,
                        "lint": [f"工具执行失败：{exc}"],
                        "error": str(exc),
                    }

                payload, meta = self._normalize_tool_output(raw_output)
                tool_result_payload = {
                    **context_payload,
                    "tool_name": tool_call.name,
                    "output": payload,
                }
                if meta:
                    tool_result_payload["meta"] = meta
                trace.append({"type": "tool_result", "payload": tool_result_payload})
                blocks.append(
                    {
                        "type": "tool_result",
                        "name": tool_call.name,
                        "output": payload,
                    }
                )
                self._emit("tool_result", tool_result_payload)
                tool_results.append(
                    {
                        "name": tool_call.name,
                        "arguments": dict(tool_call.arguments or {}),
                        "output": payload,
                        "meta": meta,
                    }
                )
                artifacts.extend(self._collect_artifact_ids(payload))
                current_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": tool_call.name,
                        "content": json.dumps(payload, ensure_ascii=False),
                    }
                )
                if payload.get("artifacts"):
                    for artifact in payload.get("artifacts") or []:
                        if isinstance(artifact, dict) and artifact.get("id"):
                            blocks.append({"type": "artifact", **artifact})

        self._emit(
            "llm_request_failed",
            {
                "agent": agent_name,
                "stage": stage,
                "request_kind": "tool_round",
                "reason": "max_rounds_exceeded",
                "max_rounds": max_rounds,
            },
        )
        raise LLMError(f"{agent_name} exceeded the maximum number of tool rounds: {max_rounds}.")

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.progress_sink:
            self.progress_sink(event_type, payload)

    @staticmethod
    def _normalize_tool_output(raw_output: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        meta: dict[str, Any] = {}
        payload = raw_output
        if isinstance(raw_output, tuple) and len(raw_output) == 2:
            payload, meta = raw_output
            if not isinstance(meta, dict):
                meta = {"value": meta}
        elif isinstance(raw_output, dict) and isinstance(raw_output.get("meta"), dict):
            meta = dict(raw_output.get("meta") or {})
        if payload is None:
            payload = {}
        if hasattr(payload, "to_payload"):
            payload = payload.to_payload()
        if not isinstance(payload, dict):
            payload = {"value": payload}
        payload.setdefault("ok", not bool(payload.get("error")))
        payload.setdefault("lint", [])
        return payload, meta

    @staticmethod
    def _collect_artifact_ids(payload: dict[str, Any]) -> list[str]:
        ids: list[str] = []
        for item in payload.get("artifacts") or []:
            if isinstance(item, dict) and item.get("id"):
                ids.append(str(item["id"]))
        return ids

    @staticmethod
    def _schema_name(schema: dict[str, Any]) -> str:
        function = schema.get("function") or {}
        return str(function.get("name") or "").strip()

    @staticmethod
    def _preview_text(value: Any, *, limit: int = 240) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value
        else:
            try:
                text = json.dumps(value, ensure_ascii=False, indent=2)
            except TypeError:
                text = str(value)
        text = text.strip()
        if len(text) <= limit:
            return text
        return f"{text[:limit]}..."
