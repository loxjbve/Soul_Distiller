from __future__ import annotations

import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db import Database
from app.llm.client import LLMError, OpenAICompatibleClient
from app.models import ChatTurn, DocumentRecord
from app.preprocess.tools import build_tool_schemas
from app.runtime_limits import background_task_slot
from app.schemas import LLMToolCall, ServiceConfig
from app.storage import repository

MENTION_PATTERN = re.compile(r'@"([^"]+)"|@([^\s@]+)')
MAX_TOOL_STEPS = 6
MAX_STDIO_CHARS = 8000
MAX_OUTPUT_BYTES = 2_000_000


@dataclass(slots=True)
class ResolvedMentions:
    documents: list[DocumentRecord]
    query_tokens: list[str]
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StreamState:
    id: str
    project_id: str
    session_id: str
    user_turn_id: str
    message: str
    events: Queue[dict[str, Any]] = field(default_factory=Queue)
    done: Event = field(default_factory=Event)


class PreprocessAgentService:
    def __init__(
        self,
        db: Database,
        config,
        retrieval,
        *,
        max_workers: int = 4,
        run_inline: bool = False,
    ) -> None:
        self.db = db
        self.config = config
        self.retrieval = retrieval
        self.run_inline = run_inline
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="preprocess-agent")
        self.streams: dict[str, StreamState] = {}
        self.futures: dict[str, Future[None]] = {}
        self.lock = Lock()

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def start_stream(self, *, project_id: str, session_id: str, message: str) -> dict[str, str]:
        with self.db.session() as session:
            chat_session = repository.get_chat_session(session, session_id, session_kind="preprocess")
            if not chat_session or chat_session.project_id != project_id:
                raise ValueError("Preprocess session not found.")
            user_turn = repository.add_chat_turn(
                session,
                session_id=session_id,
                role="user",
                content=message,
                trace_json={"kind": "preprocess_user_message"},
            )
            if not chat_session.title:
                repository.rename_chat_session(session, chat_session, title=_derive_session_title(message))
            stream_id = str(uuid4())
            state = StreamState(
                id=stream_id,
                project_id=project_id,
                session_id=session_id,
                user_turn_id=user_turn.id,
                message=message,
            )
            with self.lock:
                self.streams[stream_id] = state
        if self.run_inline:
            self._execute(state)
        else:
            future = self.executor.submit(self._execute, state)
            self.futures[stream_id] = future
        return {"stream_id": stream_id, "user_turn_id": user_turn.id}

    def stream_events(self, stream_id: str):
        with self.lock:
            state = self.streams.get(stream_id)
        if not state:
            raise KeyError(stream_id)
        while True:
            try:
                event = state.events.get(timeout=0.25)
            except Empty:
                if state.done.is_set():
                    break
                continue
            yield _format_sse(event["type"], event["payload"])
            if state.done.is_set() and state.events.empty():
                break
        with self.lock:
            self.streams.pop(stream_id, None)
            self.futures.pop(stream_id, None)

    def list_mentions(self, session: Session, project_id: str, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        documents = repository.search_project_documents(session, project_id, query, limit=limit) if query else repository.list_project_documents(session, project_id)[:limit]
        results = []
        for document in documents:
            results.append(
                {
                    "id": document.id,
                    "filename": document.filename,
                    "title": document.title or document.filename,
                    "source_type": document.source_type,
                    "status": document.ingest_status,
                }
            )
        return results

    def _execute(self, state: StreamState) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    self._run_turn(session, state)
        except Exception as exc:
            self._emit(state, "error", {"message": str(exc)})
            self._emit(state, "stream_error", {"message": str(exc)})
            with self.db.session() as session:
                chat_session = repository.get_chat_session(session, state.session_id, session_kind="preprocess")
                if chat_session:
                    repository.add_chat_turn(
                        session,
                        session_id=state.session_id,
                        role="assistant",
                        content=f"预分析执行失败：{exc}",
                        trace_json={
                            "kind": "preprocess_agent",
                            "blocks": [{"type": "error", "message": str(exc)}],
                            "resolved_mentions": [],
                            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                            "llm": {"provider_kind": "local", "api_mode": "responses", "model": "fallback"},
                            "provider_response_id": None,
                        },
                    )
        finally:
            state.done.set()

    def _run_turn(self, session: Session, state: StreamState) -> None:
        self._emit(state, "status", {"label": "准备上下文"})
        blocks: list[dict[str, Any]] = [{"type": "status", "label": "准备上下文"}]
        config = repository.get_service_config(session, "chat_service")
        embedding_config = repository.get_service_config(session, "embedding_service")
        chat_session = repository.get_chat_session(session, state.session_id, session_kind="preprocess")
        if not chat_session:
            raise ValueError("Preprocess session not found.")
        project = repository.get_project(session, state.project_id)
        if not project:
            raise ValueError("Project not found.")
        turns = repository.list_chat_turns(session, state.session_id)
        mentions = self._resolve_mentions(session, state.project_id, state.message)
        if mentions.errors:
            assistant_text = "\n".join(mentions.errors)
            trace = {
                "kind": "preprocess_agent",
                "blocks": blocks + [{"type": "status", "label": "等待澄清"}],
                "resolved_mentions": [],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "llm": {
                    "provider_kind": config.provider_kind if config else "local",
                    "api_mode": config.api_mode if config else "responses",
                    "model": config.model or "fallback" if config else "fallback",
                },
                "provider_response_id": None,
            }
            assistant_turn = repository.add_chat_turn(
                session,
                session_id=state.session_id,
                role="assistant",
                content=assistant_text,
                trace_json=trace,
            )
            self._stream_assistant(state, assistant_text, assistant_turn.id)
            return

        resolved_mentions = [
            {
                "id": document.id,
                "filename": document.filename,
                "title": document.title or document.filename,
            }
            for document in mentions.documents
        ]
        self._emit(
            state,
            "status",
            {
                "label": (
                    f"已读取 {len(resolved_mentions)} 个文件"
                    if resolved_mentions
                    else "未指定文件，整个项目工作区可用"
                )
            },
        )
        blocks.append(
            {
                "type": "status",
                "label": (
                    f"已读取 {len(resolved_mentions)} 个文件"
                    if resolved_mentions
                    else "未指定文件，整个项目工作区可用"
                ),
            }
        )

        if not config:
            assistant_text = self._fallback_response(session, state.project_id, state.message, mentions.documents)
            assistant_turn = repository.add_chat_turn(
                session,
                session_id=state.session_id,
                role="assistant",
                content=assistant_text,
                trace_json={
                    "kind": "preprocess_agent",
                    "blocks": blocks,
                    "resolved_mentions": resolved_mentions,
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                    "llm": {"provider_kind": "local", "api_mode": "responses", "model": "fallback"},
                    "provider_response_id": None,
                },
            )
            self._stream_assistant(state, assistant_text, assistant_turn.id)
            return

        client = OpenAICompatibleClient(config, log_path=str(self.config.llm_log_path))
        messages = self._build_messages(turns[:-1], state.message, project.name, mentions.documents)
        usage_totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        provider_response_id: str | None = None
        artifact_ids: list[str] = []
        assistant_text = ""

        for step_index in range(MAX_TOOL_STEPS):
            self._emit(state, "status", {"label": f"推理中 · step {step_index + 1}"})
            blocks.append({"type": "status", "label": f"推理中 · step {step_index + 1}"})
            try:
                result = client.tool_round(messages, build_tool_schemas(), model=config.model, temperature=0.2, max_tokens=1400)
            except Exception as exc:
                raise ValueError(f"Native tool request failed: {exc}") from exc
            provider_response_id = result.provider_response_id or provider_response_id
            for key in usage_totals:
                usage_totals[key] += int(result.usage.get(key, 0))

            if result.tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": result.content,
                        "tool_calls": [
                            {
                                "id": call.id,
                                "name": call.name,
                                "arguments_json": call.arguments_json,
                            }
                            for call in result.tool_calls
                        ],
                    }
                )
                for tool_call in result.tool_calls:
                    blocks.append(
                        {
                            "type": "tool_call",
                            "name": tool_call.name,
                            "arguments": tool_call.arguments,
                        }
                    )
                    self._emit(
                        state,
                        "tool_call",
                        {
                            "name": tool_call.name,
                            "arguments": tool_call.arguments,
                        },
                    )
                    tool_result = self._execute_tool(
                        session,
                        project_id=state.project_id,
                        session_id=state.session_id,
                        tool_call=tool_call,
                        embedding_config=embedding_config,
                    )
                    tool_payload = json.dumps(tool_result, ensure_ascii=False, indent=2)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "name": tool_call.name,
                            "content": tool_payload,
                        }
                    )
                    blocks.append(
                        {
                            "type": "tool_result",
                            "name": tool_call.name,
                            "output": tool_result,
                        }
                    )
                    self._emit(
                        state,
                        "tool_result",
                        {
                            "name": tool_call.name,
                            "output": tool_result,
                        },
                    )
                    for artifact in tool_result.get("artifacts", []):
                        artifact_id = artifact.get("id")
                        if artifact_id:
                            artifact_ids.append(artifact_id)
                        blocks.append({"type": "artifact", **artifact})
                continue
            assistant_text = (result.content or "").strip() or "已完成预分析。"
            break
        else:
            assistant_text = "本轮工具调用达到上限，请缩小范围或指定更明确的文件。"

        assistant_turn = repository.add_chat_turn(
            session,
            session_id=state.session_id,
            role="assistant",
            content=assistant_text,
            trace_json={
                "kind": "preprocess_agent",
                "blocks": blocks,
                "resolved_mentions": resolved_mentions,
                "usage": usage_totals,
                "llm": {
                    "provider_kind": config.provider_kind,
                    "api_mode": config.api_mode,
                    "model": config.model or client.resolve_model(),
                },
                "provider_response_id": provider_response_id,
            },
        )
        repository.attach_artifacts_to_turn(session, artifact_ids, turn_id=assistant_turn.id)
        self._stream_assistant(state, assistant_text, assistant_turn.id)

    def _build_messages(
        self,
        history: Iterable[ChatTurn],
        message: str,
        project_name: str,
        mention_documents: list[DocumentRecord],
    ) -> list[dict[str, Any]]:
        scope_lines = []
        if mention_documents:
            scope_lines.append("The user explicitly referenced these files in this turn:")
            for document in mention_documents:
                scope_lines.append(f"- {document.filename} ({document.id})")
        else:
            scope_lines.append("No explicit file mentions were resolved for this turn.")
            scope_lines.append("You can still inspect project files using the available tools.")
        system_prompt = (
            "You are a preprocessing analyst working inside a local project workspace.\n"
            "Use tools whenever you need file lists, source excerpts, search results, or Python-based transformations.\n"
            "Do not invent file contents. Prefer concise Markdown answers.\n"
            "When creating files, use run_python_transform and explain what was produced.\n"
            f"Project: {project_name}\n"
            + "\n".join(scope_lines)
        )
        messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
        for turn in list(history)[-10:]:
            if turn.role not in {"user", "assistant"}:
                continue
            messages.append({"role": turn.role, "content": turn.content})
        messages.append({"role": "user", "content": message})
        return messages

    def _resolve_mentions(self, session: Session, project_id: str, message: str) -> ResolvedMentions:
        documents = repository.list_project_documents(session, project_id)
        mentions = [item[0] or item[1] for item in MENTION_PATTERN.findall(message)]
        if not mentions:
            return ResolvedMentions(documents=[], query_tokens=[])

        resolved: list[DocumentRecord] = []
        errors: list[str] = []
        seen_ids: set[str] = set()
        for mention in mentions:
            match = self._match_document(documents, mention)
            if isinstance(match, list):
                candidate_names = ", ".join(item.filename for item in match[:6])
                errors.append(f"`@{mention}` 匹配到多个文件，请更精确一些：{candidate_names}")
                continue
            if not match:
                errors.append(f"没有找到 `@{mention}` 对应的文件。")
                continue
            if match.id not in seen_ids:
                seen_ids.add(match.id)
                resolved.append(match)
        return ResolvedMentions(documents=resolved, query_tokens=mentions, errors=errors)

    @staticmethod
    def _match_document(documents: list[DocumentRecord], token: str) -> DocumentRecord | list[DocumentRecord] | None:
        needle = token.strip().lower()
        if not needle:
            return None
        exact_matches = [
            document
            for document in documents
            if document.filename.lower() == needle or (document.title or "").lower() == needle
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            return exact_matches
        prefix_matches = [
            document
            for document in documents
            if document.filename.lower().startswith(needle) or (document.title or "").lower().startswith(needle)
        ]
        if len(prefix_matches) == 1:
            return prefix_matches[0]
        if len(prefix_matches) > 1:
            return prefix_matches
        return None

    def _execute_tool(
        self,
        session: Session,
        *,
        project_id: str,
        session_id: str,
        tool_call: LLMToolCall,
        embedding_config: ServiceConfig | None,
    ) -> dict[str, Any]:
        name = tool_call.name
        args = tool_call.arguments
        if name == "list_project_documents":
            query = str(args.get("query") or "").strip()
            limit = max(1, min(int(args.get("limit", 12)), 50))
            documents = repository.search_project_documents(session, project_id, query, limit=limit) if query else repository.list_project_documents(session, project_id)[:limit]
            return {
                "documents": [
                    {
                        "document_id": document.id,
                        "filename": document.filename,
                        "title": document.title or document.filename,
                        "source_type": document.source_type,
                        "language": document.language,
                        "char_count": len(document.clean_text or ""),
                    }
                    for document in documents
                ]
            }
        if name == "read_project_documents":
            document_ids = [str(item) for item in args.get("document_ids", [])]
            max_chars = max(200, min(int(args.get("max_chars_per_doc", 4000)), 12000))
            include_metadata = bool(args.get("include_metadata", True))
            documents = repository.list_project_documents_by_ids(session, project_id, document_ids)
            return {
                "documents": [
                    {
                        "document_id": document.id,
                        "filename": document.filename,
                        "title": document.title or document.filename,
                        "truncated": len(document.clean_text or "") > max_chars,
                        "content": (document.clean_text or "")[:max_chars],
                        "metadata": (
                            {
                                "source_type": document.source_type,
                                "language": document.language,
                                "user_note": (document.metadata_json or {}).get("user_note"),
                            }
                            if include_metadata
                            else None
                        ),
                    }
                    for document in documents
                ]
            }
        if name == "search_project_documents":
            query = str(args.get("query") or "").strip()
            if not query:
                return {"hits": []}
            allowed_ids = {str(item) for item in args.get("document_ids", []) if str(item).strip()}
            limit = max(1, min(int(args.get("limit", 6)), 20))
            hits, retrieval_mode, retrieval_trace = self.retrieval.search(
                session,
                project_id=project_id,
                query=query,
                embedding_config=embedding_config,
                llm_config=self.config,
                log_path=str(self.config.llm_log_path),
                limit=max(limit * 3, 8),
            )
            filtered_hits = [hit for hit in hits if not allowed_ids or hit.document_id in allowed_ids][:limit]
            return {
                "retrieval_mode": retrieval_mode,
                "retrieval_trace": retrieval_trace,
                "hits": [
                    {
                        "chunk_id": hit.chunk_id,
                        "anchor_chunk_id": hit.anchor_chunk_id or hit.chunk_id,
                        "anchor_chunk_index": hit.anchor_chunk_index,
                        "document_id": hit.document_id,
                        "filename": hit.filename,
                        "snippet": hit.content[:900],
                        "score": hit.score,
                        "page_number": hit.page_number,
                        "context_span": dict(hit.context_span or {}),
                    }
                    for hit in filtered_hits
                ],
            }
        if name == "run_python_transform":
            return self._run_python_transform(session, project_id=project_id, session_id=session_id, args=args)
        if name == "list_session_artifacts":
            limit = max(1, min(int(args.get("limit", 20)), 50))
            artifacts = repository.list_session_artifacts(session, session_id, limit=limit)
            return {
                "artifacts": [
                    {
                        "id": artifact.id,
                        "filename": artifact.filename,
                        "summary": artifact.summary,
                        "created_at": artifact.created_at.isoformat(),
                    }
                    for artifact in artifacts
                ]
            }
        return {"error": f"Unknown tool: {name}"}

    def _run_python_transform(
        self,
        session: Session,
        *,
        project_id: str,
        session_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        run_id = str(uuid4())
        run_dir = self.config.output_dir / project_id / session_id / "runs" / run_id
        inputs_dir = run_dir / "inputs"
        docs_dir = inputs_dir / "documents"
        artifacts_dir = inputs_dir / "artifacts"
        outputs_dir = run_dir / "outputs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        outputs_dir.mkdir(parents=True, exist_ok=True)

        manifest: dict[str, Any] = {
            "intent": str(args.get("intent") or ""),
            "documents": [],
            "artifacts": [],
            "expected_output_files": [str(item) for item in args.get("expected_output_files", [])],
            "output_dir": str(outputs_dir),
        }

        input_document_ids = [str(item) for item in args.get("input_document_ids", [])]
        for document in repository.list_project_documents_by_ids(session, project_id, input_document_ids):
            safe_name = f"{document.id}_{Path(document.filename).name}.txt"
            file_path = docs_dir / safe_name
            file_path.write_text(document.clean_text or "", encoding="utf-8")
            manifest["documents"].append(
                {
                    "document_id": document.id,
                    "filename": document.filename,
                    "title": document.title or document.filename,
                    "path": str(file_path),
                }
            )

        input_artifact_ids = [str(item) for item in args.get("input_artifact_ids", [])]
        for artifact_id in input_artifact_ids:
            artifact = repository.get_generated_artifact(session, artifact_id)
            if not artifact or artifact.session_id != session_id:
                continue
            source = Path(artifact.storage_path)
            if not source.exists() or not source.is_file():
                continue
            destination = artifacts_dir / f"{artifact.id}_{source.name}"
            shutil.copy2(source, destination)
            manifest["artifacts"].append(
                {
                    "artifact_id": artifact.id,
                    "filename": artifact.filename,
                    "path": str(destination),
                }
            )

        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        env = os.environ.copy()
        env["PREPROCESS_RUN_DIR"] = str(run_dir)
        env["PREPROCESS_INPUT_DIR"] = str(inputs_dir)
        env["PREPROCESS_OUTPUT_DIR"] = str(outputs_dir)
        env["PREPROCESS_MANIFEST"] = str(manifest_path)
        try:
            process = subprocess.run(
                [sys.executable, "-I", "-c", str(args.get("python_code") or "")],
                cwd=run_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            stdout = process.stdout[:MAX_STDIO_CHARS]
            stderr = process.stderr[:MAX_STDIO_CHARS]
            exit_code = process.returncode
        except subprocess.TimeoutExpired as exc:
            stdout = (exc.stdout or "")[:MAX_STDIO_CHARS] if isinstance(exc.stdout, str) else ""
            stderr = ((exc.stderr or "") + "\nExecution timed out.")[:MAX_STDIO_CHARS] if isinstance(exc.stderr, str) else "Execution timed out."
            exit_code = -1

        created_artifacts: list[dict[str, Any]] = []
        for output_file in sorted(path for path in outputs_dir.rglob("*") if path.is_file()):
            if output_file.stat().st_size > MAX_OUTPUT_BYTES:
                continue
            mime_type = mimetypes.guess_type(output_file.name)[0]
            artifact = repository.create_generated_artifact(
                session,
                project_id=project_id,
                session_id=session_id,
                turn_id=None,
                filename=output_file.name,
                mime_type=mime_type,
                storage_path=str(output_file),
                summary=str(args.get("intent") or "").strip() or "Generated by run_python_transform",
            )
            created_artifacts.append(
                {
                    "id": artifact.id,
                    "filename": artifact.filename,
                    "mime_type": artifact.mime_type,
                    "summary": artifact.summary,
                }
            )

        return {
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "artifacts": created_artifacts,
        }

    def _fallback_response(
        self,
        session: Session,
        project_id: str,
        message: str,
        documents: list[DocumentRecord],
    ) -> str:
        if not documents:
            documents = [doc for doc in repository.list_project_documents(session, project_id) if doc.ingest_status == "ready"][:3]
        if not documents:
            return (
                "当前项目里还没有可用文档。先上传文件，或者在问题里通过 `@文件名` 指定你要分析的文件。"
            )
        lines = ["当前处于本地降级模式，我先基于已解析文档给你一个简要摘要。", ""]
        for document in documents[:3]:
            excerpt = (document.clean_text or "").strip().replace("\n", " ")
            lines.append(f"### {document.title or document.filename}")
            lines.append(excerpt[:220] + ("..." if len(excerpt) > 220 else ""))
            lines.append("")
        lines.append(f"你的问题是：{message}")
        return "\n".join(lines).strip()

    def _stream_assistant(self, state: StreamState, text: str, assistant_turn_id: str) -> None:
        for chunk in _chunk_text(text):
            self._emit(state, "assistant_delta", {"delta": chunk})
        self._emit(
            state,
            "assistant_done",
            {
                "assistant_turn_id": assistant_turn_id,
                "content": text,
            },
        )

    @staticmethod
    def _emit(state: StreamState, event_type: str, payload: dict[str, Any]) -> None:
        state.events.put({"type": event_type, "payload": payload})


def _derive_session_title(message: str) -> str:
    cleaned = MENTION_PATTERN.sub("", message).strip()
    if not cleaned:
        return "New Preprocess Session"
    return cleaned[:48]


def _chunk_text(text: str, size: int = 120) -> list[str]:
    normalized = text or ""
    return [normalized[index : index + size] for index in range(0, len(normalized), size)] or [""]


def _format_sse(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
