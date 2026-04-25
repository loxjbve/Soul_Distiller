"""统一工具中心。

这里集中放工具的参数规范、返回值约定和可复用的工具执行逻辑。
上层 agent 只需要拿到 tool schema 和一个工具执行回调，不要再各自重复
定义 schema 或拼 lint 格式。
"""

from __future__ import annotations

import json
import mimetypes
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models import DocumentRecord
from app.retrieval.service import RetrievalService
from app.schemas import ServiceConfig
from app.storage import repository


ToolSchema = dict[str, Any]
ToolExecutor = Callable[[dict[str, Any]], Any]


@dataclass(slots=True)
class ToolResult:
    ok: bool
    value: Any = None
    lint: list[str] = field(default_factory=list)
    error: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": self.ok,
            "lint": list(self.lint),
        }
        if self.error:
            payload["error"] = self.error
        payload["value"] = self.value
        if self.meta:
            payload["meta"] = dict(self.meta)
        return payload


@dataclass(slots=True)
class WorkspaceToolContext:
    session: Session
    project_id: str
    session_id: str
    retrieval: RetrievalService
    output_dir: Path
    llm_log_path: str | None = None
    embedding_config: ServiceConfig | None = None
    llm_config: ServiceConfig | None = None


def _tool_ok(value: Any, *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    return ToolResult(ok=True, value=value, meta=dict(meta or {})).to_payload()


def _tool_error(message: str, *, lint: list[str] | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    return ToolResult(
        ok=False,
        value=None,
        lint=list(lint or [message]),
        error=message,
        meta=dict(meta or {}),
    ).to_payload()


def _normalize_schema_list(schemas: list[ToolSchema]) -> list[ToolSchema]:
    return [json.loads(json.dumps(schema, ensure_ascii=False)) for schema in schemas]


WORKSPACE_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "type": "function",
        "function": {
            "name": "list_project_documents",
            "description": "列出当前项目工作区中的文档，支持按标题或文件名过滤。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "可选的标题或文件名过滤词。"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 12},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_project_documents",
            "description": "读取一个或多个项目文档的抽取正文。",
            "parameters": {
                "type": "object",
                "properties": {
                    "document_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 12,
                    },
                    "max_chars_per_doc": {
                        "type": "integer",
                        "minimum": 200,
                        "maximum": 12000,
                        "default": 4000,
                    },
                    "include_metadata": {"type": "boolean", "default": True},
                },
                "required": ["document_ids"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_project_documents",
            "description": "在项目文档中做检索，返回相关片段。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "检索词。"},
                    "document_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 24,
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 6},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_python_transform",
            "description": "在会话工作区中运行 Python 代码，生成或转换文件。",
            "parameters": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string", "description": "这次转换的短意图说明。"},
                    "python_code": {"type": "string", "description": "可执行的 Python 代码。"},
                    "input_document_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 12,
                    },
                    "input_artifact_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 12,
                    },
                    "expected_output_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 12,
                    },
                },
                "required": ["intent", "python_code"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_session_artifacts",
            "description": "列出当前会话里已经生成的产物文件。",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                },
                "additionalProperties": False,
            },
        },
    },
)


TELEGRAM_ACTIVE_USER_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "type": "function",
        "function": {
            "name": "get_user_slice",
            "description": "获取目标用户自己的消息切片。",
            "parameters": {
                "type": "object",
                "properties": {
                    "participant_id": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_user_mentions_slice",
            "description": "获取其他用户提到该用户的消息切片。",
            "parameters": {
                "type": "object",
                "properties": {
                    "participant_id": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_database",
            "description": "分析一个紧凑切片，只返回 alias 相关的结构化 JSON。",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "participant_id": {"type": "string"},
                    "slice_kind": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
                "required": ["prompt"],
            },
        },
    },
)


TELEGRAM_WEEKLY_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "type": "function",
        "function": {
            "name": "list_weekly_candidates",
            "description": "列出周摘要候选窗口。",
            "parameters": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "week_key": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weekly_candidate",
            "description": "读取某个周摘要候选窗口的紧凑消息。",
            "parameters": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "week_key": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_top_users",
            "description": "列出已物化的活跃用户快照。",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_database",
            "description": "分析紧凑周候选切片，返回摘要、关键词和证据提示。",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "candidate_id": {"type": "string"},
                    "week_key": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
                "required": ["prompt"],
            },
        },
    },
)


TELEGRAM_ANALYSIS_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "type": "function",
        "function": {
            "name": "get_target_user_profile",
            "description": "返回目标用户的标准化分析画像。",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_related_topics",
            "description": "列出与目标用户相关的主题候选。",
            "parameters": {
                "type": "object",
                "properties": {
                    "participant_id": {"type": "string"},
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_telegram_messages",
            "description": "按主题范围查询 Telegram 消息。",
            "parameters": {
                "type": "object",
                "properties": {
                    "participant_id": {"type": "string"},
                    "topic_ids": {"type": "array", "items": {"type": "string"}},
                    "message_id_start": {"type": "integer"},
                    "message_id_end": {"type": "integer"},
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_messages",
            "description": "按 message_id 直接查消息。",
            "parameters": {
                "type": "object",
                "properties": {
                    "message_ids": {"type": "array", "items": {"type": "integer"}},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_message_context",
            "description": "围绕某条消息读取上下文。",
            "parameters": {
                "type": "object",
                "properties": {
                    "message_id": {"type": "integer"},
                    "before": {"type": "integer"},
                    "after": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_reply_chain",
            "description": "读取某条消息的回复链。",
            "parameters": {
                "type": "object",
                "properties": {
                    "message_id": {"type": "integer"},
                    "depth": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_database",
            "description": "对一小段 Telegram 切片做 JSON 分析。",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "topic_ids": {"type": "array", "items": {"type": "string"}},
                    "message_ids": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["prompt"],
            },
        },
    },
)


STONE_ANALYSIS_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "type": "function",
        "function": {
            "name": "get_corpus_overview",
            "description": "返回 Stone 语料总览。",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_article_profiles_page",
            "description": "分页读取 Stone 文章画像。",
            "parameters": {
                "type": "object",
                "properties": {
                    "offset": {"type": "integer"},
                    "limit": {"type": "integer"},
                    "query": {"type": "string"},
                    "content_type": {"type": "string"},
                    "emotion_label": {"type": "string"},
                    "length_label": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_article_profile",
            "description": "按 document_id 读取单篇文章画像。",
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {"type": "string"},
                },
                "required": ["document_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_article_text",
            "description": "按 document_id 读取文章原文。",
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {"type": "string"},
                    "start_offset": {"type": "integer"},
                    "max_chars": {"type": "integer"},
                },
                "required": ["document_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_article_text",
            "description": "在原文里搜索关键词并返回片段。",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["query"],
            },
        },
    },
)


TOOL_SCHEMA_CATALOG: dict[str, tuple[ToolSchema, ...]] = {
    "workspace": WORKSPACE_TOOL_SCHEMAS,
    "telegram_active_user": TELEGRAM_ACTIVE_USER_TOOL_SCHEMAS,
    "telegram_weekly": TELEGRAM_WEEKLY_TOOL_SCHEMAS,
    "telegram_analysis": TELEGRAM_ANALYSIS_TOOL_SCHEMAS,
    "stone_analysis": STONE_ANALYSIS_TOOL_SCHEMAS,
}


def build_tool_schemas(toolset_name: str | None = None) -> list[ToolSchema]:
    """返回给 LLM 的 OpenAI function schema 列表。

    默认仍然返回 workspace 工具，兼容当前 preprocess 流程和现有测试。
    """

    resolved_name = (toolset_name or "workspace").strip() or "workspace"
    return _normalize_schema_list(list(TOOL_SCHEMA_CATALOG.get(resolved_name, WORKSPACE_TOOL_SCHEMAS)))


def build_toolset_catalog(toolset_name: str | None = None) -> str:
    """把工具 schema 渲染成可直接放进 prompt 的中文目录文本。"""

    lines: list[str] = []
    for schema in build_tool_schemas(toolset_name):
        function = schema.get("function") or {}
        name = str(function.get("name") or "").strip()
        description = str(function.get("description") or "").strip()
        params = function.get("parameters") or {}
        required = ", ".join(params.get("required") or [])
        lines.append(f"- {name}: {description}")
        if required:
            lines.append(f"  - 必填参数: {required}")
    return "\n".join(lines).strip()


def execute_workspace_tool(context: WorkspaceToolContext, name: str, args: dict[str, Any]) -> dict[str, Any]:
    """执行 workspace 工具，并把错误标准化成 LLM 可读的 lint 结构。"""

    if name == "list_project_documents":
        query = str(args.get("query") or "").strip()
        try:
            limit = max(1, min(int(args.get("limit", 12) or 12), 50))
        except (TypeError, ValueError):
            return _tool_error("limit 必须是整数。", lint=["请把 limit 改成 1 到 50 之间的整数。"])
        documents = (
            repository.search_project_documents(context.session, context.project_id, query, limit=limit)
            if query
            else repository.list_project_documents(context.session, context.project_id)[:limit]
        )
        return _tool_ok(
            {
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
        )

    if name == "read_project_documents":
        document_ids = [str(item) for item in args.get("document_ids", []) if str(item).strip()]
        if not document_ids:
            return _tool_error(
                "document_ids 不能为空。",
                lint=["请至少提供一个 document_ids。"],
            )
        try:
            max_chars = max(200, min(int(args.get("max_chars_per_doc", 4000) or 4000), 12000))
        except (TypeError, ValueError):
            return _tool_error("max_chars_per_doc 必须是整数。", lint=["请把 max_chars_per_doc 改成整数。"])
        include_metadata = bool(args.get("include_metadata", True))
        documents = repository.list_project_documents_by_ids(context.session, context.project_id, document_ids)
        return _tool_ok(
            {
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
        )

    if name == "search_project_documents":
        query = str(args.get("query") or "").strip()
        if not query:
            return _tool_error("query 不能为空。", lint=["请先填写 query。"])
        allowed_ids = {str(item) for item in args.get("document_ids", []) if str(item).strip()}
        try:
            limit = max(1, min(int(args.get("limit", 6) or 6), 20))
        except (TypeError, ValueError):
            return _tool_error("limit 必须是整数。", lint=["请把 limit 改成整数。"])
        hits, retrieval_mode, retrieval_trace = context.retrieval.search(
            context.session,
            project_id=context.project_id,
            query=query,
            embedding_config=context.embedding_config,
            llm_config=context.llm_config,
            log_path=context.llm_log_path,
            limit=max(limit * 3, 8),
        )
        filtered_hits = [hit for hit in hits if not allowed_ids or hit.document_id in allowed_ids][:limit]
        return _tool_ok(
            {
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
        )

    if name == "run_python_transform":
        intent = str(args.get("intent") or "").strip()
        python_code = str(args.get("python_code") or "").strip()
        if not intent or not python_code:
            return _tool_error(
                "intent 或 python_code 不能为空。",
                lint=["请同时提供 intent 和 python_code。"],
            )
        run_id = str(uuid4())
        run_dir = context.output_dir / context.project_id / context.session_id / "runs" / run_id
        inputs_dir = run_dir / "inputs"
        docs_dir = inputs_dir / "documents"
        artifacts_dir = inputs_dir / "artifacts"
        outputs_dir = run_dir / "outputs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        outputs_dir.mkdir(parents=True, exist_ok=True)

        manifest: dict[str, Any] = {
            "intent": intent,
            "documents": [],
            "artifacts": [],
            "expected_output_files": [str(item) for item in args.get("expected_output_files", [])],
            "output_dir": str(outputs_dir),
        }

        input_document_ids = [str(item) for item in args.get("input_document_ids", [])]
        for document in repository.list_project_documents_by_ids(context.session, context.project_id, input_document_ids):
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
            artifact = repository.get_generated_artifact(context.session, artifact_id)
            if not artifact or artifact.session_id != context.session_id:
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
                [sys.executable, "-I", "-c", python_code],
                cwd=run_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            stdout = process.stdout[:10000]
            stderr = process.stderr[:10000]
            exit_code = process.returncode
        except subprocess.TimeoutExpired as exc:
            stdout = (exc.stdout or "")[:10000] if isinstance(exc.stdout, str) else ""
            stderr = ((exc.stderr or "") + "\nExecution timed out.")[:10000] if isinstance(exc.stderr, str) else "Execution timed out."
            exit_code = -1

        created_artifacts: list[dict[str, Any]] = []
        for output_file in sorted(path for path in outputs_dir.rglob("*") if path.is_file()):
            if output_file.stat().st_size > 10 * 1024 * 1024:
                continue
            mime_type = mimetypes.guess_type(output_file.name)[0]
            artifact = repository.create_generated_artifact(
                context.session,
                project_id=context.project_id,
                session_id=context.session_id,
                turn_id=None,
                filename=output_file.name,
                mime_type=mime_type,
                storage_path=str(output_file),
                summary=intent or "由 run_python_transform 生成",
            )
            created_artifacts.append(
                {
                    "id": artifact.id,
                    "filename": artifact.filename,
                    "mime_type": artifact.mime_type,
                    "summary": artifact.summary,
                }
            )

        return _tool_ok(
            {
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": exit_code,
                "artifacts": created_artifacts,
            }
        )

    if name == "list_session_artifacts":
        try:
            limit = max(1, min(int(args.get("limit", 20) or 20), 50))
        except (TypeError, ValueError):
            return _tool_error("limit 必须是整数。", lint=["请把 limit 改成整数。"])
        artifacts = repository.list_session_artifacts(context.session, context.session_id, limit=limit)
        return _tool_ok(
            {
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
        )

    return _tool_error(f"Unknown workspace tool: {name}", lint=[f"只允许使用 workspace 工具集内已声明的工具，当前是不支持的工具：{name}。"])
