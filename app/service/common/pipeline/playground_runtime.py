"""试聊通用运行时。

这里放和项目 mode 无关的 Skill 对话流程，避免路由层自己拼 LLM 请求。
如果后面需要更多上下文来源，优先在这里补充，而不是把聊天逻辑塞回路由。
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.service.common.llm.client import OpenAICompatibleClient
from app.schemas import ServiceConfig
from app.storage import repository


def generate_chat_reply(
    config: ServiceConfig | None,
    system_prompt: str,
    history: list[Any],
    message: str,
    evidence_block: str,
    *,
    log_path: str | None = None,
) -> tuple[str, dict[str, Any]]:
    if not config:
        prefix = "当前未配置外部 LLM，系统正在使用本地降级模式。"
        return (
            (
                f"{prefix}\n\n"
                "我会尽量按照已发布 Skill 中的语气与立场来回应。\n"
                f"你刚才说的是：{message}"
            ),
            {"provider_kind": "local", "api_mode": "responses", "model": "fallback"},
        )

    client = OpenAICompatibleClient(config, log_path=log_path)
    messages = [{"role": "system", "content": system_prompt}]
    if evidence_block:
        messages.append({"role": "system", "content": f"来源文档证据：\n{evidence_block}"})
    for turn in history[-8:]:
        messages.append({"role": turn.role, "content": turn.content})
    messages.append({"role": "user", "content": message})
    try:
        result = client.chat_completion_result(messages, model=config.model, temperature=0.7, max_tokens=900)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"对话生成失败：{exc}") from exc
    return (
        result.content,
        {
            "provider_kind": config.provider_kind,
            "api_mode": config.api_mode,
            "model": result.model,
            "usage": result.usage,
            "request_url": result.request_url,
        },
    )


def playground_chat(
    request: Request,
    session: Session,
    project_id: str,
    *,
    message: str,
    session_id: str | None = None,
) -> dict[str, Any]:
    version = repository.get_latest_skill_version(session, project_id)
    if not version:
        raise HTTPException(status_code=400, detail="请先发布一个 Skill 版本，再进入试聊。")
    chat_config = repository.get_service_config(session, "chat_service")
    if session_id:
        chat_session = repository.get_chat_session(session, session_id, session_kind="playground")
        if not chat_session:
            raise HTTPException(status_code=404, detail="未找到试聊会话。")
    else:
        chat_session = repository.get_or_create_chat_session(session, project_id, session_kind="playground")
    history = sorted(chat_session.turns, key=lambda item: item.created_at)
    repository.add_chat_turn(session, session_id=chat_session.id, role="user", content=message)

    assistant_reply, llm_meta = generate_chat_reply(
        chat_config,
        version.system_prompt,
        history,
        message,
        "",
        log_path=str(request.app.state.config.llm_log_path),
    )
    trace = {
        "skill_version_id": version.id,
        "skill_version_number": version.version_number,
        "prompt_excerpt": f"SKILL:\n{version.system_prompt[:1800]}",
        "llm": llm_meta,
    }
    assistant_turn = repository.add_chat_turn(
        session,
        session_id=chat_session.id,
        role="assistant",
        content=assistant_reply,
        trace_json=trace,
    )
    return {
        "session_id": chat_session.id,
        "assistant_turn_id": assistant_turn.id,
        "response": assistant_reply,
        "trace": trace,
    }
