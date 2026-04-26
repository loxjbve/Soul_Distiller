from __future__ import annotations

from typing import Any

from app.models import DocumentRecord


def _serialize_document(document: DocumentRecord) -> dict[str, Any]:
    metadata = document.metadata_json or {}
    return {
        "id": document.id,
        "filename": document.filename,
        "title": document.title or document.filename,
        "source_type": document.source_type,
        "language": getattr(document, "language", None),
        "ingest_status": document.ingest_status,
        "error_message": document.error_message,
        "metadata_json": metadata,
        "created_at": document.created_at.isoformat() if getattr(document, "created_at", None) else None,
        "updated_at": document.updated_at.isoformat() if getattr(document, "updated_at", None) else None,
    }

def _serialize_draft(draft) -> dict[str, Any]:
    return {
        "id": draft.id,
        "asset_kind": getattr(draft, "asset_kind", "cc_skill"),
        "status": draft.status,
        "markdown_text": draft.markdown_text,
        "json_payload": draft.json_payload,
        "prompt_text": draft.system_prompt,
        "system_prompt": draft.system_prompt,
        "notes": draft.notes,
    }


def _serialize_chat_session(chat_session) -> dict[str, Any]:
    turns = sorted(chat_session.turns, key=lambda item: item.created_at) if getattr(chat_session, "turns", None) else []
    return {
        "id": chat_session.id,
        "session_kind": chat_session.session_kind,
        "title": chat_session.title or "未命名会话",
        "created_at": chat_session.created_at.isoformat() if chat_session.created_at else None,
        "last_active_at": chat_session.last_active_at.isoformat() if chat_session.last_active_at else None,
        "turn_count": len(turns),
    }

def _serialize_chat_turn(turn) -> dict[str, Any]:
    return {
        "id": turn.id,
        "role": turn.role,
        "content": turn.content,
        "trace": turn.trace_json or {},
        "created_at": turn.created_at.isoformat(),
    }

def _serialize_writing_session_detail(chat_session) -> dict[str, Any]:
    turns = sorted(chat_session.turns, key=lambda item: item.created_at)
    timeline_turns: list[dict[str, Any]] = []
    for turn in turns:
        timeline_turns.extend(_expand_writing_timeline_turn(turn))
    return {
        **_serialize_chat_session(chat_session),
        "turns": timeline_turns,
        "timeline_turn_count": len(timeline_turns),
    }


def _expand_writing_timeline_turn(turn) -> list[dict[str, Any]]:
    trace = turn.trace_json or {}
    if turn.role == "user":
        return [_serialize_writing_user_turn(turn)]
    if turn.role == "assistant" and trace.get("kind") == "writing_result":
        timeline = trace.get("timeline")
        if not isinstance(timeline, list) or not timeline:
            timeline = _build_writing_timeline_from_trace(trace)
        if timeline:
            return [
                _serialize_writing_timeline_item(
                    turn,
                    item,
                    index=index,
                    is_final=index == len(timeline) - 1,
                    parent_trace=trace,
                )
                for index, item in enumerate(timeline)
            ]
    return [_serialize_writing_generic_turn(turn)]


def _serialize_writing_user_turn(turn) -> dict[str, Any]:
    trace = turn.trace_json or {}
    raw_message = str(trace.get("raw_message") or "").strip()
    return {
        "id": turn.id,
        "role": "user",
        "content": raw_message or turn.content,
        "trace": trace,
        "created_at": turn.created_at.isoformat(),
        "actor_id": "user",
        "actor_name": "用户",
        "actor_role": "user",
        "message_kind": "request",
    }


def _serialize_writing_generic_turn(turn) -> dict[str, Any]:
    trace = turn.trace_json or {}
    actor_role = "writer" if turn.role == "assistant" else turn.role
    actor_name = "写作 Agent" if turn.role == "assistant" else "用户"
    return {
        "id": turn.id,
        "role": turn.role,
        "content": turn.content,
        "trace": trace,
        "created_at": turn.created_at.isoformat(),
        "actor_id": actor_role,
        "actor_name": actor_name,
        "actor_role": actor_role,
        "message_kind": "final" if turn.role == "assistant" else "request",
    }


def _normalize_writing_actor_name(value: Any, *, fallback: str = "写作 Agent") -> str:
    name = str(value or "").strip()
    if not name:
        return fallback
    lowered = name.lower()
    if name in {"鍐欎綔 Agent", "鍐", "写作 Agent"}:
        return "写作 Agent"
    if name in {"浣?", "用户", "你"}:
        return "用户"
    if "agent" in lowered and ("鍐" in name or "写作" in name):
        return "写作 Agent"
    return name


def _build_writing_timeline_from_trace(trace: dict[str, Any]) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    topic_translation = trace.get("topic_translation") if isinstance(trace.get("topic_translation"), dict) else None
    if topic_translation:
        lines: list[str] = []
        for title, key in (
            ("Scene", "scene"),
            ("Imagery", "imagery"),
            ("Felt Cost", "felt_cost"),
            ("Relationship Pressure", "relationship_pressure"),
            ("Stance", "stance"),
            ("Emotional Arc", "emotional_arc"),
            ("Not To Write", "not_to_write"),
        ):
            values = [str(item).strip() for item in topic_translation.get(key) or [] if str(item).strip()]
            if not values:
                continue
            lines.append(f"{title}:")
            lines.extend(f"- {item}" for item in values[:6])
        timeline.append(
            {
                "actor_id": "writer-topic_translation",
                "actor_name": "写作 Agent",
                "actor_role": "writer",
                "message_kind": "topic_translation",
                "body": "\n".join(lines).strip(),
                "detail": topic_translation,
            }
        )
    outline = trace.get("outline") if isinstance(trace.get("outline"), dict) else None
    if outline:
        outline_lines = [
            f"目标字数：{outline.get('target_word_count')}",
            f"段落数：{outline.get('paragraph_count')}",
        ]
        for item in outline.get("paragraphs") or []:
            outline_lines.append(
                f"P{item.get('index')}: {item.get('function')} | {item.get('emotional_position')} | {', '.join(item.get('anchor_ids') or [])}"
            )
        timeline.append(
            {
                "actor_id": "writer-outline",
                "actor_name": "写作 Agent",
                "actor_role": "writer",
                "message_kind": "outline",
                "body": "\n".join(outline_lines).strip(),
                "detail": outline,
            }
        )
    draft = str(trace.get("draft") or "").strip()
    if draft:
        timeline.append(
            {
                "actor_id": "writer-draft",
                "actor_name": "写作 Agent",
                "actor_role": "writer",
                "message_kind": "draft",
                "body": draft,
                "detail": {},
            }
        )
    for review in trace.get("reviews") or []:
        if not isinstance(review, dict):
            continue
        timeline.append(
            {
                "actor_id": f"reviewer-{review.get('dimension_key') or 'reviewer'}",
                "actor_name": review.get("dimension_label") or review.get("dimension") or "Reviewer",
                "actor_role": "reviewer",
                "message_kind": "review",
                "body": _render_writing_review_message(review),
                "detail": review,
            }
        )
    review_plan = trace.get("review_plan") if isinstance(trace.get("review_plan"), dict) else None
    if review_plan:
        timeline.append(
            {
                "actor_id": "writer-review_synthesis",
                "actor_name": "写作 Agent",
                "actor_role": "writer",
                "message_kind": "review_synthesis",
                "body": str(review_plan.get("summary") or "").strip(),
                "detail": review_plan,
            }
        )
    final_text = str(trace.get("final_text") or "").strip()
    if final_text:
        timeline.append(
            {
                "actor_id": "writer-final",
                "actor_name": "写作 Agent",
                "actor_role": "writer",
                "message_kind": "final",
                "body": final_text,
                "detail": {
                    "review_plan": trace.get("review_plan"),
                    "final_assessment": trace.get("final_assessment"),
                },
            }
        )
    return timeline


def _serialize_writing_timeline_item(
    turn,
    item: dict[str, Any],
    *,
    index: int,
    is_final: bool,
    parent_trace: dict[str, Any],
) -> dict[str, Any]:
    detail = item.get("detail") if isinstance(item.get("detail"), dict) else {}
    trace = {
        "message_kind": item.get("message_kind"),
        "debug": detail,
        "source_turn_id": turn.id,
    }
    if is_final:
        trace = {**parent_trace, **trace}
    return {
        "id": f"{turn.id}:{index}",
        "role": "assistant",
        "content": str(item.get("body") or "").strip(),
        "trace": trace,
        "created_at": str(item.get("created_at") or turn.created_at.isoformat()),
        "actor_id": str(item.get("actor_id") or f"assistant-{index}"),
        "actor_name": _normalize_writing_actor_name(item.get("actor_name")),
        "actor_role": str(item.get("actor_role") or "assistant"),
        "message_kind": str(item.get("message_kind") or "update"),
    }


def _render_writing_review_message(review: dict[str, Any]) -> str:
    parts = [
        f"结论：{'通过' if review.get('pass') else '需要修改'}",
        f"分数：{int(round(float(review.get('score') or 0.0) * 100))}/100",
    ]
    anchor_ids = [str(item).strip() for item in review.get("anchor_ids") or [] if str(item).strip()]
    strengths = [str(item).strip() for item in review.get("must_keep_spans") or review.get("strengths") or [] if str(item).strip()]
    issues = [item for item in review.get("violations") or [] if isinstance(item, dict)]
    instructions = [item for item in review.get("revision_instructions") or [] if isinstance(item, dict)]
    if anchor_ids:
        parts.append("")
        parts.append("Anchor：")
        parts.extend(f"- {item}" for item in anchor_ids[:4])
    if strengths:
        parts.append("")
        parts.append("保留：")
        parts.extend(f"- {item}" for item in strengths[:4])
    if issues:
        parts.append("")
        parts.append("问题：")
        parts.extend(
            f"- [{str(item.get('anchor_id') or '').strip()}] {str(item.get('issue') or item.get('instruction') or item.get('span') or '').strip()}"
            for item in issues[:4]
        )
    if instructions:
        parts.append("")
        parts.append("修改建议：")
        parts.extend(
            f"- [{str(item.get('anchor_id') or '').strip()}] {str(item.get('instruction') or item.get('issue') or '').strip()}"
            for item in instructions[:5]
        )
    return "\n".join(parts).strip()

serialize_document = _serialize_document
serialize_draft = _serialize_draft
serialize_chat_session = _serialize_chat_session
serialize_chat_turn = _serialize_chat_turn
serialize_writing_session_detail = _serialize_writing_session_detail
build_writing_timeline_from_trace = _build_writing_timeline_from_trace
serialize_writing_timeline_item = _serialize_writing_timeline_item
serialize_writing_user_turn = _serialize_writing_user_turn
serialize_writing_generic_turn = _serialize_writing_generic_turn

__all__ = [
    "_serialize_document",
    "_serialize_draft",
    "_serialize_chat_session",
    "_serialize_chat_turn",
    "_serialize_writing_session_detail",
    "_expand_writing_timeline_turn",
    "_serialize_writing_user_turn",
    "_serialize_writing_generic_turn",
    "_normalize_writing_actor_name",
    "_build_writing_timeline_from_trace",
    "_serialize_writing_timeline_item",
    "_render_writing_review_message",
    "serialize_document",
    "serialize_draft",
    "serialize_chat_session",
    "serialize_chat_turn",
    "serialize_writing_session_detail",
    "build_writing_timeline_from_trace",
    "serialize_writing_timeline_item",
    "serialize_writing_user_turn",
    "serialize_writing_generic_turn",
]
