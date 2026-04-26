from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models import (
    TelegramPreprocessActiveUser,
    TelegramPreprocessRun,
    TelegramPreprocessTopic,
    TelegramPreprocessTopUser,
    TelegramPreprocessWeeklyTopicCandidate,
    TelegramRelationshipSnapshot,
)
from app.storage import repository


def _serialize_telegram_preprocess_run(run: TelegramPreprocessRun) -> dict[str, Any]:
    summary = dict(run.summary_json or {})
    weekly_concurrency = int(summary.get("weekly_summary_concurrency") or 1)
    completed_week_count = int(summary.get("completed_week_count") or summary.get("topic_count") or run.topic_count or 0)
    total_week_count = int(summary.get("weekly_candidate_count") or summary.get("window_count") or run.window_count or 0)
    remaining_week_count = max(int(summary.get("remaining_week_count") or (total_week_count - completed_week_count)), 0)
    current_topic_total = int(summary.get("current_topic_total") or total_week_count or 0)
    current_topic_index = int(
        summary.get("current_topic_index")
        or (current_topic_total if str(run.status or "").lower() == "completed" and current_topic_total else 0)
    )
    return {
        "id": run.id,
        "status": run.status,
        "chat_id": run.chat_id,
        "llm_model": run.llm_model,
        "progress_percent": int(run.progress_percent or summary.get("progress_percent") or 0),
        "current_stage": run.current_stage or summary.get("current_stage"),
        "prompt_tokens": int(run.prompt_tokens or 0),
        "completion_tokens": int(run.completion_tokens or 0),
        "total_tokens": int(run.total_tokens or 0),
        "cache_creation_tokens": int(run.cache_creation_tokens or 0),
        "cache_read_tokens": int(run.cache_read_tokens or 0),
        "window_count": int(run.window_count or summary.get("window_count") or summary.get("weekly_candidate_count") or 0),
        "top_user_count": int(summary.get("top_user_count") or 0),
        "weekly_candidate_count": int(summary.get("weekly_candidate_count") or summary.get("window_count") or 0),
        "topic_count": int(run.topic_count or summary.get("topic_count") or 0),
        "weekly_summary_concurrency": weekly_concurrency,
        "requested_weekly_concurrency": weekly_concurrency,
        "active_agents": int(summary.get("active_agents") or 0),
        "completed_week_count": completed_week_count,
        "remaining_week_count": remaining_week_count,
        "active_user_count": int(summary.get("active_user_count") or run.active_user_count or 0),
        "relationship_snapshot_id": summary.get("relationship_snapshot_id"),
        "relationship_status": summary.get("relationship_status"),
        "relationship_edge_count": int(summary.get("relationship_edge_count") or 0),
        "relationship_summary": dict(summary.get("relationship_summary") or {}),
        "current_topic_index": max(current_topic_index, 0),
        "current_topic_total": max(current_topic_total, 0),
        "current_topic_label": str(summary.get("current_topic_label") or "").strip(),
        "resume_available": bool(summary.get("resume_available")),
        "resume_count": int(summary.get("resume_count") or 0),
        "error_message": run.error_message,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": summary.get("updated_at") or (run.finished_at.isoformat() if run.finished_at else None),
        "snapshot_version": int(summary.get("snapshot_version") or 0),
        "trace_event_count": int(summary.get("trace_event_count") or 0),
        "trace_events": [dict(item) for item in (summary.get("trace_events") or []) if isinstance(item, dict)],
        "summary": summary,
    }


def _serialize_telegram_preprocess_top_user(user: TelegramPreprocessTopUser) -> dict[str, Any]:
    label = user.display_name or user.username or user.uid or user.participant_id
    return {
        "id": user.id,
        "run_id": user.run_id,
        "participant_id": user.participant_id,
        "rank": user.rank,
        "uid": user.uid,
        "username": user.username,
        "display_name": user.display_name,
        "primary_alias": label,
        "label": label,
        "message_count": user.message_count,
        "first_seen_at": user.first_seen_at.isoformat() if user.first_seen_at else None,
        "last_seen_at": user.last_seen_at.isoformat() if user.last_seen_at else None,
        "metadata": user.metadata_json or {},
    }


def _serialize_telegram_preprocess_weekly_candidate(candidate: TelegramPreprocessWeeklyTopicCandidate) -> dict[str, Any]:
    return {
        "id": candidate.id,
        "run_id": candidate.run_id,
        "week_key": candidate.week_key,
        "window_index": int(candidate.window_index or 1),
        "start_at": candidate.start_at.isoformat() if candidate.start_at else None,
        "end_at": candidate.end_at.isoformat() if candidate.end_at else None,
        "start_message_id": candidate.start_message_id,
        "end_message_id": candidate.end_message_id,
        "message_count": candidate.message_count,
        "participant_count": candidate.participant_count,
        "top_participants": list(candidate.top_participants_json or []),
        "sample_messages": list(candidate.sample_messages_json or [])[:12],
        "metadata": candidate.metadata_json or {},
    }


def _serialize_telegram_preprocess_topic(topic: TelegramPreprocessTopic) -> dict[str, Any]:
    metadata = dict(topic.metadata_json or {})
    quotes = sorted(
        list(topic.quotes or []),
        key=lambda item: (
            item.participant_id or "",
            int(item.rank or 0),
            int(item.telegram_message_id or 0),
        ),
    )
    quotes_by_participant: dict[str, list[dict[str, Any]]] = {}
    flat_quotes: list[dict[str, Any]] = []
    for quote in quotes:
        payload = {
            "participant_id": quote.participant_id,
            "display_name": quote.participant.display_name if quote.participant else None,
            "username": quote.participant.username if quote.participant else None,
            "rank": int(quote.rank or 0),
            "message_id": quote.telegram_message_id,
            "sent_at": quote.sent_at.isoformat() if quote.sent_at else None,
            "quote": quote.quote,
        }
        flat_quotes.append(payload)
        quotes_by_participant.setdefault(quote.participant_id, []).append(payload)
    return {
        "id": topic.id,
        "topic_index": topic.topic_index,
        "week_key": topic.week_key or metadata.get("week_key"),
        "week_topic_index": int(topic.week_topic_index or 0),
        "title": topic.title,
        "summary": topic.summary,
        "start_at": topic.start_at.isoformat() if topic.start_at else None,
        "end_at": topic.end_at.isoformat() if topic.end_at else None,
        "start_message_id": topic.start_message_id,
        "end_message_id": topic.end_message_id,
        "message_count": topic.message_count,
        "participant_count": topic.participant_count,
        "keywords": topic.keywords_json or [],
        "evidence": topic.evidence_json or [],
        "subtopics": [str(item).strip() for item in (metadata.get("subtopics") or []) if str(item).strip()],
        "interaction_patterns": [
            str(item).strip()
            for item in (metadata.get("interaction_patterns") or [])
            if str(item).strip()
        ],
        "participant_viewpoints": [
            dict(item)
            for item in (metadata.get("participant_viewpoints") or [])
            if isinstance(item, dict)
        ],
        "participant_quotes": flat_quotes,
        "metadata": metadata,
        "participants": [
            {
                "participant_id": link.participant_id,
                "display_name": link.participant.display_name if link.participant else None,
                "username": link.participant.username if link.participant else None,
                "role_hint": link.role_hint,
                "stance_summary": link.stance_summary,
                "message_count": link.message_count,
                "mention_count": link.mention_count,
                "quotes": quotes_by_participant.get(link.participant_id, []),
            }
            for link in topic.participants
        ],
    }


def _serialize_telegram_preprocess_active_user(user: TelegramPreprocessActiveUser) -> dict[str, Any]:
    return {
        "id": user.id,
        "run_id": user.run_id,
        "participant_id": user.participant_id,
        "rank": user.rank,
        "uid": user.uid,
        "username": user.username,
        "display_name": user.display_name,
        "primary_alias": user.primary_alias,
        "aliases": user.aliases_json or [],
        "message_count": user.message_count,
        "first_seen_at": user.first_seen_at.isoformat() if user.first_seen_at else None,
        "last_seen_at": user.last_seen_at.isoformat() if user.last_seen_at else None,
        "evidence": user.evidence_json or [],
    }


def _serialize_telegram_relationship_snapshot(snapshot: TelegramRelationshipSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "run_id": snapshot.run_id,
        "project_id": snapshot.project_id,
        "chat_id": snapshot.chat_id,
        "status": snapshot.status,
        "analyzed_user_count": int(snapshot.analyzed_user_count or 0),
        "candidate_pair_count": int(snapshot.candidate_pair_count or 0),
        "llm_pair_count": int(snapshot.llm_pair_count or 0),
        "label_scheme": snapshot.label_scheme,
        "error_message": snapshot.error_message,
        "started_at": snapshot.started_at.isoformat() if snapshot.started_at else None,
        "finished_at": snapshot.finished_at.isoformat() if snapshot.finished_at else None,
        "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
        "updated_at": snapshot.updated_at.isoformat() if snapshot.updated_at else None,
        "summary": dict(snapshot.summary_json or {}),
    }


def _serialize_telegram_relationship_bundle(
    session: Session,
    project_id: str,
    snapshot: TelegramRelationshipSnapshot,
) -> dict[str, Any]:
    active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=snapshot.run_id)
    if active_users:
        participant_lookup = {
            item.participant_id: {
                "participant_id": item.participant_id,
                "label": item.primary_alias or item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in active_users
        }
        participant_rows = [
            {
                "participant_id": item.participant_id,
                "label": item.primary_alias or item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in active_users
        ]
    else:
        top_users = repository.list_telegram_preprocess_top_users(session, project_id, run_id=snapshot.run_id)
        participant_lookup = {
            item.participant_id: {
                "participant_id": item.participant_id,
                "label": item.display_name or item.username or item.uid or item.participant_id,
                "message_count": int(item.message_count or 0),
                "username": item.username,
                "uid": item.uid,
                "rank": int(item.rank or 0),
            }
            for item in top_users
        }
        participant_rows = list(participant_lookup.values())

    edges = []
    edges_by_participant: dict[str, list[dict[str, Any]]] = {}
    for edge in repository.list_telegram_relationship_edges(session, snapshot.id):
        participant_a = participant_lookup.get(edge.participant_a_id, {})
        participant_b = participant_lookup.get(edge.participant_b_id, {})
        payload = {
            "id": edge.id,
            "participant_a_id": edge.participant_a_id,
            "participant_b_id": edge.participant_b_id,
            "participant_a_label": participant_a.get("label") or edge.participant_a_id,
            "participant_b_label": participant_b.get("label") or edge.participant_b_id,
            "relation_label": edge.relation_label,
            "interaction_strength": round(float(edge.interaction_strength or 0.0), 4),
            "confidence": round(float(edge.confidence or 0.0), 4),
            "summary": edge.summary,
            "evidence": list(edge.evidence_json or []),
            "counterevidence": list(edge.counterevidence_json or []),
            "metrics": dict(edge.metrics_json or {}),
        }
        edges.append(payload)
        edges_by_participant.setdefault(edge.participant_a_id, []).append(payload)
        edges_by_participant.setdefault(edge.participant_b_id, []).append(payload)

    users = []
    for participant in participant_rows:
        participant_id = str(participant.get("participant_id") or "").strip()
        relation_edges = sorted(
            list(edges_by_participant.get(participant_id, [])),
            key=lambda item: (float(item.get("interaction_strength") or 0.0), float(item.get("confidence") or 0.0)),
            reverse=True,
        )
        strongest_edges = []
        for edge in relation_edges[:3]:
            counterpart_id = edge["participant_b_id"] if edge["participant_a_id"] == participant_id else edge["participant_a_id"]
            counterpart_label = edge["participant_b_label"] if edge["participant_a_id"] == participant_id else edge["participant_a_label"]
            strongest_edges.append(
                {
                    "counterpart_id": counterpart_id,
                    "counterpart_label": counterpart_label,
                    "relation_label": edge["relation_label"],
                    "interaction_strength": edge["interaction_strength"],
                    "confidence": edge["confidence"],
                }
            )
        users.append(
            {
                "participant_id": participant_id,
                "label": participant.get("label") or participant_id,
                "message_count": int(participant.get("message_count") or 0),
                "ally_count": sum(1 for edge in relation_edges if edge.get("relation_label") == "friendly"),
                "tense_count": sum(1 for edge in relation_edges if edge.get("relation_label") == "tense"),
                "strongest_edges": strongest_edges,
                "relations": relation_edges,
            }
        )

    users.sort(
        key=lambda item: (
            int(item.get("ally_count") or 0) + int(item.get("tense_count") or 0),
            int(item.get("message_count") or 0),
        ),
        reverse=True,
    )
    return {
        "snapshot": _serialize_telegram_relationship_snapshot(snapshot),
        "users": users,
        "edges": edges,
    }


def _serialize_telegram_preprocess_detail(
    session: Session,
    project_id: str,
    run: TelegramPreprocessRun,
) -> dict[str, Any]:
    payload = _serialize_telegram_preprocess_run(run)
    active_users = repository.list_telegram_preprocess_active_users(session, project_id, run_id=run.id)
    payload["top_users"] = [
        _serialize_telegram_preprocess_top_user(item)
        for item in repository.list_telegram_preprocess_top_users(session, project_id, run_id=run.id)
    ]
    payload["weekly_candidates"] = [
        _serialize_telegram_preprocess_weekly_candidate(item)
        for item in repository.list_telegram_preprocess_weekly_topic_candidates(session, project_id, run_id=run.id)
    ]
    payload["topics"] = [
        _serialize_telegram_preprocess_topic(item)
        for item in repository.list_telegram_preprocess_topics(session, project_id, run_id=run.id)
    ]
    payload["active_users"] = [_serialize_telegram_preprocess_active_user(item) for item in active_users]
    payload["active_user_count"] = len(active_users)
    relationship_snapshot = repository.get_telegram_relationship_snapshot_for_run(session, run.id)
    payload["relationship_snapshot"] = (
        _serialize_telegram_relationship_snapshot(relationship_snapshot)
        if relationship_snapshot
        else None
    )
    return payload

serialize_telegram_preprocess_run = _serialize_telegram_preprocess_run
serialize_telegram_preprocess_top_user = _serialize_telegram_preprocess_top_user
serialize_telegram_preprocess_weekly_candidate = _serialize_telegram_preprocess_weekly_candidate
serialize_telegram_preprocess_topic = _serialize_telegram_preprocess_topic
serialize_telegram_preprocess_active_user = _serialize_telegram_preprocess_active_user
serialize_telegram_relationship_snapshot = _serialize_telegram_relationship_snapshot
serialize_telegram_relationship_bundle = _serialize_telegram_relationship_bundle
serialize_telegram_preprocess_detail = _serialize_telegram_preprocess_detail

__all__ = [
    "_serialize_telegram_preprocess_run",
    "_serialize_telegram_preprocess_top_user",
    "_serialize_telegram_preprocess_weekly_candidate",
    "_serialize_telegram_preprocess_topic",
    "_serialize_telegram_preprocess_active_user",
    "_serialize_telegram_relationship_snapshot",
    "_serialize_telegram_relationship_bundle",
    "_serialize_telegram_preprocess_detail",
    "serialize_telegram_preprocess_run",
    "serialize_telegram_preprocess_top_user",
    "serialize_telegram_preprocess_weekly_candidate",
    "serialize_telegram_preprocess_topic",
    "serialize_telegram_preprocess_active_user",
    "serialize_telegram_relationship_snapshot",
    "serialize_telegram_relationship_bundle",
    "serialize_telegram_preprocess_detail",
]
