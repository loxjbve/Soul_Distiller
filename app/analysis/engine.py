from __future__ import annotations

import json
import traceback
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import asdict
from time import perf_counter
from typing import Any

from app.analysis.prompts import build_facet_analysis_messages
from app.db import Database
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.analysis.facets import FACETS, FacetDefinition, get_facet_prompt_profile
from app.analysis.streaming import AnalysisStreamHub
from app.analysis.telegram_agent import TelegramAnalysisAgent
from app.llm.client import LLMError, OpenAICompatibleClient, normalize_api_mode, parse_json_response
from app.models import (
    AnalysisFacet,
    AnalysisRun,
    DocumentRecord,
    Project,
    TelegramMessage,
    TelegramParticipant,
    TelegramPreprocessRun,
    TelegramPreprocessTopic,
    TelegramTopicReport,
    TextChunk,
    utcnow,
)
from app.retrieval.service import RetrievalService
from app.schemas import (
    DEFAULT_ANALYSIS_CONCURRENCY,
    MAX_ANALYSIS_CONCURRENCY,
    MIN_ANALYSIS_CONCURRENCY,
    FacetResult,
    RetrievedChunk,
    ServiceConfig,
)
from app.storage import repository
from app.utils.text import top_terms

FACET_EVIDENCE_LIMIT = 20
FACET_BULLET_LIMIT = 8
RAW_TEXT_PREVIEW_LIMIT = 20000
GLOBAL_PERSONA_CARD_LABELS = (
    "角色规则",
    "心智模型",
    "决策启发式",
    "表达DNA",
    "表达 DNA",
    "时间线",
    "价值观",
    "反模式",
    "诚实边界",
    "智识谱系",
)


class AnalysisCancelledError(RuntimeError):
    pass


def _parse_confidence(val: Any, default: float) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        if isinstance(val, str):
            s = val.strip().lower()
            if "high" in s:
                return 0.8
            if "medium" in s:
                return 0.5
            if "low" in s:
                return 0.2
        return default


def _normalize_concurrency(value: Any) -> int:
    if value is None:
        return DEFAULT_ANALYSIS_CONCURRENCY
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        candidate = DEFAULT_ANALYSIS_CONCURRENCY
    return max(MIN_ANALYSIS_CONCURRENCY, min(MAX_ANALYSIS_CONCURRENCY, candidate))


def _collapse_whitespace(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _strip_persona_card_label(text: str) -> tuple[str | None, str]:
    for label in GLOBAL_PERSONA_CARD_LABELS:
        for separator in ("：", ":", "-", " - "):
            prefix = f"{label}{separator}"
            if text.startswith(prefix):
                return label, text[len(prefix):].strip()
    return None, text


def _facet_keyword_score(text: str, facet_key: str) -> int:
    profile = get_facet_prompt_profile(facet_key)
    return sum(1 for term in profile.relevance_terms if term and term in text)


def _best_foreign_facet_match(text: str, facet_key: str) -> tuple[str | None, int]:
    best_key: str | None = None
    best_score = 0
    for other in FACETS:
        if other.key == facet_key:
            continue
        score = _facet_keyword_score(text, other.key)
        if score > best_score:
            best_key = other.key
            best_score = score
    return best_key, best_score


def _normalize_facet_bullets(items: list[Any], facet: FacetDefinition) -> tuple[list[str], int]:
    normalized: list[str] = []
    seen: set[str] = set()
    removed = 0
    for item in list(items or [])[: FACET_BULLET_LIMIT * 3]:
        raw_text = _collapse_whitespace(item)
        if not raw_text:
            continue
        _, stripped_text = _strip_persona_card_label(raw_text)
        text = _collapse_whitespace(stripped_text)
        if not text:
            removed += 1
            continue
        current_score = _facet_keyword_score(text, facet.key)
        _, foreign_score = _best_foreign_facet_match(text, facet.key)
        if current_score == 0 and foreign_score > 0:
            removed += 1
            continue
        if current_score == 0 and stripped_text != raw_text:
            removed += 1
            continue
        if text in seen:
            continue
        normalized.append(text)
        seen.add(text)
        if len(normalized) >= FACET_BULLET_LIMIT:
            break
    return normalized, removed


def _build_facet_summary_from_bullets(facet: FacetDefinition, bullets: list[str]) -> str:
    cleaned_bullets = [item.rstrip("。；;，, ") for item in bullets if item]
    if not cleaned_bullets:
        return f"现有证据主要支持从 {facet.label} 继续观察，但可直接复用的细节仍然有限。"
    if len(cleaned_bullets) == 1:
        return f"围绕 {facet.label}，材料最稳定地显示：{cleaned_bullets[0]}。"
    return f"围绕 {facet.label}，材料最稳定地显示：{cleaned_bullets[0]}；{cleaned_bullets[1]}。"


def _normalize_facet_summary(summary: Any, bullets: list[str], facet: FacetDefinition) -> tuple[str, bool]:
    text = _collapse_whitespace(summary)
    if not text:
        return _build_facet_summary_from_bullets(facet, bullets), True
    current_score = _facet_keyword_score(text, facet.key)
    _, foreign_score = _best_foreign_facet_match(text, facet.key)
    global_label_hits = sum(1 for label in GLOBAL_PERSONA_CARD_LABELS if label in text)
    if current_score == 0 and (foreign_score > 0 or global_label_hits >= 2):
        return _build_facet_summary_from_bullets(facet, bullets), True
    return text, False


def analyze_facet_worker(
    facet: FacetDefinition,
    project_name: str,
    chunks: list[dict[str, Any]],
    llm_config: dict[str, Any] | None,
    llm_log_path: str | None,
    target_role: str | None,
    analysis_context: str | None,
    stream_callback: Any | None = None,
) -> dict[str, Any]:
    try:
        if llm_config:
            try:
                payload = _analyze_with_llm(
                    facet,
                    project_name,
                    chunks,
                    llm_config,
                    llm_log_path=llm_log_path,
                    target_role=target_role,
                    analysis_context=analysis_context,
                    stream_callback=stream_callback,
                )
            except Exception as exc:
                payload = _analyze_heuristically(
                    facet,
                    chunks,
                    target_role=target_role,
                    analysis_context=analysis_context,
                )
                payload["_meta"] = {
                    **dict(payload.get("_meta") or {}),
                    "llm_called": True,
                    "llm_success": False,
                    "llm_attempts": 1,
                    "provider_kind": llm_config.get("provider_kind"),
                    "api_mode": normalize_api_mode(llm_config.get("api_mode")),
                    "llm_error": str(exc),
                    "raw_text": getattr(exc, "raw_text", None),
                    "request_url": getattr(exc, "request_url", None),
                    "request_payload": getattr(exc, "request_payload", None),
                    "log_path": llm_log_path,
                }
                payload["notes"] = (
                    f"{payload.get('notes') or ''}\n"
                    f"LLM returned an unusable response, so the result was recovered with heuristic fallback: {exc}"
                ).strip()
        else:
            payload = _analyze_heuristically(
                facet,
                chunks,
                target_role=target_role,
                analysis_context=analysis_context,
            )
        return asdict(
            FacetResult(
                facet_key=facet.key,
                status="completed",
                confidence=_parse_confidence(payload.get("confidence"), 0.55),
                summary=payload.get("summary", ""),
                bullets=list(payload.get("bullets", [])),
                evidence=list(payload.get("evidence", [])),
                conflicts=list(payload.get("conflicts", [])),
                notes=payload.get("notes"),
                raw_payload=payload,
            )
        )
    except Exception as exc:
        return asdict(
            FacetResult(
                facet_key=facet.key,
                status="failed",
                confidence=0.0,
                summary="",
                bullets=[],
                evidence=[],
                conflicts=[],
                notes=str(exc),
                raw_payload={
                    "_meta": {
                        "llm_called": bool(llm_config),
                        "llm_success": False,
                        "llm_attempts": 1 if llm_config else 0,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        )


class AnalysisEngine:
    def __init__(
        self,
        retrieval: RetrievalService | None = None,
        *,
        db: Database | None = None,
        llm_log_path: str | None = None,
        use_processes: bool = True,
        facet_max_workers: int = DEFAULT_ANALYSIS_CONCURRENCY,
        stream_hub: AnalysisStreamHub | None = None,
        cancel_checker: Callable[[str, str], bool] | None = None,
    ) -> None:
        self.retrieval = retrieval or RetrievalService()
        self.db = db
        self.llm_log_path = llm_log_path
        self.use_processes = use_processes
        self.facet_max_workers = _normalize_concurrency(facet_max_workers)
        self.stream_hub = stream_hub
        self.cancel_checker = cancel_checker

    def set_cancel_checker(self, cancel_checker: Callable[[str, str], bool] | None) -> None:
        self.cancel_checker = cancel_checker

    def create_run(
        self,
        session: Session,
        project_id: str,
        *,
        target_role: str | None = None,
        target_user_query: str | None = None,
        participant_id: str | None = None,
        analysis_context: str | None = None,
        concurrency: int | None = None,
    ) -> AnalysisRun:
        project = repository.get_project(session, project_id)
        if not project:
            raise ValueError("Project not found.")
        summary = self._build_initial_summary(
            session,
            project_id,
            target_role=target_role,
            target_user_query=target_user_query,
            participant_id=participant_id,
            analysis_context=analysis_context,
        )
        summary["concurrency"] = _normalize_concurrency(concurrency)
        run = repository.create_analysis_run(
            session,
            project_id,
            status="queued",
            summary_json=summary,
        )
        for index, facet in enumerate(FACETS, start=1):
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="queued",
                confidence=0.0,
                findings_json={
                    "label": facet.label,
                    "summary": "",
                    "bullets": [],
                    "notes": None,
                    "retrieval_mode": None,
                    "hit_count": 0,
                    "target_role": summary.get("target_role"),
                    "target_user": summary.get("target_user"),
                    "target_user_query": summary.get("target_user_query"),
                    "participant_id": summary.get("participant_id"),
                    "preprocess_run_id": summary.get("preprocess_run_id"),
                    "analysis_context": summary.get("analysis_context"),
                    "llm_called": False,
                    "llm_success": None,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "duration_ms": 0,
                    "phase": "queued",
                    "queue_position": index,
                    "started_at": None,
                    "finished_at": None,
                },
                evidence_json=[],
                conflicts_json=[],
                error_message=None,
            )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="lifecycle",
            message="分析任务已加入队列。",
            payload_json={
                "target_role": summary.get("target_role"),
                "analysis_context": summary.get("analysis_context"),
            },
        )
        session.flush()
        return repository.get_analysis_run(session, run.id) or run

    def run(
        self,
        session: Session,
        project_id: str,
        *,
        target_role: str | None = None,
        target_user_query: str | None = None,
        participant_id: str | None = None,
        analysis_context: str | None = None,
        concurrency: int | None = None,
    ) -> AnalysisRun:
        run = self.create_run(
            session,
            project_id,
            target_role=target_role,
            target_user_query=target_user_query,
            participant_id=participant_id,
            analysis_context=analysis_context,
            concurrency=concurrency,
        )
        session.commit()
        self.execute_run(session, run.id)
        return repository.get_analysis_run(session, run.id) or run

    def execute_run(self, session: Session, run_id: str) -> AnalysisRun:
        run = repository.get_analysis_run(session, run_id)
        if not run:
            raise ValueError("Analysis run not found.")
        project = repository.get_project(session, run.project_id)
        if not project:
            raise ValueError("Project not found.")
        self._ensure_run_active(run.id, run.project_id)

        chat_config = repository.get_service_config(session, "chat_service")
        embedding_config = repository.get_service_config(session, "embedding_service")
        llm_payload = asdict(chat_config) if chat_config else None
        summary = dict(run.summary_json or {})

        run.status = "running"
        run.started_at = utcnow()
        run.finished_at = None
        summary["current_stage"] = "准备证据"
        summary["current_phase"] = "retrieving"
        summary["current_facet"] = None
        summary["concurrency"] = _normalize_concurrency(summary.get("concurrency") or self.facet_max_workers)
        summary["started_at"] = run.started_at.isoformat()
        run.summary_json = summary
        self._recalculate_run_summary(session, run)
        repository.add_analysis_event(
            session,
            run.id,
            event_type="lifecycle",
            message="分析任务开始执行。",
            payload_json={
                "llm_enabled": bool(chat_config),
                "embedding_enabled": bool(embedding_config) and project.mode != "telegram",
                "analysis_mode": project.mode,
            },
        )
        self._persist_progress(session, run.id)
        self._ensure_run_active(run.id, run.project_id)

        if project.mode == "telegram":
            self._execute_telegram_run(session, run, project, chat_config)
        elif self.use_processes:
            self._run_parallel_processes(
                session,
                run,
                project.name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
            )
        else:
            self._run_parallel_threads(
                session,
                run,
                project.name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
            )

        self._ensure_run_active(run.id, run.project_id)
        summary = dict(run.summary_json or {})
        total_processed = int(summary.get("completed_facets", 0)) + int(summary.get("failed_facets", 0))
        run.finished_at = utcnow()
        run.status = "completed" if int(summary.get("failed_facets", 0)) == 0 else "partial_failed"
        summary["current_stage"] = "分析完成"
        summary["current_facet"] = None
        summary["progress_percent"] = 100 if total_processed else 0
        summary["finished_at"] = run.finished_at.isoformat()
        run.summary_json = summary
        self._recalculate_run_summary(session, run)
        repository.add_analysis_event(
            session,
            run.id,
            event_type="lifecycle",
            message=f"分析完成，成功 {summary.get('completed_facets', 0)} / 失败 {summary.get('failed_facets', 0)}。",
            level="success" if run.status == "completed" else "warning",
            payload_json={
                "total_tokens": summary.get("total_tokens", 0),
                "llm_successes": summary.get("llm_successes", 0),
                "llm_failures": summary.get("llm_failures", 0),
            },
        )
        self._persist_progress(session, run.id)
        return repository.get_analysis_run(session, run.id) or run

    def rerun_facet(self, session: Session, project_id: str, facet_key: str) -> AnalysisRun:
        run = repository.get_latest_analysis_run(session, project_id)
        project = repository.get_project(session, project_id)
        if not run or not project:
            raise ValueError("Analysis run not found.")
        self._ensure_run_active(run.id, run.project_id)
        facet_def = next((item for item in FACETS if item.key == facet_key), None)
        if not facet_def:
            raise ValueError("Unknown facet.")

        summary = dict(run.summary_json or {})
        chat_config = repository.get_service_config(session, "chat_service")
        embedding_config = repository.get_service_config(session, "embedding_service")
        llm_payload = asdict(chat_config) if chat_config else None
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"重新执行 {facet_def.label}。",
            payload_json={"facet_key": facet_def.key},
        )
        run.status = "running"
        run.finished_at = None
        if project.mode == "telegram":
            return self._rerun_telegram_facet(
                session,
                run,
                project,
                facet_def,
                chat_config,
                llm_payload=llm_payload,
            )
        self._mark_facet_preparing(session, run, facet_def)
        prepared = self._prepare_facet_execution(
            session,
            run,
            facet_def,
            embedding_config=embedding_config,
            llm_payload=llm_payload,
        )
        self._ensure_run_active(run.id, run.project_id)
        if not prepared:
            run.finished_at = utcnow()
            summary = dict(run.summary_json or {})
            run.status = "completed" if int(summary.get("failed_facets", 0)) == 0 else "partial_failed"
            summary["finished_at"] = run.finished_at.isoformat()
            run.summary_json = summary
            self._recalculate_run_summary(session, run)
            self._persist_progress(session, run.id)
            return repository.get_analysis_run(session, run.id) or run

        hits, retrieval_mode, retrieval_trace = prepared
        self._mark_facet_running(
            session,
            run,
            facet_def,
            retrieval_mode,
            retrieval_trace,
            len(hits),
            llm_payload=llm_payload,
        )

        result = FacetResult(
            **analyze_facet_worker(
                facet_def,
                project.name,
                [self._serialize_hit(hit) for hit in hits],
                llm_payload,
                self.llm_log_path,
                summary.get("target_role"),
                summary.get("analysis_context"),
                self._build_stream_callback(run.id, facet_def),
            )
        )
        self._ensure_run_active(run.id, run.project_id)
        self._apply_facet_result(session, run, facet_def, retrieval_mode, retrieval_trace, len(hits), result)
        run.finished_at = utcnow()
        summary = dict(run.summary_json or {})
        run.status = "completed" if int(summary.get("failed_facets", 0)) == 0 else "partial_failed"
        summary["finished_at"] = run.finished_at.isoformat()
        run.summary_json = summary
        self._recalculate_run_summary(session, run)
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet_def.label} 已重新完成。",
            level="success" if result.status == "completed" else "error",
            payload_json={"facet_key": facet_def.key},
        )
        self._persist_progress(session, run.id)
        return repository.get_analysis_run(session, run.id) or run

    def _build_telegram_agent(
        self,
        session: Session,
        project: Project,
        chat_config: ServiceConfig | None,
    ) -> TelegramAnalysisAgent:
        source_project_id = repository.get_target_project_id(session, project.id)
        source_project = repository.get_project(session, source_project_id) or project
        return TelegramAnalysisAgent(
            session,
            source_project,
            llm_config=chat_config,
            log_path=self.llm_log_path,
        )

    def _refresh_telegram_summary_counts(
        self,
        session: Session,
        run: AnalysisRun,
    ) -> dict[str, Any]:
        summary = dict(run.summary_json or {})
        source_project_id = repository.get_target_project_id(session, run.project_id)
        preprocess_run = None
        preprocess_run_id = summary.get("preprocess_run_id")
        if preprocess_run_id:
            preprocess_run = repository.get_telegram_preprocess_run(session, str(preprocess_run_id))
        if not preprocess_run:
            preprocess_run = repository.get_latest_successful_telegram_preprocess_run(session, source_project_id)
        summary["chunk_count"] = 0
        summary["telegram_source_project_id"] = source_project_id
        summary["telegram_message_count"] = int(
            session.scalar(
                select(func.count()).select_from(TelegramMessage).where(TelegramMessage.project_id == source_project_id)
            )
            or 0
        )
        summary["telegram_participant_count"] = int(
            session.scalar(
                select(func.count()).select_from(TelegramParticipant).where(TelegramParticipant.project_id == source_project_id)
            )
            or 0
        )
        summary["telegram_report_count"] = 0
        summary["preprocess_run_id"] = preprocess_run.id if preprocess_run else None
        summary["telegram_preprocess_topic_count"] = (
            int(
                session.scalar(
                    select(func.count()).select_from(TelegramPreprocessTopic).where(
                        TelegramPreprocessTopic.run_id == preprocess_run.id
                    )
                )
                or 0
            )
            if preprocess_run
            else 0
        )
        preprocess_summary = dict(preprocess_run.summary_json or {}) if preprocess_run else {}
        summary["weekly_candidate_count"] = int(preprocess_summary.get("weekly_candidate_count") or 0)
        summary["weekly_topic_count"] = int(summary.get("telegram_preprocess_topic_count") or 0)
        run.summary_json = summary
        return summary

    @staticmethod
    def _facet_result_from_payload(
        facet: FacetDefinition,
        payload: dict[str, Any],
    ) -> FacetResult:
        normalized_payload = dict(payload or {})
        return FacetResult(
            facet_key=facet.key,
            status=str(normalized_payload.get("status") or "completed"),
            confidence=_parse_confidence(normalized_payload.get("confidence"), 0.65),
            summary=str(normalized_payload.get("summary") or ""),
            bullets=list(normalized_payload.get("bullets") or []),
            evidence=list(normalized_payload.get("evidence") or []),
            conflicts=list(normalized_payload.get("conflicts") or []),
            notes=normalized_payload.get("notes"),
            raw_payload=normalized_payload,
        )

    def _execute_telegram_run(
        self,
        session: Session,
        run: AnalysisRun,
        project: Project,
        chat_config: ServiceConfig | None,
    ) -> None:
        agent = self._build_telegram_agent(session, project, chat_config)
        agent.trace_callback = lambda event: self._publish_trace(run.id, event)
        summary = self._refresh_telegram_summary_counts(session, run)
        if not summary.get("preprocess_run_id"):
            raise ValueError("Telegram analysis requires a completed preprocess run.")
        target_user = agent.resolve_target_user(
            target_user_query=(run.summary_json or {}).get("target_user_query"),
            participant_id=(run.summary_json or {}).get("participant_id"),
            preprocess_run_id=(run.summary_json or {}).get("preprocess_run_id"),
        )
        summary["target_user"] = target_user
        summary["target_role"] = target_user.get("label") or target_user.get("primary_alias") or target_user.get("display_name")
        run.summary_json = summary
        repository.add_analysis_event(
            session,
            run.id,
            event_type="lifecycle",
            message="Telegram preprocess snapshot is ready.",
            payload_json={
                "preprocess_run_id": summary.get("preprocess_run_id"),
                "target_user": target_user,
                "telegram_message_count": summary.get("telegram_message_count", 0),
                "telegram_participant_count": summary.get("telegram_participant_count", 0),
                "telegram_preprocess_topic_count": summary.get("telegram_preprocess_topic_count", 0),
            },
        )
        self._persist_progress(session, run.id)

        llm_payload = asdict(chat_config) if chat_config else None
        placeholder_trace = {
            "mode": "telegram_agent",
            "evidence_kind": "telegram_messages",
            "tool_calls": [],
            "preprocess_run_id": summary.get("preprocess_run_id"),
            "target_user": target_user,
            "topic_ids": [],
            "queried_message_ids": [],
        }
        for facet_def in FACETS:
            self._ensure_run_active(run.id, run.project_id)
            self._mark_facet_preparing(session, run, facet_def)
            self._mark_facet_running(
                session,
                run,
                facet_def,
                "telegram_agent",
                placeholder_trace,
                0,
                llm_payload=llm_payload,
            )
            try:
                analysis = agent.analyze_facet(
                    facet_def,
                    target_user_query=(run.summary_json or {}).get("target_user_query"),
                    participant_id=(run.summary_json or {}).get("participant_id"),
                    analysis_context=(run.summary_json or {}).get("analysis_context"),
                    preprocess_run_id=(run.summary_json or {}).get("preprocess_run_id"),
                )
                self._ensure_run_active(run.id, run.project_id)
                retrieval_trace = dict(analysis.retrieval_trace or {})
                retrieval_mode = str(retrieval_trace.get("mode") or "telegram_agent")
                summary = dict(run.summary_json or {})
                summary["topic_count_used"] = max(
                    int(summary.get("topic_count_used", 0) or 0),
                    int(retrieval_trace.get("topic_count_used", 0) or 0),
                )
                run.summary_json = summary
                result = self._facet_result_from_payload(facet_def, analysis.payload)
                self._apply_facet_result(
                    session,
                    run,
                    facet_def,
                    retrieval_mode,
                    retrieval_trace,
                    int(analysis.hit_count or 0),
                    result,
                )
            except Exception as exc:
                retrieval_mode = "telegram_agent_failed"
                retrieval_trace = {
                    "mode": retrieval_mode,
                    "evidence_kind": "telegram_messages",
                    "preprocess_run_id": summary.get("preprocess_run_id"),
                    "error": self._format_exception(exc),
                    "traceback": traceback.format_exc(),
                }
                result = self._build_failed_facet_result(
                    facet_def,
                    exc,
                    llm_called=bool(chat_config),
                )
                self._apply_facet_result(
                    session,
                    run,
                    facet_def,
                    retrieval_mode,
                    retrieval_trace,
                    0,
                    result,
                )

    def _rerun_telegram_facet(
        self,
        session: Session,
        run: AnalysisRun,
        project: Project,
        facet_def: FacetDefinition,
        chat_config: ServiceConfig | None,
        *,
        llm_payload: dict[str, Any] | None,
    ) -> AnalysisRun:
        agent = self._build_telegram_agent(session, project, chat_config)
        agent.trace_callback = lambda event: self._publish_trace(run.id, event)
        self._refresh_telegram_summary_counts(session, run)
        if not (run.summary_json or {}).get("preprocess_run_id"):
            raise ValueError("Telegram analysis requires a completed preprocess run.")
        target_user = agent.resolve_target_user(
            target_user_query=(run.summary_json or {}).get("target_user_query"),
            participant_id=(run.summary_json or {}).get("participant_id"),
            preprocess_run_id=(run.summary_json or {}).get("preprocess_run_id"),
        )
        summary = dict(run.summary_json or {})
        summary["target_user"] = target_user
        summary["target_role"] = target_user.get("label") or target_user.get("primary_alias") or target_user.get("display_name")
        run.summary_json = summary
        self._mark_facet_preparing(session, run, facet_def)
        self._mark_facet_running(
            session,
            run,
            facet_def,
            "telegram_agent",
            {
                "mode": "telegram_agent",
                "evidence_kind": "telegram_messages",
                "tool_calls": [],
                "preprocess_run_id": (run.summary_json or {}).get("preprocess_run_id"),
                "target_user": target_user,
                "topic_ids": [],
                "queried_message_ids": [],
            },
            0,
            llm_payload=llm_payload,
        )
        try:
            analysis = agent.analyze_facet(
                facet_def,
                target_user_query=(run.summary_json or {}).get("target_user_query"),
                participant_id=(run.summary_json or {}).get("participant_id"),
                analysis_context=(run.summary_json or {}).get("analysis_context"),
                preprocess_run_id=(run.summary_json or {}).get("preprocess_run_id"),
            )
            self._ensure_run_active(run.id, run.project_id)
            retrieval_trace = dict(analysis.retrieval_trace or {})
            retrieval_mode = str(retrieval_trace.get("mode") or "telegram_agent")
            summary = dict(run.summary_json or {})
            summary["topic_count_used"] = max(
                int(summary.get("topic_count_used", 0) or 0),
                int(retrieval_trace.get("topic_count_used", 0) or 0),
            )
            run.summary_json = summary
            result = self._facet_result_from_payload(facet_def, analysis.payload)
            self._apply_facet_result(
                session,
                run,
                facet_def,
                retrieval_mode,
                retrieval_trace,
                int(analysis.hit_count or 0),
                result,
            )
        except Exception as exc:
            retrieval_mode = "telegram_agent_failed"
            retrieval_trace = {
                "mode": retrieval_mode,
                "evidence_kind": "telegram_messages",
                "preprocess_run_id": (run.summary_json or {}).get("preprocess_run_id"),
                "error": self._format_exception(exc),
                "traceback": traceback.format_exc(),
            }
            result = self._build_failed_facet_result(
                facet_def,
                exc,
                llm_called=bool(chat_config),
            )
            self._apply_facet_result(
                session,
                run,
                facet_def,
                retrieval_mode,
                retrieval_trace,
                0,
                result,
            )

        run.finished_at = utcnow()
        summary = dict(run.summary_json or {})
        run.status = "completed" if int(summary.get("failed_facets", 0)) == 0 else "partial_failed"
        summary["finished_at"] = run.finished_at.isoformat()
        run.summary_json = summary
        self._recalculate_run_summary(session, run)
        refreshed_facet = repository.get_facet(session, run.id, facet_def.key)
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet_def.label} 宸查噸鏂板畬鎴愩€?",
            level="success" if refreshed_facet and refreshed_facet.status == "completed" else "error",
            payload_json={"facet_key": facet_def.key},
        )
        self._persist_progress(session, run.id)
        return repository.get_analysis_run(session, run.id) or run

    def _run_parallel_processes(
        self,
        session: Session,
        run: AnalysisRun,
        project_name: str,
        *,
        llm_payload: dict[str, Any] | None,
        embedding_config: ServiceConfig | None,
    ) -> list[tuple[FacetDefinition, str, int, FacetResult]]:
        run_concurrency = self._resolve_run_concurrency(run)
        executor = ProcessPoolExecutor(max_workers=min(len(FACETS), run_concurrency))
        try:
            return self._execute_facets_with_executor(
                session,
                run,
                project_name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
                executor=executor,
            )
        except AnalysisCancelledError:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    def _run_parallel_threads(
        self,
        session: Session,
        run: AnalysisRun,
        project_name: str,
        *,
        llm_payload: dict[str, Any] | None,
        embedding_config: ServiceConfig | None,
    ) -> list[tuple[FacetDefinition, str, int, FacetResult]]:
        run_concurrency = self._resolve_run_concurrency(run)
        executor = ThreadPoolExecutor(
            max_workers=min(len(FACETS), run_concurrency),
            thread_name_prefix="facet-thread",
        )
        try:
            return self._execute_facets_with_executor(
                session,
                run,
                project_name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
                executor=executor,
            )
        except AnalysisCancelledError:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    def _execute_facets_with_executor(
        self,
        session: Session,
        run: AnalysisRun,
        project_name: str,
        *,
        llm_payload: dict[str, Any] | None,
        embedding_config: ServiceConfig | None,
        executor: Any,
    ) -> list[tuple[FacetDefinition, str, int, FacetResult]]:
        future_map: dict[Future[dict[str, Any]], tuple[FacetDefinition, str, dict[str, Any], int]] = {}
        pending_facets = list(FACETS)
        run_concurrency = self._resolve_run_concurrency(run)
        results: list[tuple[FacetDefinition, str, int, FacetResult]] = []

        try:
            while pending_facets or future_map:
                self._ensure_run_active(run.id, run.project_id)
                while pending_facets and len(future_map) < run_concurrency:
                    self._ensure_run_active(run.id, run.project_id)
                    facet = pending_facets.pop(0)
                    self._mark_facet_preparing(session, run, facet)
                    prepared = self._prepare_facet_execution(
                        session,
                        run,
                        facet,
                        embedding_config=embedding_config,
                        llm_payload=llm_payload,
                    )
                    self._ensure_run_active(run.id, run.project_id)
                    if not prepared:
                        continue

                    hits, retrieval_mode, retrieval_trace = prepared
                    self._mark_facet_running(
                        session,
                        run,
                        facet,
                        retrieval_mode,
                        retrieval_trace,
                        len(hits),
                        llm_payload=llm_payload,
                    )
                    future = self._submit_facet_work(
                        executor,
                        run,
                        facet,
                        project_name,
                        hits,
                        llm_payload=llm_payload,
                    )
                    future_map[future] = (facet, retrieval_mode, retrieval_trace, len(hits))

                if not future_map:
                    continue

                completed_futures, _ = wait(list(future_map.keys()), timeout=0.25, return_when=FIRST_COMPLETED)
                self._ensure_run_active(run.id, run.project_id)
                for future in completed_futures:
                    facet, retrieval_mode, retrieval_trace, hit_count = future_map.pop(future)
                    self._ensure_run_active(run.id, run.project_id)
                    try:
                        result = FacetResult(**future.result())
                    except Exception as exc:
                        result = self._build_failed_facet_result(facet, exc, llm_called=bool(llm_payload))
                    self._ensure_run_active(run.id, run.project_id)
                    self._apply_facet_result(session, run, facet, retrieval_mode, retrieval_trace, hit_count, result)
                    results.append((facet, retrieval_mode, hit_count, result))

            return results
        finally:
            for future in future_map:
                future.cancel()

    def _submit_facet_work(
        self,
        executor: Any,
        run: AnalysisRun,
        facet: FacetDefinition,
        project_name: str,
        hits: list[RetrievedChunk],
        *,
        llm_payload: dict[str, Any] | None,
    ) -> Future[dict[str, Any]]:
        return executor.submit(
            analyze_facet_worker,
            facet,
            project_name,
            [self._serialize_hit(hit) for hit in hits],
            llm_payload,
            self.llm_log_path,
            (run.summary_json or {}).get("target_role"),
            (run.summary_json or {}).get("analysis_context"),
            self._build_stream_callback(run.id, facet),
        )

    def _resolve_run_concurrency(self, run: AnalysisRun) -> int:
        return _normalize_concurrency((run.summary_json or {}).get("concurrency") or self.facet_max_workers)

    def _prepare_facet_execution(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        *,
        embedding_config: ServiceConfig | None,
        llm_payload: dict[str, Any] | None,
    ) -> tuple[list[RetrievedChunk], str, dict[str, Any]] | None:
        try:
            hits, retrieval_mode, retrieval_trace = self._retrieve_hits(
                session,
                run.project_id,
                facet,
                embedding_config=embedding_config,
                llm_payload=llm_payload,
                target_role=(run.summary_json or {}).get("target_role"),
                analysis_context=(run.summary_json or {}).get("analysis_context"),
            )
        except Exception as exc:
            self._handle_facet_setup_error(
                session,
                run,
                facet,
                exc,
                embedding_config=embedding_config,
            )
            return None
        return hits, retrieval_mode, retrieval_trace

    def _handle_facet_setup_error(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        exc: Exception,
        *,
        embedding_config: ServiceConfig | None,
    ) -> None:
        retrieval_mode = "retrieval_failed"
        retrieval_trace = self._build_retrieval_exception_trace(exc, embedding_config=embedding_config)
        error_text = self._format_exception(exc)
        self._record_retrieval_event(
            session,
            run.id,
            facet,
            retrieval_mode,
            retrieval_trace,
            0,
            level="error",
            message=f"{facet.label} 在检索阶段失败：{error_text}",
        )
        result = self._build_failed_facet_result(facet, exc, llm_called=False)
        self._apply_facet_result(session, run, facet, retrieval_mode, retrieval_trace, 0, result)

    def _build_retrieval_exception_trace(
        self,
        exc: Exception,
        *,
        embedding_config: ServiceConfig | None,
    ) -> dict[str, Any]:
        embedding_url = None
        if embedding_config:
            try:
                embedding_url = OpenAICompatibleClient(
                    embedding_config,
                    log_path=self.llm_log_path,
                ).endpoint_url("/embeddings")
            except Exception:
                embedding_url = None
        return {
            "mode": "retrieval_failed",
            "embedding_configured": bool(embedding_config),
            "embedding_attempted": False,
            "embedding_api_called": False,
            "embedding_success": False,
            "embedding_error": None,
            "embedding_url": embedding_url,
            "embedding_skip_reason": None,
            "fallback_reason": "retrieval_exception",
            "error": self._format_exception(exc),
            "traceback": traceback.format_exc(),
        }

    def _record_retrieval_event(
        self,
        session: Session,
        run_id: str,
        facet: FacetDefinition,
        retrieval_mode: str,
        retrieval_trace: dict[str, Any],
        hit_count: int,
        *,
        level: str = "info",
        message: str | None = None,
    ) -> None:
        trace = dict(retrieval_trace or {})
        if not message:
            trace_error = str(trace.get("error") or "").strip()
            if trace_error:
                message = f"{facet.label} 在检索阶段失败：{trace_error}"
            elif trace.get("embedding_api_called"):
                message = f"{facet.label} 已检索到 {hit_count} 个证据片段，并实际调用了 embeddings。"
            elif trace.get("embedding_configured") and trace.get("embedding_skip_reason"):
                message = (
                    f"{facet.label} 已检索到 {hit_count} 个证据片段，未调用 embeddings："
                    f"{trace.get('embedding_skip_reason')}。"
                )
            else:
                message = f"{facet.label} 已检索到 {hit_count} 个证据片段。"
        repository.add_analysis_event(
            session,
            run_id,
            event_type="retrieval",
            level=level,
            message=message,
            payload_json={
                "facet_key": facet.key,
                "retrieval_mode": retrieval_mode,
                "hit_count": hit_count,
                "retrieval_trace": trace,
            },
        )

    @staticmethod
    def _build_failed_facet_result(
        facet: FacetDefinition,
        exc: Exception,
        *,
        llm_called: bool,
    ) -> FacetResult:
        error_text = AnalysisEngine._format_exception(exc)
        return FacetResult(
            facet_key=facet.key,
            status="failed",
            confidence=0.0,
            summary="",
            bullets=[],
            evidence=[],
            conflicts=[],
            notes=error_text,
            raw_payload={
                "_meta": {
                    "llm_called": llm_called,
                    "llm_success": False,
                    "llm_attempts": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "duration_ms": 0,
                },
                "error": error_text,
                "traceback": traceback.format_exc(),
            },
        )

    @staticmethod
    def _format_exception(exc: Exception) -> str:
        text = str(exc).strip()
        if text:
            return text
        return exc.__class__.__name__

    def _mark_facet_preparing(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
    ) -> None:
        run.status = "running"
        run.finished_at = None
        summary = dict(run.summary_json or {})
        repository.upsert_facet(
            session,
            run.id,
            facet.key,
            status="preparing",
            confidence=0.0,
            findings_json={
                "label": facet.label,
                "summary": "",
                "bullets": [],
                "notes": None,
                "retrieval_mode": None,
                "retrieval_trace": None,
                "hit_count": 0,
                "target_role": summary.get("target_role"),
                "target_user": summary.get("target_user"),
                "target_user_query": summary.get("target_user_query"),
                "participant_id": summary.get("participant_id"),
                "preprocess_run_id": summary.get("preprocess_run_id"),
                "analysis_context": summary.get("analysis_context"),
                "llm_live_text": "",
                "llm_called": False,
                "llm_success": None,
                "llm_attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "duration_ms": 0,
                "llm_response_text": None,
                "llm_request_payload": None,
                "llm_request_url": None,
                "llm_error": None,
                "llm_log_path": None,
                "phase": "retrieving",
                "queue_position": None,
                "started_at": utcnow().isoformat(),
                "finished_at": None,
            },
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        self._recalculate_run_summary(session, run)
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet.label} claimed an execution slot and started retrieval.",
            payload_json={
                "facet_key": facet.key,
                "phase": "retrieving",
            },
        )
        self._persist_progress(session, run.id)

    def _mark_facet_running(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        retrieval_mode: str,
        retrieval_trace: dict[str, Any],
        hit_count: int,
        llm_payload: dict[str, Any] | None = None,
    ) -> None:
        summary = dict(run.summary_json or {})
        existing = repository.get_facet(session, run.id, facet.key)
        existing_findings = dict(existing.findings_json or {}) if existing and existing.findings_json else {}
        phase = "llm" if llm_payload else "analyzing"
        repository.upsert_facet(
            session,
            run.id,
            facet.key,
            status="running",
            confidence=0.0,
            findings_json={
                "label": facet.label,
                "summary": "",
                "bullets": [],
                "notes": None,
                "retrieval_mode": retrieval_mode,
                "retrieval_trace": retrieval_trace,
                "hit_count": hit_count,
                "target_role": summary.get("target_role"),
                "target_user": summary.get("target_user"),
                "target_user_query": summary.get("target_user_query"),
                "participant_id": summary.get("participant_id"),
                "preprocess_run_id": summary.get("preprocess_run_id"),
                "analysis_context": summary.get("analysis_context"),
                "llm_live_text": existing_findings.get("llm_live_text", ""),
                "llm_called": False,
                "llm_success": None,
                "llm_attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "duration_ms": 0,
                "llm_response_text": None,
                "llm_request_payload": None,
                "llm_request_url": None,
                "llm_error": None,
                "llm_log_path": None,
                "phase": phase,
                "queue_position": None,
                "started_at": existing_findings.get("started_at") or utcnow().isoformat(),
                "finished_at": None,
            },
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        self._recalculate_run_summary(session, run)
        self._record_retrieval_event(
            session,
            run.id,
            facet,
            retrieval_mode,
            retrieval_trace,
            hit_count,
        )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet.label} started analysis with {hit_count} retrieved evidence chunks.",
            payload_json={
                "facet_key": facet.key,
                "phase": phase,
                "retrieval_mode": retrieval_mode,
                "hit_count": hit_count,
            },
        )
        if llm_payload:
            config = ServiceConfig(**llm_payload)
            endpoint_path = "/responses" if normalize_api_mode(config.api_mode) == "responses" else "/chat/completions"
            repository.add_analysis_event(
                session,
                run.id,
                event_type="llm_request",
                message=f"{facet.label} prepared an LLM request.",
                payload_json={
                    "facet_key": facet.key,
                    "url": OpenAICompatibleClient(config, log_path=self.llm_log_path).endpoint_url(endpoint_path),
                    "model": config.model,
                    "provider_kind": config.provider_kind,
                    "api_mode": normalize_api_mode(config.api_mode),
                    "log_path": self.llm_log_path,
                    "request_payload": None,
                },
            )
        self._persist_progress(session, run.id)

    def _mark_facet_started(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        retrieval_mode: str,
        retrieval_trace: dict[str, Any],
        hit_count: int,
        llm_payload: dict[str, Any] | None = None,
    ) -> None:
        summary = dict(run.summary_json or {})
        summary["current_stage"] = f"分析 {facet.label}"
        summary["current_facet"] = facet.key
        run.summary_json = summary
        repository.upsert_facet(
            session,
            run.id,
            facet.key,
            status="running",
            confidence=0.0,
            findings_json={
                "label": facet.label,
                "summary": "",
                "bullets": [],
                "notes": None,
                "retrieval_mode": retrieval_mode,
                "retrieval_trace": retrieval_trace,
                "hit_count": hit_count,
                "target_role": summary.get("target_role"),
                "target_user": summary.get("target_user"),
                "target_user_query": summary.get("target_user_query"),
                "participant_id": summary.get("participant_id"),
                "preprocess_run_id": summary.get("preprocess_run_id"),
                "analysis_context": summary.get("analysis_context"),
                "llm_live_text": "",
                "llm_called": False,
                "llm_success": None,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "duration_ms": 0,
            },
            evidence_json=[],
            conflicts_json=[],
            error_message=None,
        )
        self._recalculate_run_summary(session, run)
        self._record_retrieval_event(
            session,
            run.id,
            facet,
            retrieval_mode,
            retrieval_trace,
            hit_count,
        )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet.label} 已启动，召回到 {hit_count} 个证据片段。",
            payload_json={
                "facet_key": facet.key,
                "retrieval_mode": retrieval_mode,
                "retrieval_trace": retrieval_trace,
                "hit_count": hit_count,
            },
        )
        if llm_payload:
            config = ServiceConfig(**llm_payload)
            endpoint_path = "/responses" if normalize_api_mode(config.api_mode) == "responses" else "/chat/completions"
            repository.add_analysis_event(
                session,
                run.id,
                event_type="llm_request",
                message=f"{facet.label} preparing LLM request.",
                payload_json={
                    "facet_key": facet.key,
                    "url": OpenAICompatibleClient(config, log_path=self.llm_log_path).endpoint_url(endpoint_path),
                    "model": config.model,
                    "provider_kind": config.provider_kind,
                    "api_mode": normalize_api_mode(config.api_mode),
                    "log_path": self.llm_log_path,
                    "request_payload": None,
                },
            )
        self._persist_progress(session, run.id)

    def _apply_facet_result(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        retrieval_mode: str,
        retrieval_trace: dict[str, Any],
        hit_count: int,
        result: FacetResult,
    ) -> None:
        existing = repository.get_facet(session, run.id, facet.key)
        existing_findings = dict(existing.findings_json or {}) if existing and existing.findings_json else {}
        meta = dict(result.raw_payload.get("_meta") or {})
        finished_at = utcnow().isoformat()
        started_at = existing_findings.get("started_at") or finished_at
        phase = "completed" if result.status == "completed" else "failed"
        findings_json = {
            "label": facet.label,
            "summary": result.summary,
            "bullets": result.bullets,
            "notes": result.notes,
            "retrieval_mode": retrieval_mode,
            "retrieval_trace": retrieval_trace,
            "hit_count": hit_count,
            "target_role": (run.summary_json or {}).get("target_role"),
            "target_user": (run.summary_json or {}).get("target_user"),
            "target_user_query": (run.summary_json or {}).get("target_user_query"),
            "participant_id": (run.summary_json or {}).get("participant_id"),
            "preprocess_run_id": (run.summary_json or {}).get("preprocess_run_id"),
            "analysis_context": (run.summary_json or {}).get("analysis_context"),
            "llm_called": bool(meta.get("llm_called", False)),
            "llm_success": meta.get("llm_success"),
            "llm_model": meta.get("llm_model"),
            "llm_attempts": int(meta.get("llm_attempts", 0)),
            "prompt_tokens": int(meta.get("prompt_tokens", 0)),
            "completion_tokens": int(meta.get("completion_tokens", 0)),
            "total_tokens": int(meta.get("total_tokens", 0)),
            "duration_ms": int(meta.get("duration_ms", 0)),
            "llm_request_url": meta.get("request_url"),
            "llm_request_payload": meta.get("request_payload"),
            "llm_live_text": (
                (meta.get("raw_text") if meta.get("raw_text") is not None else existing_findings.get("llm_live_text"))
                or ""
            )[:RAW_TEXT_PREVIEW_LIMIT],
            "llm_response_text": (
                meta.get("raw_text") if meta.get("raw_text") is not None else existing_findings.get("llm_response_text")
            ),
            "llm_error": meta.get("llm_error"),
            "llm_log_path": meta.get("log_path"),
            "phase": phase,
            "queue_position": None,
            "started_at": started_at,
            "finished_at": finished_at,
        }
        repository.upsert_facet(
            session,
            run.id,
            facet.key,
            status=result.status,
            confidence=result.confidence,
            findings_json=findings_json,
            evidence_json=result.evidence,
            conflicts_json=result.conflicts,
            error_message=result.notes if result.status == "failed" else None,
        )

        summary = dict(run.summary_json or {})
        summary["current_stage"] = f"已完成 {facet.label}"
        run.summary_json = summary

        message = f"{facet.label} 完成。"
        summary["current_facet"] = facet.key
        summary["current_stage"] = f"{facet.label} {'已完成' if result.status == 'completed' else '失败'}"
        run.summary_json = summary
        self._recalculate_run_summary(session, run)

        if findings_json["llm_called"]:
            llm_state = "成功" if findings_json["llm_success"] else "失败"
            message = f"{message} LLM {llm_state}，消耗 {findings_json['total_tokens']} tokens。"
        message = (
            f"{facet.label} {'完成' if result.status == 'completed' else '失败'}。"
            if not findings_json["llm_called"]
            else (
                f"{facet.label} {'完成' if result.status == 'completed' else '失败'}。"
                f" LLM {'成功' if findings_json['llm_success'] else '失败'}，"
                f"消耗 {findings_json['total_tokens']} tokens。"
            )
        )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=message,
            level="success" if result.status == "completed" else "error",
            payload_json={
                "facet_key": facet.key,
                "status": result.status,
                "total_tokens": findings_json["total_tokens"],
                "llm_success": findings_json["llm_success"],
            },
        )
        if meta.get("llm_called"):
            repository.add_analysis_event(
                session,
                run.id,
                event_type="llm_request",
                level="info",
                message=f"{facet.label} 调用了 LLM 接口。",
                payload_json={
                    "facet_key": facet.key,
                    "url": meta.get("request_url"),
                    "model": meta.get("llm_model"),
                    "provider_kind": meta.get("provider_kind"),
                    "api_mode": meta.get("api_mode"),
                    "log_path": meta.get("log_path"),
                    "request_payload": meta.get("request_payload"),
                },
            )
            repository.add_analysis_event(
                session,
                run.id,
                event_type="llm_response",
                level="success" if meta.get("llm_success") else "warning",
                message=f"{facet.label} 收到了 LLM 返回。",
                payload_json={
                    "facet_key": facet.key,
                    "success": meta.get("llm_success"),
                    "error": meta.get("llm_error"),
                    "provider_kind": meta.get("provider_kind"),
                    "api_mode": meta.get("api_mode"),
                    "response_text": meta.get("raw_text"),
                    "log_path": meta.get("log_path"),
                },
            )
        self._persist_progress(session, run.id)

    def _recalculate_run_summary(self, session: Session, run: AnalysisRun) -> None:
        order = {facet.key: index for index, facet in enumerate(FACETS)}
        facets = sorted(
            list(session.scalars(select(AnalysisFacet).where(AnalysisFacet.run_id == run.id))),
            key=lambda item: order.get(item.facet_key, 999),
        )
        summary = dict(run.summary_json or {})
        completed = 0
        failed = 0
        active = 0
        queued = 0
        llm_calls = 0
        llm_successes = 0
        llm_failures = 0
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        active_facet: AnalysisFacet | None = None

        for facet in facets:
            normalized_status = str(facet.status or "queued").strip().lower().replace(" ", "_")
            if normalized_status in {"", "pending"}:
                normalized_status = "queued"
            if normalized_status not in {"queued", "preparing", "running", "completed", "failed"}:
                normalized_status = "queued"
            if facet.status != normalized_status:
                facet.status = normalized_status

            findings = dict(facet.findings_json or {})
            findings["label"] = findings.get("label") or facet.facet_key

            if normalized_status == "completed":
                completed += 1
                findings["phase"] = "completed"
                findings["queue_position"] = None
            elif normalized_status == "failed":
                failed += 1
                findings["phase"] = "failed"
                findings["queue_position"] = None
            elif normalized_status == "preparing":
                active += 1
                findings["phase"] = "retrieving"
                findings["queue_position"] = None
                findings["finished_at"] = None
                if active_facet is None:
                    active_facet = facet
            elif normalized_status == "running":
                active += 1
                findings["phase"] = findings.get("phase") or "analyzing"
                findings["queue_position"] = None
                findings["finished_at"] = None
                if active_facet is None:
                    active_facet = facet
            else:
                queued += 1
                findings["phase"] = "queued"

            if findings.get("llm_called"):
                llm_calls += int(findings.get("llm_attempts", 1) or 1)
                if findings.get("llm_success") is True:
                    llm_successes += 1
                elif findings.get("llm_success") is False:
                    llm_failures += 1
                prompt_tokens += int(findings.get("prompt_tokens", 0) or 0)
                completion_tokens += int(findings.get("completion_tokens", 0) or 0)
                total_tokens += int(findings.get("total_tokens", 0) or 0)

            facet.findings_json = findings

        queue_position = 1
        for facet in facets:
            findings = dict(facet.findings_json or {})
            if facet.status == "queued":
                findings["queue_position"] = queue_position
                queue_position += 1
            else:
                findings["queue_position"] = None
            facet.findings_json = findings

        summary["completed_facets"] = completed
        summary["failed_facets"] = failed
        summary["active_facets"] = active
        summary["queued_facets"] = queued
        summary["concurrency"] = _normalize_concurrency(summary.get("concurrency") or self.facet_max_workers)
        summary["llm_calls"] = llm_calls
        summary["llm_successes"] = llm_successes
        summary["llm_failures"] = llm_failures
        summary["prompt_tokens"] = prompt_tokens
        summary["completion_tokens"] = completion_tokens
        summary["total_tokens"] = total_tokens
        progress_done = completed + failed
        summary["progress_percent"] = int((progress_done / len(FACETS)) * 100)

        if active_facet is not None:
            active_findings = dict(active_facet.findings_json or {})
            current_phase = str(active_findings.get("phase") or "").strip().lower() or "running"
            summary["current_facet"] = active_facet.facet_key
            summary["current_phase"] = current_phase
            label = active_findings.get("label") or active_facet.facet_key
            if current_phase == "retrieving":
                summary["current_stage"] = f"{label}: retrieving evidence"
            elif current_phase == "llm":
                summary["current_stage"] = f"{label}: generating with LLM"
            elif current_phase == "analyzing":
                summary["current_stage"] = f"{label}: analyzing"
            else:
                summary["current_stage"] = f"{label}: in progress"
        elif run.status == "completed":
            summary["current_facet"] = None
            summary["current_phase"] = "completed"
            summary["current_stage"] = "Analysis completed"
        elif run.status in {"failed", "partial_failed"}:
            summary["current_facet"] = None
            summary["current_phase"] = "failed"
            summary["current_stage"] = "Analysis finished with failures"
        elif queued:
            summary["current_facet"] = None
            summary["current_phase"] = "queued"
            summary["current_stage"] = f"{queued} facet(s) waiting for a slot"
        elif run.status == "running":
            summary["current_facet"] = None
            summary["current_phase"] = "persisting"
            summary["current_stage"] = "Finalizing analysis"
        else:
            summary["current_facet"] = None
            summary["current_phase"] = "queued"
            summary["current_stage"] = "Waiting to start"

        run.summary_json = summary

    def _build_stream_callback(self, run_id: str, facet: FacetDefinition):
        if not self.db:
            return None

        state = {"text": "", "event_text": "", "stream_db_disabled": False}

        def callback(delta: str) -> None:
            if not delta:
                return
            state["text"] = f"{state['text']}{delta}"
            if len(state["text"]) > RAW_TEXT_PREVIEW_LIMIT:
                state["text"] = state["text"][-RAW_TEXT_PREVIEW_LIMIT:]
            if state["stream_db_disabled"]:
                return
            state["event_text"] += delta
            if len(state["event_text"]) > RAW_TEXT_PREVIEW_LIMIT:
                state["event_text"] = state["event_text"][-RAW_TEXT_PREVIEW_LIMIT:]
            if len(state["event_text"]) < 80 and not delta.endswith(("\n", ".", "}", "]")):
                return
            persisted = self._flush_stream_delta(run_id, facet, state["text"], state["event_text"])
            if persisted:
                state["event_text"] = ""
                return
            state["stream_db_disabled"] = True
            state["event_text"] = ""

        def flush_remaining() -> None:
            if state["stream_db_disabled"] or not state["event_text"]:
                return
            persisted = self._flush_stream_delta(run_id, facet, state["text"], state["event_text"])
            if not persisted:
                state["stream_db_disabled"] = True
            state["event_text"] = ""

        setattr(callback, "_flush_remaining", flush_remaining)
        return callback

    def _flush_stream_delta(self, run_id: str, facet: FacetDefinition, text: str, delta: str) -> bool:
        if not self.db or (not text and not delta):
            return True
        for _ in range(3):
            try:
                with self.db.session() as session:
                    run = repository.get_analysis_run(session, run_id)
                    if not run:
                        return True
                    if self._is_run_cancelled(run.id, run.project_id):
                        return True
                    facet_record = repository.get_facet(session, run_id, facet.key)
                    if facet_record:
                        findings = dict(facet_record.findings_json or {})
                        findings["llm_live_text"] = text[:RAW_TEXT_PREVIEW_LIMIT]
                        facet_record.findings_json = findings
                return True
            except Exception:
                import time
                time.sleep(0.1)
        return False

    def _retrieve_hits(
        self,
        session: Session,
        project_id: str,
        facet: FacetDefinition,
        *,
        embedding_config: ServiceConfig | None,
        llm_payload: dict[str, Any] | None,
        target_role: str | None,
        analysis_context: str | None,
    ) -> tuple[list[RetrievedChunk], str, dict[str, Any]]:
        target_project_id = repository.get_target_project_id(session, project_id)
        query_parts = [facet.search_query]
        if target_role:
            query_parts.append(target_role)
        if analysis_context:
            query_parts.append(analysis_context)
        query_text = " ".join(part for part in query_parts if part).strip()
        llm_config = ServiceConfig(**llm_payload) if llm_payload else None
        hits, retrieval_mode, retrieval_trace = self.retrieval.search(
            session,
            project_id=target_project_id,
            query=query_text,
            embedding_config=embedding_config,
            llm_config=llm_config,
            log_path=self.llm_log_path,
            limit=FACET_EVIDENCE_LIMIT,
        )
        retrieval_trace = dict(retrieval_trace or {})
        retrieval_trace["query"] = query_text
        retrieval_trace["query_parts"] = [part for part in query_parts if part]
        retrieval_trace["requested_limit"] = FACET_EVIDENCE_LIMIT
        retrieval_trace["result_count"] = len(hits)
        if not hits:
            # Removed fallback hits as it pollutes LLM context
            retrieval_trace["fallback_used"] = False
            retrieval_trace["fallback_hit_count"] = 0
        retrieval_trace["result_count"] = len(hits)
        return hits, retrieval_mode, retrieval_trace

    def _build_initial_summary(
        self,
        session: Session,
        project_id: str,
        *,
        target_role: str | None,
        target_user_query: str | None,
        participant_id: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        project = repository.get_project(session, project_id)
        is_telegram = bool(project and project.mode == "telegram")
        target_project_id = repository.get_target_project_id(session, project_id)
        document_count = (
            session.scalar(
                select(func.count()).select_from(DocumentRecord).where(DocumentRecord.project_id == target_project_id)
            )
            or 0
        )
        chunk_count = (
            0
            if is_telegram
            else (
                session.scalar(
                    select(func.count()).select_from(TextChunk).where(TextChunk.project_id == target_project_id)
                )
                or 0
            )
        )
        failed_count = (
            session.scalar(
                select(func.count()).select_from(DocumentRecord).where(
                    DocumentRecord.project_id == target_project_id,
                    DocumentRecord.ingest_status == "failed",
                )
            )
            or 0
        )
        telegram_message_count = 0
        telegram_participant_count = 0
        telegram_report_count = 0
        preprocess_run_id = None
        preprocess_topic_count = 0
        weekly_candidate_count = 0
        if is_telegram:
            telegram_message_count = (
                session.scalar(
                    select(func.count()).select_from(TelegramMessage).where(TelegramMessage.project_id == target_project_id)
                )
                or 0
            )
            telegram_participant_count = (
                session.scalar(
                    select(func.count()).select_from(TelegramParticipant).where(TelegramParticipant.project_id == target_project_id)
                )
                or 0
            )
            telegram_report_count = (
                session.scalar(
                    select(func.count()).select_from(TelegramTopicReport).where(TelegramTopicReport.project_id == target_project_id)
                )
                or 0
            )
            latest_preprocess_run = repository.get_latest_successful_telegram_preprocess_run(session, target_project_id)
            if latest_preprocess_run:
                preprocess_run_id = latest_preprocess_run.id
                weekly_candidate_count = int((latest_preprocess_run.summary_json or {}).get("weekly_candidate_count") or 0)
                preprocess_topic_count = (
                    session.scalar(
                        select(func.count()).select_from(TelegramPreprocessTopic).where(TelegramPreprocessTopic.run_id == latest_preprocess_run.id)
                    )
                    or 0
                )
        return {
            "document_count": document_count,
            "chunk_count": chunk_count,
            "failed_document_count": failed_count,
            "project_mode": project.mode if project else None,
            "telegram_message_count": telegram_message_count,
            "telegram_participant_count": telegram_participant_count,
            "telegram_report_count": telegram_report_count,
            "telegram_source_project_id": target_project_id if is_telegram else None,
            "preprocess_run_id": preprocess_run_id,
            "topic_count_used": 0,
            "weekly_candidate_count": weekly_candidate_count,
            "weekly_topic_count": preprocess_topic_count,
            "telegram_preprocess_topic_count": preprocess_topic_count,
            "generated_at": utcnow().isoformat(),
            "target_role": (target_role or "").strip() or None,
            "target_user_query": (target_user_query or "").strip() or None,
            "participant_id": (participant_id or "").strip() or None,
            "target_user": None,
            "analysis_context": (analysis_context or "").strip() or None,
            "total_facets": len(FACETS),
            "completed_facets": 0,
            "failed_facets": 0,
            "progress_percent": 0,
            "current_stage": "排队中",
            "current_phase": "queued",
            "current_facet": None,
            "concurrency": DEFAULT_ANALYSIS_CONCURRENCY,
            "active_facets": 0,
            "queued_facets": len(FACETS),
            "llm_calls": 0,
            "llm_successes": 0,
            "llm_failures": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    def _fallback_hits(self, session: Session, project_id: str) -> list[RetrievedChunk]:
        target_project_id = repository.get_target_project_id(session, project_id)
        stmt = (
            select(TextChunk, DocumentRecord)
            .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
            .where(TextChunk.project_id == target_project_id)
            .order_by(TextChunk.chunk_index.asc())
            .limit(FACET_EVIDENCE_LIMIT)
        )
        hits = []
        for chunk, document in session.execute(stmt):
            hits.append(
                RetrievedChunk(
                    chunk_id=chunk.id,
                    document_id=document.id,
                    document_title=document.title or document.filename,
                    filename=document.filename,
                    source_type=document.source_type,
                    content=chunk.content,
                    score=1.0,
                    page_number=chunk.page_number,
                    metadata=chunk.metadata_json or {},
                    anchor_chunk_id=chunk.id,
                    anchor_chunk_index=chunk.chunk_index,
                    context_span={"left": 0, "right": 0, "total_chars": len(chunk.content or "")},
                )
            )
        return hits

    @staticmethod
    def _serialize_hit(hit: RetrievedChunk) -> dict[str, Any]:
        return {
            "chunk_id": hit.chunk_id,
            "document_id": hit.document_id,
            "document_title": hit.document_title,
            "filename": hit.filename,
            "source_type": hit.source_type,
            "content": hit.content,
            "score": hit.score,
            "page_number": hit.page_number,
            "metadata": hit.metadata,
            "anchor_chunk_id": hit.anchor_chunk_id,
            "anchor_chunk_index": hit.anchor_chunk_index,
            "context_span": dict(hit.context_span or {}),
        }

    def _persist_progress(self, session: Session, run_id: str | None = None) -> None:
        session.flush()
        session.commit()
        if run_id and self.stream_hub:
            self.stream_hub.publish(run_id)

    def _publish_trace(self, run_id: str, event: dict[str, Any] | None) -> None:
        if not self.stream_hub or not run_id:
            return
        payload = dict(event or {})
        payload.setdefault("timestamp", utcnow().isoformat())
        self.stream_hub.publish(run_id, event="trace", payload=payload)

    def _is_run_cancelled(self, run_id: str, project_id: str) -> bool:
        if not self.cancel_checker:
            return False
        try:
            return bool(self.cancel_checker(run_id, project_id))
        except Exception:
            return False

    def _ensure_run_active(self, run_id: str, project_id: str) -> None:
        if self._is_run_cancelled(run_id, project_id):
            raise AnalysisCancelledError("Analysis run cancelled.")


def _analyze_with_llm(
    facet: FacetDefinition,
    project_name: str,
    chunks: list[dict[str, Any]],
    llm_config: dict[str, Any],
    *,
    llm_log_path: str | None,
    target_role: str | None,
    analysis_context: str | None,
    stream_callback: Any | None = None,
) -> dict[str, Any]:
    config = ServiceConfig(**llm_config)
    client = OpenAICompatibleClient(config, log_path=llm_log_path)
    excerpt_text = "\n\n".join(
        f"[{chunk['chunk_id']}] {chunk['document_title']} / {chunk['filename']}\n{chunk['content'][:900]}"
        for chunk in chunks
    )
    messages = build_facet_analysis_messages(
        project_name,
        facet,
        excerpt_text,
        target_role=target_role,
        analysis_context=analysis_context,
    )
    endpoint_path = "/responses" if normalize_api_mode(config.api_mode) == "responses" else "/chat/completions"
    request_payload: dict[str, Any] = {
        "messages": messages,
        "model": config.model,
        "api_mode": config.api_mode,
        "endpoint_url": client.endpoint_url(endpoint_path),
    }
    started = perf_counter()
    last_error: Exception | None = None
    attempts = 0
    for _ in range(2):
        attempts += 1
        try:
            completion = client.chat_completion_result(
                messages,
                model=config.model,
                temperature=0.2,
                max_tokens=None,
                stream_handler=stream_callback,
            )
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            try:
                parsed = parse_json_response(completion.content, fallback=False)
                llm_success = True
            except LLMError as exc:
                parsed = parse_json_response(completion.content, fallback=True)
                llm_success = False
                llm_error_text = str(exc)
            normalized = _normalize_facet_payload(parsed, chunks, facet)
            if not llm_success:
                normalized["notes"] = (
                    f"{normalized.get('notes') or ''}\n"
                    "LLM returned non-JSON text, so the facet was recovered with fallback parsing."
                ).strip()
            normalized["_meta"] = {
                "llm_called": True,
                "llm_success": llm_success,
                "llm_attempts": attempts,
                "provider_kind": config.provider_kind,
                "api_mode": normalize_api_mode(config.api_mode),
                "llm_model": completion.model,
                "prompt_tokens": completion.usage.get("prompt_tokens", 0),
                "completion_tokens": completion.usage.get("completion_tokens", 0),
                "total_tokens": completion.usage.get("total_tokens", 0),
                "duration_ms": int((perf_counter() - started) * 1000),
                "request_url": completion.request_url,
                "request_payload": completion.request_payload or request_payload,
                "raw_text": completion.content[:RAW_TEXT_PREVIEW_LIMIT],
                "llm_error": None if llm_success else llm_error_text,
                "log_path": llm_log_path,
            }
            return normalized
        except (LLMError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            if getattr(exc, "request_payload", None) is None:
                setattr(exc, "request_payload", request_payload)
            if getattr(exc, "request_url", None) is None:
                setattr(exc, "request_url", client.endpoint_url(endpoint_path))
            last_error = exc
    raise LLMError(
        str(last_error) if last_error else "Failed to analyze facet.",
        raw_text=(getattr(last_error, "raw_text", None) or "")[:RAW_TEXT_PREVIEW_LIMIT] or None,
        request_url=getattr(last_error, "request_url", None),
        request_payload=getattr(last_error, "request_payload", None),
    )


def _analyze_heuristically(
    facet: FacetDefinition,
    chunks: list[dict[str, Any]],
    *,
    target_role: str | None,
    analysis_context: str | None,
) -> dict[str, Any]:
    started = perf_counter()
    joined = "\n".join(chunk["content"] for chunk in chunks)
    terms = top_terms(joined, limit=10)
    profile = get_facet_prompt_profile(facet.key)
    label_cycle = iter(profile.bullet_labels)
    bullets: list[str] = []
    if target_role:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：分析对象为 {target_role}，当前只归纳 {facet.label}。")
    if analysis_context:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：语境约束为 {analysis_context[:100]}。")
    if terms:
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：高频词包括 {', '.join(terms[:5])}。")
    for chunk in chunks[:4]:
        preview = chunk["content"][:100].replace("\n", " ")
        bullets.append(f"{next(label_cycle, profile.bullet_labels[-1])}：{preview}")
    bullets, _ = _normalize_facet_bullets(bullets, facet)
    summary_focus = "、".join(terms[:4]) or profile.focus.split("、", 1)[0]
    evidence = [
        {
            "chunk_id": chunk["chunk_id"],
            "reason": f"{facet.label} 的代表片段",
            "quote": chunk["content"][:160],
            "document_title": chunk["document_title"],
            "filename": chunk["filename"],
            "page_number": chunk["page_number"],
        }
        for chunk in chunks[:FACET_EVIDENCE_LIMIT]
    ]
    return {
        "summary": (
            f"围绕 {facet.label}，现有 {len(chunks)} 个高相关片段主要指向 {summary_focus}；"
            "由于未调用 LLM，当前结果以证据驱动的保守归纳为主。"
        ),
        "bullets": bullets[:FACET_BULLET_LIMIT],
        "confidence": min(0.45 + (len(chunks) * 0.07), 0.78),
        "evidence": evidence,
        "conflicts": [],
        "notes": "LLM 未配置，结果来自启发式降级分析。",
        "_meta": {
            "llm_called": False,
            "llm_success": False,
            "llm_attempts": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "duration_ms": int((perf_counter() - started) * 1000),
        },
    }


def _normalize_facet_payload(
    payload: dict[str, Any],
    chunks: list[dict[str, Any]],
    facet: FacetDefinition,
) -> dict[str, Any]:
    chunk_map = {chunk["chunk_id"]: chunk for chunk in chunks}
    evidence: list[dict[str, Any]] = []
    for item in payload.get("evidence", [])[:FACET_EVIDENCE_LIMIT]:
        chunk_id = item.get("chunk_id")
        if not chunk_id or chunk_id not in chunk_map:
            continue
        source = chunk_map[chunk_id]
        evidence.append(
            {
                "chunk_id": chunk_id,
                "reason": item.get("reason", ""),
                "quote": item.get("quote", source["content"][:160]),
                "document_title": source["document_title"],
                "filename": source["filename"],
                "page_number": source["page_number"],
            }
        )
    seen = {item["chunk_id"] for item in evidence}
    for chunk in chunks:
        if len(evidence) >= FACET_EVIDENCE_LIMIT:
            break
        if chunk["chunk_id"] in seen:
            continue
        evidence.append(
            {
                "chunk_id": chunk["chunk_id"],
                "reason": "Retrieved evidence candidate",
                "quote": chunk["content"][:160],
                "document_title": chunk["document_title"],
                "filename": chunk["filename"],
                "page_number": chunk["page_number"],
            }
        )
        seen.add(chunk["chunk_id"])
    bullets, removed_bullets = _normalize_facet_bullets(payload.get("bullets", []), facet)
    summary, summary_rebuilt = _normalize_facet_summary(payload.get("summary", ""), bullets, facet)
    notes_parts = []
    raw_notes = _collapse_whitespace(payload.get("notes"))
    if raw_notes:
        notes_parts.append(raw_notes)
    if removed_bullets:
        notes_parts.append(
            f"Normalization removed {removed_bullets} off-facet bullet(s) so {facet.label} stays scoped to the current dimension."
        )
    if summary_rebuilt:
        notes_parts.append("Summary was rebuilt during normalization to keep the result focused on the current facet.")
    return {
        "summary": summary,
        "bullets": bullets,
        "confidence": _parse_confidence(payload.get("confidence"), 0.65),
        "evidence": evidence,
        "conflicts": [
            {
                "title": _collapse_whitespace(item.get("title")),
                "detail": _collapse_whitespace(item.get("detail")),
            }
            for item in payload.get("conflicts", [])[:5]
            if _collapse_whitespace(item.get("title")) or _collapse_whitespace(item.get("detail"))
        ],
        "notes": "\n".join(notes_parts) or None,
    }
