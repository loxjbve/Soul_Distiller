from __future__ import annotations

import json
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from time import perf_counter
from typing import Any

from app.analysis.prompts import build_facet_analysis_messages
from app.db import Database
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.analysis.facets import FACETS, FacetDefinition
from app.llm.client import LLMError, OpenAICompatibleClient, normalize_api_mode, parse_json_response
from app.models import AnalysisFacet, AnalysisRun, DocumentRecord, TextChunk, utcnow
from app.retrieval.service import RetrievalService
from app.schemas import FacetResult, RetrievedChunk, ServiceConfig
from app.storage import repository
from app.utils.text import top_terms

FACET_EVIDENCE_LIMIT = 50
RAW_TEXT_PREVIEW_LIMIT = 20000


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
        facet_max_workers: int = 1,
    ) -> None:
        self.retrieval = retrieval or RetrievalService()
        self.db = db
        self.llm_log_path = llm_log_path
        self.use_processes = use_processes
        self.facet_max_workers = max(1, facet_max_workers)

    def create_run(
        self,
        session: Session,
        project_id: str,
        *,
        target_role: str | None = None,
        analysis_context: str | None = None,
    ) -> AnalysisRun:
        project = repository.get_project(session, project_id)
        if not project:
            raise ValueError("Project not found.")
        summary = self._build_initial_summary(
            session,
            project_id,
            target_role=target_role,
            analysis_context=analysis_context,
        )
        run = repository.create_analysis_run(
            session,
            project_id,
            status="queued",
            summary_json=summary,
        )
        for facet in FACETS:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="pending",
                confidence=0.0,
                findings_json={
                    "label": facet.label,
                    "summary": "",
                    "bullets": [],
                    "notes": None,
                    "retrieval_mode": None,
                    "hit_count": 0,
                    "target_role": summary.get("target_role"),
                    "analysis_context": summary.get("analysis_context"),
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
        analysis_context: str | None = None,
    ) -> AnalysisRun:
        run = self.create_run(
            session,
            project_id,
            target_role=target_role,
            analysis_context=analysis_context,
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

        chat_config = repository.get_service_config(session, "chat_service")
        embedding_config = repository.get_service_config(session, "embedding_service")
        llm_payload = asdict(chat_config) if chat_config else None
        summary = dict(run.summary_json or {})

        run.status = "running"
        run.started_at = utcnow()
        summary["current_stage"] = "准备证据"
        summary["current_facet"] = None
        summary["started_at"] = run.started_at.isoformat()
        run.summary_json = summary
        repository.add_analysis_event(
            session,
            run.id,
            event_type="lifecycle",
            message="分析任务开始执行。",
            payload_json={"llm_enabled": bool(chat_config), "embedding_enabled": bool(embedding_config)},
        )
        self._persist_progress(session)

        if self.use_processes:
            results = self._run_parallel_processes(
                session,
                run,
                project.name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
            )
        else:
            results = self._run_parallel_threads(
                session,
                run,
                project.name,
                llm_payload=llm_payload,
                embedding_config=embedding_config,
            )

        summary = dict(run.summary_json or {})
        total_processed = int(summary.get("completed_facets", 0)) + int(summary.get("failed_facets", 0))
        run.finished_at = utcnow()
        run.status = "completed" if int(summary.get("failed_facets", 0)) == 0 else "partial_failed"
        summary["current_stage"] = "分析完成"
        summary["current_facet"] = None
        summary["progress_percent"] = 100 if total_processed else 0
        summary["finished_at"] = run.finished_at.isoformat()
        run.summary_json = summary
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
        self._persist_progress(session)
        return repository.get_analysis_run(session, run.id) or run

    def rerun_facet(self, session: Session, project_id: str, facet_key: str) -> AnalysisRun:
        run = repository.get_latest_analysis_run(session, project_id)
        project = repository.get_project(session, project_id)
        if not run or not project:
            raise ValueError("Analysis run not found.")
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
        try:
            hits, retrieval_mode, retrieval_trace = self._retrieve_hits(
                session,
                run.project_id,
                facet_def,
                embedding_config=embedding_config,
                llm_payload=llm_payload,
                target_role=summary.get("target_role"),
                analysis_context=summary.get("analysis_context"),
            )
        except Exception as exc:
            self._handle_facet_setup_error(
                session,
                run,
                facet_def,
                exc,
                embedding_config=embedding_config,
            )
            run.finished_at = utcnow()
            run.status = "completed" if int((run.summary_json or {}).get("failed_facets", 0)) == 0 else "partial_failed"
            self._persist_progress(session)
            return repository.get_analysis_run(session, run.id) or run
        run.status = "running"
        run.finished_at = None
        self._mark_facet_started(
            session,
            run,
            facet_def,
            retrieval_mode,
            retrieval_trace,
            len(hits),
            llm_payload=llm_payload,
        )
        self._persist_progress(session)

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
        self._apply_facet_result(session, run, facet_def, retrieval_mode, retrieval_trace, len(hits), result)
        run.finished_at = utcnow()
        run.status = "completed" if int((run.summary_json or {}).get("failed_facets", 0)) == 0 else "partial_failed"
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"{facet_def.label} 已重新完成。",
            level="success" if result.status == "completed" else "error",
            payload_json={"facet_key": facet_def.key},
        )
        self._persist_progress(session)
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
        future_map: dict[Future[dict[str, Any]], tuple[FacetDefinition, str, dict[str, Any], int]] = {}
        with ProcessPoolExecutor(max_workers=min(len(FACETS), self.facet_max_workers if llm_payload else 4)) as executor:
            for facet in FACETS:
                prepared = self._prepare_facet_execution(
                    session,
                    run,
                    facet,
                    embedding_config=embedding_config,
                    llm_payload=llm_payload,
                )
                if not prepared:
                    continue
                hits, retrieval_mode, retrieval_trace = prepared
                future = executor.submit(
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
                future_map[future] = (facet, retrieval_mode, retrieval_trace, len(hits))

            results: list[tuple[FacetDefinition, str, int, FacetResult]] = []
            for future in as_completed(future_map):
                facet, retrieval_mode, retrieval_trace, hit_count = future_map[future]
                try:
                    result = FacetResult(**future.result())
                except Exception as exc:
                    result = self._build_failed_facet_result(facet, exc, llm_called=bool(llm_payload))
                self._apply_facet_result(session, run, facet, retrieval_mode, retrieval_trace, hit_count, result)
                results.append((facet, retrieval_mode, hit_count, result))
            return results

    def _run_parallel_threads(
        self,
        session: Session,
        run: AnalysisRun,
        project_name: str,
        *,
        llm_payload: dict[str, Any] | None,
        embedding_config: ServiceConfig | None,
    ) -> list[tuple[FacetDefinition, str, int, FacetResult]]:
        future_map: dict[Future[dict[str, Any]], tuple[FacetDefinition, str, dict[str, Any], int]] = {}
        with ThreadPoolExecutor(
            max_workers=min(len(FACETS), self.facet_max_workers if llm_payload else 4),
            thread_name_prefix="facet-thread",
        ) as executor:
            for facet in FACETS:
                prepared = self._prepare_facet_execution(
                    session,
                    run,
                    facet,
                    embedding_config=embedding_config,
                    llm_payload=llm_payload,
                )
                if not prepared:
                    continue
                hits, retrieval_mode, retrieval_trace = prepared
                future = executor.submit(
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
                future_map[future] = (facet, retrieval_mode, retrieval_trace, len(hits))

            results: list[tuple[FacetDefinition, str, int, FacetResult]] = []
            for future in as_completed(future_map):
                facet, retrieval_mode, retrieval_trace, hit_count = future_map[future]
                try:
                    result = FacetResult(**future.result())
                except Exception as exc:
                    result = self._build_failed_facet_result(facet, exc, llm_called=bool(llm_payload))
                self._apply_facet_result(session, run, facet, retrieval_mode, retrieval_trace, hit_count, result)
                results.append((facet, retrieval_mode, hit_count, result))
            return results

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
        self._mark_facet_started(
            session,
            run,
            facet,
            retrieval_mode,
            retrieval_trace,
            len(hits),
            llm_payload=llm_payload,
        )
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
        self._persist_progress(session)

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
        meta = dict(result.raw_payload.get("_meta") or {})
        findings_json = {
            "label": facet.label,
            "summary": result.summary,
            "bullets": result.bullets,
            "notes": result.notes,
            "retrieval_mode": retrieval_mode,
            "retrieval_trace": retrieval_trace,
            "hit_count": hit_count,
            "target_role": (run.summary_json or {}).get("target_role"),
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
            "llm_live_text": (meta.get("raw_text") or "")[:RAW_TEXT_PREVIEW_LIMIT],
            "llm_response_text": meta.get("raw_text"),
            "llm_error": meta.get("llm_error"),
            "llm_log_path": meta.get("log_path"),
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
        self._persist_progress(session)

    def _recalculate_run_summary(self, session: Session, run: AnalysisRun) -> None:
        facets = list(session.scalars(select(AnalysisFacet).where(AnalysisFacet.run_id == run.id)))
        summary = dict(run.summary_json or {})
        completed = 0
        failed = 0
        llm_calls = 0
        llm_successes = 0
        llm_failures = 0
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        for facet in facets:
            if facet.status == "completed":
                completed += 1
            elif facet.status == "failed":
                failed += 1
            findings = dict(facet.findings_json or {})
            if findings.get("llm_called"):
                llm_calls += int(findings.get("llm_attempts", 1) or 1)
                if findings.get("llm_success") is True:
                    llm_successes += 1
                elif findings.get("llm_success") is False:
                    llm_failures += 1
                prompt_tokens += int(findings.get("prompt_tokens", 0) or 0)
                completion_tokens += int(findings.get("completion_tokens", 0) or 0)
                total_tokens += int(findings.get("total_tokens", 0) or 0)
        summary["completed_facets"] = completed
        summary["failed_facets"] = failed
        summary["llm_calls"] = llm_calls
        summary["llm_successes"] = llm_successes
        summary["llm_failures"] = llm_failures
        summary["prompt_tokens"] = prompt_tokens
        summary["completion_tokens"] = completion_tokens
        summary["total_tokens"] = total_tokens
        progress_done = completed + failed
        summary["progress_percent"] = int((progress_done / len(FACETS)) * 100)
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
        analysis_context: str | None,
    ) -> dict[str, Any]:
        target_project_id = repository.get_target_project_id(session, project_id)
        document_count = (
            session.scalar(
                select(func.count()).select_from(DocumentRecord).where(DocumentRecord.project_id == target_project_id)
            )
            or 0
        )
        chunk_count = (
            session.scalar(
                select(func.count()).select_from(TextChunk).where(TextChunk.project_id == target_project_id)
            )
            or 0
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
        return {
            "document_count": document_count,
            "chunk_count": chunk_count,
            "failed_document_count": failed_count,
            "generated_at": utcnow().isoformat(),
            "target_role": (target_role or "").strip() or None,
            "analysis_context": (analysis_context or "").strip() or None,
            "total_facets": len(FACETS),
            "completed_facets": 0,
            "failed_facets": 0,
            "progress_percent": 0,
            "current_stage": "排队中",
            "current_facet": None,
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

    @staticmethod
    def _persist_progress(session: Session) -> None:
        session.flush()
        session.commit()


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
                stream_handler=stream_callback,
            )
            flush_remaining = getattr(stream_callback, "_flush_remaining", None)
            if callable(flush_remaining):
                flush_remaining()
            try:
                parsed = parse_json_response(completion.content, fallback=True)
            except LLMError as exc:
                raise LLMError(
                    str(exc),
                    raw_text=completion.content[:RAW_TEXT_PREVIEW_LIMIT],
                    request_url=completion.request_url,
                    request_payload=completion.request_payload or request_payload,
                ) from exc
            normalized = _normalize_facet_payload(parsed, chunks)
            normalized["_meta"] = {
                "llm_called": True,
                "llm_success": True,
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
    bullets: list[str] = []
    if target_role:
        bullets.append(f"目标角色：{target_role}")
    if analysis_context:
        bullets.append(f"用户补充说明：{analysis_context[:100]}")
    if terms:
        bullets.append(f"高频关键词：{', '.join(terms[:5])}")
    for chunk in chunks[:3]:
        preview = chunk["content"][:100].replace("\n", " ")
        bullets.append(f"代表片段来自 {chunk['filename']}：{preview}")
    summary_focus = ", ".join(terms[:4]) or "代表性表达"
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
        "summary": f"{facet.label}主要由 {len(chunks)} 个高相关片段归纳，重点围绕 {summary_focus}。",
        "bullets": bullets[:6],
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


def _normalize_facet_payload(payload: dict[str, Any], chunks: list[dict[str, Any]]) -> dict[str, Any]:
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
    return {
        "summary": str(payload.get("summary", "")),
        "bullets": [str(item) for item in payload.get("bullets", [])[:6]],
        "confidence": _parse_confidence(payload.get("confidence"), 0.65),
        "evidence": evidence,
        "conflicts": [
            {
                "title": str(item.get("title", "")),
                "detail": str(item.get("detail", "")),
            }
            for item in payload.get("conflicts", [])[:5]
        ],
        "notes": payload.get("notes"),
    }
