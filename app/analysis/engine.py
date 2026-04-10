from __future__ import annotations

import json
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from time import perf_counter
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.analysis.facets import FACETS, FacetDefinition
from app.llm.client import LLMError, OpenAICompatibleClient, parse_json_response
from app.models import AnalysisFacet, AnalysisRun, DocumentRecord, TextChunk, utcnow
from app.retrieval.service import RetrievalService
from app.schemas import FacetResult, RetrievedChunk, ServiceConfig
from app.storage import repository
from app.utils.text import top_terms

FACET_EVIDENCE_LIMIT = 50


def analyze_facet_worker(
    facet: FacetDefinition,
    project_name: str,
    chunks: list[dict[str, Any]],
    llm_config: dict[str, Any] | None,
    target_role: str | None,
    analysis_context: str | None,
) -> dict[str, Any]:
    try:
        if llm_config:
            payload = _analyze_with_llm(
                facet,
                project_name,
                chunks,
                llm_config,
                target_role=target_role,
                analysis_context=analysis_context,
            )
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
                confidence=float(payload.get("confidence", 0.55)),
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
    def __init__(self, retrieval: RetrievalService | None = None, *, use_processes: bool = True) -> None:
        self.retrieval = retrieval or RetrievalService()
        self.use_processes = use_processes

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
        hits, retrieval_mode = self._retrieve_hits(
            session,
            run.project_id,
            facet_def,
            embedding_config=embedding_config,
            target_role=summary.get("target_role"),
            analysis_context=summary.get("analysis_context"),
        )
        repository.add_analysis_event(
            session,
            run.id,
            event_type="facet",
            message=f"重新执行 {facet_def.label}。",
            payload_json={"facet_key": facet_def.key},
        )
        self._persist_progress(session)

        result = FacetResult(
            **analyze_facet_worker(
                facet_def,
                project.name,
                [self._serialize_hit(hit) for hit in hits],
                asdict(chat_config) if chat_config else None,
                summary.get("target_role"),
                summary.get("analysis_context"),
            )
        )
        self._apply_facet_result(session, run, facet_def, retrieval_mode, len(hits), result)
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
        future_map: dict[Future[dict[str, Any]], tuple[FacetDefinition, str, int]] = {}
        with ProcessPoolExecutor(max_workers=min(len(FACETS), 4)) as executor:
            for facet in FACETS:
                hits, retrieval_mode = self._retrieve_hits(
                    session,
                    run.project_id,
                    facet,
                    embedding_config=embedding_config,
                    target_role=(run.summary_json or {}).get("target_role"),
                    analysis_context=(run.summary_json or {}).get("analysis_context"),
                )
                self._mark_facet_started(session, run, facet, retrieval_mode, len(hits))
                future = executor.submit(
                    analyze_facet_worker,
                    facet,
                    project_name,
                    [self._serialize_hit(hit) for hit in hits],
                    llm_payload,
                    (run.summary_json or {}).get("target_role"),
                    (run.summary_json or {}).get("analysis_context"),
                )
                future_map[future] = (facet, retrieval_mode, len(hits))

            results: list[tuple[FacetDefinition, str, int, FacetResult]] = []
            for future in as_completed(future_map):
                facet, retrieval_mode, hit_count = future_map[future]
                result = FacetResult(**future.result())
                self._apply_facet_result(session, run, facet, retrieval_mode, hit_count, result)
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
        future_map: dict[Future[dict[str, Any]], tuple[FacetDefinition, str, int]] = {}
        with ThreadPoolExecutor(max_workers=min(len(FACETS), 4), thread_name_prefix="facet-thread") as executor:
            for facet in FACETS:
                hits, retrieval_mode = self._retrieve_hits(
                    session,
                    run.project_id,
                    facet,
                    embedding_config=embedding_config,
                    target_role=(run.summary_json or {}).get("target_role"),
                    analysis_context=(run.summary_json or {}).get("analysis_context"),
                )
                self._mark_facet_started(session, run, facet, retrieval_mode, len(hits))
                future = executor.submit(
                    analyze_facet_worker,
                    facet,
                    project_name,
                    [self._serialize_hit(hit) for hit in hits],
                    llm_payload,
                    (run.summary_json or {}).get("target_role"),
                    (run.summary_json or {}).get("analysis_context"),
                )
                future_map[future] = (facet, retrieval_mode, len(hits))

            results: list[tuple[FacetDefinition, str, int, FacetResult]] = []
            for future in as_completed(future_map):
                facet, retrieval_mode, hit_count = future_map[future]
                result = FacetResult(**future.result())
                self._apply_facet_result(session, run, facet, retrieval_mode, hit_count, result)
                results.append((facet, retrieval_mode, hit_count, result))
            return results

    def _mark_facet_started(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        retrieval_mode: str,
        hit_count: int,
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
                "hit_count": hit_count,
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
            event_type="facet",
            message=f"{facet.label} 已启动，召回到 {hit_count} 个证据片段。",
            payload_json={"facet_key": facet.key, "retrieval_mode": retrieval_mode, "hit_count": hit_count},
        )
        self._persist_progress(session)

    def _apply_facet_result(
        self,
        session: Session,
        run: AnalysisRun,
        facet: FacetDefinition,
        retrieval_mode: str,
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
        if result.status == "completed":
            summary["completed_facets"] = int(summary.get("completed_facets", 0)) + 1
        else:
            summary["failed_facets"] = int(summary.get("failed_facets", 0)) + 1

        if meta.get("llm_called"):
            summary["llm_calls"] = int(summary.get("llm_calls", 0)) + int(meta.get("llm_attempts", 1) or 1)
            if meta.get("llm_success"):
                summary["llm_successes"] = int(summary.get("llm_successes", 0)) + 1
            else:
                summary["llm_failures"] = int(summary.get("llm_failures", 0)) + 1
            summary["prompt_tokens"] = int(summary.get("prompt_tokens", 0)) + int(meta.get("prompt_tokens", 0))
            summary["completion_tokens"] = int(summary.get("completion_tokens", 0)) + int(meta.get("completion_tokens", 0))
            summary["total_tokens"] = int(summary.get("total_tokens", 0)) + int(meta.get("total_tokens", 0))
        progress_done = int(summary.get("completed_facets", 0)) + int(summary.get("failed_facets", 0))
        summary["progress_percent"] = int((progress_done / len(FACETS)) * 100)
        summary["current_stage"] = f"已完成 {facet.label}"
        run.summary_json = summary

        message = f"{facet.label} 完成。"
        if findings_json["llm_called"]:
            llm_state = "成功" if findings_json["llm_success"] else "失败"
            message = f"{message} LLM {llm_state}，消耗 {findings_json['total_tokens']} tokens。"
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
        self._persist_progress(session)

    def _retrieve_hits(
        self,
        session: Session,
        project_id: str,
        facet: FacetDefinition,
        *,
        embedding_config: ServiceConfig | None,
        target_role: str | None,
        analysis_context: str | None,
    ) -> tuple[list[RetrievedChunk], str]:
        query_parts = [facet.search_query]
        if target_role:
            query_parts.append(target_role)
        if analysis_context:
            query_parts.append(analysis_context)
        hits, retrieval_mode = self.retrieval.search(
            session,
            project_id=project_id,
            query=" ".join(query_parts),
            embedding_config=embedding_config,
            limit=FACET_EVIDENCE_LIMIT,
        )
        if not hits:
            hits = self._fallback_hits(session, project_id)
        return hits, retrieval_mode

    def _build_initial_summary(
        self,
        session: Session,
        project_id: str,
        *,
        target_role: str | None,
        analysis_context: str | None,
    ) -> dict[str, Any]:
        document_count = (
            session.scalar(
                select(func.count()).select_from(DocumentRecord).where(DocumentRecord.project_id == project_id)
            )
            or 0
        )
        chunk_count = (
            session.scalar(
                select(func.count()).select_from(TextChunk).where(TextChunk.project_id == project_id)
            )
            or 0
        )
        failed_count = (
            session.scalar(
                select(func.count()).select_from(DocumentRecord).where(
                    DocumentRecord.project_id == project_id,
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
        stmt = (
            select(TextChunk, DocumentRecord)
            .join(DocumentRecord, TextChunk.document_id == DocumentRecord.id)
            .where(TextChunk.project_id == project_id)
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
    target_role: str | None,
    analysis_context: str | None,
) -> dict[str, Any]:
    config = ServiceConfig(**llm_config)
    client = OpenAICompatibleClient(config)
    excerpt_text = "\n\n".join(
        f"[{chunk['chunk_id']}] {chunk['document_title']} / {chunk['filename']}\n{chunk['content'][:320]}"
        for chunk in chunks
    )
    context_lines = []
    if target_role:
        context_lines.append(f"Target role: {target_role}")
    if analysis_context:
        context_lines.append(f"User context: {analysis_context}")
    context_block = "\n".join(context_lines)
    started = perf_counter()
    last_error: Exception | None = None
    attempts = 0
    for _ in range(2):
        attempts += 1
        try:
            completion = client.chat_completion_result(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are a persona distillation analyst. "
                            "Reply with JSON only. Use the evidence exactly from the provided chunk ids."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Project: {project_name}\n"
                            f"Facet: {facet.label} ({facet.key})\n"
                            f"Goal: {facet.purpose}\n"
                            f"{context_block}\n\n"
                            "Return a JSON object with keys: summary, bullets, confidence, evidence, conflicts, notes.\n"
                            "evidence must be a list of objects containing chunk_id, reason, quote.\n"
                            "conflicts must be a list of objects containing title and detail.\n\n"
                            f"Evidence excerpts:\n{excerpt_text}"
                        ),
                    },
                ],
                model=config.model,
                temperature=0.2,
            )
            parsed = parse_json_response(completion.content)
            normalized = _normalize_facet_payload(parsed, chunks)
            normalized["_meta"] = {
                "llm_called": True,
                "llm_success": True,
                "llm_attempts": attempts,
                "llm_model": completion.model,
                "prompt_tokens": completion.usage.get("prompt_tokens", 0),
                "completion_tokens": completion.usage.get("completion_tokens", 0),
                "total_tokens": completion.usage.get("total_tokens", 0),
                "duration_ms": int((perf_counter() - started) * 1000),
            }
            return normalized
        except (LLMError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            last_error = exc
    raise LLMError(str(last_error) if last_error else "Failed to analyze facet.")


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
        "confidence": float(payload.get("confidence", 0.65)),
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
