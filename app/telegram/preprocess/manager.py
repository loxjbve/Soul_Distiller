from __future__ import annotations

from app.telegram.preprocess import helpers as _helpers
from app.telegram.preprocess.worker import TelegramPreprocessWorker

globals().update(
    {
        name: getattr(_helpers, name)
        for name in dir(_helpers)
        if not name.startswith("__")
    }
)

class TelegramPreprocessManager:
    def __init__(
        self,
        db: Database,
        *,
        llm_log_path: str | None = None,
        max_workers: int = 2,
        run_inline: bool = False,
        stream_hub: AnalysisStreamHub | None = None,
    ) -> None:
        self.db = db
        self.llm_log_path = Path(llm_log_path) if llm_log_path else None
        self.run_inline = run_inline
        self.stream_hub = stream_hub
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="telegram-preprocess")
        self.futures: dict[str, Future[None]] = {}
        self._project_by_future: dict[str, str] = {}
        self._cancelled_projects: set[str] = set()
        self._trace_sequences: dict[str, int] = {}
        self._lock = Lock()

    @staticmethod
    def _touch_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
        updated = dict(summary or {})
        updated["snapshot_version"] = int(updated.get("snapshot_version") or 0) + 1
        updated["updated_at"] = utcnow().isoformat()
        return updated

    def resume_interrupted_runs(self) -> None:
        with self.db.session() as session:
            for run in session.scalars(select(TelegramPreprocessRun)):
                if run.status not in {"queued", "running"}:
                    continue
                run.status = "failed"
                run.finished_at = utcnow()
                run.error_message = "Service restarted while Telegram preprocess was still running. Resume this run to continue from the saved checkpoint."
                run.progress_percent = int(run.progress_percent or 0)
                summary = dict(run.summary_json or {})
                summary["current_stage"] = "interrupted"
                summary["resume_available"] = True
                run.summary_json = self._touch_summary(summary)
                self._trace_sequences[run.id] = int(summary.get("trace_event_count") or 0)
            session.commit()

    def submit(self, project_id: str, *, weekly_summary_concurrency: int | None = None) -> Any:
        with self.db.session() as session:
            project = repository.get_project(session, project_id)
            if not project or project.mode != "telegram":
                raise ValueError("Telegram project not found.")
            chat = repository.get_latest_telegram_chat(session, project_id)
            if not chat:
                raise ValueError("Telegram chat not found.")
            chat_config = repository.get_service_config(session, "chat_service")
            normalized_concurrency = TelegramPreprocessWorker._normalize_weekly_summary_concurrency(
                weekly_summary_concurrency
            )
            active_run = repository.get_active_telegram_preprocess_run(session, project_id)
            if active_run and self.is_tracking(active_run.id):
                return active_run
            resumable_run = active_run or repository.get_latest_resumable_telegram_preprocess_run(session, project_id)
            if resumable_run and resumable_run.chat_id == chat.id:
                summary = dict(resumable_run.summary_json or {})
                summary["resume_available"] = True
                summary["resume_count"] = int(summary.get("resume_count") or 0) + 1
                summary["weekly_summary_concurrency"] = normalized_concurrency
                summary["progress_percent"] = int(
                    summary.get("progress_percent") or resumable_run.progress_percent or 0
                )
                resumable_run.status = "queued"
                resumable_run.finished_at = None
                resumable_run.error_message = None
                resumable_run.current_stage = str(summary.get("current_stage") or resumable_run.current_stage or "queued")
                resumable_run.progress_percent = int(summary.get("progress_percent") or 0)
                resumable_run.summary_json = self._touch_summary(summary)
                session.commit()
                run_id = resumable_run.id
                self._trace_sequences[run_id] = int(summary.get("trace_event_count") or 0)
                self._record_trace(
                    session,
                    run_id,
                    {
                        "timestamp": utcnow().isoformat(),
                        "kind": "stage_progress",
                        "stage": "resume",
                        "message": "Resuming Telegram preprocess from the latest saved checkpoint.",
                        "topic_count": int(summary.get("topic_count") or 0),
                        "weekly_candidate_count": int(summary.get("weekly_candidate_count") or 0),
                        "weekly_summary_concurrency": normalized_concurrency,
                    },
                    persist=True,
                )
            else:
                run = repository.create_telegram_preprocess_run(
                    session,
                    project_id=project_id,
                    chat_id=chat.id,
                    status="queued",
                    llm_model=chat_config.model if chat_config else None,
                    summary_json={
                        "current_stage": "queued",
                        "progress_percent": 0,
                        "window_count": 0,
                        "top_user_count": 0,
                        "weekly_candidate_count": 0,
                        "topic_count": 0,
                        "current_topic_index": 0,
                        "current_topic_total": 0,
                        "current_topic_label": "",
                        "weekly_summary_concurrency": normalized_concurrency,
                        "active_agents": 0,
                        "completed_week_count": 0,
                        "remaining_week_count": 0,
                        "active_user_count": 0,
                        "relationship_snapshot_id": None,
                        "relationship_status": None,
                        "relationship_edge_count": 0,
                        "relationship_summary": {},
                        "trace_events": [],
                        "trace_event_count": 0,
                        "resume_count": 0,
                        "snapshot_version": 0,
                    },
                )
                run.summary_json = self._touch_summary(run.summary_json or {})
                session.commit()
                run_id = run.id
                self._trace_sequences[run_id] = 0
        if self.run_inline:
            self._execute(run_id)
        else:
            future = self.executor.submit(self._execute, run_id)
            with self._lock:
                self.futures[run_id] = future
                self._project_by_future[run_id] = project_id
        with self.db.session() as session:
            return repository.get_telegram_preprocess_run(session, run_id)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)

    def is_tracking(self, run_id: str) -> bool:
        with self._lock:
            future = self.futures.get(run_id)
        return future is not None and not future.done()

    def cancel_project(self, project_id: str) -> bool:
        with self._lock:
            self._cancelled_projects.add(project_id)
            future_items = [
                (future_key, self.futures.get(future_key))
                for future_key, mapped_project_id in self._project_by_future.items()
                if mapped_project_id == project_id
            ]
        all_cancelled = True
        for future_key, future in future_items:
            if future is None:
                continue
            if not future.cancel():
                all_cancelled = False
                continue
            self._finish_future(future_key)
        return all_cancelled or not self.has_project_activity(project_id)

    def has_project_activity(self, project_id: str) -> bool:
        with self._lock:
            for future_key, mapped_project_id in self._project_by_future.items():
                if mapped_project_id != project_id:
                    continue
                future = self.futures.get(future_key)
                if future is not None and not future.done():
                    return True
            return False

    def _execute(self, run_id: str) -> None:
        try:
            with background_task_slot():
                with self.db.session() as session:
                    run = repository.get_telegram_preprocess_run(session, run_id)
                    if not run:
                        return
                    project = repository.get_project(session, run.project_id)
                    if not project:
                        raise ValueError("Project not found.")
                    chat_config = repository.get_service_config(session, "chat_service")
                    existing_summary = dict(run.summary_json or {})
                    existing_progress = int(existing_summary.get("progress_percent") or run.progress_percent or 0)
                    is_resume = bool(
                        existing_summary.get("weekly_candidate_count")
                        or existing_summary.get("topic_count")
                    )
                    run.status = "running"
                    run.started_at = run.started_at or utcnow()
                    run.finished_at = None
                    run.error_message = None
                    run.progress_percent = max(existing_progress, 3)
                    summary = existing_summary
                    summary["current_stage"] = "running"
                    summary["progress_percent"] = run.progress_percent
                    summary["resume_available"] = False
                    run.summary_json = self._touch_summary(summary)
                    session.commit()
                    if is_resume:
                        self._record_trace(
                            session,
                            run_id,
                            {
                                "timestamp": utcnow().isoformat(),
                                "kind": "stage_progress",
                                "stage": "resume",
                                "message": "Continuing Telegram preprocess from previously saved stage outputs.",
                                "topic_count": int(summary.get("topic_count") or 0),
                                "weekly_candidate_count": int(summary.get("weekly_candidate_count") or 0),
                                "weekly_summary_concurrency": int(summary.get("weekly_summary_concurrency") or 1),
                            },
                            persist=True,
                        )
                    self._publish_snapshot(run.id)

                    worker = TelegramPreprocessWorker(
                        session,
                        project,
                        llm_config=chat_config,
                        log_path=str(self.llm_log_path) if self.llm_log_path else None,
                        cancel_checker=lambda: self._is_cancelled(project.id),
                        trace_callback=lambda event, persist=True: self._record_trace(session, run_id, event, persist=persist),
                    )

                    def progress(stage: str, progress_percent: int, summary_patch: dict[str, Any] | None) -> None:
                        live_run = repository.get_telegram_preprocess_run(session, run_id)
                        if not live_run:
                            return
                        live_run.progress_percent = progress_percent
                        live_run.current_stage = stage
                        summary = dict(live_run.summary_json or {})
                        summary["current_stage"] = stage
                        summary["progress_percent"] = progress_percent
                        if summary_patch:
                            summary.update(summary_patch)
                        usage = dict(summary.get("usage") or {})
                        live_run.prompt_tokens = int(usage.get("prompt_tokens") or live_run.prompt_tokens or 0)
                        live_run.completion_tokens = int(usage.get("completion_tokens") or live_run.completion_tokens or 0)
                        live_run.total_tokens = int(usage.get("total_tokens") or live_run.total_tokens or 0)
                        live_run.cache_creation_tokens = int(
                            usage.get("cache_creation_tokens") or live_run.cache_creation_tokens or 0
                        )
                        live_run.cache_read_tokens = int(
                            usage.get("cache_read_tokens") or live_run.cache_read_tokens or 0
                        )
                        live_run.window_count = int(
                            summary.get("weekly_candidate_count") or summary.get("window_count") or live_run.window_count or 0
                        )
                        live_run.topic_count = int(summary.get("topic_count") or live_run.topic_count or 0)
                        live_run.active_user_count = int(summary.get("active_user_count") or live_run.active_user_count or 0)
                        live_run.summary_json = self._touch_summary(summary)
                        session.commit()
                        self._publish_snapshot(run_id)

                    result = worker.process(run, progress_callback=progress)
                    live_run = repository.get_telegram_preprocess_run(session, run_id)
                    if not live_run:
                        return
                    final_topics = repository.replace_telegram_preprocess_topics(
                        session,
                        run_id=live_run.id,
                        project_id=project.id,
                        chat_id=live_run.chat_id,
                        topics=list(result.get("topics") or []),
                    )
                    try:
                        relationship_result = worker.build_relationship_snapshot(
                            live_run,
                            progress_callback=progress,
                        )
                    except Exception as exc:
                        fallback_snapshot = repository.create_or_replace_telegram_relationship_snapshot(
                            session,
                            run_id=live_run.id,
                            project_id=project.id,
                            chat_id=live_run.chat_id,
                            status="failed",
                            analyzed_user_count=0,
                            candidate_pair_count=0,
                            llm_pair_count=0,
                            started_at=utcnow(),
                            finished_at=utcnow(),
                            error_message=str(exc),
                            summary_json={
                                "friendly_count": 0,
                                "neutral_count": 0,
                                "tense_count": 0,
                                "unclear_count": 0,
                                "edge_count": 0,
                                "central_users": [],
                                "isolated_users": [],
                                "snapshot_notes": [str(exc)],
                            },
                        )
                        relationship_result = {
                            "snapshot_id": fallback_snapshot.id,
                            "status": "failed",
                            "active_user_count": 0,
                            "candidate_pair_count": 0,
                            "edge_count": 0,
                            "summary": dict(fallback_snapshot.summary_json or {}),
                        }
                        self._record_trace(
                            session,
                            run_id,
                            {
                                "timestamp": utcnow().isoformat(),
                                "kind": "run_failed",
                                "stage": "relationship_snapshot",
                                "message": "Relationship snapshot generation failed, but Telegram preprocess completed with weekly topics intact.",
                                "error": str(exc),
                            },
                            persist=True,
                        )
                    usage = dict(worker.usage_totals)
                    live_run.status = "completed"
                    live_run.finished_at = utcnow()
                    live_run.progress_percent = 100
                    live_run.current_stage = "completed"
                    live_run.prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
                    live_run.completion_tokens = int(usage.get("completion_tokens", 0) or 0)
                    live_run.total_tokens = int(usage.get("total_tokens", 0) or 0)
                    live_run.cache_creation_tokens = int(usage.get("cache_creation_tokens", 0) or 0)
                    live_run.cache_read_tokens = int(usage.get("cache_read_tokens", 0) or 0)
                    live_run.window_count = int(result.get("window_count", 0) or 0)
                    live_run.topic_count = len(final_topics)
                    live_run.active_user_count = int(relationship_result.get("active_user_count") or 0)
                    live_run.summary_json = {
                        **dict(live_run.summary_json or {}),
                        "current_stage": "completed",
                        "progress_percent": 100,
                        "window_count": live_run.window_count,
                        "top_user_count": int(result.get("top_user_count", 0) or 0),
                        "weekly_candidate_count": int(result.get("weekly_candidate_count", 0) or 0),
                        "topic_count": live_run.topic_count,
                        "weekly_summary_concurrency": int(
                            (live_run.summary_json or {}).get("weekly_summary_concurrency") or 1
                        ),
                        "bootstrap": result.get("bootstrap") or {},
                        "usage": usage,
                        "active_user_count": int(relationship_result.get("active_user_count") or 0),
                        "relationship_snapshot_id": relationship_result.get("snapshot_id"),
                        "relationship_status": relationship_result.get("status"),
                        "relationship_edge_count": int(relationship_result.get("edge_count") or 0),
                        "relationship_summary": dict(relationship_result.get("summary") or {}),
                        "resume_available": False,
                        "current_topic_index": int(result.get("current_topic_index") or 0),
                        "current_topic_total": int(result.get("current_topic_total") or 0),
                        "current_topic_label": str(result.get("current_topic_label") or "").strip(),
                    }
                    live_run.summary_json = self._touch_summary(live_run.summary_json or {})
                    session.commit()
                    self._record_trace(
                        session,
                        run_id,
                        {
                            "timestamp": utcnow().isoformat(),
                            "kind": "run_completed",
                            "stage": "completed",
                            "message": f"Telegram preprocess completed with {live_run.topic_count} weekly topics and {int(relationship_result.get('edge_count') or 0)} relationship edges.",
                        },
                        persist=True,
                    )
                    self._publish_snapshot(run_id)
        except Exception as exc:
            with self.db.session() as session:
                run = repository.get_telegram_preprocess_run(session, run_id)
                if run:
                    self._mark_run_failed(session, run, str(exc).strip() or exc.__class__.__name__)
                    summary = dict(run.summary_json or {})
                    summary["traceback"] = traceback.format_exc()
                    run.summary_json = summary
                    session.commit()
                    self._record_trace(
                        session,
                        run_id,
                        {
                            "timestamp": utcnow().isoformat(),
                            "kind": "run_failed",
                            "stage": "failed",
                            "message": run.error_message or "Telegram preprocess failed.",
                            "error": str(exc),
                        },
                        persist=True,
                    )
                    self._publish_snapshot(run_id)
        finally:
            self._finish_future(run_id)

    def _mark_run_failed(self, session: Session, run, error_message: str) -> None:
        run.status = "failed"
        run.finished_at = utcnow()
        run.error_message = error_message
        summary = dict(run.summary_json or {})
        summary["current_stage"] = "failed"
        summary["progress_percent"] = int(run.progress_percent or 0)
        summary["resume_available"] = True
        run.summary_json = self._touch_summary(summary)

    def _next_trace_seq(self, run_id: str) -> int:
        with self._lock:
            next_value = int(self._trace_sequences.get(run_id, 0) or 0) + 1
            self._trace_sequences[run_id] = next_value
        return next_value

    def _publish_snapshot(self, run_id: str) -> None:
        if self.stream_hub:
            self.stream_hub.publish(run_id, event="snapshot", payload={"run_id": run_id})

    def _record_trace(
        self,
        session: Session,
        run_id: str,
        event: dict[str, Any],
        *,
        persist: bool = True,
    ) -> dict[str, Any]:
        normalized = dict(event or {})
        normalized.setdefault("timestamp", utcnow().isoformat())
        if persist:
            live_run = session.get(TelegramPreprocessRun, run_id)
            if live_run:
                summary = dict(live_run.summary_json or {})
                trace_events = [
                    dict(item)
                    for item in (summary.get("trace_events") or [])
                    if isinstance(item, dict)
                ]
                normalized["seq"] = self._next_trace_seq(run_id)
                trace_events.append(normalized)
                summary["trace_event_count"] = normalized["seq"]
                summary["trace_events"] = trace_events[-TELEGRAM_PREPROCESS_TRACE_LIMIT:]
                live_run.summary_json = self._touch_summary(summary)
                session.commit()
        else:
            normalized.setdefault("seq", None)
        if self.stream_hub:
            self.stream_hub.publish(run_id, event="trace", payload=normalized)
        return normalized

    def _finish_future(self, future_key: str) -> None:
        project_id: str | None
        with self._lock:
            self.futures.pop(future_key, None)
            project_id = self._project_by_future.pop(future_key, None)
            self._trace_sequences.pop(future_key, None)
            if project_id is None:
                return
            if any(
                mapped_project_id == project_id
                and self.futures.get(other_key) is not None
                and not self.futures[other_key].done()
                for other_key, mapped_project_id in self._project_by_future.items()
            ):
                return
            self._cancelled_projects.discard(project_id)

    def _is_cancelled(self, project_id: str) -> bool:
        with self._lock:
            return project_id in self._cancelled_projects

__all__ = ["TelegramPreprocessManager"]

