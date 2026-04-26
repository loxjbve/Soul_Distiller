from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any

from app.analysis import stone_v3_profiles as _stone_v3_profiles
from app.llm import OpenAICompatibleClient, parse_json_response
from app.schemas import ServiceConfig

globals().update(
    {
        name: getattr(_stone_v3_profiles, name)
        for name in dir(_stone_v3_profiles)
        if not name.startswith("__")
    }
)

logger = logging.getLogger(__name__)

class StoneV3BaselineSynthesizer:
    def __init__(self, *, log_path: str | None = None) -> None:
        self.log_path = log_path

    def build(
        self,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
        config: ServiceConfig | None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
        checkpoint_callback: StoneV3CheckpointCallback | None = None,
        resume_from: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not config:
            raise ValueError("Stone v3 baseline synthesis requires a configured chat model.")
        client = OpenAICompatibleClient(config, log_path=self.log_path)
        compact_profiles = [compact_stone_profile_v3(profile) for profile in profiles]
        checkpoint_state = self._coerce_resume_checkpoint(
            resume_from,
            project_name=project_name,
            compact_profiles=compact_profiles,
            documents=documents,
        )
        stage_trace: list[dict[str, Any]] = list(checkpoint_state.get("stage_trace") or [])
        stage_trace_lock = Lock()
        if checkpoint_state.get("resume_available"):
            self._emit_progress(
                progress_callback,
                phase="resume_checkpoint_v3",
                progress_percent=8,
                message=(
                    "Resuming from saved Stone v3 checkpoint: "
                    f"{self._resume_summary(checkpoint_state)}."
                ),
                stage="resume_checkpoint_v3",
                status="running",
            )
        self._emit_progress(
            progress_callback,
            phase="family_induction_v3",
            progress_percent=16,
            message=f"Starting family induction across {len(compact_profiles)} compact profiles.",
            stage="family_induction_v3",
        )
        families = list(checkpoint_state.get("families") or [])
        if families:
            self._emit_progress(
                progress_callback,
                phase="family_induction_v3",
                progress_percent=48,
                message="Loaded saved family induction checkpoint.",
                stage="family_induction_v3_resume",
            )
        else:
            batch_family_outputs = self._run_family_batches(
                client,
                project_name,
                compact_profiles,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            self._ensure_not_cancelled(cancel_requested, stage="family_induction_v3_finalize")
            self._emit_progress(
                progress_callback,
                phase="family_induction_v3",
                progress_percent=44,
                message="Finalizing Stone v3 prototype families from batch outputs.",
                stage="family_induction_v3_finalize",
            )
            families = self._run_family_finalize(
                client,
                project_name,
                compact_profiles,
                batch_family_outputs,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["families"] = families
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._ensure_not_cancelled(cancel_requested, stage="author_model_v3")
        author_model = dict(checkpoint_state.get("author_model") or {})
        if author_model:
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=60,
                message="Loaded saved author-model checkpoint.",
                stage="author_model_v3_resume",
            )
        else:
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=52,
                message="Synthesizing the Stone v3 author model.",
                stage="author_model_v3",
            )
            author_model = self._run_author_model(
                client,
                project_name=project_name,
                profiles=profiles,
                compact_profiles=compact_profiles,
                families=families,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["author_model"] = author_model
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._emit_progress(
            progress_callback,
            phase="author_model_v3",
            progress_percent=60,
            message="Author model synthesis completed. Starting prototype card batches.",
            stage="author_model_v3",
        )
        prototype_index = dict(checkpoint_state.get("prototype_index") or {})
        if prototype_index:
            self._emit_progress(
                progress_callback,
                phase="prototype_index_v3",
                progress_percent=92,
                message="Loaded saved prototype-index checkpoint.",
                stage="prototype_index_v3_resume",
            )
        else:
            prototype_batch_outputs = self._run_prototype_batches(
                client,
                project_name,
                compact_profiles,
                documents,
                families,
                stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            self._ensure_not_cancelled(cancel_requested, stage="prototype_index_v3_finalize")
            self._emit_progress(
                progress_callback,
                phase="prototype_index_v3",
                progress_percent=88,
                message="Finalizing the Stone v3 prototype index.",
                stage="prototype_index_v3_finalize",
            )
            prototype_index = self._run_prototype_finalize(
                client,
                project_name=project_name,
                profiles=profiles,
                compact_profiles=compact_profiles,
                families=families,
                batch_outputs=prototype_batch_outputs,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["prototype_index"] = prototype_index
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        self._ensure_not_cancelled(cancel_requested, stage="baseline_critic_v3")
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=94,
            message="Running the Stone v3 baseline critic.",
            stage="baseline_critic_v3",
        )
        critic_review = dict(checkpoint_state.get("critic_review") or {})
        if critic_review:
            self._emit_progress(
                progress_callback,
                phase="baseline_critic_v3",
                progress_percent=97,
                message="Loaded saved baseline-critic checkpoint.",
                stage="baseline_critic_v3_resume",
            )
        else:
            critic_review = self._run_baseline_critic(
                client,
                project_name=project_name,
                author_model=author_model,
                prototype_index=prototype_index,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            checkpoint_state["critic_review"] = critic_review
            checkpoint_state["stage_trace"] = stage_trace
            self._persist_checkpoint(checkpoint_callback, checkpoint_state)
        validate_stone_v3_asset_payload("stone_author_model_v3", author_model)
        validate_stone_v3_asset_payload("stone_prototype_index_v3", prototype_index)
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=97,
            message="Stone v3 baseline synthesis completed. Persisting generated assets next.",
            stage="baseline_ready_v3",
            status="running",
        )
        return {
            "author_model": author_model,
            "prototype_index": prototype_index,
            "families": families,
            "critic_review": critic_review,
            "stage_trace": stage_trace,
        }

    def _call_json_stage(
        self,
        client: OpenAICompatibleClient,
        *,
        stage: str,
        phase: str | None = None,
        messages: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        progress_percent: int | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = 2200,
        timeout: float = STONE_V3_STAGE_TIMEOUT_SECONDS,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        phase_label = phase or stage
        for attempt in range(1, STONE_V3_MAX_RETRIES + 1):
            self._ensure_not_cancelled(cancel_requested, stage=stage)
            self._emit_progress(
                progress_callback,
                phase=phase_label,
                stage=stage,
                progress_percent=progress_percent or 0,
                message=f"{stage} attempt {attempt}/{STONE_V3_MAX_RETRIES} started.",
                status="running",
                attempt=attempt,
            )
            logger.info("Stone v3 stage %s attempt %s started.", stage, attempt)
            try:
                response = client.chat_completion_result(
                    messages,
                    model=client.config.model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=timeout,
                )
                parsed = parse_json_response(response.content, fallback=True)
                if not isinstance(parsed, dict):
                    raise ValueError(f"{stage} did not return a JSON object.")
                self._append_stage_trace(
                    stage_trace,
                    {
                        "stage": stage,
                        "attempt": attempt,
                        "status": "completed",
                        "model": response.model,
                        "usage": dict(response.usage or {}),
                        "output_preview": _trim_text(response.content, 320),
                        "failure_reason": "",
                    },
                    stage_trace_lock=stage_trace_lock,
                )
                self._emit_progress(
                    progress_callback,
                    phase=phase_label,
                    stage=stage,
                    progress_percent=progress_percent or 0,
                    message=f"{stage} completed on attempt {attempt}.",
                    status="running",
                    attempt=attempt,
                    output_preview=_trim_text(response.content, 160),
                )
                logger.info("Stone v3 stage %s attempt %s completed.", stage, attempt)
                return parsed
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                failure_reason = _trim_text(exc, 240)
                self._append_stage_trace(
                    stage_trace,
                    {
                        "stage": stage,
                        "attempt": attempt,
                        "status": "failed",
                        "model": client.config.model,
                        "usage": {},
                        "output_preview": "",
                        "failure_reason": failure_reason,
                    },
                    stage_trace_lock=stage_trace_lock,
                )
                self._emit_progress(
                    progress_callback,
                    phase=phase_label,
                    stage=stage,
                    progress_percent=progress_percent or 0,
                    message=f"{stage} attempt {attempt} failed: {failure_reason}",
                    status="running",
                    attempt=attempt,
                    failure_reason=failure_reason,
                )
                logger.warning("Stone v3 stage %s attempt %s failed: %s", stage, attempt, failure_reason)
        raise RuntimeError(f"{stage} failed after {STONE_V3_MAX_RETRIES} attempts: {last_error}")

    def _run_family_batches(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        batches = self._chunk_items_to_fit_budget(
            compact_profiles,
            build_messages=lambda batch: self._family_batch_messages(project_name, batch),
            max_items=STONE_V3_FAMILY_BATCH_SIZE,
        )
        batch_specs = [(index, batch) for index, batch in enumerate(batches, start=1)]
        if not batch_specs:
            return []
        completed = 0
        outputs: dict[int, dict[str, Any]] = {}
        max_workers = min(STONE_V3_BATCH_CONCURRENCY, len(batch_specs))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-v3-family") as executor:
            future_map = {
                executor.submit(
                    self._call_json_stage,
                    client,
                    stage=f"family_induction_v3_batch_{position}",
                    phase="family_induction_v3",
                    messages=self._family_batch_messages(project_name, batch),
                    stage_trace=stage_trace,
                    stage_trace_lock=stage_trace_lock,
                    progress_callback=progress_callback,
                    progress_percent=16,
                    timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                    cancel_requested=cancel_requested,
                ): position
                for position, batch in batch_specs
            }
            total_batches = len(future_map)
            for future in as_completed(future_map):
                position = future_map[future]
                self._ensure_not_cancelled(cancel_requested, stage=f"family_induction_v3_batch_{position}")
                outputs[position] = future.result()
                completed += 1
                self._emit_progress(
                    progress_callback,
                    phase="family_induction_v3",
                    stage=f"family_induction_v3_batch_{position}",
                    progress_percent=self._interpolate_progress(16, 40, completed, total_batches),
                    message=f"Completed family batch {position}/{total_batches}.",
                    status="running",
                    batch_index=position,
                    batch_total=total_batches,
                )
        return [outputs[position] for position, _batch in batch_specs if position in outputs]

    def _run_family_finalize(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        messages = self._family_finalize_messages(project_name, compact_profiles, batch_outputs)
        if self._message_token_count(messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            payload = self._call_json_stage(
                client,
                stage="family_induction_v3_finalize",
                phase="family_induction_v3",
                messages=messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=46,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
        else:
            batch_shards = self._chunk_items_to_fit_budget(
                batch_outputs,
                build_messages=lambda shard: self._family_finalize_messages(project_name, compact_profiles[:24], shard),
                max_items=8,
            )
            shard_outputs: list[dict[str, Any]] = []
            total_shards = len(batch_shards)
            for index, shard in enumerate(batch_shards, start=1):
                self._emit_progress(
                    progress_callback,
                    phase="family_induction_v3",
                    progress_percent=self._interpolate_progress(44, 46, index - 1, max(total_shards, 1)),
                    message=f"Family-finalize input exceeded budget; merging shard {index}/{total_shards}.",
                    stage=f"family_induction_v3_finalize_shard_{index}",
                    batch_index=index,
                    batch_total=total_shards,
                )
                shard_outputs.append(
                    self._call_json_stage(
                        client,
                        stage=f"family_induction_v3_finalize_shard_{index}",
                        phase="family_induction_v3",
                        messages=self._family_finalize_messages(project_name, compact_profiles[:24], shard),
                        stage_trace=stage_trace,
                        stage_trace_lock=stage_trace_lock,
                        progress_callback=progress_callback,
                        progress_percent=self._interpolate_progress(45, 47, index, max(total_shards, 1)),
                        timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                        cancel_requested=cancel_requested,
                    )
                )
            payload = self._call_json_stage(
                client,
                stage="family_induction_v3_finalize",
                phase="family_induction_v3",
                messages=self._family_finalize_messages(project_name, compact_profiles[:36], shard_outputs),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=47,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
        families = payload.get("families") if isinstance(payload.get("families"), list) else []
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(families, start=1):
            if not isinstance(item, dict):
                continue
            label = _normalize_short_text(item.get("label"), limit=120)
            family_id = _normalize_short_text(item.get("family_id"), limit=40) or f"family-{index}"
            normalized.append(
                {
                    "family_id": family_id,
                    "label": label or family_id,
                    "description": _normalize_short_text(item.get("description"), limit=180),
                    "selection_cues": _normalize_string_list(item.get("selection_cues"), limit=6, item_limit=40),
                    "motif_tags": _normalize_string_list(item.get("motif_tags"), limit=4, item_limit=24),
                    "member_count": int(item.get("member_count") or 0),
                }
            )
        if normalized:
            return normalized
        raise RuntimeError("family_induction_v3_finalize returned no valid families.")

    def _run_author_model(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        direct_messages = self._author_model_messages(project_name, compact_profiles, families)
        if self._message_token_count(direct_messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            author_raw = self._call_json_stage(
                client,
                stage="author_model_v3",
                phase="author_model_v3",
                messages=direct_messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=56,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
            return normalize_stone_author_model_v3(
                author_raw,
                project_name=project_name,
                profiles=profiles,
                families=families,
            )

        profile_shards = self._chunk_items_to_fit_budget(
            compact_profiles,
            build_messages=lambda shard: self._author_model_messages(project_name, shard, families),
            max_items=32,
        )
        shard_models: list[dict[str, Any]] = []
        total_shards = len(profile_shards)
        for index, shard in enumerate(profile_shards, start=1):
            shard_document_ids = {str(item.get("document_id") or "") for item in shard}
            shard_profiles = [
                item
                for item in profiles
                if str(item.get("document_id") or "") in shard_document_ids
            ]
            self._emit_progress(
                progress_callback,
                phase="author_model_v3",
                progress_percent=self._interpolate_progress(52, 56, index - 1, max(total_shards, 1)),
                message=f"Author-model input exceeded budget; synthesizing shard {index}/{total_shards}.",
                stage=f"author_model_v3_shard_{index}",
                batch_index=index,
                batch_total=total_shards,
            )
            shard_raw = self._call_json_stage(
                client,
                stage=f"author_model_v3_shard_{index}",
                phase="author_model_v3",
                messages=self._author_model_messages(project_name, shard, families),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=self._interpolate_progress(53, 57, index, max(total_shards, 1)),
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )
            shard_models.append(
                self._compact_author_model_for_critic(
                    normalize_stone_author_model_v3(
                        shard_raw,
                        project_name=project_name,
                        profiles=shard_profiles,
                        families=families,
                    )
                )
            )
        author_raw = self._call_json_stage(
            client,
            stage="author_model_v3",
            phase="author_model_v3",
            messages=self._author_model_finalize_messages(project_name, families, shard_models),
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=58,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
        )
        return normalize_stone_author_model_v3(
            author_raw,
            project_name=project_name,
            profiles=profiles,
            families=families,
        )

    def _run_prototype_batches(
        self,
        client: OpenAICompatibleClient,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
        families: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        *,
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> list[dict[str, Any]]:
        by_document = {str(item.get("document_id") or item.get("id") or ""): item for item in documents}
        seed_entries = []
        for item in compact_profiles:
            seed_entries.append(
                {
                    "profile": item,
                    "document": {
                        "document_id": item.get("document_id"),
                        "title": item.get("title"),
                        "text": _trim_text((by_document.get(str(item.get("document_id") or "")) or {}).get("text"), 1500),
                        "opening": item.get("opening"),
                        "closing": item.get("closing"),
                    },
                }
            )
        entry_groups = self._chunk_items_to_fit_budget(
            seed_entries,
            build_messages=lambda entries: self._prototype_batch_messages(
                project_name,
                [entry["profile"] for entry in entries],
                [entry["document"] for entry in entries],
                families,
            ),
            max_items=STONE_V3_PROTOTYPE_BATCH_SIZE,
        )
        batch_specs = [
            (
                position,
                [entry["profile"] for entry in group],
                [entry["document"] for entry in group],
            )
            for position, group in enumerate(entry_groups, start=1)
        ]
        if not batch_specs:
            return []
        completed = 0
        outputs: dict[int, dict[str, Any]] = {}
        max_workers = min(STONE_V3_BATCH_CONCURRENCY, len(batch_specs))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-v3-prototype") as executor:
            future_map = {
                executor.submit(
                    self._call_json_stage,
                    client,
                    stage=f"prototype_index_v3_batch_{position}",
                    phase="prototype_index_v3",
                    messages=self._prototype_batch_messages(project_name, batch, batch_docs, families),
                    stage_trace=stage_trace,
                    stage_trace_lock=stage_trace_lock,
                    progress_callback=progress_callback,
                    progress_percent=62,
                    max_tokens=2800,
                    timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                    cancel_requested=cancel_requested,
                ): position
                for position, batch, batch_docs in batch_specs
            }
            total_batches = len(future_map)
            for future in as_completed(future_map):
                position = future_map[future]
                self._ensure_not_cancelled(cancel_requested, stage=f"prototype_index_v3_batch_{position}")
                outputs[position] = future.result()
                completed += 1
                self._emit_progress(
                    progress_callback,
                    phase="prototype_index_v3",
                    stage=f"prototype_index_v3_batch_{position}",
                    progress_percent=self._interpolate_progress(62, 86, completed, total_batches),
                    message=f"Completed prototype batch {position}/{total_batches}.",
                    status="running",
                    batch_index=position,
                    batch_total=total_batches,
                )
        return [outputs[position] for position, _batch, _batch_docs in batch_specs if position in outputs]

    def _run_prototype_finalize(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        profiles: list[dict[str, Any]],
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        merged = self._merge_prototype_batch_outputs(batch_outputs)
        corpus_summary = self._compact_prototype_index_for_critic(
            {
                "documents": merged["documents"],
                "families": families,
                "anchor_registry": merged["anchor_registry"],
                "document_count": len(merged["documents"]),
                "family_count": len(families),
            }
        )
        guidance_messages = self._prototype_finalize_guidance_messages(
            project_name,
            compact_profiles,
            families,
            corpus_summary,
        )
        if self._message_token_count(guidance_messages) > STONE_V3_PROMPT_TOKEN_BUDGET:
            corpus_summary["sample_documents"] = list(corpus_summary.get("sample_documents") or [])[:16]
            corpus_summary["anchor_samples"] = list(corpus_summary.get("anchor_samples") or [])[:8]
            guidance_messages = self._prototype_finalize_guidance_messages(
                project_name,
                compact_profiles[:24],
                families[:12],
                corpus_summary,
            )
        if self._message_token_count(guidance_messages) > STONE_V3_PROMPT_TOKEN_BUDGET:
            raise RuntimeError("prototype_index_v3_finalize still exceeded the 128k prompt budget after compaction.")
        prototype_guidance = self._call_json_stage(
            client,
            stage="prototype_index_v3_finalize",
            phase="prototype_index_v3",
            messages=guidance_messages,
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=90,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
        )
        return normalize_stone_prototype_index_v3(
            {
                "documents": merged["documents"],
                "families": prototype_guidance.get("families") or families,
                "retrieval_policy": prototype_guidance.get("retrieval_policy") or {},
                "selection_guides": prototype_guidance.get("selection_guides") or {},
                "anchor_registry": merged["anchor_registry"],
            },
            project_name=project_name,
            profiles=profiles,
            documents=[
                {
                    "document_id": item.get("document_id"),
                    "title": item.get("title"),
                }
                for item in merged["documents"]
            ],
            families=families,
        )

    def _run_baseline_critic(
        self,
        client: OpenAICompatibleClient,
        *,
        project_name: str,
        author_model: dict[str, Any],
        prototype_index: dict[str, Any],
        stage_trace: list[dict[str, Any]],
        stage_trace_lock: Lock | None = None,
        progress_callback: StoneV3ProgressCallback | None = None,
        cancel_requested: StoneV3CancelRequested | None = None,
    ) -> dict[str, Any]:
        author_summary = self._compact_author_model_for_critic(author_model)
        prototype_summary = self._compact_prototype_index_for_critic(prototype_index)
        direct_messages = self._baseline_critic_messages(project_name, author_summary, prototype_summary)
        if self._message_token_count(direct_messages) <= STONE_V3_PROMPT_TOKEN_BUDGET:
            return self._call_json_stage(
                client,
                stage="baseline_critic_v3",
                phase="baseline_critic_v3",
                messages=direct_messages,
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=96,
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
            )

        shards = self._build_prototype_critic_shards(prototype_index)
        shard_reviews: list[dict[str, Any]] = []
        total_shards = len(shards)
        for index, shard in enumerate(shards, start=1):
            self._emit_progress(
                progress_callback,
                phase="baseline_critic_v3",
                progress_percent=self._interpolate_progress(94, 96, index - 1, max(total_shards, 1)),
                message=f"Critic input exceeded budget; reviewing shard {index}/{total_shards}.",
                stage=f"baseline_critic_v3_shard_{index}",
                batch_index=index,
                batch_total=total_shards,
            )
            shard_review = self._call_json_stage(
                client,
                stage=f"baseline_critic_v3_shard_{index}",
                phase="baseline_critic_v3",
                messages=self._baseline_critic_shard_messages(project_name, author_summary, prototype_summary, shard, index, total_shards),
                stage_trace=stage_trace,
                stage_trace_lock=stage_trace_lock,
                progress_callback=progress_callback,
                progress_percent=self._interpolate_progress(94, 96, index, max(total_shards, 1)),
                timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
                cancel_requested=cancel_requested,
                max_tokens=1400,
            )
            shard_reviews.append(
                {
                    "shard_index": index,
                    "document_count": len(list(shard.get("documents") or [])),
                    "review": shard_review,
                }
            )
        self._emit_progress(
            progress_callback,
            phase="baseline_critic_v3",
            progress_percent=96,
            message=f"Summarizing {total_shards} critic shard reviews into a final verdict.",
            stage="baseline_critic_v3_finalize",
        )
        return self._call_json_stage(
            client,
            stage="baseline_critic_v3",
            phase="baseline_critic_v3",
            messages=self._baseline_critic_finalize_messages(project_name, author_summary, prototype_summary, shard_reviews),
            stage_trace=stage_trace,
            stage_trace_lock=stage_trace_lock,
            progress_callback=progress_callback,
            progress_percent=96,
            timeout=STONE_V3_STAGE_TIMEOUT_SECONDS,
            cancel_requested=cancel_requested,
            max_tokens=1600,
        )

    @staticmethod
    def _persist_checkpoint(
        checkpoint_callback: StoneV3CheckpointCallback | None,
        checkpoint_state: dict[str, Any],
    ) -> None:
        if checkpoint_callback:
            checkpoint_callback(dict(checkpoint_state))

    @staticmethod
    def _coerce_resume_checkpoint(
        payload: dict[str, Any] | None,
        *,
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
    ) -> dict[str, Any]:
        fingerprint = StoneV3BaselineSynthesizer._build_corpus_fingerprint(compact_profiles, documents)
        checkpoint = dict(payload or {})
        valid = (
            checkpoint.get("version") == STONE_V3_CHECKPOINT_VERSION
            and checkpoint.get("project_name") == project_name
            and checkpoint.get("corpus_fingerprint") == fingerprint
        )
        if not valid:
            checkpoint = {}
        families = list(checkpoint.get("families") or []) if isinstance(checkpoint.get("families"), list) else []
        author_model = dict(checkpoint.get("author_model") or {}) if isinstance(checkpoint.get("author_model"), dict) else {}
        prototype_index = dict(checkpoint.get("prototype_index") or {}) if isinstance(checkpoint.get("prototype_index"), dict) else {}
        critic_review = dict(checkpoint.get("critic_review") or {}) if isinstance(checkpoint.get("critic_review"), dict) else {}
        if author_model:
            try:
                validate_stone_v3_asset_payload("stone_author_model_v3", author_model)
            except ValueError:
                author_model = {}
        if prototype_index:
            try:
                validate_stone_v3_asset_payload("stone_prototype_index_v3", prototype_index)
            except ValueError:
                prototype_index = {}
        return {
            "version": STONE_V3_CHECKPOINT_VERSION,
            "project_name": project_name,
            "profile_count": len(compact_profiles),
            "document_count": len(documents),
            "corpus_fingerprint": fingerprint,
            "resume_count": int(checkpoint.get("resume_count") or 0) + (1 if valid else 0),
            "resume_available": valid,
            "families": families,
            "author_model": author_model,
            "prototype_index": prototype_index,
            "critic_review": critic_review,
            "stage_trace": list(checkpoint.get("stage_trace") or []),
        }

    @staticmethod
    def _resume_summary(checkpoint_state: dict[str, Any]) -> str:
        recovered: list[str] = []
        if checkpoint_state.get("families"):
            recovered.append("families")
        if checkpoint_state.get("author_model"):
            recovered.append("author model")
        if checkpoint_state.get("prototype_index"):
            recovered.append("prototype index")
        if checkpoint_state.get("critic_review"):
            recovered.append("baseline critic")
        return ", ".join(recovered) if recovered else "no reusable stage"

    @staticmethod
    def _build_corpus_fingerprint(
        compact_profiles: list[dict[str, Any]],
        documents: list[dict[str, Any]],
    ) -> str:
        profile_parts = [
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "summary": item.get("summary"),
                "opening": item.get("opening"),
                "closing": item.get("closing"),
            }
            for item in compact_profiles
        ]
        document_parts = [
            {
                "document_id": item.get("document_id"),
                "title": item.get("title"),
                "text": _trim_text(item.get("text"), 240),
            }
            for item in documents
        ]
        digest = hashlib.sha256(
            json.dumps(
                {"profiles": profile_parts, "documents": document_parts},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return digest

    @staticmethod
    def _message_token_count(messages: list[dict[str, Any]]) -> int:
        return sum(estimate_stone_prompt_tokens(item.get("content")) for item in messages)

    def _chunk_items_to_fit_budget(
        self,
        items: list[Any],
        *,
        build_messages: Callable[[list[Any]], list[dict[str, Any]]],
        max_items: int,
    ) -> list[list[Any]]:
        if not items:
            return []
        chunks: list[list[Any]] = []
        current: list[Any] = []
        for item in items:
            candidate = [*current, item]
            if current and (
                len(candidate) > max_items
                or self._message_token_count(build_messages(candidate)) > STONE_V3_PROMPT_TOKEN_BUDGET
            ):
                chunks.append(current)
                current = [item]
            else:
                current = candidate
            if self._message_token_count(build_messages(current)) > STONE_V3_PROMPT_TOKEN_BUDGET:
                raise RuntimeError("A single Stone v3 batch item exceeded the 128k prompt budget.")
        if current:
            chunks.append(current)
        return chunks

    @staticmethod
    def _merge_prototype_batch_outputs(batch_outputs: list[dict[str, Any]]) -> dict[str, Any]:
        documents_by_id: dict[str, dict[str, Any]] = {}
        anchor_seen: set[str] = set()
        anchor_registry: list[dict[str, Any]] = []
        for batch in batch_outputs:
            for document in list((batch or {}).get("documents") or []):
                if not isinstance(document, dict):
                    continue
                document_id = str(document.get("document_id") or "").strip()
                if not document_id:
                    continue
                existing = dict(documents_by_id.get(document_id) or {})
                merged = dict(existing)
                for key, value in document.items():
                    if value in (None, "", [], {}):
                        continue
                    if key == "anchor_registry":
                        continue
                    merged[key] = value
                merged_anchors = []
                merged_anchor_seen: set[str] = set()
                for anchor in list(existing.get("anchor_registry") or []) + list(document.get("anchor_registry") or []):
                    if not isinstance(anchor, dict):
                        continue
                    anchor_id = _normalize_short_text(anchor.get("id"), limit=96)
                    anchor_key = anchor_id or json.dumps(anchor, ensure_ascii=False, sort_keys=True)
                    if anchor_key in merged_anchor_seen:
                        continue
                    merged_anchors.append(dict(anchor))
                    merged_anchor_seen.add(anchor_key)
                    if anchor_key not in anchor_seen:
                        anchor_registry.append(dict(anchor))
                        anchor_seen.add(anchor_key)
                merged["anchor_registry"] = merged_anchors
                documents_by_id[document_id] = merged
        return {
            "documents": list(documents_by_id.values()),
            "anchor_registry": anchor_registry,
        }

    @staticmethod
    def _compact_author_model_for_critic(author_model: dict[str, Any]) -> dict[str, Any]:
        author_core = dict(author_model.get("author_core") or {})
        critic_rubrics = dict(author_model.get("critic_rubrics") or {})
        translation_rules = []
        for item in list(author_model.get("translation_rules") or [])[:8]:
            if not isinstance(item, dict):
                continue
            translation_rules.append(
                {
                    "value_lens": _normalize_short_text(item.get("value_lens"), limit=48),
                    "preferred_motifs": _normalize_string_list(item.get("preferred_motifs"), limit=4, item_limit=24),
                    "opening_moves": _normalize_string_list(item.get("opening_moves"), limit=3, item_limit=60),
                    "closure_moves": _normalize_string_list(item.get("closure_moves"), limit=3, item_limit=60),
                }
            )
        evidence = []
        for item in list(author_model.get("global_evidence") or [])[:12]:
            if not isinstance(item, dict):
                continue
            evidence.append(
                {
                    "document_id": _normalize_short_text(item.get("document_id"), limit=80),
                    "title": _normalize_short_text(item.get("title"), limit=120),
                    "summary": _normalize_short_text(item.get("summary"), limit=180),
                    "opening": _normalize_short_text(item.get("opening"), limit=180),
                    "closing": _normalize_short_text(item.get("closing"), limit=180),
                }
            )
        return {
            "author_core": {
                "voice_summary": _normalize_short_text(author_core.get("voice_summary"), limit=180),
                "worldview_summary": _normalize_short_text(author_core.get("worldview_summary"), limit=180),
                "tone_summary": _normalize_short_text(author_core.get("tone_summary"), limit=180),
                "signature_motifs": _normalize_string_list(author_core.get("signature_motifs"), limit=6, item_limit=24),
            },
            "translation_rules": translation_rules,
            "stable_moves": _normalize_string_list(author_model.get("stable_moves"), limit=8, item_limit=72),
            "forbidden_moves": _normalize_string_list(author_model.get("forbidden_moves"), limit=8, item_limit=72),
            "family_map": list(author_model.get("family_map") or [])[:12],
            "critic_rubrics": {
                key: _normalize_string_list(critic_rubrics.get(key), limit=6, item_limit=72)
                for key in ("formal_fidelity", "worldview_translation", "syntheticness")
            },
            "global_evidence": evidence,
        }

    @staticmethod
    def _compact_prototype_document_for_critic(document: dict[str, Any]) -> dict[str, Any]:
        retrieval_handles = dict(document.get("retrieval_handles") or {})
        anchors = list(document.get("anchor_registry") or [])
        compact_anchors = []
        for item in anchors[:2]:
            if not isinstance(item, dict):
                continue
            compact_anchors.append(
                {
                    "role": _normalize_short_text(item.get("role"), limit=24),
                    "quote": _normalize_short_text(item.get("quote"), limit=180),
                }
            )
        return {
            "document_id": _normalize_short_text(document.get("document_id"), limit=80),
            "title": _normalize_short_text(document.get("title"), limit=120),
            "family_label": _normalize_short_text(document.get("family_label") or document.get("family_id"), limit=120),
            "length_band": _normalize_short_text(document.get("length_band"), limit=24),
            "surface_form": _normalize_short_text(document.get("surface_form"), limit=32),
            "document_summary": _normalize_short_text(document.get("document_summary"), limit=180),
            "keywords": _normalize_string_list(retrieval_handles.get("keywords"), limit=6, item_limit=24),
            "anchors": compact_anchors,
        }

    @staticmethod
    def _compact_prototype_index_for_critic(prototype_index: dict[str, Any]) -> dict[str, Any]:
        documents = list(prototype_index.get("documents") or [])
        families = list(prototype_index.get("families") or [])
        family_distribution = Counter(
            _normalize_short_text(item.get("family_label") or item.get("family_id"), limit=120)
            for item in documents
            if isinstance(item, dict)
        )
        sampled_documents: list[dict[str, Any]] = []
        seen_families: set[str] = set()
        for item in documents:
            if not isinstance(item, dict):
                continue
            family_label = _normalize_short_text(item.get("family_label") or item.get("family_id"), limit=120)
            if family_label and family_label in seen_families and len(sampled_documents) >= 24:
                continue
            if family_label:
                seen_families.add(family_label)
            sampled_documents.append(StoneV3BaselineSynthesizer._compact_prototype_document_for_critic(item))
            if len(sampled_documents) >= 32:
                break
        return {
            "document_count": int(prototype_index.get("document_count") or len(documents)),
            "family_count": int(prototype_index.get("family_count") or len(families)),
            "retrieval_policy": dict(prototype_index.get("retrieval_policy") or {}),
            "selection_guides": dict(prototype_index.get("selection_guides") or {}),
            "families": list(families)[:16],
            "family_distribution": [
                {"family": family, "count": count}
                for family, count in family_distribution.most_common(16)
                if family
            ],
            "sample_documents": sampled_documents,
            "anchor_samples": [
                {
                    "document_id": _normalize_short_text(item.get("document_id"), limit=80),
                    "role": _normalize_short_text(item.get("role"), limit=24),
                    "quote": _normalize_short_text(item.get("quote"), limit=180),
                }
                for item in list(prototype_index.get("anchor_registry") or [])[:16]
                if isinstance(item, dict)
            ],
        }

    @staticmethod
    def _build_prototype_critic_shards(prototype_index: dict[str, Any]) -> list[dict[str, Any]]:
        documents = [
            StoneV3BaselineSynthesizer._compact_prototype_document_for_critic(item)
            for item in list(prototype_index.get("documents") or [])
            if isinstance(item, dict)
        ]
        shards: list[dict[str, Any]] = []
        for index in range(0, len(documents), STONE_V3_CRITIC_SHARD_SIZE):
            shards.append(
                {
                    "documents": documents[index : index + STONE_V3_CRITIC_SHARD_SIZE],
                }
            )
        return shards or [{"documents": []}]

    @staticmethod
    def _append_stage_trace(
        stage_trace: list[dict[str, Any]],
        item: dict[str, Any],
        *,
        stage_trace_lock: Lock | None = None,
    ) -> None:
        if stage_trace_lock is None:
            stage_trace.append(item)
            return
        with stage_trace_lock:
            stage_trace.append(item)

    @staticmethod
    def _emit_progress(
        progress_callback: StoneV3ProgressCallback | None,
        *,
        phase: str,
        progress_percent: int,
        message: str,
        stage: str | None = None,
        status: str = "running",
        attempt: int | None = None,
        batch_index: int | None = None,
        batch_total: int | None = None,
        output_preview: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        if not progress_callback:
            return
        payload = {
            "phase": phase,
            "stage": stage or phase,
            "status": status,
            "progress_percent": int(progress_percent),
            "message": message,
        }
        if attempt is not None:
            payload["attempt"] = attempt
        if batch_index is not None:
            payload["batch_index"] = batch_index
        if batch_total is not None:
            payload["batch_total"] = batch_total
        if output_preview:
            payload["output_preview"] = output_preview
        if failure_reason:
            payload["failure_reason"] = failure_reason
        progress_callback(payload)

    @staticmethod
    def _ensure_not_cancelled(
        cancel_requested: StoneV3CancelRequested | None,
        *,
        stage: str,
    ) -> None:
        if cancel_requested and cancel_requested():
            raise TimeoutError(f"{stage} cancelled after 120 seconds without stream activity.")

    @staticmethod
    def _interpolate_progress(start: int, end: int, completed: int, total: int) -> int:
        if total <= 0:
            return end
        span = max(0, end - start)
        return min(end, start + round(span * max(0, completed) / total))

    @staticmethod
    def _family_batch_messages(project_name: str, batch: list[dict[str, Any]]) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 family induction stage.\n"
                    "Read a batch of compact document profiles and propose draft prototype families.\n"
                    "Return JSON only with {\"families\": [...]}.\n"
                    "Each family needs family_id, label, description, selection_cues, motif_tags, member_count."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(batch, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _family_finalize_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 family synthesis stage.\n"
                    "Merge batch-level draft families into a canonical family map for the whole corpus.\n"
                    "Return JSON only with {\"families\": [...]}.\n"
                    "Keep labels concrete and retrieval-friendly."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Corpus compact profiles JSON:\n{json.dumps(compact_profiles[:48], ensure_ascii=False, indent=2)}\n\n"
                    f"Batch family drafts JSON:\n{json.dumps(batch_outputs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _author_model_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 author-model synthesizer.\n"
                    "Return JSON only.\n"
                    "Build a corpus-level author model with keys author_core, translation_rules, stable_moves, "
                    "forbidden_moves, family_map, critic_rubrics, global_evidence."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles[:60], ensure_ascii=False, indent=2)}\n\n"
                    f"Canonical family map JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _author_model_finalize_messages(
        project_name: str,
        families: list[dict[str, Any]],
        shard_models: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 author-model finalizer.\n"
                    "Merge shard-level author-model syntheses into one canonical author model.\n"
                    "Return JSON only.\n"
                    "Build keys author_core, translation_rules, stable_moves, forbidden_moves, family_map, "
                    "critic_rubrics, and global_evidence."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Canonical family map JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Shard author models JSON:\n{json.dumps(shard_models, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_batch_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        batch_docs: list[dict[str, Any]],
        families: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-card synthesis stage.\n"
                    "Return JSON only with {\"documents\": [...]}.\n"
                    "Each document needs document_id, family_id, family_label, document_summary, retrieval_handles, "
                    "selection_guides, and anchor_registry."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles, ensure_ascii=False, indent=2)}\n\n"
                    f"Document excerpts JSON:\n{json.dumps(batch_docs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_finalize_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        batch_outputs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-index finalizer.\n"
                    "Return JSON only with documents, families, retrieval_policy, selection_guides, and anchor_registry.\n"
                    "Preserve batch-level document cards and synthesize corpus-level retrieval guidance."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles JSON:\n{json.dumps(compact_profiles[:60], ensure_ascii=False, indent=2)}\n\n"
                    f"Families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype batch outputs JSON:\n{json.dumps(batch_outputs, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _prototype_finalize_guidance_messages(
        project_name: str,
        compact_profiles: list[dict[str, Any]],
        families: list[dict[str, Any]],
        corpus_summary: dict[str, Any],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 prototype-index finalizer.\n"
                    "The per-document prototype cards already exist.\n"
                    "Review the corpus summary and return JSON only with families, retrieval_policy, and selection_guides.\n"
                    "Do not rewrite the full document list."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Compact profiles sample JSON:\n{json.dumps(compact_profiles[:48], ensure_ascii=False, indent=2)}\n\n"
                    f"Canonical families JSON:\n{json.dumps(families, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(corpus_summary, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_messages(
        project_name: str,
        author_model: dict[str, Any],
        prototype_index: dict[str, Any],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic.\n"
                    "Review only the baseline quality and corpus grounding.\n"
                    "Return JSON only with verdict, score, strengths, risks, and repair_notes."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_model, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype index summary JSON:\n{json.dumps(prototype_index, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_shard_messages(
        project_name: str,
        author_summary: dict[str, Any],
        prototype_summary: dict[str, Any],
        shard: dict[str, Any],
        shard_index: int,
        shard_total: int,
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic working on one shard of prototype documents.\n"
                    "Review only this shard for corpus grounding, retrieval usefulness, and synthetic drift risk.\n"
                    "Return JSON only with strengths, risks, repair_notes, and shard_focus."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n"
                    f"Shard: {shard_index}/{shard_total}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(prototype_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype shard JSON:\n{json.dumps(shard, ensure_ascii=False, indent=2)}"
                ),
            },
        ]

    @staticmethod
    def _baseline_critic_finalize_messages(
        project_name: str,
        author_summary: dict[str, Any],
        prototype_summary: dict[str, Any],
        shard_reviews: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "You are the Stone v3 baseline critic finalizer.\n"
                    "Merge shard reviews into one baseline verdict.\n"
                    "Return JSON only with verdict, score, strengths, risks, and repair_notes."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Project: {project_name}\n\n"
                    f"Author model summary JSON:\n{json.dumps(author_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Prototype corpus summary JSON:\n{json.dumps(prototype_summary, ensure_ascii=False, indent=2)}\n\n"
                    f"Critic shard reviews JSON:\n{json.dumps(shard_reviews, ensure_ascii=False, indent=2)}"
                ),
            },
        ]
