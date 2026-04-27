from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib import import_module

_service = import_module("app.agents.stone.writing.service")

globals().update(
    {
        name: value
        for name, value in vars(_service).items()
        if not (name.startswith("__") and name.endswith("__"))
    }
)


def _normalize_packet_critic_payload(
    payload: dict[str, Any],
    *,
    writing_packet: dict[str, Any],
) -> dict[str, Any]:
    allowed_anchor_ids = {
        str(item).strip()
        for item in list(writing_packet.get("anchor_ids") or [])
        if str(item).strip()
    }
    anchor_ids = [
        anchor_id
        for anchor_id in _normalize_string_list(payload.get("anchor_ids"), limit=8)
        if anchor_id in allowed_anchor_ids
    ]
    if not anchor_ids:
        anchor_ids = list(writing_packet.get("anchor_ids") or [])[:4]
    verdict = str(payload.get("verdict") or "").strip()
    if verdict not in {"approve", "rebuild_packet"}:
        verdict = "approve" if payload.get("pass", True) else "rebuild_packet"
    return {
        "critic_key": "packet_critic",
        "critic_label": "写作包审判",
        "pass": bool(payload.get("pass", verdict == "approve")),
        "score": _clamp_score(payload.get("score"), default=0.76 if verdict == "approve" else 0.58),
        "verdict": verdict,
        "anchor_ids": anchor_ids,
        "matched_signals": _normalize_string_list(payload.get("matched_signals"), limit=6),
        "unsupported_fields": _normalize_string_list(payload.get("unsupported_fields"), limit=8),
        "defaulted_fields": _normalize_string_list(payload.get("defaulted_fields"), limit=8),
        "overfit_risks": _normalize_string_list(payload.get("overfit_risks"), limit=6),
        "repair_instructions": _normalize_string_list(payload.get("repair_instructions"), limit=8),
        "risks": _normalize_string_list(payload.get("risks"), limit=6),
    }


def _render_packet_critic_v3(payload: dict[str, Any]) -> str:
    lines = [
        f"结论：{payload.get('verdict')}",
        f"分数：{int(round(float(payload.get('score') or 0.0) * 100))}/100",
    ]
    if payload.get("unsupported_fields"):
        lines.extend(["", "证据不足字段：", *[f"- {item}" for item in (payload.get("unsupported_fields") or [])[:6]]])
    if payload.get("defaulted_fields"):
        lines.extend(["", "默认兜底字段：", *[f"- {item}" for item in (payload.get("defaulted_fields") or [])[:6]]])
    if payload.get("repair_instructions"):
        lines.extend(["", "修正建议：", *[f"- {item}" for item in (payload.get("repair_instructions") or [])[:6]]])
    if payload.get("risks"):
        lines.extend(["", "风险：", *[f"- {item}" for item in (payload.get("risks") or [])[:4]]])
    return "\n".join(lines).strip()


def _build_packet_critic_message_payload_v3(
    critic: dict[str, Any],
    *,
    stream_key: str | None = None,
    stage: str = "packet_critic",
    label: str = "写作包审判",
) -> dict[str, Any]:
    return {
        "stage": stage,
        "label": label,
        "actor_id": "critic-packet",
        "actor_name": "写作包审判",
        "actor_role": "critic",
        "message_kind": "critic",
        "body": _render_packet_critic_v3(critic),
        "detail": critic,
        "created_at": _iso_now(),
        "stream_key": stream_key,
        "stream_state": "complete",
        "render_mode": "markdown",
    }


def _critic_stream_key_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    critic_key: str,
    *,
    round_index: int,
) -> str:
    return self._stream_key(state, critic_key, suffix=f"{critic_key}_round_{round_index}")


def _review_packet_v3(
    self: WritingAgentService,
    state: WritingStreamState,
    analysis_bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    writing_packet: dict[str, Any],
    profile_selection: dict[str, Any],
    client: OpenAICompatibleClient | None,
    *,
    round_index: int = 1,
) -> dict[str, Any]:
    del profile_selection
    if not client:
        raise WritingPipelineError("packet_critic", "写作包审判需要可用的写作模型。")
    label = "写作包审判" if round_index == 1 else f"写作包审判 第{round_index}轮"
    stream_key = self._stream_key(state, "packet_critic", suffix=f"round_{round_index}")
    stream_handler, finalize_stream = self._make_stage_stream_handler(
        state,
        message_kind="critic",
        label=label,
        stage="packet_critic",
        stream_key=stream_key,
        actor_name="写作包审判",
        actor_id="critic-packet",
        actor_role="critic",
    )
    response = None
    started_at = time.perf_counter()
    try:
        response = client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的写作包审判器。\n"
                        "你的任务是在正文起草之前，检查 writing_packet_v3 有没有模板化、证据稀薄、通用兜底过多的问题。\n"
                        "重点盯住五个核心字段：value_lens、judgment_mode、distance、entry_scene、felt_cost。\n"
                        "如果这些字段里有太多通用兜底、缺少锚点或与题目不匹配，你必须要求重建写作包。\n"
                        f"{_stone_json_chinese_instruction(preserve_tokens='verdict, anchor_ids')}\n"
                        "只返回 JSON。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        f"llm_rerank_v3 JSON：\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                        f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                        "请特别检查：\n"
                        "1. 关键字段是不是只靠通用默认值在撑。\n"
                        "2. 题目相关的画像切片有没有真的进入 packet。\n"
                        "3. packet 有没有被压成过于规整的模板。\n"
                        "4. 是否存在让后续正文写成“像标签、不像作者”的风险。\n\n"
                        "返回 JSON：\n"
                        "{\n"
                        '  "pass": boolean,\n'
                        '  "score": number,\n'
                        '  "verdict": "approve|rebuild_packet",\n'
                        '  "anchor_ids": ["anchor ids"],\n'
                        '  "matched_signals": ["命中的强项"],\n'
                        '  "unsupported_fields": ["证据不足的关键字段"],\n'
                        '  "defaulted_fields": ["仍然依赖通用兜底的字段"],\n'
                        '  "overfit_risks": ["模板化或过拟合风险"],\n'
                        '  "repair_instructions": ["如何修正写作包"],\n'
                        '  "risks": ["剩余风险"]\n'
                        "}"
                    ),
                },
            ],
            model=client.config.model,
            temperature=0.1,
            max_tokens=1800,
            stream_handler=stream_handler,
        )
        finalize_stream()
        payload = parse_json_response(response.content, fallback=True)
        if not isinstance(payload, dict):
            raise ValueError("packet_critic did not return a JSON object.")
        self._record_llm_usage(
            state,
            stage="packet_critic",
            label=label,
            stream_key=stream_key,
            attempt=1,
            success=True,
            usage=getattr(response, "usage", None),
            duration_ms=int((time.perf_counter() - started_at) * 1000),
        )
        return _normalize_packet_critic_payload(payload, writing_packet=writing_packet)
    except Exception as exc:
        finalize_stream()
        if response is not None:
            self._record_llm_usage(
                state,
                stage="packet_critic",
                label=label,
                stream_key=stream_key,
                attempt=1,
                success=False,
                usage=getattr(response, "usage", None),
                duration_ms=int((time.perf_counter() - started_at) * 1000),
            )
        raise WritingPipelineError("packet_critic", f"写作包审判失败：{exc}") from exc


def _review_with_v3_critic(
    self: WritingAgentService,
    state: WritingStreamState,
    critic_key: str,
    draft: str,
    analysis_bundle: StoneWritingAnalysisBundle,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    writing_packet: dict[str, Any],
    blueprint: dict[str, Any],
    client: OpenAICompatibleClient | None,
    *,
    round_index: int = 1,
    previous_final_text: str | None = None,
    revision_request: str | None = None,
    style_fingerprint_brief: dict[str, Any] | None = None,
    draft_fingerprint_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    spec = _critic_spec_v3(critic_key)
    if not client:
        raise WritingPipelineError("critic", f"{spec['label']}审判器需要可用的写作模型。")
    stage_name = critic_key
    label = f"{spec['label']}审判" if round_index == 1 else f"{spec['label']}审判 第{round_index}轮"
    stream_key = _critic_stream_key_v3(self, state, critic_key, round_index=round_index)
    stream_handler, finalize_stream = self._make_stage_stream_handler(
        state,
        message_kind="critic",
        label=label,
        stage=stage_name,
        stream_key=stream_key,
        actor_name=spec["label"],
        actor_id=f"critic-{critic_key}",
        actor_role="critic",
    )
    response = None
    started_at = time.perf_counter()
    try:
        response = client.chat_completion_result(
            [
                {
                    "role": "system",
                    "content": (
                        f"你是 Stone v3 的 {spec['label']} 审判代理。\n"
                        f"审判维度键：{critic_key}\n"
                        "只审这一条维度。\n"
                        "所有判断都必须锚定在选中的 v3 证据上。\n"
                        f"{_stone_json_chinese_instruction(preserve_tokens='verdict, anchor_ids, quoted draft spans')}\n"
                        "只返回 JSON。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"审判焦点：{spec['focus']}\n\n"
                        f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        + (
                            f"用户修改意见：\n{revision_request}\n\n"
                            if str(revision_request or "").strip()
                            else ""
                        )
                        + (
                            f"上一版成稿：\n{previous_final_text}\n\n"
                            if str(previous_final_text or "").strip()
                            else ""
                        )
                        + f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        + f"llm_rerank_v3 JSON：\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                        + f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        + f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                        + f"style_fingerprint_brief JSON：\n{json.dumps(style_fingerprint_brief or {}, ensure_ascii=False, indent=2)}\n\n"
                        + f"draft_fingerprint_report JSON：\n{json.dumps(draft_fingerprint_report or {}, ensure_ascii=False, indent=2)}\n\n"
                        + f"critic_rubric JSON：\n{json.dumps((analysis_bundle.author_model.get('critic_rubrics') or {}).get(critic_key) or [], ensure_ascii=False, indent=2)}\n\n"
                        + f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                        + f"当前草稿：\n{draft}\n\n"
                        + "除 `verdict`、`anchor_ids` 与直接引用的正文片段外，其余字段请用简体中文填写。\n"
                        + "返回 JSON：\n"
                        + "{\n"
                        + '  "pass": boolean,\n'
                        + '  "score": number,\n'
                        + '  "verdict": "approve|line_edit|redraft",\n'
                        + '  "anchor_ids": ["selected anchor ids"],\n'
                        + '  "matched_signals": ["命中的信号"],\n'
                        + '  "must_keep_spans": ["必须保留的正文片段，直接引用原文"],\n'
                        + '  "line_edits": ["要修改的句子，并说明怎么改"],\n'
                        + '  "redraft_reason": "需要整篇重写的原因",\n'
                        + '  "risks": ["剩余风险"]\n'
                        + "}"
                    ),
                },
            ],
            model=client.config.model,
            temperature=0.12,
            max_tokens=1800,
            stream_handler=stream_handler,
        )
        finalize_stream()
        payload = parse_json_response(response.content, fallback=True)
        if not isinstance(payload, dict):
            raise ValueError(f"{critic_key} did not return a JSON object.")
        self._record_llm_usage(
            state,
            stage=stage_name,
            label=label,
            stream_key=stream_key,
            attempt=1,
            success=True,
            usage=getattr(response, "usage", None),
            duration_ms=int((time.perf_counter() - started_at) * 1000),
        )
        return _normalize_v3_critic_payload(
            payload,
            critic_key=critic_key,
            selected_anchor_ids=list(rerank.get("anchor_ids") or []),
        )
    except Exception as exc:
        finalize_stream()
        if response is not None:
            self._record_llm_usage(
                state,
                stage=stage_name,
                label=label,
                stream_key=stream_key,
                attempt=1,
                success=False,
                usage=getattr(response, "usage", None),
                duration_ms=int((time.perf_counter() - started_at) * 1000),
            )
        raise WritingPipelineError(stage_name, f"{spec['label']}审判失败：{exc}") from exc


def _run_v3_critics(
    self: WritingAgentService,
    state: WritingStreamState,
    analysis_bundle: StoneWritingAnalysisBundle,
    draft: str,
    request_adapter: dict[str, Any],
    rerank: dict[str, Any],
    writing_packet: dict[str, Any],
    blueprint: dict[str, Any],
    client: OpenAICompatibleClient | None,
    *,
    round_index: int = 1,
    previous_final_text: str | None = None,
    revision_request: str | None = None,
    style_fingerprint_brief: dict[str, Any] | None = None,
    draft_fingerprint_report: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    critic_keys = [
        "feature_density",
        "cross_domain_generalization",
        "rhythm_entropy",
        "extreme_state_handling",
        "ending_landing",
    ]
    if str(previous_final_text or "").strip() and str(revision_request or "").strip():
        critic_keys.extend(["language_fluency", "logic_flow"])
    if not critic_keys:
        return []

    def _run_one(critic_key: str) -> dict[str, Any]:
        return _review_with_v3_critic(
            self,
            state,
            critic_key,
            draft,
            analysis_bundle,
            request_adapter,
            rerank,
            writing_packet,
            blueprint,
            client,
            round_index=round_index,
            previous_final_text=previous_final_text,
            revision_request=revision_request,
            style_fingerprint_brief=style_fingerprint_brief,
            draft_fingerprint_report=draft_fingerprint_report,
        )

    max_workers = max(1, min(int(state.resolved_max_concurrency or 1), len(critic_keys)))
    if max_workers == 1:
        return [_run_one(critic_key) for critic_key in critic_keys]

    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stone-writing-critic") as executor:
        future_map = {executor.submit(_run_one, critic_key): critic_key for critic_key in critic_keys}
        for future in as_completed(future_map):
            critic_key = future_map[future]
            results[critic_key] = future.result()
    return [results[critic_key] for critic_key in critic_keys]


def _revision_request_adapter_v3(
    source_trace: dict[str, Any],
    writing_packet: dict[str, Any],
    state: WritingStreamState,
) -> dict[str, Any]:
    payload = source_trace.get("request_adapter_v3")
    if isinstance(payload, dict) and payload:
        return dict(payload)
    return {
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "desired_length_band": _resolve_length_band_v3(state.target_word_count),
        "surface_form": "scene_vignette",
        "value_lens": str(writing_packet.get("value_lens") or "").strip() or "cost",
        "judgment_mode": str(writing_packet.get("judgment_mode") or "").strip() or "贴身细节里稳定判断",
        "distance": str(writing_packet.get("distance") or "").strip() or "回收式第一人称",
        "entry_scene": str(writing_packet.get("entry_scene") or "").strip() or "从一个具体动作或物件进入。",
        "felt_cost": str(writing_packet.get("felt_cost") or "").strip() or "先把压力落成体感代价，再进入解释。",
        "query_terms": _v3_keyword_units(state.topic, state.extra_requirements, limit=10),
        "motif_terms": list(writing_packet.get("motif_obligations") or [])[:8],
        "anchor_preferences": ["opening", "closing", "signature"],
        "hard_constraints": ["全文必须使用简体中文。"],
        "reasoning": "沿用上一版写作包，按用户意见改写。",
    }


def _revision_rerank_v3(source_trace: dict[str, Any], writing_packet: dict[str, Any]) -> dict[str, Any]:
    payload = source_trace.get("llm_rerank_v3")
    if isinstance(payload, dict) and payload:
        return dict(payload)
    return {
        "selected_documents": [],
        "anchor_ids": list(writing_packet.get("anchor_ids") or [])[:8],
        "selection_reason": "沿用上一版写作包绑定的证据集合。",
        "rerank_notes": ["本轮改稿跳过重排，直接复用既有锚点。"],
    }


def _build_revision_trace_blocks_v3(
    analysis_bundle: StoneWritingAnalysisBundle,
    writing_packet: dict[str, Any],
    blueprint: dict[str, Any],
    revision_rounds: list[dict[str, Any]],
    revision_action: str,
    *,
    source_turn_id: str | None,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "stage",
            "stage": "generation_packet",
            "label": f"Stone v3 基线就绪（{analysis_bundle.version_label}）",
            "baseline": analysis_bundle.generation_packet.get("baseline", {}),
        },
        {
            "type": "stage",
            "stage": "revision_source",
            "label": "已载入上一版成稿，准备改写",
            "source_turn_id": source_turn_id,
            "anchor_ids": list(writing_packet.get("anchor_ids") or [])[:8],
        },
        {
            "type": "stage",
            "stage": "blueprint_v3",
            "label": "改稿蓝图已生成",
            "anchor_ids": blueprint.get("anchor_ids") or [],
            "paragraph_map": list(blueprint.get("paragraph_map") or [])[:6],
        },
        {
            "type": "stage",
            "stage": "redraft",
            "label": "改稿正文已完成",
        },
    ]
    for round_payload in revision_rounds:
        blocks.append(
            {
                "type": "revision_round",
                "round": round_payload.get("round"),
                "stage": round_payload.get("stage"),
                "revision_action": round_payload.get("revision_action"),
                "word_count": round_payload.get("word_count"),
                "critic_count": len(round_payload.get("critics") or []),
            }
        )
    blocks.append(
        {
            "type": "stage",
            "stage": "revision",
            "label": f"改稿动作：{revision_action}",
        }
    )
    return blocks


run_v3_critics = _run_v3_critics

__all__ = [
    "_normalize_packet_critic_payload",
    "_render_packet_critic_v3",
    "_build_packet_critic_message_payload_v3",
    "_critic_stream_key_v3",
    "_review_packet_v3",
    "_review_with_v3_critic",
    "_run_v3_critics",
    "_revision_request_adapter_v3",
    "_revision_rerank_v3",
    "_build_revision_trace_blocks_v3",
    "run_v3_critics",
]
