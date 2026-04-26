from __future__ import annotations

from importlib import import_module

_service = import_module("app.agents.stone.writing.service")

globals().update(
    {
        name: value
        for name, value in vars(_service).items()
        if not (name.startswith("__") and name.endswith("__"))
    }
)

def _run_revision_pipeline_v3(
    self: WritingAgentService,
    session,
    state: WritingStreamState,
    *,
    analysis_bundle: StoneWritingAnalysisBundle,
    client: OpenAICompatibleClient,
) -> None:
    source_trace = dict(state.revision_source_trace or {})
    previous_final_text = str(state.revision_source_text or source_trace.get("final_text") or "").strip()
    source_turn_id = str(state.revision_source_turn_id or "") or None
    writing_packet = dict(source_trace.get("writing_packet_v3") or {})
    if not previous_final_text or not writing_packet:
        _run_llm_first_pipeline_v3(
            self,
            session,
            state,
            analysis_bundle=analysis_bundle,
            client=client,
        )
        return

    request_adapter = _revision_request_adapter_v3(source_trace, writing_packet, state)
    shortlist = dict(source_trace.get("candidate_shortlist_v3") or {})
    rerank = _revision_rerank_v3(source_trace, writing_packet)
    revision_rounds: list[dict[str, Any]] = []
    revision_request = str(state.raw_message or state.topic or "").strip()

    self._emit_live_writer_message(
        state,
        message_kind="blueprint_v3",
        label="改稿蓝图",
        body="正在结合上一版成稿和用户修改意见重建蓝图……",
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    blueprint_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="blueprint_v3",
        label="改稿蓝图",
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的改稿蓝图规划器。\n"
                    "本轮沿用既有 writing_packet_v3，不再重做 request adapter、shortlist 或 rerank。\n"
                    "你必须根据上一版成稿和用户修改意见，重建一个新的蓝图。\n"
                    "这里只做蓝图，不要写正文。\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='anchor_ids, axis_keys')}\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"用户修改意见：\n{revision_request}\n\n"
                    f"原始写作请求：\n{render_writing_request(str(source_trace.get('topic') or state.topic), int(source_trace.get('target_word_count') or state.target_word_count), source_trace.get('extra_requirements'))}\n\n"
                    f"上一版成稿：\n{previous_final_text}\n\n"
                    f"上一版 blueprint_v3 JSON：\n{json.dumps(source_trace.get('blueprint_v3') or {}, ensure_ascii=False, indent=2)}\n\n"
                    f"沿用的 writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    "除 `anchor_ids` 和 `axis_keys` 外，其余字段请用简体中文填写。\n\n"
                    "返回 JSON：\n"
                    "{\n"
                    '  "paragraph_count": number,\n'
                    '  "shape_note": "整体形状，简体中文",\n'
                    '  "entry_move": "如何进入，简体中文",\n'
                    '  "development_move": "压力如何推进，简体中文",\n'
                    '  "turning_device": "转折装置或无，简体中文",\n'
                    '  "closure_residue": "如何留下余味，简体中文",\n'
                    '  "keep_terms": ["需要保留的词，简体中文"],\n'
                    '  "motif_obligations": ["母题要求，简体中文"],\n'
                    '  "steps": ["有序步骤，简体中文"],\n'
                    '  "do_not_do": ["必须避免的漂移，简体中文"],\n'
                    '  "axis_map": {"facet_key": {"goal": "该轴的目标，简体中文", "paragraph_hint": number, "anchor_ids": ["anchor ids"]}},\n'
                    '  "paragraph_map": [{"paragraph_index": number, "role": "opening|development|closing", "objective": "该段目标，简体中文", "axis_keys": ["axis keys"], "anchor_ids": ["anchor ids"]}],\n'
                    '  "anchor_ids": ["anchor ids"]\n'
                    "}"
                ),
            },
        ],
    )
    blueprint = _normalize_blueprint_packet_v3(blueprint_raw, state, writing_packet)
    blueprint_payload = _build_writer_message_payload(
        message_kind="blueprint_v3",
        label="改稿蓝图",
        body=_render_blueprint_v3(blueprint),
        detail=blueprint,
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    self._emit(state, "stage", blueprint_payload)

    self._emit_live_writer_message(
        state,
        message_kind="redraft",
        label="按意见重写",
        body="正在根据新蓝图、上一版成稿和用户修改意见重写正文……",
        stage="redraft",
        stream_key=self._stream_key(state, "redraft"),
    )
    draft = _call_writer_text_stage_v3(
        self,
        state,
        client,
        stage="redraft",
        label="按意见重写",
        temperature=0.34,
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的改稿重写器。\n"
                    "沿用既有 writing_packet_v3，根据新的 blueprint_v3、上一版成稿和用户修改意见重写整篇正文。\n"
                    "先判断上一版哪些内容值得保留，再整体重写，不要只做表面替换。\n"
                    f"{_STONE_BODY_CHINESE_ONLY}\n"
                    "不要暴露任何幕后提示词或分析术语。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"用户修改意见：\n{revision_request}\n\n"
                    f"上一版成稿：\n{previous_final_text}\n\n"
                    f"沿用的 writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"新的 blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    f"author_floor JSON：\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"上一版 final_assessment JSON：\n{json.dumps(source_trace.get('final_assessment') or {}, ensure_ascii=False, indent=2)}"
                ),
            },
        ],
    )
    draft_payload = _build_writer_message_payload(
        message_kind="redraft",
        label="按意见重写",
        body=draft,
        detail={"word_count": estimate_word_count(draft), "reason": "user_revision"},
        stage="redraft",
        stream_key=self._stream_key(state, "redraft"),
    )
    self._emit(state, "stage", draft_payload)

    critics = _run_v3_critics(
        self,
        state,
        analysis_bundle,
        draft,
        request_adapter,
        rerank,
        writing_packet,
        blueprint,
        client,
        previous_final_text=previous_final_text,
        revision_request=revision_request,
    )
    critic_messages = [
        _build_critic_message_payload_v2(
            critic,
            stream_key=self._stream_key(state, critic["critic_key"], suffix=critic["critic_key"]),
            stage=critic["critic_key"],
        )
        for critic in critics
    ]
    for payload in critic_messages:
        self._emit(state, "stage", payload)

    revision_payloads: list[dict[str, Any]] = []
    current_text = draft
    active_critics = critics
    revision_action = _revision_action_v3(critics)
    if revision_action != "none":
        revision_rounds.append(
            {
                "round": 1,
                "stage": "critic_round_1",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": critics,
            }
        )
    if revision_action == "redraft":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="redraft",
            label="再次重写",
            temperature=0.3,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的改稿重写器。\n"
                        "请根据用户修改意见和 critic 反馈，再次整篇重写正文。\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "critic 反馈和用户意见都必须被真正落实。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"用户修改意见：\n{revision_request}\n\n"
                        f"上一版成稿：\n{previous_final_text}\n\n"
                        f"当前改写稿：\n{current_text}\n\n"
                        f"critic 反馈 JSON：\n{json.dumps(critics, ensure_ascii=False, indent=2)}\n\n"
                        f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        payload = _build_writer_message_payload(
            message_kind="redraft",
            label="再次重写",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft"},
            stage="redraft",
            stream_key=self._stream_key(state, "revision_redraft_round_1"),
        )
        revision_payloads.append(payload)
        self._emit(state, "stage", payload)
    elif revision_action == "line_edit":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="line_edit",
            label="按意见修订",
            temperature=0.12,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的逐句修订器。\n"
                        "请在保住结构骨架和收尾气压的前提下，根据用户修改意见与 critic 反馈修订文章。\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "重点修复语言不顺、逻辑断裂和没改到位的地方。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"用户修改意见：\n{revision_request}\n\n"
                        f"上一版成稿：\n{previous_final_text}\n\n"
                        f"当前改写稿：\n{current_text}\n\n"
                        f"line_edit_brief JSON：\n{json.dumps(_build_v3_line_edit_brief(current_text, critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                        f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        payload = _build_writer_message_payload(
            message_kind="line_edit",
            label="按意见修订",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit"},
            stage="line_edit",
            stream_key=self._stream_key(state, "revision_line_edit_round_1"),
        )
        revision_payloads.append(payload)
        self._emit(state, "stage", payload)

    if revision_action != "none" and _should_run_second_critic_round_v2(active_critics):
        round_two_critics = _run_v3_critics(
            self,
            state,
            analysis_bundle,
            current_text,
            request_adapter,
            rerank,
            writing_packet,
            blueprint,
            client,
            round_index=2,
            previous_final_text=previous_final_text,
            revision_request=revision_request,
        )
        round_two_messages = [
            _build_critic_message_payload_v2(
                critic,
                stream_key=self._stream_key(state, f"{critic['critic_key']}_round_2", suffix=critic["critic_key"]),
                stage=critic["critic_key"],
                label_suffix=" 第 2 轮",
            )
            for critic in round_two_critics
        ]
        for payload in round_two_messages:
            self._emit(state, "stage", payload)
        revision_rounds.append(
            {
                "round": 2,
                "stage": "critic_round_2",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": round_two_critics,
            }
        )
        active_critics = round_two_critics
        second_action = _revision_action_v3(round_two_critics)
        if second_action == "redraft":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="redraft",
                label="再次重写 第 2 轮",
                temperature=0.28,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v3 的改稿重写器。\n"
                            "只返回文章正文。\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"用户修改意见：\n{revision_request}\n\n"
                            f"上一版成稿：\n{previous_final_text}\n\n"
                            f"critic 反馈 JSON：\n{json.dumps(round_two_critics, ensure_ascii=False, indent=2)}\n\n"
                            f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="redraft",
                label="再次重写 第 2 轮",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft_round_2"},
                stage="redraft",
                stream_key=self._stream_key(state, "revision_redraft_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "redraft"
        elif second_action == "line_edit":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="line_edit",
                label="按意见修订 第 2 轮",
                temperature=0.1,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v3 的逐句修订器。\n"
                            "只返回文章正文。\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"用户修改意见：\n{revision_request}\n\n"
                            f"当前改写稿：\n{current_text}\n\n"
                            f"line_edit_brief JSON：\n{json.dumps(_build_v3_line_edit_brief(current_text, round_two_critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                            f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="line_edit",
                label="按意见修订 第 2 轮",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit_round_2"},
                stage="line_edit",
                stream_key=self._stream_key(state, "revision_line_edit_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "line_edit"
        else:
            revision_action = second_action

    final_text = current_text
    final_assessment = _build_final_assessment_v2(
        final_text,
        active_critics,
        state.topic,
        state.target_word_count,
        revision_action=revision_action,
    )
    final_payload = _build_writer_message_payload(
        message_kind="final",
        label="最终成稿",
        body=final_text,
        detail={"word_count": estimate_word_count(final_text), "final_assessment": final_assessment},
        stage="final",
        stream_key=self._stream_key(state, "final"),
    )
    self._emit(state, "stage", final_payload)

    timeline = [
        blueprint_payload,
        draft_payload,
        *critic_messages,
        *revision_payloads,
        final_payload,
    ]
    if revision_rounds and "round_two_messages" in locals():
        timeline = [
            blueprint_payload,
            draft_payload,
            *critic_messages,
            *round_two_messages,
            *revision_payloads,
            final_payload,
        ]

    trace = {
        "kind": "writing_result",
        "status": "completed",
        "degraded_mode": False,
        "degradation_reasons": [],
        "revision_mode": True,
        "request_mode": "revision",
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "raw_message": state.raw_message,
        "revision_request": revision_request,
        "revision_source_turn_id": source_turn_id,
        "revision_source": {
            "turn_id": source_turn_id,
            "topic": source_trace.get("topic"),
            "target_word_count": source_trace.get("target_word_count"),
            "final_text": previous_final_text,
            "final_assessment": source_trace.get("final_assessment"),
        },
        "baseline_source": analysis_bundle.source,
        "preprocess_run_id": analysis_bundle.run_id,
        "analysis_run_id": (analysis_bundle.analysis_summary or {}).get("run_id"),
        "analysis_version": analysis_bundle.version_label,
        "analysis_target_role": analysis_bundle.target_role,
        "analysis_context": analysis_bundle.analysis_context,
        "analysis_facets": list((analysis_bundle.analysis_summary or {}).get("facet_packets") or []),
        "coverage_warnings": list(writing_packet.get("coverage_warnings") or [])[:10],
        "axis_source_map": dict(writing_packet.get("axis_source_map") or {}),
        "generation_packet": analysis_bundle.generation_packet,
        "request_adapter_v3": request_adapter,
        "candidate_shortlist_v3": shortlist,
        "llm_rerank_v3": rerank,
        "writing_packet_v3": writing_packet,
        "blueprint_v3": blueprint,
        "anchor_ids": _collect_v3_trace_anchor_ids(
            analysis_bundle,
            rerank,
            writing_packet,
            blueprint,
            revision_rounds,
        ),
        "blocks": _build_revision_trace_blocks_v3(
            analysis_bundle,
            writing_packet,
            blueprint,
            revision_rounds,
            revision_action,
            source_turn_id=source_turn_id,
        ),
        "critics": active_critics,
        "revision_rounds": revision_rounds,
        "draft": draft,
        "final_text": final_text,
        "final_assessment": final_assessment,
        "timeline": timeline,
    }
    assistant_turn = repository.add_chat_turn(
        session,
        session_id=state.session_id,
        role="assistant",
        content=final_text,
        trace_json=trace,
    )
    done_payload = {
        **final_payload,
        "assistant_turn_id": assistant_turn.id,
        "baseline_source": analysis_bundle.source,
        "analysis_run_id": (analysis_bundle.analysis_summary or {}).get("run_id"),
        "review_count": len(active_critics),
        "generation_packet": analysis_bundle.generation_packet.get("baseline", {}),
        "final_assessment": final_assessment,
    }
    self._emit(state, "done", done_payload)


def _run_llm_first_pipeline_v3(
    self: WritingAgentService,
    session,
    state: WritingStreamState,
    *,
    analysis_bundle: StoneWritingAnalysisBundle,
    client: OpenAICompatibleClient,
) -> None:
    revision_rounds: list[dict[str, Any]] = []

    self._emit_live_writer_message(
        state,
        message_kind="request_adapter_v3",
        label="请求适配",
        body="正在把题目翻译进作者的价值镜头、自我距离和起笔方式……",
        stage="request_adapter_v3",
        stream_key=self._stream_key(state, "request_adapter_v3"),
    )
    request_adapter_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="request_adapter_v3",
        label="请求适配",
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的请求适配器。\n"
                    "请先把写作需求翻译进作者自己的世界，再进入起草。\n"
                    "不要为了填满字段而使用空泛、安全、通用的模板词。\n"
                    "如果某个关键字段没有足够证据，请尽量回到 author_model 与 corpus 先验，而不是发明一个漂亮但无根的说法。\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='desired_length_band, surface_form, anchor_preferences')}\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"author_model_v3 JSON：\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"writing_guide JSON：\n{json.dumps(analysis_bundle.writing_guide, ensure_ascii=False, indent=2)}\n\n"
                    f"profile_index JSON：\n{json.dumps(analysis_bundle.profile_index, ensure_ascii=False, indent=2)}\n\n"
                    f"prototype_index_v3.selection_guides JSON：\n{json.dumps(analysis_bundle.prototype_index.get('selection_guides') or {}, ensure_ascii=False, indent=2)}\n\n"
                    f"prototype_families JSON:\n{json.dumps((analysis_bundle.author_model.get('family_map') or [])[:8], ensure_ascii=False, indent=2)}\n\n"
                    "除固定枚举字段外，其余自然语言字段请用简体中文填写。\n\n"
                    "返回 JSON：\n"
                    "{\n"
                    '  "desired_length_band": "micro|short|medium|long",\n'
                    '  "surface_form": "scene_vignette|rant|confession|anecdote|aphorism|dialogue_bit|manifesto|list_bit",\n'
                    '  "value_lens": "作者的价值镜头，简体中文",\n'
                    '  "judgment_mode": "作者的判断方式，简体中文",\n'
                    '  "distance": "作者与对象的距离感，简体中文",\n'
                    '  "entry_scene": "适合起笔的进入方式，简体中文",\n'
                    '  "felt_cost": "题目应落到什么体感代价上，简体中文",\n'
                    '  "query_terms": ["检索词，优先简体中文"],\n'
                    '  "motif_terms": ["需要召回的母题，优先简体中文"],\n'
                    '  "anchor_preferences": ["opening|pivot|closing|signature"],\n'
                    '  "hard_constraints": ["硬约束，简体中文"],\n'
                    '  "reasoning": "简短理由，简体中文"\n'
                    "}"
                ),
            },
        ],
    )
    request_adapter = _normalize_request_adapter_v3(request_adapter_raw, state, analysis_bundle)
    request_adapter_payload = _build_writer_message_payload(
        message_kind="request_adapter_v3",
        label="请求适配",
        body=_render_request_adapter_v3(request_adapter),
        detail=request_adapter,
        stage="request_adapter_v3",
        stream_key=self._stream_key(state, "request_adapter_v3"),
    )
    self._emit(state, "stage", request_adapter_payload)

    self._emit_live_writer_message(
        state,
        message_kind="profile_selection_v3",
        label="画像取证",
        body="正在按题目、母题和价值镜头动态抽取最相关的逐篇画像切片……",
        stage="profile_selection_v3",
        stream_key=self._stream_key(state, "profile_selection_v3"),
    )
    profile_selection = _select_profile_slices_for_request_v3(bundle=analysis_bundle, request_adapter=request_adapter)
    profile_selection_payload = _build_writer_message_payload(
        message_kind="profile_selection_v3",
        label="画像取证",
        body=_render_profile_selection_v3(profile_selection),
        detail=profile_selection,
        stage="profile_selection_v3",
        stream_key=self._stream_key(state, "profile_selection_v3"),
    )
    self._emit(state, "stage", profile_selection_payload)

    self._emit_live_writer_message(
        state,
        message_kind="candidate_shortlist_v3",
        label="候选切片",
        body="正在根据原型家族、锚点覆盖和题目重心筛选候选材料……",
        stage="candidate_shortlist_v3",
        stream_key=self._stream_key(state, "candidate_shortlist_v3"),
    )
    shortlist = _build_candidate_shortlist_v3(
        analysis_bundle,
        request_adapter,
        profile_selection=profile_selection,
    )
    if not shortlist.get("documents"):
        raise WritingPipelineError("candidate_shortlist_v3", "No prototype candidates were available for v3 reranking.")
    shortlist_payload = _build_writer_message_payload(
        message_kind="candidate_shortlist_v3",
        label="候选切片",
        body=_render_candidate_shortlist_v3(shortlist),
        detail=shortlist,
        stage="candidate_shortlist_v3",
        stream_key=self._stream_key(state, "candidate_shortlist_v3"),
    )
    self._emit(state, "stage", shortlist_payload)

    self._emit_live_writer_message(
        state,
        message_kind="llm_rerank_v3",
        label="证据重排",
        body="正在结合 v3 作者模型和证据预算，对 shortlist 做最终重排……",
        stage="llm_rerank_v3",
        stream_key=self._stream_key(state, "llm_rerank_v3"),
    )
    rerank_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="llm_rerank_v3",
        label="证据重排",
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的重排器。\n"
                    "请从 shortlist 中挑出最终示例文档和 anchor ids。\n"
                    "shortlist 只是为了控制 prompt 体积，你的选择就是最终落地集合。\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='selected_documents, anchor_ids')}\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"analysis_summary_compact JSON：\n{json.dumps(_compact_analysis_summary_for_prompt_v3(analysis_bundle.analysis_summary), ensure_ascii=False, indent=2)}\n\n"
                    f"profile_index JSON：\n{json.dumps(analysis_bundle.profile_index, ensure_ascii=False, indent=2)}\n\n"
                    f"profile_selection_v3 JSON：\n{json.dumps(profile_selection, ensure_ascii=False, indent=2)}\n\n"
                    f"profile_slices JSON：\n{json.dumps((profile_selection.get('profile_slices') or [])[:18], ensure_ascii=False, indent=2)}\n\n"
                    f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"author_model_v3 JSON：\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"prototype_index_v3.retrieval_policy JSON：\n{json.dumps(analysis_bundle.prototype_index.get('retrieval_policy') or {}, ensure_ascii=False, indent=2)}\n\n"
                    f"candidate_shortlist_v3 JSON：\n{json.dumps(_compact_shortlist_for_prompt_v3(shortlist), ensure_ascii=False, indent=2)}\n\n"
                    "除 document id 与 anchor id 外，其余说明请用简体中文填写。\n\n"
                    "返回 JSON：\n"
                    "{\n"
                    '  "selected_documents": ["document ids"],\n'
                    '  "anchor_ids": ["anchor ids"],\n'
                    '  "selection_reason": "简短选择理由，简体中文",\n'
                    '  "rerank_notes": ["补充说明，简体中文"]\n'
                    "}"
                ),
            },
        ],
    )
    rerank = _normalize_rerank_v3(rerank_raw, analysis_bundle, shortlist)
    rerank_payload = _build_writer_message_payload(
        message_kind="llm_rerank_v3",
        label="证据重排",
        body=_render_rerank_v3(rerank),
        detail=rerank,
        stage="llm_rerank_v3",
        stream_key=self._stream_key(state, "llm_rerank_v3"),
    )
    self._emit(state, "stage", rerank_payload)

    self._emit_live_writer_message(
        state,
        message_kind="writing_packet_v3",
        label="写作包",
        body="正在把已选证据、分析分面和画像切片压缩成一个可执行的写作包……",
        stage="writing_packet_v3",
        stream_key=self._stream_key(state, "writing_packet_v3"),
    )
    writing_packet_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="writing_packet_v3",
        label="写作包",
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的写作包组装器。\n"
                    "请把分析运行、采样画像和已选示例收束成一个紧凑可执行的 writing packet。\n"
                    "关键字段必须尽量锚定在题目相关的画像切片、分析分面和选中锚点上，不要把 packet 压成任何题目都能套用的模板。\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='family_labels, anchor_ids, selected_profile_ids')}\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"analysis_summary_compact JSON：\n{json.dumps(_compact_analysis_summary_for_prompt_v3(analysis_bundle.analysis_summary), ensure_ascii=False, indent=2)}\n\n"
                    f"profile_index JSON：\n{json.dumps(analysis_bundle.profile_index, ensure_ascii=False, indent=2)}\n\n"
                    f"profile_selection_v3 JSON：\n{json.dumps(profile_selection, ensure_ascii=False, indent=2)}\n\n"
                    f"profile_slices JSON：\n{json.dumps((profile_selection.get('profile_slices') or [])[:18], ensure_ascii=False, indent=2)}\n\n"
                    f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"llm_rerank_v3 JSON:\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                    f"analysis_facet_packets JSON：\n{json.dumps(_compact_analysis_summary_for_prompt_v3(analysis_bundle.analysis_summary).get('facet_packets') or [], ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    f"author_model_v3 JSON：\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    "除 `family_labels`、`anchor_ids` 与 `selected_profile_ids` 外，其余自然语言字段请用简体中文填写。\n\n"
                    "返回 JSON：\n"
                    "{\n"
                    '  "entry_scene": "起笔方向，简体中文",\n'
                    '  "felt_cost": "体感代价，简体中文",\n'
                    '  "value_lens": "价值镜头，简体中文",\n'
                    '  "judgment_mode": "判断方式，简体中文",\n'
                    '  "distance": "作者距离感，简体中文",\n'
                    '  "family_labels": ["family labels"],\n'
                    '  "anchor_ids": ["anchor ids"],\n'
                    '  "selected_profile_ids": ["profile ids"],\n'
                    '  "lexicon_keep": ["需要保留的词，简体中文"],\n'
                    '  "motif_obligations": ["必须落地的母题，简体中文"],\n'
                    '  "syntax_rules": ["句法规则，简体中文"],\n'
                    '  "structure_recipe": ["结构步骤，简体中文"],\n'
                    '  "do_not_do": ["必须避开的漂移，简体中文"],\n'
                    '  "style_thesis": "风格总纲，简体中文"\n'
                    "}"
                ),
            },
        ],
    )
    writing_packet = _normalize_writing_packet_v3(
        writing_packet_raw,
        bundle=analysis_bundle,
        request_adapter=request_adapter,
        rerank=rerank,
        profile_selection=profile_selection,
    )
    writing_packet_payload = _build_writer_message_payload(
        message_kind="writing_packet_v3",
        label="写作包",
        body=_render_writing_packet_v3(writing_packet),
        detail=writing_packet,
        stage="writing_packet_v3",
        stream_key=self._stream_key(state, "writing_packet_v3"),
    )
    self._emit(state, "stage", writing_packet_payload)

    packet_critic_rounds: list[dict[str, Any]] = []
    packet_critic = _review_packet_v3(
        self,
        state,
        analysis_bundle,
        request_adapter,
        rerank,
        writing_packet,
        profile_selection,
        client,
    )
    packet_critic_rounds.append(packet_critic)
    packet_critic_payload = _build_packet_critic_message_payload_v3(
        packet_critic,
        stream_key=self._stream_key(state, "packet_critic", suffix="round_1"),
    )
    self._emit(state, "stage", packet_critic_payload)

    packet_critic_payloads = [packet_critic_payload]
    if packet_critic.get("verdict") == "rebuild_packet":
        self._emit_live_writer_message(
            state,
            message_kind="writing_packet_v3",
            label="写作包修正",
            body="写作包审判认为当前 packet 过于模板化，正在依据反馈重建写作包……",
            stage="writing_packet_v3",
            stream_key=self._stream_key(state, "writing_packet_v3", suffix="repair_round_1"),
        )
        repaired_packet_raw = _call_writer_json_stage_v3(
            self,
            state,
            client,
            stage="writing_packet_v3",
            label="写作包修正",
            stream_suffix="repair_round_1",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的写作包组装器。\n"
                        "这是一轮写作包修正。你必须根据 packet_critic 的反馈，去掉模板化和通用兜底，把请求真正贴回作者证据。\n"
                        f"{_stone_json_chinese_instruction(preserve_tokens='family_labels, anchor_ids, selected_profile_ids')}\n"
                        "只返回 JSON。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"packet_critic JSON：\n{json.dumps(packet_critic, ensure_ascii=False, indent=2)}\n\n"
                        f"profile_selection_v3 JSON：\n{json.dumps(profile_selection, ensure_ascii=False, indent=2)}\n\n"
                        f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        f"llm_rerank_v3 JSON：\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                        f"上一版 writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                        f"author_model_v3 JSON：\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                        "请优先修正关键字段缺证据、过度泛化和模板化的问题。"
                    ),
                },
            ],
        )
        writing_packet = _normalize_writing_packet_v3(
            repaired_packet_raw,
            bundle=analysis_bundle,
            request_adapter=request_adapter,
            rerank=rerank,
            profile_selection=profile_selection,
        )
        writing_packet_payload = _build_writer_message_payload(
            message_kind="writing_packet_v3",
            label="写作包修正",
            body=_render_writing_packet_v3(writing_packet),
            detail=writing_packet,
            stage="writing_packet_v3",
            stream_key=self._stream_key(state, "writing_packet_v3", suffix="repair_round_1"),
        )
        self._emit(state, "stage", writing_packet_payload)
        packet_critic = _review_packet_v3(
            self,
            state,
            analysis_bundle,
            request_adapter,
            rerank,
            writing_packet,
            profile_selection,
            client,
            round_index=2,
        )
        packet_critic_rounds.append(packet_critic)
        packet_critic_payload = _build_packet_critic_message_payload_v3(
            packet_critic,
            stream_key=self._stream_key(state, "packet_critic", suffix="round_2"),
            label="写作包审判 第 2 轮",
        )
        packet_critic_payloads.append(packet_critic_payload)
        self._emit(state, "stage", packet_critic_payload)

    self._emit_live_writer_message(
        state,
        message_kind="blueprint_v3",
        label="蓝图规划",
        body="正在把写作包变成可执行的文章蓝图……",
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    blueprint_raw = _call_writer_json_stage_v3(
        self,
        state,
        client,
        stage="blueprint_v3",
        label="蓝图规划",
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的蓝图规划器。\n"
                    "这里只做蓝图，不要写正文。\n"
                    f"{_stone_json_chinese_instruction(preserve_tokens='anchor_ids, axis_keys')}\n"
                    "只返回 JSON。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    "除 `anchor_ids` 和 `axis_keys` 外，其余字段请用简体中文填写。\n\n"
                    "返回 JSON：\n"
                    "{\n"
                    '  "paragraph_count": number,\n'
                    '  "shape_note": "整体形状，简体中文",\n'
                    '  "entry_move": "如何进入，简体中文",\n'
                    '  "development_move": "压力如何推进，简体中文",\n'
                    '  "turning_device": "转折装置或无，简体中文",\n'
                    '  "closure_residue": "如何留下余味，简体中文",\n'
                    '  "keep_terms": ["需要保留的词，简体中文"],\n'
                    '  "motif_obligations": ["母题要求，简体中文"],\n'
                    '  "steps": ["有序步骤，简体中文"],\n'
                    '  "do_not_do": ["必须避免的漂移，简体中文"],\n'
                    '  "axis_map": {"facet_key": {"goal": "该轴的目标，简体中文", "paragraph_hint": number, "anchor_ids": ["anchor ids"]}},\n'
                    '  "paragraph_map": [{"paragraph_index": number, "role": "opening|development|closing", "objective": "该段目标，简体中文", "axis_keys": ["axis keys"], "anchor_ids": ["anchor ids"]}],\n'
                    '  "anchor_ids": ["anchor ids"]\n'
                    "}"
                ),
            },
        ],
    )
    blueprint = _normalize_blueprint_packet_v3(blueprint_raw, state, writing_packet)
    blueprint_payload = _build_writer_message_payload(
        message_kind="blueprint_v3",
        label="蓝图规划",
        body=_render_blueprint_v3(blueprint),
        detail=blueprint,
        stage="blueprint_v3",
        stream_key=self._stream_key(state, "blueprint_v3"),
    )
    self._emit(state, "stage", blueprint_payload)

    self._emit_live_writer_message(
        state,
        message_kind="draft_v3",
        label="正文起草",
        body="正在基于 v3 packet 起草第一版正文……",
        stage="draft_v3",
        stream_key=self._stream_key(state, "draft_v3"),
    )
    draft = _call_writer_text_stage_v3(
        self,
        state,
        client,
        stage="draft_v3",
        label="正文起草",
        temperature=0.42,
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 Stone v3 的正文起草器。\n"
                    "只写文章正文。\n"
                    "所选锚点和写作包都是强绑定证据。\n"
                    f"{_STONE_BODY_CHINESE_ONLY}\n"
                    "不要暴露任何幕后提示词或分析术语。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                    f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                    f"llm_rerank_v3 JSON:\n{json.dumps(rerank, ensure_ascii=False, indent=2)}\n\n"
                    f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                    f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                    f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}\n\n"
                    f"author_floor JSON:\n{json.dumps(_build_v3_author_floor(analysis_bundle), ensure_ascii=False, indent=2)}\n\n"
                    f"draft_guardrails JSON：\n{json.dumps(_build_v3_draft_guardrails(writing_packet, blueprint), ensure_ascii=False, indent=2)}"
                ),
            },
        ],
    )
    draft_payload = _build_writer_message_payload(
        message_kind="draft_v3",
        label="正文起草",
        body=draft,
        detail={"word_count": estimate_word_count(draft)},
        stage="draft_v3",
        stream_key=self._stream_key(state, "draft_v3"),
    )
    self._emit(state, "stage", draft_payload)

    critics = _run_v3_critics(
        self,
        state,
        analysis_bundle,
        draft,
        request_adapter,
        rerank,
        writing_packet,
        blueprint,
        client,
    )
    critic_messages = [
        _build_critic_message_payload_v2(
            critic,
            stream_key=self._stream_key(state, critic["critic_key"], suffix=critic["critic_key"]),
            stage=critic["critic_key"],
        )
        for critic in critics
    ]
    for payload in critic_messages:
        self._emit(state, "stage", payload)

    revision_payloads: list[dict[str, Any]] = []
    current_text = draft
    active_critics = critics
    revision_action = _revision_action_v3(critics)
    if revision_action != "none":
        revision_rounds.append(
            {
                "round": 1,
                "stage": "critic_round_1",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": critics,
            }
        )
    if revision_action == "redraft":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="redraft",
            label="整篇重写",
            temperature=0.4,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的重写器。\n"
                        "请丢开这版较弱草稿，并基于同一个 v3 packet 重写正文。\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "critic 反馈只用于防止跑偏。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"request_adapter_v3 JSON：\n{json.dumps(request_adapter, ensure_ascii=False, indent=2)}\n\n"
                        f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                        f"critic 反馈 JSON：\n{json.dumps(critics, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        redraft_payload = _build_writer_message_payload(
            message_kind="redraft",
            label="整篇重写",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft"},
            stage="redraft",
            stream_key=self._stream_key(state, "redraft"),
        )
        revision_payloads.append(redraft_payload)
        self._emit(state, "stage", redraft_payload)
    elif revision_action == "line_edit":
        current_text = _call_writer_text_stage_v3(
            self,
            state,
            client,
            stage="line_edit",
            label="逐句修订",
            temperature=0.15,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Stone v3 的逐句修订器。\n"
                        "请保住这篇文章的结构骨架和收尾气压。\n"
                        f"{_STONE_BODY_CHINESE_ONLY}\n"
                        "只修改发虚、拼贴感过重或明显跑偏的句子。"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                        f"当前草稿：\n{current_text}\n\n"
                        f"line_edit_brief JSON：\n{json.dumps(_build_v3_line_edit_brief(current_text, critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                    f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                        f"selected_anchors JSON：\n{json.dumps(_selected_anchor_records_v3(analysis_bundle, rerank), ensure_ascii=False, indent=2)}"
                    ),
                },
            ],
        )
        line_edit_payload = _build_writer_message_payload(
            message_kind="line_edit",
            label="逐句修订",
            body=current_text,
            detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit"},
            stage="line_edit",
            stream_key=self._stream_key(state, "line_edit"),
        )
        revision_payloads.append(line_edit_payload)
        self._emit(state, "stage", line_edit_payload)

    if revision_action != "none" and _should_run_second_critic_round_v2(active_critics):
        round_two_critics = _run_v3_critics(
            self,
            state,
            analysis_bundle,
            current_text,
            request_adapter,
            rerank,
            writing_packet,
            blueprint,
            client,
            round_index=2,
        )
        round_two_messages = [
            _build_critic_message_payload_v2(
                critic,
                stream_key=self._stream_key(state, f"{critic['critic_key']}_round_2", suffix=critic["critic_key"]),
                stage=critic["critic_key"],
                label_suffix=" 第 2 轮",
            )
            for critic in round_two_critics
        ]
        for payload in round_two_messages:
            self._emit(state, "stage", payload)
        revision_rounds.append(
            {
                "round": 2,
                "stage": "critic_round_2",
                "revision_action": revision_action,
                "word_count": estimate_word_count(current_text),
                "critics": round_two_critics,
            }
        )
        active_critics = round_two_critics
        second_action = _revision_action_v3(round_two_critics)
        if second_action == "redraft":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="redraft",
                label="整篇重写 第 2 轮",
                temperature=0.38,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v3 的重写器。\n"
                            "只返回文章正文。\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"写作请求：\n{render_writing_request(state.topic, state.target_word_count, state.extra_requirements)}\n\n"
                            f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}\n\n"
                            f"blueprint_v3 JSON：\n{json.dumps(blueprint, ensure_ascii=False, indent=2)}\n\n"
                            f"critic 反馈 JSON：\n{json.dumps(round_two_critics, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="redraft",
                label="整篇重写 第 2 轮",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_redraft_round_2"},
                stage="redraft",
                stream_key=self._stream_key(state, "redraft_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "redraft"
        elif second_action == "line_edit":
            current_text = _call_writer_text_stage_v3(
                self,
                state,
                client,
                stage="line_edit",
                label="逐句修订 第 2 轮",
                temperature=0.14,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "你是 Stone v3 的逐句修订器。\n"
                            "只返回文章正文。\n"
                            f"{_STONE_BODY_CHINESE_ONLY}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"当前草稿：\n{current_text}\n\n"
                            f"line_edit_brief JSON：\n{json.dumps(_build_v3_line_edit_brief(current_text, round_two_critics, blueprint, state.target_word_count), ensure_ascii=False, indent=2)}\n\n"
                            f"writing_packet_v3 JSON：\n{json.dumps(writing_packet, ensure_ascii=False, indent=2)}"
                        ),
                    },
                ],
            )
            payload = _build_writer_message_payload(
                message_kind="line_edit",
                label="逐句修订 第 2 轮",
                body=current_text,
                detail={"word_count": estimate_word_count(current_text), "reason": "critic_line_edit_round_2"},
                stage="line_edit",
                stream_key=self._stream_key(state, "line_edit_round_2"),
            )
            revision_payloads.append(payload)
            self._emit(state, "stage", payload)
            revision_action = "line_edit"
        else:
            revision_action = second_action
    final_text = current_text
    final_assessment = _build_final_assessment_v2(
        final_text,
        active_critics,
        state.topic,
        state.target_word_count,
        revision_action=revision_action,
    )
    final_payload = _build_writer_message_payload(
        message_kind="final",
        label="最终成稿",
        body=final_text,
        detail={"word_count": estimate_word_count(final_text), "final_assessment": final_assessment},
        stage="final",
        stream_key=self._stream_key(state, "final"),
    )
    self._emit(state, "stage", final_payload)

    timeline = [
        request_adapter_payload,
        profile_selection_payload,
        shortlist_payload,
        rerank_payload,
        writing_packet_payload,
        *packet_critic_payloads,
        blueprint_payload,
        draft_payload,
        *critic_messages,
        *revision_payloads,
        final_payload,
    ]
    if revision_rounds and "round_two_messages" in locals():
        timeline = [
            request_adapter_payload,
            profile_selection_payload,
            shortlist_payload,
            rerank_payload,
            writing_packet_payload,
            *packet_critic_payloads,
            blueprint_payload,
            draft_payload,
            *critic_messages,
            *round_two_messages,
            *revision_payloads,
            final_payload,
        ]
    trace = {
        "kind": "writing_result",
        "status": "completed",
        "degraded_mode": False,
        "degradation_reasons": [],
        "revision_mode": False,
        "request_mode": state.request_mode or "draft",
        "topic": state.topic,
        "target_word_count": state.target_word_count,
        "extra_requirements": state.extra_requirements,
        "raw_message": state.raw_message,
        "baseline_source": analysis_bundle.source,
        "preprocess_run_id": analysis_bundle.run_id,
        "analysis_run_id": (analysis_bundle.analysis_summary or {}).get("run_id"),
        "analysis_version": analysis_bundle.version_label,
        "analysis_target_role": analysis_bundle.target_role,
        "analysis_context": analysis_bundle.analysis_context,
        "analysis_facets": list((analysis_bundle.analysis_summary or {}).get("facet_packets") or []),
        "coverage_warnings": list(writing_packet.get("coverage_warnings") or [])[:10],
        "axis_source_map": dict(writing_packet.get("axis_source_map") or {}),
        "generation_packet": analysis_bundle.generation_packet,
        "request_adapter_v3": request_adapter,
        "profile_selection_v3": profile_selection,
        "candidate_shortlist_v3": shortlist,
        "llm_rerank_v3": rerank,
        "writing_packet_v3": writing_packet,
        "packet_critic_v3": packet_critic_rounds[-1] if packet_critic_rounds else {},
        "packet_critic_rounds": packet_critic_rounds,
        "blueprint_v3": blueprint,
        "anchor_ids": _collect_v3_trace_anchor_ids(
            analysis_bundle,
            rerank,
            writing_packet,
            blueprint,
            revision_rounds,
        ),
        "blocks": _build_trace_blocks_v3(
            analysis_bundle,
            request_adapter,
            profile_selection,
            shortlist,
            rerank,
            writing_packet,
            packet_critic_rounds,
            blueprint,
            revision_rounds,
            revision_action,
        ),
        "critics": active_critics,
        "revision_rounds": revision_rounds,
        "draft": draft,
        "final_text": final_text,
        "final_assessment": final_assessment,
        "timeline": timeline,
    }
    assistant_turn = repository.add_chat_turn(
        session,
        session_id=state.session_id,
        role="assistant",
        content=final_text,
        trace_json=trace,
    )
    done_payload = {
        **final_payload,
        "assistant_turn_id": assistant_turn.id,
        "baseline_source": analysis_bundle.source,
        "analysis_run_id": (analysis_bundle.analysis_summary or {}).get("run_id"),
        "review_count": len(active_critics),
        "generation_packet": analysis_bundle.generation_packet.get("baseline", {}),
        "final_assessment": final_assessment,
    }
    self._emit(state, "done", done_payload)


def _run_turn_v3(self: WritingAgentService, session, state: WritingStreamState) -> None:
    self._ensure_stream_active(state)
    project = repository.get_project(session, state.project_id)
    if not project:
        raise ValueError("Project not found.")
    if project.mode != "stone":
        raise ValueError("Only stone projects can use the writing workspace.")

    self._emit_live_writer_message(
        state,
        message_kind="generation_packet",
        label="基线装载",
        body="正在读取 Stone v3 基线、分析结果和运行时资产……",
        stage="generation_packet",
        stream_key=self._stream_key(state, "generation_packet"),
    )
    analysis_bundle = _resolve_analysis_bundle_v3(self, session, state.project_id)
    baseline = dict(analysis_bundle.generation_packet.get("baseline") or {})
    label = "已加载 Stone v3 基线：预处理、作者模型与原型索引均可用"
    self._emit(
        state,
        "status",
        {
            "stage": "generation_packet",
            "label": label,
            "baseline_source": analysis_bundle.source,
            "preprocess_run_id": analysis_bundle.run_id,
            "analysis_run_id": (analysis_bundle.analysis_summary or {}).get("run_id"),
            "analysis_version": analysis_bundle.version_label,
            "analysis_target_role": analysis_bundle.target_role,
            "baseline_components": baseline,
        },
    )
    self._emit_live_writer_message(
        state,
        message_kind="generation_packet",
        label="基线装载",
        body=(
            f"{label}\n"
            f"逐篇画像 {len(analysis_bundle.stone_profiles)} | 采样切片 {len(analysis_bundle.profile_slices)}\n"
            f"分析运行 {(analysis_bundle.analysis_summary or {}).get('run_id') or '缺失'} + 作者模型 v3 + 原型索引 v3"
        ),
        detail=baseline,
        stage="generation_packet",
        stream_key=self._stream_key(state, "generation_packet"),
        stream_state="complete",
    )

    config = repository.get_service_config(session, "chat_service")
    client = self._build_client(config)
    if not client:
        raise WritingPipelineError("generation_packet", "Writing model is not configured.")

    if state.request_mode == "revision" and state.revision_source_turn_id:
        _run_revision_pipeline_v3(
            self,
            session,
            state,
            analysis_bundle=analysis_bundle,
            client=client,
        )
        return

    _run_llm_first_pipeline_v3(
        self,
        session,
        state,
        analysis_bundle=analysis_bundle,
        client=client,
    )

run_turn_v3 = _run_turn_v3

__all__ = [
    "_run_revision_pipeline_v3",
    "_run_llm_first_pipeline_v3",
    "_run_turn_v3",
    "run_turn_v3",
]
