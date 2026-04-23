from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from sqlalchemy import func, select

from app.analysis.facets import get_facets_for_mode
from app.analysis.stone import normalize_stone_profile
from app.analysis.stone_agent import StoneAnalysisAgent
from app.llm.client import OpenAICompatibleClient
from app.models import TextChunk
from app.schemas import ChatCompletionResult, LLMToolCall, ToolRoundResult
from app.storage import repository


def _wait_for_ready(client, project_id: str, document_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    latest = {}
    while time.time() < deadline:
        latest = client.get(f"/api/projects/{project_id}/documents").json()
        for item in latest.get("documents", []):
            if item["id"] == document_id and item["ingest_status"] == "ready":
                return item
        time.sleep(0.1)
    raise AssertionError(f"document {document_id} did not become ready: {latest}")


def _wait_for_analysis(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/analysis", params={"run_id": run_id}).json()
    return payload


def _collect_sse_events(client, url: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    with client.stream("GET", url) as response:
        assert response.status_code == 200
        current_event: str | None = None
        data_lines: list[str] = []
        for raw_line in response.iter_lines():
            line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
            if line == "":
                if current_event is not None:
                    payload = json.loads("\n".join(data_lines)) if data_lines else {}
                    events.append((current_event, payload))
                current_event = None
                data_lines = []
                continue
            if line.startswith("event: "):
                current_event = line[7:]
            elif line.startswith("data: "):
                data_lines.append(line[6:])
    return events


def _ensure_service_config(app, service_name: str, *, model: str) -> None:
    with app.state.db.session() as session:
        repository.upsert_setting(
            session,
            service_name,
            {
                "provider_kind": "openai-compatible",
                "base_url": "https://example.com/v1",
                "api_key": "sk-test",
                "model": model,
                "api_mode": "responses",
            },
        )


def _count_document_chunks(app, document_id: str) -> int:
    with app.state.db.session() as session:
        return int(
            session.scalar(
                select(func.count()).select_from(TextChunk).where(TextChunk.document_id == document_id)
            )
            or 0
        )


def _seed_stone_analysis(app, project_id: str) -> None:
    facet_catalog = get_facets_for_mode("stone")
    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    storage_path = upload_dir / "seed-stone.txt"
    storage_path.write_text("夜里写字的人，总会回到代价、关系和沉默。", encoding="utf-8")

    with app.state.db.session() as session:
        repository.create_document(
            session,
            id=str(uuid4()),
            project_id=project_id,
            filename="seed-stone.txt",
            mime_type="text/plain",
            extension=".txt",
            source_type="essay",
            title="Seed Stone",
            author_guess="Author",
            created_at_guess=None,
            raw_text="夜里写字的人，总会回到代价、关系和沉默。",
            clean_text="夜里写字的人，总会回到代价、关系和沉默。",
            language="zh",
            metadata_json={
                "stone_profile": {
                    "content_summary": "夜里写字的人总会回到代价、关系和沉默这些母题。",
                    "content_type": "抽象",
                    "length_label": "短文",
                    "emotion_label": "不确定",
                    "selected_passages": ["夜里写字的人，总会回到代价、关系和沉默。"],
                }
            },
            ingest_status="ready",
            error_message=None,
            storage_path=str(storage_path),
        )
        run = repository.create_analysis_run(
            session,
            project_id,
            status="completed",
            summary_json={
                "facet_keys": [facet.key for facet in facet_catalog],
                "facet_labels": [facet.label for facet in facet_catalog],
                "target_role": "Author",
                "analysis_context": "stone corpus",
            },
        )
        for facet in facet_catalog:
            repository.upsert_facet(
                session,
                run.id,
                facet.key,
                status="completed",
                confidence=0.84,
                findings_json={
                    "label": facet.label,
                    "summary": f"{facet.label} summary",
                    "bullets": [f"{facet.key} bullet 1", f"{facet.key} bullet 2"],
                },
                evidence_json=[],
                conflicts_json=[],
                error_message=None,
            )


def _wait_for_stone_preprocess(client, project_id: str, run_id: str, *, timeout_s: float = 12.0) -> dict:
    deadline = time.time() + timeout_s
    payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    while payload["status"] in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)
        payload = client.get(f"/api/projects/{project_id}/preprocess/runs/{run_id}").json()
    return payload


def test_stone_mode_text_document_api_and_analysis_flow(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Project", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    home = client.get("/")
    assert home.status_code == 200
    assert 'value="stone"' in home.text

    project_page = client.get(f"/projects/{project_id}")
    assert project_page.status_code == 200
    assert 'id="upload-dropzone"' in project_page.text

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。",
            "source_type": "essay",
            "user_note": "first import",
        },
    )
    assert create_doc.status_code == 200
    document_payload = create_doc.json()
    document_id = document_payload["id"]
    assert document_payload["request_status"] == "ok"
    assert document_payload["source_type"] == "essay"
    assert document_payload["status"] in {"queued", "parsing", "chunking", "storing", "completed"}

    document_detail = _wait_for_ready(client, project_id, document_id)
    assert document_detail["ingest_status"] == "ready"
    assert document_detail["metadata_json"]["user_note"] == "first import"
    assert document_detail["metadata_json"]["stone_text_entry"] is True
    assert _count_document_chunks(app, document_id) == 0

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_run_id = preprocess_response.json()["id"]
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_run_id)
    assert preprocess_payload["status"] == "completed"
    detail_profile = preprocess_payload["documents"][0]["stone_profile"]
    assert set(detail_profile) == {
        "content_summary",
        "content_type",
        "length_label",
        "emotion_label",
        "selected_passages",
    }
    assert detail_profile["content_summary"]
    assert detail_profile["selected_passages"]

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "stone corpus", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    run_id = run_response.json()["id"]

    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"
    stone_keys = [facet.key for facet in get_facets_for_mode("stone")]
    assert analysis_payload["summary"]["facet_keys"] == stone_keys
    assert analysis_payload["summary"]["chunk_count"] == 0
    assert len(analysis_payload["facets"]) == len(stone_keys)

    refreshed_docs = client.get(f"/api/projects/{project_id}/documents").json()["documents"]
    profiled_doc = next(item for item in refreshed_docs if item["id"] == document_id)
    stone_profile = profiled_doc["metadata_json"]["stone_profile"]
    assert set(stone_profile) == {
        "content_summary",
        "content_type",
        "length_label",
        "emotion_label",
        "selected_passages",
    }
    assert stone_profile["content_summary"] == "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。"
    assert stone_profile["content_type"] == ""
    assert stone_profile["length_label"] == "短文"
    assert stone_profile["emotion_label"] == ""
    assert stone_profile["selected_passages"] == [
        "夜里写字的人，总会把白天没说完的话重新从沉默里拽出来，再慢慢摆回桌面。"
    ]


def test_stone_preprocess_form_route_redirects_and_completes(client):
    create_response = client.post("/api/projects", json={"name": "Stone Form", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "这是一段足够长的石川文风样本文本，用来验证表单入口触发预分析时会正确创建并完成后台任务。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    response = client.post(
        f"/projects/{project_id}/preprocess/run",
        data={"concurrency": "1"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    location = response.headers["location"]
    parsed = urlparse(location)
    assert parsed.path == f"/projects/{project_id}/preprocess"
    run_id = parse_qs(parsed.query)["run_id"][0]

    preprocess_payload = _wait_for_stone_preprocess(client, project_id, run_id)
    assert preprocess_payload["status"] == "completed"


def test_stone_preprocess_reuses_failed_run(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Resume", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "这段文本用于验证失败后的预分析 run 会被复用，而不是重新生成一条新的 run 记录。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    with app.state.db.session() as session:
        run = repository.create_stone_preprocess_run(
            session,
            project_id=project_id,
            status="failed",
            summary_json={
                "concurrency": 1,
                "stone_profile_total": 1,
                "stone_profile_completed": 0,
            },
        )
        run.progress_percent = 47
        run.current_stage = "Failed"
        run.error_message = "previous failure"
        failed_run_id = run.id

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    assert preprocess_response.json()["id"] == failed_run_id

    preprocess_payload = _wait_for_stone_preprocess(client, project_id, failed_run_id)
    assert preprocess_payload["status"] == "completed"
    assert preprocess_payload["progress_percent"] == 100


def test_stone_json_upload_splits_articles_and_filters_noise(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone JSON", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    payload = {
        "name": "Stone Feed",
        "messages": [
            {"id": 1, "type": "message", "text": "太短了，不算文章。"},
            {
                "id": 2,
                "type": "message",
                "text": "This is a long English only paragraph with more than fifty characters, but it should still be filtered out.",
            },
            {
                "id": 3,
                "type": "message",
                "text": "第一篇文章在这里展开。它有足够长的中文内容，会被系统识别成一篇完整文章，而且不需要再拆分成 chunks。",
            },
            {
                "id": 4,
                "type": "message",
                "text": [
                    "第二篇文章来自富文本数组，它同样有足够长的中文正文，",
                    {"type": "hashtag", "text": "#忽略这个标签"},
                    "并且应该被导入成一篇独立文章，而不是保留成原始 JSON 消息。",
                ],
            },
        ],
    }
    response = client.post(
        f"/api/projects/{project_id}/documents",
        files=[
            (
                "files",
                ("articles.json", json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json"),
            )
        ],
    )
    assert response.status_code == 200
    response_payload = response.json()
    assert len(response_payload["documents"]) == 2
    assert len(response_payload["tasks"]) == 2

    document_ids = [item["id"] for item in response_payload["documents"]]
    for document_id in document_ids:
        ready_document = _wait_for_ready(client, project_id, document_id)
        assert ready_document["ingest_status"] == "ready"
        assert ready_document["metadata_json"]["stone_json_import"]["source_filename"] == "articles.json"
        assert _count_document_chunks(app, document_id) == 0


def test_stone_analysis_agent_records_raw_text_tool_usage(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Agent", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    create_doc = client.post(
        f"/api/projects/{project_id}/documents/text",
        json={
            "content": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
            "source_type": "essay",
        },
    )
    assert create_doc.status_code == 200
    document_id = create_doc.json()["id"]
    _wait_for_ready(client, project_id, document_id)

    _ensure_service_config(app, "chat_service", model="demo-model")

    def fake_chat_completion_result(self, messages, **kwargs):
        payload = {
            "content_summary": "作者习惯先压低声调，再把情绪往句子深处回收。",
            "content_type": "自嘲式抱怨",
            "length_label": "短文",
            "emotion_label": "闷着难受",
            "selected_passages": [],
        }
        return ChatCompletionResult(
            content=json.dumps(payload, ensure_ascii=False),
            model="demo-model",
            usage={"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
            request_url="https://example.com/v1/responses",
            request_payload={"messages": messages},
        )

    def fake_tool_round(self, messages, tools, **kwargs):
        has_tool_result = any(message.get("role") == "tool" for message in messages)
        if not has_tool_result:
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
                tool_calls=[
                    LLMToolCall(
                        id="call-1",
                        name="read_article_text",
                        arguments_json=json.dumps({"document_id": document_id, "max_chars": 300}, ensure_ascii=False),
                        arguments={"document_id": document_id, "max_chars": 300},
                    )
                ],
                provider_response_id="resp-1",
            )
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "作者在这个维度上依赖压低声调、延迟释放和半开式收束。",
                    "bullets": [
                        "经常先把声调压低，再把核心情绪往后放。",
                        "句尾常常留一个没有完全封死的收口。",
                    ],
                    "confidence": 0.88,
                    "fewshots": [
                        {
                            "document_id": document_id,
                            "situation": "为了验证句尾收束方式，回读原文",
                            "expression": "先压低声调，再把情绪回收",
                            "quote": "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。",
                            "reason": "直接支持当前 facet 的判断",
                        }
                    ],
                    "conflicts": [],
                    "notes": "基于文章画像总结，并用原文回读做了核对。",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
            tool_calls=[],
            provider_response_id="resp-2",
        )

    monkeypatch.setattr(OpenAICompatibleClient, "chat_completion_result", fake_chat_completion_result)
    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    preprocess_response = client.post(f"/api/projects/{project_id}/preprocess/runs")
    assert preprocess_response.status_code == 200
    preprocess_run_id = preprocess_response.json()["id"]
    preprocess_payload = _wait_for_stone_preprocess(client, project_id, preprocess_run_id)
    assert preprocess_payload["status"] == "completed"
    assert preprocess_payload["prompt_tokens"] == 12
    assert preprocess_payload["completion_tokens"] == 8
    assert preprocess_payload["total_tokens"] == 20
    assert preprocess_payload["documents"][0]["stone_profile"]["content_summary"] == "作者习惯先压低声调，再把情绪往句子深处回收。"
    assert preprocess_payload["documents"][0]["stone_profile"]["content_type"] == "自嘲式抱怨"
    assert preprocess_payload["documents"][0]["stone_profile"]["emotion_label"] == "闷着难受"
    assert preprocess_payload["documents"][0]["stone_profile"]["selected_passages"] == [
        "作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。"
    ]

    run_response = client.post(
        f"/api/projects/{project_id}/analyze",
        json={"analysis_context": "tool-augmented stone run", "target_role": "Author"},
    )
    assert run_response.status_code == 200
    run_id = run_response.json()["id"]

    analysis_payload = _wait_for_analysis(client, project_id, run_id)
    assert analysis_payload["status"] == "completed"
    first_facet = analysis_payload["facets"][0]
    retrieval_trace = first_facet["findings"]["retrieval_trace"]
    assert retrieval_trace["mode"] == "stone_agent"
    assert retrieval_trace["queried_document_ids"] == [document_id]
    assert retrieval_trace["tool_calls"]

    refreshed_docs = client.get(f"/api/projects/{project_id}/documents").json()["documents"]
    stone_profile = refreshed_docs[0]["metadata_json"]["stone_profile"]
    assert set(stone_profile) == {
        "content_summary",
        "content_type",
        "length_label",
        "emotion_label",
        "selected_passages",
    }
    assert stone_profile["content_summary"] == "作者习惯先压低声调，再把情绪往句子深处回收。"
    assert stone_profile["content_type"] == "自嘲式抱怨"
    assert stone_profile["emotion_label"] == "闷着难受"
    assert stone_profile["selected_passages"] == ["作者总是先压低声调，再把情绪推回句子深处，最后留一个没有完全关上的收口。"]


def test_normalize_stone_profile_supports_raw_summary_sentinel_and_short_text_full_passage():
    article_text = "就这一句，但我今天确实有点不舒服。"
    profile = normalize_stone_profile(
        {
            "content_summary": "raw",
            "content_type": "随手抱怨",
            "length_label": "短文",
            "emotion_label": "轻微烦闷",
            "selected_passages": [],
        },
        article_text=article_text,
        fallback_title="短文测试",
    )
    assert profile["content_summary"] == article_text
    assert profile["content_type"] == "随手抱怨"
    assert profile["length_label"] == "短文"
    assert profile["emotion_label"] == "轻微烦闷"
    assert profile["selected_passages"] == [article_text]


def test_stone_project_detail_exposes_analysis_concurrency_input(client):
    create_response = client.post("/api/projects", json={"name": "Stone Console", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]

    response = client.get(f"/projects/{project_id}")
    assert response.status_code == 200
    assert 'textarea name="analysis_context"' in response.text
    assert 'input type="number" name="concurrency"' in response.text
    assert 'type="hidden" name="concurrency"' not in response.text


def test_stone_analysis_agent_starts_from_corpus_overview_and_pages_profiles(client, app, monkeypatch):
    create_response = client.post("/api/projects", json={"name": "Stone Paging", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _ensure_service_config(app, "chat_service", model="demo-model")

    upload_dir = app.state.config.upload_dir / project_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    with app.state.db.session() as session:
        for index in range(30):
            doc_id = str(uuid4())
            text = f"第{index}篇文章独有标记_{index}，作者在这里反复写深夜、疲惫和关系。"
            storage_path = upload_dir / f"paging-{index}.txt"
            storage_path.write_text(text, encoding="utf-8")
            emotion = "低落" if index < 18 else "轻松"
            content_type = "诉苦" if index < 18 else "分享"
            repository.create_document(
                session,
                id=doc_id,
                project_id=project_id,
                filename=f"paging-{index}.txt",
                mime_type="text/plain",
                extension=".txt",
                source_type="essay",
                title=f"第{index}篇文章",
                author_guess="Author",
                created_at_guess=None,
                raw_text=text,
                clean_text=text,
                language="zh",
                metadata_json={
                    "stone_profile": {
                        "content_summary": text,
                        "content_type": content_type,
                        "length_label": "短文",
                        "emotion_label": emotion,
                        "selected_passages": [text],
                    }
                },
                ingest_status="ready",
                error_message=None,
                storage_path=str(storage_path),
            )
        session.commit()

    captured: dict[str, object] = {}

    def fake_tool_round(self, messages, tools, **kwargs):
        has_tool_result = any(message.get("role") == "tool" for message in messages)
        if not has_tool_result:
            prompt_text = "\n\n".join(
                str(message.get("content") or "")
                for message in messages
                if message.get("role") in {"system", "user"}
            )
            captured["first_prompt"] = prompt_text
            assert "作品总数：30" in prompt_text
            assert "性质分布" in prompt_text
            assert "情绪分布" in prompt_text
            assert "独有标记_29" not in prompt_text
            assert "第29篇文章独有标记_29" not in prompt_text
            return ToolRoundResult(
                content="",
                model="demo-model",
                usage={"prompt_tokens": 20, "completion_tokens": 6, "total_tokens": 26},
                tool_calls=[
                    LLMToolCall(
                        id="call-page-1",
                        name="list_article_profiles_page",
                        arguments_json=json.dumps({"offset": 0, "limit": 6, "emotion_label": "低落"}, ensure_ascii=False),
                        arguments={"offset": 0, "limit": 6, "emotion_label": "低落"},
                    )
                ],
                provider_response_id="resp-page-1",
            )

        tool_message = next(message for message in messages if message.get("role") == "tool")
        page_payload = json.loads(tool_message["content"])
        captured["page_payload"] = page_payload
        assert page_payload["returned"] == 6
        assert page_payload["total_profiles"] == 18
        assert page_payload["has_more"] is True
        assert 0 <= int(page_payload["remaining_profile_budget"]) < page_payload["total_profiles"]
        serialized_page = json.dumps(page_payload, ensure_ascii=False)
        assert "独有标记_29" not in serialized_page

        first_profile = page_payload["profiles"][0]
        return ToolRoundResult(
            content=json.dumps(
                {
                    "summary": "作者在这一维度上主要表现为持续低落、自我消耗和深夜叙述。",
                    "bullets": [
                        "前几个分页样本集中指向疲惫、深夜和关系压力。",
                        "低落情绪在抽样页面中明显高频出现。",
                    ],
                    "confidence": 0.81,
                    "fewshots": [
                        {
                            "document_id": first_profile["document_id"],
                            "document_title": first_profile["title"],
                            "situation": "分页抽样时观察作者低落表达",
                            "expression": "把疲惫和关系压力直接写进短文",
                            "quote": first_profile["content_summary"],
                            "reason": "足以支撑当前 facet 的初步判断",
                        }
                    ],
                    "conflicts": [],
                    "notes": "先基于总体分布筛选低落样本，再按分页读取文章画像。",
                },
                ensure_ascii=False,
            ),
            model="demo-model",
            usage={"prompt_tokens": 18, "completion_tokens": 8, "total_tokens": 26},
            tool_calls=[],
            provider_response_id="resp-page-2",
        )

    monkeypatch.setattr(OpenAICompatibleClient, "tool_round", fake_tool_round)

    with app.state.db.session() as session:
        project = repository.get_project(session, project_id)
        assert project is not None
        chat_config = repository.get_service_config(session, "chat_service")
        assert chat_config is not None
        facet = get_facets_for_mode("stone")[0]
        agent = StoneAnalysisAgent(session, project, llm_config=chat_config)
        result = agent.analyze_facet(
            facet,
            target_role="Author",
            analysis_context="分页读取测试",
        )

    assert "作品总数：30" in str(captured["first_prompt"])
    assert result.retrieval_trace["tool_calls"][0]["tool"] == "list_article_profiles_page"
    assert result.retrieval_trace["corpus_overview"]["total_documents"] == 30
    assert result.payload["fewshots"][0]["document_id"] == captured["page_payload"]["profiles"][0]["document_id"]


def test_stone_writing_workspace_uses_latest_analysis_even_if_writing_guide_exists(client, app):
    create_response = client.post("/api/projects", json={"name": "Stone Writing", "mode": "stone"})
    assert create_response.status_code == 200
    project_id = create_response.json()["id"]
    _seed_stone_analysis(app, project_id)

    assets_page = client.get(f"/projects/{project_id}/assets")
    assert assets_page.status_code == 200
    assert "Writing Guide" in assets_page.text
    assert "Claude Code Skill" not in assets_page.text

    writing_page = client.get(f"/projects/{project_id}/writing")
    assert writing_page.status_code == 200
    assert "analysis" in writing_page.text
    assert "writing_guide" not in writing_page.text

    session_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Draft Session"},
    ).json()
    session_id = session_payload["id"]

    message_payload = client.post(
        f"/api/projects/{project_id}/writing/sessions/{session_id}/messages",
        json={"topic": "Rainy Night Station", "target_word_count": 600, "extra_requirements": "keep it restrained"},
    ).json()
    stream_id = message_payload["stream_id"]
    events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{session_id}/streams/{stream_id}",
    )
    event_names = [name for name, _payload in events]
    assert event_names.count("stage") >= len(get_facets_for_mode("stone")) + 3
    assert "done" in event_names

    detail_payload = client.get(f"/api/projects/{project_id}/writing/sessions/{session_id}").json()
    assistant_turns = [turn for turn in detail_payload["turns"] if turn["role"] == "assistant"]
    assert assistant_turns
    latest_turn = assistant_turns[-1]
    assert latest_turn["trace"]["baseline_source"] == "analysis_run"
    assert latest_turn["trace"]["analysis_facets"] == [facet.key for facet in get_facets_for_mode("stone")]
    assert len(latest_turn["trace"]["reviews"]) == len(get_facets_for_mode("stone"))
    assert latest_turn["trace"]["review_plan"]
    assert latest_turn["trace"]["final_assessment"]

    draft_response = client.post(f"/api/projects/{project_id}/assets/generate", json={"asset_kind": "writing_guide"})
    assert draft_response.status_code == 200
    draft_payload = draft_response.json()
    draft_id = draft_payload["id"]
    assert draft_payload["asset_kind"] == "writing_guide"

    publish_response = client.post(
        f"/api/projects/{project_id}/assets/{draft_id}/publish",
        json={"asset_kind": "writing_guide"},
    )
    assert publish_response.status_code == 200
    assert publish_response.json()["asset_kind"] == "writing_guide"

    published_session = client.post(
        f"/api/projects/{project_id}/writing/sessions",
        json={"title": "Published Session"},
    ).json()
    published_session_id = published_session["id"]
    published_message = client.post(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/messages",
        json={"topic": "Window Before Dawn", "target_word_count": 550, "extra_requirements": "leave some aftertaste"},
    ).json()
    published_events = _collect_sse_events(
        client,
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}/streams/{published_message['stream_id']}",
    )
    assert "done" in [name for name, _payload in published_events]

    published_detail = client.get(
        f"/api/projects/{project_id}/writing/sessions/{published_session_id}"
    ).json()
    published_assistant_turns = [turn for turn in published_detail["turns"] if turn["role"] == "assistant"]
    assert published_assistant_turns[-1]["trace"]["baseline_source"] == "analysis_run"
    assert len(published_assistant_turns[-1]["trace"]["reviews"]) == len(get_facets_for_mode("stone"))
