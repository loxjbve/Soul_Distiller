from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from app.core.config import AppConfig, default_config
from app.db import Database
from app.models import utcnow
from app.retrieval.service import RetrievalService
from app.retrieval.vector_store import VectorStoreManager
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.service import AppServices, ServiceRegistry
from app.service.common.jobs.analysis_runner import AnalysisTaskRunner
from app.service.common.pipeline.analysis_runtime import AnalysisEngine
from app.service.common.pipeline.asset_runtime import AssetSynthesizer
from app.service.common.pipeline.ingest import DocumentIngestService
from app.service.common.pipeline.ingest_task import IngestTaskManager
from app.service.common.pipeline.project_deletion import ProjectDeletionManager
from app.service.common.pipeline.preprocess_runtime import PreprocessAgentService
from app.service.common.pipeline.rechunk import RechunkTaskManager
from app.service.common.pipeline.stone_assets_runtime import StoneV3BaselineSynthesizer
from app.service.common.pipeline.stone_preprocess_runtime import StonePreprocessStreamHub, StonePreprocessWorker
from app.service.common.pipeline.stone_writing_runtime import WritingAgentService
from app.service.common.pipeline.telegram_runtime import TelegramPreprocessManager
from app.service.common.streaming.analysis import AnalysisStreamHub
from app.service.group import GroupModePipeline
from app.service.single import SingleModePipeline
from app.service.stone import StoneModePipeline
from app.service.telegram import TelegramModePipeline
from app.storage import repository


def _recover_interrupted_analysis_runs(database: Database) -> None:
    with database.session() as session:
        active_runs = repository.list_active_analysis_runs(session)
        for run in active_runs:
            summary = dict(run.summary_json or {})
            summary["current_stage"] = "服务重启，旧的后台任务已终止"
            summary["current_facet"] = None
            summary["finished_at"] = utcnow().isoformat()
            run.summary_json = summary
            run.status = "failed"
            run.finished_at = utcnow()
            repository.add_analysis_event(
                session,
                run.id,
                event_type="lifecycle",
                level="warning",
                message="检测到服务重启，未完成的分析任务已被标记为失败。",
                payload_json={"recovered_after_restart": True},
            )


@dataclass(slots=True)
class AppContainer:
    config: AppConfig
    db: Database
    services: AppServices
    vector_store_manager: VectorStoreManager
    analysis_runner: AnalysisTaskRunner
    ingest_task_manager: IngestTaskManager
    rechunk_manager: RechunkTaskManager
    stone_pipeline: StoneModePipeline

    @classmethod
    def build(cls, config: AppConfig | None = None) -> "AppContainer":
        config = config or default_config()
        config.ensure_dirs()
        database = Database(config)
        database.create_all()

        vector_store_manager = VectorStoreManager(config.data_dir)
        retrieval = RetrievalService(vector_store=vector_store_manager)
        analysis_stream_hub = AnalysisStreamHub()
        telegram_preprocess_stream_hub = AnalysisStreamHub()
        stone_preprocess_stream_hub = StonePreprocessStreamHub()

        analysis_engine = AnalysisEngine(
            retrieval,
            db=database,
            llm_log_path=str(config.llm_log_path),
            use_processes=False,
            facet_max_workers=DEFAULT_ANALYSIS_CONCURRENCY,
            stream_hub=analysis_stream_hub,
        )
        analysis_runner = AnalysisTaskRunner(
            database,
            analysis_engine,
            max_workers=4,
            error_log_path=str(config.analysis_error_log_path),
        )
        ingest_service = DocumentIngestService(config)
        ingest_task_manager = IngestTaskManager(
            database,
            vector_store_manager,
            max_workers=4,
            llm_log_path=str(config.llm_log_path),
        )
        rechunk_manager = RechunkTaskManager(
            database,
            vector_store_manager,
            llm_log_path=str(config.llm_log_path),
            max_workers=1,
        )
        asset_synthesizer = AssetSynthesizer(log_path=str(config.llm_log_path))
        stone_v3_synthesizer = StoneV3BaselineSynthesizer(log_path=str(config.llm_log_path))
        preprocess_service = PreprocessAgentService(database, config, retrieval, max_workers=4)
        writing_service = WritingAgentService(database, config, max_workers=4)
        telegram_preprocess_manager = TelegramPreprocessManager(
            database,
            llm_log_path=str(config.llm_log_path),
            max_workers=2,
            stream_hub=telegram_preprocess_stream_hub,
        )
        stone_preprocess_worker = StonePreprocessWorker(
            database,
            stream_hub=stone_preprocess_stream_hub,
            llm_log_path=str(config.llm_log_path),
        )

        single_pipeline = SingleModePipeline(
            preprocess_service=preprocess_service,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            asset_synthesizer=asset_synthesizer,
        )
        group_pipeline = GroupModePipeline(
            preprocess_service=preprocess_service,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            asset_synthesizer=asset_synthesizer,
        )
        telegram_pipeline = TelegramModePipeline(
            preprocess_manager=telegram_preprocess_manager,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            asset_synthesizer=asset_synthesizer,
        )
        stone_pipeline = StoneModePipeline(
            preprocess_worker=stone_preprocess_worker,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            asset_synthesizer=stone_v3_synthesizer,
            writing_service=writing_service,
        )
        service_registry = ServiceRegistry.build(
            single=single_pipeline,
            group=group_pipeline,
            telegram=telegram_pipeline,
            stone=stone_pipeline,
        )
        project_deletion_manager = ProjectDeletionManager(
            db=database,
            config=config,
            vector_store_manager=vector_store_manager,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            analysis_runner=analysis_runner,
            service_registry=service_registry,
        )
        services = AppServices(
            registry=service_registry,
            retrieval=retrieval,
            vector_store_manager=vector_store_manager,
            analysis_stream_hub=analysis_stream_hub,
            telegram_preprocess_stream_hub=telegram_preprocess_stream_hub,
            stone_preprocess_stream_hub=stone_preprocess_stream_hub,
            analysis_runner=analysis_runner,
            ingest_service=ingest_service,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            project_deletion_manager=project_deletion_manager,
        )

        _recover_interrupted_analysis_runs(database)
        telegram_preprocess_manager.resume_interrupted_runs()
        project_deletion_manager.resume_pending_deletions()

        return cls(
            config=config,
            db=database,
            services=services,
            vector_store_manager=vector_store_manager,
            analysis_runner=analysis_runner,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            stone_pipeline=stone_pipeline,
        )

    def attach_to_app(self, app: FastAPI) -> None:
        app.state.config = self.config
        app.state.db = self.db
        app.state.services = self.services

    @asynccontextmanager
    async def lifespan(self, _: FastAPI):
        self.stone_pipeline.preprocess_worker.bind_loop(asyncio.get_running_loop())
        self.stone_pipeline.preprocess_worker.resume_interrupted_runs()
        try:
            yield
        finally:
            self.services.project_deletion_manager.shutdown()
            self.analysis_runner.shutdown()
            self.services.shutdown_mode_pipelines()
            await self.stone_pipeline.preprocess_worker.shutdown()
            self.rechunk_manager.shutdown()
            self.ingest_task_manager.shutdown()
            self.vector_store_manager.save_all()
            self.db.close()
