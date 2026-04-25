from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from app.service import ServiceRegistry
from app.service.registry import AppServices
from app.service.common.subagents.runner import AgentOrchestrator as PreprocessAgentOrchestrator
from app.service.stone.subagent_runner import StoneAgentOrchestrator
from app.service.common.subagents.runner import AgentOrchestrator as TelegramAgentOrchestrator
from app.service.common.workspace_analysis import AnalysisEngine
from app.service.common.jobs.analysis_runner import AnalysisTaskRunner
from app.service.common.streaming.analysis import AnalysisStreamHub
from app.service.common.workspace_assets import AssetSynthesizer
from app.service.stone.assets_support import StoneV3BaselineSynthesizer
from app.core.config import AppConfig, default_config
from app.db import Database
from app.models import utcnow
from app.service.common.pipeline.ingest import DocumentIngestService
from app.service.common.pipeline.ingest_task import IngestTaskManager
from app.service.common.pipeline.project_deletion import ProjectDeletionManager
from app.service.common.pipeline.rechunk import RechunkTaskManager
from app.service.common.workspace_preprocess import PreprocessAgentService
from app.retrieval.service import RetrievalService
from app.retrieval.vector_store import VectorStoreManager
from app.schemas import DEFAULT_ANALYSIS_CONCURRENCY
from app.service.stone.preprocess import StonePreprocessStreamHub, StonePreprocessWorker
from app.storage import repository
from app.service.telegram.preprocess import TelegramPreprocessManager
from app.service.stone.writing import WritingAgentService


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
    retrieval: RetrievalService
    analysis_stream_hub: AnalysisStreamHub
    telegram_preprocess_stream_hub: AnalysisStreamHub
    stone_preprocess_stream_hub: StonePreprocessStreamHub
    analysis_engine: AnalysisEngine
    analysis_runner: AnalysisTaskRunner
    ingest_service: DocumentIngestService
    ingest_task_manager: IngestTaskManager
    rechunk_manager: RechunkTaskManager
    asset_synthesizer: AssetSynthesizer
    stone_v3_synthesizer: StoneV3BaselineSynthesizer
    preprocess_service: PreprocessAgentService
    writing_service: WritingAgentService
    telegram_preprocess_manager: TelegramPreprocessManager
    stone_preprocess_worker: StonePreprocessWorker
    project_deletion_manager: ProjectDeletionManager
    stone_agents: StoneAgentOrchestrator
    telegram_agents: TelegramAgentOrchestrator
    preprocess_agents: PreprocessAgentOrchestrator

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
        project_deletion_manager = ProjectDeletionManager(
            db=database,
            config=config,
            vector_store_manager=vector_store_manager,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            analysis_runner=analysis_runner,
            preprocess_service=preprocess_service,
            writing_service=writing_service,
            telegram_preprocess_manager=telegram_preprocess_manager,
        )
        stone_agents = StoneAgentOrchestrator(retrieval_service=retrieval)
        telegram_agents = TelegramAgentOrchestrator()
        preprocess_agents = PreprocessAgentOrchestrator()
        service_registry = ServiceRegistry.build(
            single_preprocess=preprocess_service,
            group_preprocess=preprocess_service,
            telegram_preprocess=telegram_preprocess_manager,
            stone_preprocess=stone_preprocess_worker,
            single_analysis=analysis_engine,
            group_analysis=analysis_engine,
            telegram_analysis=analysis_engine,
            stone_analysis=analysis_engine,
            single_assets=asset_synthesizer,
            group_assets=asset_synthesizer,
            telegram_assets=asset_synthesizer,
            stone_assets=stone_v3_synthesizer,
            stone_writing=writing_service,
        )
        services = AppServices(
            registry=service_registry,
            retrieval=retrieval,
            vector_store_manager=vector_store_manager,
            analysis_stream_hub=analysis_stream_hub,
            telegram_preprocess_stream_hub=telegram_preprocess_stream_hub,
            stone_preprocess_stream_hub=stone_preprocess_stream_hub,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            ingest_service=ingest_service,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            asset_synthesizer=asset_synthesizer,
            stone_v3_synthesizer=stone_v3_synthesizer,
            preprocess_service=preprocess_service,
            writing_service=writing_service,
            telegram_preprocess_manager=telegram_preprocess_manager,
            stone_preprocess_worker=stone_preprocess_worker,
            project_deletion_manager=project_deletion_manager,
            stone_agents=stone_agents,
            telegram_agents=telegram_agents,
            preprocess_agents=preprocess_agents,
        )

        _recover_interrupted_analysis_runs(database)
        telegram_preprocess_manager.resume_interrupted_runs()
        project_deletion_manager.resume_pending_deletions()

        return cls(
            config=config,
            db=database,
            services=services,
            vector_store_manager=vector_store_manager,
            retrieval=retrieval,
            analysis_stream_hub=analysis_stream_hub,
            telegram_preprocess_stream_hub=telegram_preprocess_stream_hub,
            stone_preprocess_stream_hub=stone_preprocess_stream_hub,
            analysis_engine=analysis_engine,
            analysis_runner=analysis_runner,
            ingest_service=ingest_service,
            ingest_task_manager=ingest_task_manager,
            rechunk_manager=rechunk_manager,
            asset_synthesizer=asset_synthesizer,
            stone_v3_synthesizer=stone_v3_synthesizer,
            preprocess_service=preprocess_service,
            writing_service=writing_service,
            telegram_preprocess_manager=telegram_preprocess_manager,
            stone_preprocess_worker=stone_preprocess_worker,
            project_deletion_manager=project_deletion_manager,
            stone_agents=stone_agents,
            telegram_agents=telegram_agents,
            preprocess_agents=preprocess_agents,
        )

    def attach_to_app(self, app: FastAPI) -> None:
        app.state.config = self.config
        app.state.db = self.db
        app.state.services = self.services

    @asynccontextmanager
    async def lifespan(self, _: FastAPI):
        self.stone_preprocess_worker.bind_loop(asyncio.get_running_loop())
        self.stone_preprocess_worker.resume_interrupted_runs()
        try:
            yield
        finally:
            self.project_deletion_manager.shutdown()
            self.analysis_runner.shutdown()
            self.preprocess_service.shutdown()
            self.writing_service.shutdown()
            self.telegram_preprocess_manager.shutdown()
            await self.stone_preprocess_worker.shutdown()
            self.rechunk_manager.shutdown()
            self.ingest_task_manager.shutdown()
            self.vector_store_manager.save_all()
            self.db.close()
