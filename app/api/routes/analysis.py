from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.api.schemas.analysis import AnalysisRequestPayload
from app.core.deps import SessionDep
from app.web import runtime

router = APIRouter(tags=["analysis"])


@router.post("/api/projects/{project_id}/analyze")
def analyze_project_api(
    request: Request,
    project_id: str,
    payload: AnalysisRequestPayload,
    session: SessionDep,
):
    return runtime.analyze_project_api(request, project_id, payload, session)


@router.get("/api/projects/{project_id}/analysis")
def get_analysis_api(project_id: str, session: SessionDep, run_id: str | None = Query(default=None)):
    return runtime.get_analysis_api(project_id, session, run_id)


@router.post("/api/projects/{project_id}/analysis/{facet_key}/rerun")
def rerun_facet_api(request: Request, project_id: str, facet_key: str, session: SessionDep):
    return runtime.rerun_facet_api(request, project_id, facet_key, session)
