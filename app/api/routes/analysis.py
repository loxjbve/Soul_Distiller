from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.web import routes as legacy

router = APIRouter(tags=["analysis"])


@router.post("/api/projects/{project_id}/analyze")
def analyze_project_api(
    request: Request,
    project_id: str,
    payload: legacy.AnalysisRequestPayload,
    session: legacy.SessionDep,
):
    return legacy.analyze_project_api(request, project_id, payload, session)


@router.get("/api/projects/{project_id}/analysis")
def get_analysis_api(project_id: str, session: legacy.SessionDep, run_id: str | None = Query(default=None)):
    return legacy.get_analysis_api(project_id, session, run_id)


@router.post("/api/projects/{project_id}/analysis/{facet_key}/rerun")
def rerun_facet_api(request: Request, project_id: str, facet_key: str, session: legacy.SessionDep):
    return legacy.rerun_facet_api(request, project_id, facet_key, session)
