from __future__ import annotations

from fastapi import APIRouter, File, Request, UploadFile

from app.web import routes as legacy

router = APIRouter(tags=["projects"])


@router.post("/api/projects")
def create_project_api(payload: legacy.ProjectCreatePayload, session: legacy.SessionDep):
    return legacy.create_project_api(payload, session)


@router.post("/api/projects/{project_id}/deletion")
def delete_project_api_v2(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.delete_project_api_v2(request, project_id, session)


@router.get("/api/projects/{project_id}/deletion")
def get_project_deletion_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.get_project_deletion_api(request, project_id, session)


@router.delete("/api/projects/{project_id}")
def delete_project_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.delete_project_api(request, project_id, session)


@router.post("/api/projects/{project_id}/documents")
async def upload_documents_api(
    request: Request,
    project_id: str,
    session: legacy.SessionDep,
    files: list[UploadFile] = File(...),
):
    return await legacy.upload_documents_api(request, project_id, session, files)


@router.post("/api/projects/{project_id}/documents/text")
def create_text_document_api(
    request: Request,
    project_id: str,
    payload: legacy.TextDocumentCreatePayload,
    session: legacy.SessionDep,
):
    return legacy.create_text_document_api(request, project_id, payload, session)


@router.get("/api/projects/{project_id}/documents")
def list_documents_api(project_id: str, session: legacy.SessionDep, offset: int = 0, limit: int = 20):
    return legacy.list_documents_api(project_id, session, offset, limit)


@router.get("/api/projects/{project_id}/documents/{document_id}/task")
def get_document_task_status(request: Request, project_id: str, document_id: str):
    return legacy.get_document_task_status(request, project_id, document_id)


@router.get("/api/projects/{project_id}/tasks")
def get_project_tasks(request: Request, project_id: str):
    return legacy.get_project_tasks(request, project_id)


@router.post("/api/projects/{project_id}/documents/{document_id}/process")
def process_document_api(request: Request, project_id: str, document_id: str, session: legacy.SessionDep):
    return legacy.process_document_api(request, project_id, document_id, session)


@router.post("/api/projects/{project_id}/process-all")
def process_all_documents_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.process_all_documents_api(request, project_id, session)


@router.post("/api/projects/{project_id}/retry-all")
def retry_all_documents_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.retry_all_documents_api(request, project_id, session)


@router.post("/api/projects/{project_id}/stop-processing")
def stop_processing_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.stop_processing_api(request, project_id, session)


@router.post("/api/projects/{project_id}/documents/{document_id}")
def update_document_api(
    project_id: str,
    document_id: str,
    payload: legacy.DocumentUpdatePayload,
    session: legacy.SessionDep,
):
    return legacy.update_document_api(project_id, document_id, payload, session)


@router.post("/api/projects/{project_id}/documents/{document_id}/delete")
def delete_document_api(project_id: str, document_id: str, session: legacy.SessionDep):
    return legacy.delete_document_api(project_id, document_id, session)


@router.get("/api/projects/{project_id}/documents/mentions")
def list_document_mentions_api(request: Request, project_id: str, session: legacy.SessionDep, q: str = ""):
    return legacy.list_document_mentions_api(request, project_id, session, q)


@router.post("/api/projects/{project_id}/rechunk")
def start_rechunk_api(request: Request, project_id: str, session: legacy.SessionDep):
    return legacy.start_rechunk_api(request, project_id, session)


@router.get("/api/projects/{project_id}/rechunk/{task_id}")
def get_rechunk_task_api(request: Request, project_id: str, task_id: str, session: legacy.SessionDep):
    return legacy.get_rechunk_task_api(request, project_id, task_id, session)
