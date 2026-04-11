"""Run mutation and lifecycle routes."""

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from python_execution_service.app.api.routes._shared import start_run_record, start_run_worker
from python_execution_service.domain.runs.service import (
    _request_from_run,
    append_chat_message,
    now_iso,
    push_user_message,
)
from python_execution_service.domain.runs.state import (
    CANCEL_FLAGS,
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
)
from python_execution_service.infrastructure.persistence.sqlite.store import RunStore
from python_execution_service.shared.models.runs import (
    ResumeRunConfig,
    RunRecord,
    StartRunResponse,
)

router = APIRouter(tags=["run-actions"])


@router.post("/v1/runs/{run_id}/cancel")
def cancel_run(run_id: str) -> dict[str, str]:
    with RUN_LOCK:
        run = RUNS.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        flag = CANCEL_FLAGS.get(run_id)
        if flag:
            flag.set()
    return {"status": "canceled"}


@router.post("/v1/runs/{run_id}/chat")
def send_chat_message(run_id: str, body: dict[str, str]) -> dict[str, Any]:
    """Send a user message to the active or completed agent session."""
    message = (body.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")

    start_follow_up_worker = False
    event_index = 0

    with RUN_LOCK:
        run = RUNS.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        event_index = len(run.events)
        if run.status in ("running", "queued"):
            pass
        elif run.status in ("completed", "failed", "awaiting_input"):
            run.status = "queued"
            run.error = None
            run.updatedAt = now_iso()
            PROJECT_LOCKS[run.projectId] = run_id
            RunStore.update_run_status(run_id, "queued", error=None, updated_at=run.updatedAt)
            start_follow_up_worker = True
        else:
            raise HTTPException(
                status_code=409,
                detail=f"Run is not active (status: {run.status})",
            )

    append_chat_message(run, role="user", kind="user_input", content=message)
    push_user_message(run_id, message)

    if start_follow_up_worker:
        start_run_worker(run_id, is_follow_up_chat=True)

    return {"status": "queued", "eventIndex": event_index}


@router.post(
    "/v1/runs/{run_id}/retry",
    response_model=StartRunResponse,
)
def retry_run(run_id: str) -> StartRunResponse:
    with RUN_LOCK:
        existing = RUNS.get(run_id)
    if not existing:
        data = RunStore.get_run(run_id)
        if not data:
            raise HTTPException(status_code=404, detail="Run not found")
        existing = RunRecord.model_validate(data)
    req = _request_from_run(existing)
    return start_run_record(req)


@router.post(
    "/v1/runs/{run_id}/resume",
    response_model=StartRunResponse,
)
async def resume_run(
    run_id: str,
    ddl_file: UploadFile = File(...),
    resume_from_stage: str = Form(default="execute_sql"),
    last_executed_file_index: int = Form(default=-1),
    missing_objects: str = Form(default=""),
) -> StartRunResponse:
    if not ddl_file.filename:
        raise HTTPException(status_code=400, detail="DDL file is required")

    with RUN_LOCK:
        existing = RUNS.get(run_id)
    if not existing:
        data = RunStore.get_run(run_id)
        if not data:
            raise HTTPException(status_code=404, detail="Run not found")
        existing = RunRecord.model_validate(data)
    if not existing.requiresDdlUpload:
        raise HTTPException(status_code=409, detail="Run is not waiting for DDL upload")

    ddl_content = await ddl_file.read()
    if not ddl_content:
        raise HTTPException(status_code=400, detail="Uploaded DDL file is empty")

    objects_from_form: list[str] = []
    if missing_objects.strip():
        try:
            parsed = json.loads(missing_objects)
            if isinstance(parsed, list):
                objects_from_form = [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            objects_from_form = [item.strip() for item in missing_objects.split(",") if item.strip()]

    objects = objects_from_form or list(existing.missingObjects)
    req = _request_from_run(existing)
    resume_config = ResumeRunConfig(
        ddl_content=ddl_content,
        ddl_filename=ddl_file.filename,
        missing_objects=objects,
        resume_from_stage=resume_from_stage,
        last_executed_file_index=last_executed_file_index,
    )
    return start_run_record(req, resume_config=resume_config)
