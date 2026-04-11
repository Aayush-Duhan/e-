"""Shared helpers used by route modules."""

import threading
import uuid
from pathlib import Path

from fastapi import HTTPException

from python_execution_service.app.config.settings import OUTPUT_ROOT
from python_execution_service.domain.migration.workflow import execute_run_sync
from python_execution_service.domain.runs.service import (
    _sanitize_upload_filename,
    get_steps_template,
    now_iso,
    persist_run,
)
from python_execution_service.domain.runs.state import (
    CANCEL_FLAGS,
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
)
from python_execution_service.shared.models.runs import (
    ResumeRunConfig,
    RunRecord,
    StartRunRequest,
    StartRunResponse,
)


def start_run_worker(run_id: str, *, is_follow_up_chat: bool = False) -> None:
    worker = threading.Thread(
        target=execute_run_sync,
        args=(run_id,),
        kwargs={"is_follow_up_chat": is_follow_up_chat},
        daemon=True,
    )
    worker.start()


def start_run_record(
    request: StartRunRequest,
    resume_config: ResumeRunConfig | None = None,
) -> StartRunResponse:
    if not Path(request.sourcePath).exists():
        raise HTTPException(status_code=404, detail="Source file path not found")
    if request.schemaPath and not Path(request.schemaPath).exists():
        raise HTTPException(status_code=404, detail="Schema file path not found")

    with RUN_LOCK:
        locked_by = PROJECT_LOCKS.get(request.projectId)
        if locked_by and RUNS.get(locked_by) and RUNS[locked_by].status == "running":
            raise HTTPException(status_code=409, detail="Project already has an active run")

        run_id = str(uuid.uuid4())
        output_dir = OUTPUT_ROOT / request.projectId / run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        ddl_upload_path = ""
        missing_objects: list[str] = []
        requires_ddl_upload = False
        resume_from_stage = ""
        last_executed_file_index = -1

        if resume_config:
            safe_name = _sanitize_upload_filename(resume_config.ddl_filename)
            ddl_file_path = output_dir / f"resume-ddl-{safe_name}"
            ddl_file_path.write_bytes(resume_config.ddl_content)
            ddl_upload_path = str(ddl_file_path.resolve())
            missing_objects = list(resume_config.missing_objects)
            requires_ddl_upload = False
            resume_from_stage = resume_config.resume_from_stage or "execute_sql"
            last_executed_file_index = max(-1, int(resume_config.last_executed_file_index))

        record = RunRecord(
            runId=run_id,
            projectId=request.projectId,
            projectName=request.projectName,
            sourceId=request.sourceId,
            schemaId=request.schemaId or "",
            sourceLanguage=request.sourceLanguage,
            sourcePath=request.sourcePath,
            schemaPath=request.schemaPath or "",
            sfAccount=request.sfAccount,
            sfUser=request.sfUser,
            sfRole=request.sfRole,
            sfWarehouse=request.sfWarehouse,
            sfDatabase=request.sfDatabase,
            sfSchema=request.sfSchema,
            sfAuthenticator=request.sfAuthenticator,
            status="queued",
            createdAt=now_iso(),
            updatedAt=now_iso(),
            steps=get_steps_template(),
            outputDir=str(output_dir),
            missingObjects=missing_objects,
            requiresDdlUpload=requires_ddl_upload,
            resumeFromStage=resume_from_stage,
            lastExecutedFileIndex=last_executed_file_index,
            ddlUploadPath=ddl_upload_path,
        )

        RUNS[run_id] = record
        PROJECT_LOCKS[request.projectId] = run_id
        CANCEL_FLAGS[run_id] = threading.Event()

    persist_run(record)
    start_run_worker(run_id)
    return StartRunResponse(runId=run_id)

