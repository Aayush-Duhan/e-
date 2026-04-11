"""FastAPI route handlers."""

import asyncio
import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from fastapi import File, Form, Header, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect

from python_execution_service.config import (
    CANCEL_FLAGS,
    OUTPUT_ROOT,
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
)
from python_execution_service.helpers import (
    _request_from_run,
    _sanitize_upload_filename,
    append_chat_message,
    get_steps_template,
    now_iso,
    persist_run,
    push_user_message,
    require_auth,
)
from python_execution_service.models import (
    ResumeRunConfig,
    RunRecord,
    StartRunRequest,
    StartRunResponse,
)
from python_execution_service.sqlite_store import RunStore
from python_execution_service.workflow import execute_run_sync


# ── Internal helpers ────────────────────────────────────────────

def _start_run_record(
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

    _start_run_worker(run_id)
    return StartRunResponse(runId=run_id)


def _start_run_worker(run_id: str, *, is_follow_up_chat: bool = False) -> None:
    worker = threading.Thread(
        target=execute_run_sync,
        args=(run_id,),
        kwargs={"is_follow_up_chat": is_follow_up_chat},
        daemon=True,
    )
    worker.start()


# ── Route registration ─────────────────────────────────────────

def register_routes(app) -> None:
    """Attach all route handlers to the given FastAPI application."""

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/runs/start", response_model=StartRunResponse)
    def start_run(
        request: StartRunRequest,
        x_execution_token: str | None = Header(default=None),
    ) -> StartRunResponse:
        require_auth(x_execution_token)
        return _start_run_record(request)

    @app.get("/v1/runs")
    def list_runs(
        x_execution_token: str | None = Header(default=None),
        limit: int = Query(default=100, ge=1, le=500),
        status: str | None = Query(default=None),
        projectId: str | None = Query(default=None),
    ) -> dict[str, Any]:
        require_auth(x_execution_token)
        summaries = RunStore.list_runs_summary(
            limit=limit, status=status, project_id=projectId,
        )
        return {"runs": summaries}

    @app.get("/v1/runs/{run_id}")
    def get_run(
        run_id: str,
        x_execution_token: str | None = Header(default=None),
    ) -> dict[str, Any]:
        require_auth(x_execution_token)
        # Prefer in-memory copy for active runs (has live events), fall back to DB
        with RUN_LOCK:
            run = RUNS.get(run_id)
        if run:
            return run.model_dump()
        data = RunStore.get_run(run_id)
        if not data:
            raise HTTPException(status_code=404, detail="Run not found")
        return data

    @app.post("/v1/runs/{run_id}/cancel")
    def cancel_run(
        run_id: str,
        x_execution_token: str | None = Header(default=None),
    ) -> dict[str, str]:
        require_auth(x_execution_token)
        with RUN_LOCK:
            run = RUNS.get(run_id)
            if not run:
                raise HTTPException(status_code=404, detail="Run not found")
            flag = CANCEL_FLAGS.get(run_id)
            if flag:
                flag.set()
        return {"status": "canceled"}

    @app.post("/v1/runs/{run_id}/chat")
    def send_chat_message(
        run_id: str,
        body: dict[str, str],
        x_execution_token: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """Send a user message to the active or completed agent session."""
        require_auth(x_execution_token)
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

        # Emit user message to chat and push to agent queue
        append_chat_message(run, role="user", kind="user_input", content=message)
        push_user_message(run_id, message)

        if start_follow_up_worker:
            _start_run_worker(run_id, is_follow_up_chat=True)

        return {"status": "queued", "eventIndex": event_index}

    @app.post("/v1/runs/{run_id}/retry", response_model=StartRunResponse)
    def retry_run(
        run_id: str,
        x_execution_token: str | None = Header(default=None),
    ) -> StartRunResponse:
        require_auth(x_execution_token)
        with RUN_LOCK:
            existing = RUNS.get(run_id)
        if not existing:
            data = RunStore.get_run(run_id)
            if not data:
                raise HTTPException(status_code=404, detail="Run not found")
            existing = RunRecord.model_validate(data)
        req = _request_from_run(existing)
        return _start_run_record(req)

    @app.post("/v1/runs/{run_id}/resume", response_model=StartRunResponse)
    async def resume_run(
        run_id: str,
        ddl_file: UploadFile = File(...),
        resume_from_stage: str = Form(default="execute_sql"),
        last_executed_file_index: int = Form(default=-1),
        missing_objects: str = Form(default=""),
        x_execution_token: str | None = Header(default=None),
    ) -> StartRunResponse:
        require_auth(x_execution_token)
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
        return _start_run_record(req, resume_config=resume_config)

    @app.get("/v1/runs/{run_id}/events")
    async def stream_events(
        run_id: str,
        last_event_index: int | None = Query(default=None),
        x_execution_token: str | None = Header(default=None),
        last_event_id: str | None = Header(default=None),
    ):
        require_auth(x_execution_token)
        with RUN_LOCK:
            if run_id not in RUNS:
                raise HTTPException(status_code=404, detail="Run not found")

        from fastapi.responses import StreamingResponse

        async def iterator():
            idx = 0
            # Prefer query param (set by frontend after follow-up) over header
            if last_event_index is not None:
                idx = max(0, last_event_index)
            elif last_event_id is not None:
                try:
                    idx = max(0, int(last_event_id) + 1)
                except ValueError:
                    idx = 0
            heartbeat_at = time.time()
            while True:
                with RUN_LOCK:
                    run = RUNS.get(run_id)
                    if not run:
                        break
                    events = run.events[idx:]
                    status = run.status
                    total_events = len(run.events)
                for event in events:
                    event_id = idx
                    yield f"event: {event['type']}\n".encode("utf-8")
                    yield f"id: {event_id}\n".encode("utf-8")
                    payload = event.get("payload", {})
                    yield f"data: {json.dumps(payload)}\n\n".encode("utf-8")
                    idx += 1
                now = time.time()
                if now - heartbeat_at >= 20:
                    heartbeat_at = now
                    yield b": heartbeat\n\n"
                if status in ("completed", "failed", "canceled", "awaiting_input") and idx >= total_events:
                    break
                await asyncio.sleep(0.25)

        return StreamingResponse(iterator(), media_type="text/event-stream")

    # ── Terminal WebSocket (per-run isolation) ────────────────────
    @app.websocket("/ws/terminal/agent/{run_id}")
    async def ws_agent_terminal(websocket: WebSocket, run_id: str) -> None:
        """Stream raw PTY output for a specific run to the frontend terminal.

        Each WebSocket connection subscribes to a single run's channel,
        so concurrent runs do not leak terminal output to each other.
        """
        from python_execution_service import terminal_bridge

        with RUN_LOCK:
            if run_id not in RUNS:
                await websocket.close(code=4004, reason="Run not found")
                return

        await websocket.accept()
        q = terminal_bridge.subscribe(run_id)

        async def _reader() -> None:
            """Read from the run's broadcast queue and send to WebSocket."""
            try:
                while True:
                    data = await q.get()
                    if data:
                        await websocket.send_text(data)
            except Exception:
                pass

        reader_task = asyncio.create_task(_reader())

        try:
            while True:
                raw = await websocket.receive_text()
                # Handle resize control messages (like bolt.new)
                if raw.startswith("{"):
                    try:
                        msg = json.loads(raw)
                        if msg.get("type") == "resize":
                            continue
                    except (ValueError, KeyError):
                        pass
        except WebSocketDisconnect:
            pass
        finally:
            reader_task.cancel()
            terminal_bridge.unsubscribe(run_id, q)

    # ── Cortex CLI Terminal ───────────────────────────────────────

    @app.post("/v1/cortex-terminal/spawn")
    def spawn_cortex_terminal(
        body: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Spawn a new interactive Cortex CLI PTY session."""
        from python_execution_service import cortex_pty

        cols = 80
        rows = 24
        if body:
            cols = int(body.get("cols", 80))
            rows = int(body.get("rows", 24))

        session_id = cortex_pty.spawn_session(cols=cols, rows=rows)
        return {"sessionId": session_id}

    @app.websocket("/ws/terminal/cortex/{session_id}")
    async def ws_cortex_terminal(websocket: WebSocket, session_id: str) -> None:
        """Bidirectional WebSocket for an interactive Cortex CLI PTY session.

        Browser sends keystrokes → PTY stdin.
        PTY stdout → browser via async queue.
        Resize control messages are supported.
        """
        from python_execution_service import cortex_pty

        session = cortex_pty.get_session(session_id)
        if session is None:
            await websocket.close(code=4004, reason="Session not found")
            return

        await websocket.accept()
        q = session.subscribe()

        async def _reader() -> None:
            """Read from the PTY broadcast queue and send to WebSocket."""
            try:
                while True:
                    data = await q.get()
                    if data:
                        await websocket.send_text(data)
            except Exception:
                pass

        reader_task = asyncio.create_task(_reader())

        try:
            while True:
                raw = await websocket.receive_text()
                # Handle resize control messages
                if raw.startswith("{"):
                    try:
                        msg = json.loads(raw)
                        if msg.get("type") == "resize":
                            session.resize(
                                cols=int(msg.get("cols", 80)),
                                rows=int(msg.get("rows", 24)),
                            )
                            continue
                    except (ValueError, KeyError):
                        pass
                # Forward keystrokes to PTY stdin
                session.write(raw)
        except WebSocketDisconnect:
            pass
        finally:
            reader_task.cancel()
            session.unsubscribe(q)
