"""LangGraph agent-driven workflow execution."""

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from python_execution_service.agentic_core.agent.graph import (
    build_agent_graph,
    cleanup_agent_session,
    create_checkpointer,
)
from python_execution_service.agentic_core.agent.tools import get_active_context, set_active_context
from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState
from python_execution_service.config import (
    AGENT_GRAPHS,
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
    STEP_LABELS,
)
from python_execution_service.helpers import (
    add_log,
    append_chat_message,
    append_event,
    append_terminal_output,
    emit_chat_delta,
    ensure_not_canceled,
    format_activity_log_entry,
    pop_user_message,
    send_terminal_data,
    set_run_status,
    update_step,
)
from python_execution_service.models import RunRecord
from python_execution_service.sqlite_store import RunStore

logger = logging.getLogger(__name__)


def _perf_log(run: RunRecord, message: str) -> None:
    try:
        perf_file = Path(run.outputDir) / "performance.txt"
        perf_file.parent.mkdir(parents=True, exist_ok=True)
        with perf_file.open("a", encoding="utf-8", newline="") as f:
            f.write(f"[{time.strftime('%H:%M:%S')}] {message}\n")
    except Exception:
        pass


def _get_checkpointer_path(run: RunRecord) -> str:
    db_dir = Path(run.outputDir)
    db_dir.mkdir(parents=True, exist_ok=True)
    return str(db_dir / "langgraph_checkpoints.db")


def execute_run_sync(run_id: str, *, is_follow_up_chat: bool = False) -> None:  # noqa: C901
    """Execute a migration run using the LangGraph autonomous agent.

    The agent graph uses a StateGraph with call_model -> call_tools loop.
    """
    run_t0 = time.monotonic()
    with RUN_LOCK:
        run = RUNS[run_id]
        resume_ddl_path = run.ddlUploadPath
        resume_from_stage = run.resumeFromStage or "execute_sql"
        resume_missing_objects = list(run.missingObjects)
        resume_last_executed_file_index = int(run.lastExecutedFileIndex)
    _perf_log(run, f"RUN_START   run_id={run_id}")

    agent_info = None

    try:
        set_run_status(run, "running")
        if not is_follow_up_chat:
            append_event(run, "run:started", {"runId": run_id})
            append_chat_message(
                run,
                role="system",
                kind="run_status",
                content="Migration started. The agent is analyzing the task...",
            )

        # ── Sink callbacks ─────────────────────────────────────

        def activity_log_sink(entry: dict[str, Any]) -> None:
            formatted = format_activity_log_entry(entry)
            if formatted:
                stage = entry.get("stage")
                step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
                is_progress = bool(entry.get("data", {}).get("is_progress"))
                add_log(run, formatted, step_id=step_id, is_progress=is_progress)

        def terminal_output_sink(text: str, is_progress: bool = False) -> None:
            stage = context.current_stage.value if context.current_stage else None
            step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
            append_terminal_output(run, text, is_progress=is_progress, step_id=step_id)

        def raw_terminal_output_sink(raw_chunk: str) -> None:
            send_terminal_data(run, raw_chunk)

        def sync_execution_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.executionLog = updated.execution_log or []
                run.executionErrors = updated.execution_errors or []
                run.missingObjects = updated.missing_objects or []
                run.requiresDdlUpload = bool(updated.requires_ddl_upload)
                run.resumeFromStage = updated.resume_from_stage or ""
                run.lastExecutedFileIndex = int(updated.last_executed_file_index)
                run.ddlUploadPath = updated.ddl_upload_path or ""
            RunStore.update_run_fields(
                run_id,
                missingObjects=run.missingObjects,
                requiresDdlUpload=run.requiresDdlUpload,
                resumeFromStage=run.resumeFromStage,
                lastExecutedFileIndex=run.lastExecutedFileIndex,
                ddlUploadPath=run.ddlUploadPath,
            )

        _streamed_stmt_lock = threading.Lock()
        _streamed_stmt_count = 0

        def realtime_execution_event_sink(entry: dict[str, Any]) -> None:
            nonlocal _streamed_stmt_count
            stmt_index = entry.get("statement_index")
            label = f"Stmt {int(stmt_index) + 1}" if isinstance(stmt_index, int) else "Stmt ?"
            output_preview = entry.get("output_preview", [])
            output_text = ""
            if isinstance(output_preview, list) and output_preview:
                try:
                    output_text = json.dumps(output_preview, ensure_ascii=False, default=str, indent=2)
                except Exception:
                    output_text = str(output_preview)

            append_event(
                run,
                "execute_sql:statement",
                {
                    "runId": run_id,
                    "file": entry.get("file"),
                    "fileIndex": entry.get("fileIndex"),
                    "statementIndex": stmt_index,
                    "statement": entry.get("statement"),
                    "status": entry.get("status"),
                    "rowCount": entry.get("row_count", 0),
                    "outputPreview": output_preview,
                },
            )
            append_chat_message(
                run,
                role="agent",
                kind="sql_statement",
                content=label,
                step={"id": "execute_sql", "label": STEP_LABELS["execute_sql"]},
                sql={
                    "statement": str(entry.get("statement") or ""),
                    "output": output_text,
                },
            )
            with _streamed_stmt_lock:
                _streamed_stmt_count += 1

        # ── Build migration context ───────────────────────────

        if is_follow_up_chat:
            try:
                context = get_active_context(run_id)
            except Exception:
                context = MigrationContext(
                    project_name=run.projectName,
                    source_language=run.sourceLanguage.lower(),
                    source_directory=str(Path(run.sourcePath).resolve().parent),
                    source_files=[run.sourcePath],
                    mapping_csv_path=run.schemaPath,
                    sf_account=run.sfAccount or "",
                    sf_user=run.sfUser or "",
                    sf_role=run.sfRole or "",
                    sf_warehouse=run.sfWarehouse or "",
                    sf_database=run.sfDatabase or "",
                    sf_schema=run.sfSchema or "",
                    sf_authenticator=run.sfAuthenticator or "externalbrowser",
                    session_id=run_id,
                )
        else:
            context = MigrationContext(
                project_name=run.projectName,
                source_language=run.sourceLanguage.lower(),
                source_directory=str(Path(run.sourcePath).resolve().parent),
                source_files=[run.sourcePath],
                mapping_csv_path=run.schemaPath,
                sf_account=run.sfAccount or "",
                sf_user=run.sfUser or "",
                sf_role=run.sfRole or "",
                sf_warehouse=run.sfWarehouse or "",
                sf_database=run.sfDatabase or "",
                sf_schema=run.sfSchema or "",
                sf_authenticator=run.sfAuthenticator or "externalbrowser",
                session_id=run_id,
            )

        context.project_name = run.projectName
        context.source_language = run.sourceLanguage.lower()
        context.source_directory = str(Path(run.sourcePath).resolve().parent)
        context.source_files = [run.sourcePath]
        context.mapping_csv_path = run.schemaPath
        context.activity_log_sink = activity_log_sink
        context.execution_event_sink = realtime_execution_event_sink
        context.terminal_output_sink = terminal_output_sink
        context.raw_terminal_output_sink = raw_terminal_output_sink
        context.sf_account = run.sfAccount or ""
        context.sf_user = run.sfUser or ""
        context.sf_role = run.sfRole or ""
        context.sf_warehouse = run.sfWarehouse or ""
        context.sf_database = run.sfDatabase or ""
        context.sf_schema = run.sfSchema or ""
        context.sf_authenticator = run.sfAuthenticator or "externalbrowser"
        context.session_id = run_id

        if resume_ddl_path:
            context.requires_ddl_upload = True
            context.ddl_upload_path = resume_ddl_path
            context.resume_from_stage = resume_from_stage
            context.last_executed_file_index = max(-1, resume_last_executed_file_index)
            context.missing_objects = resume_missing_objects
            add_log(
                run,
                f"Applying uploaded DDL ({Path(resume_ddl_path).name}) before resuming execute_sql.",
            )

        # ── Agent callbacks ────────────────────────────────────

        def message_callback(role: str, kind: str, content: str) -> None:
            append_chat_message(run, role=role, kind=kind, content=content)

        def step_callback(step_id: str, status: str) -> None:
            ensure_not_canceled(run_id)
            update_step(run, step_id, status)
            if status == "running":
                append_event(
                    run,
                    "step:started",
                    {"runId": run_id, "stepId": step_id, "label": STEP_LABELS.get(step_id, step_id)},
                )
            elif status in ("completed", "failed"):
                append_event(
                    run,
                    "step:completed" if status == "completed" else "step:failed",
                    {"runId": run_id, "stepId": step_id, "label": STEP_LABELS.get(step_id, step_id)},
                )
                try:
                    updated_ctx = get_active_context(run_id)
                    sync_execution_state(updated_ctx)
                except Exception:
                    pass

        def user_message_getter() -> str | None:
            ensure_not_canceled(run_id)
            return pop_user_message(run_id)

        def conversation_callback(history: list[dict[str, str]]) -> None:
            with RUN_LOCK:
                run.conversationHistory = history

        # ── Build and run the LangGraph agent ──────────────────

        _perf_log(run, "AGENT_BUILD_START")
        checkpointer = create_checkpointer(_get_checkpointer_path(run))

        conv_history_copy = list(run.conversationHistory)

        agent_info = build_agent_graph(
            context,
            message_callback=message_callback,
            step_callback=step_callback,
            user_message_getter=user_message_getter,
            conversation_history=conv_history_copy,
            conversation_callback=conversation_callback,
            delta_callback=lambda mid, tok: emit_chat_delta(run, mid, tok),
            consume_user_messages_from_start=is_follow_up_chat,
            start_with_migration_prompt=not is_follow_up_chat,
            checkpointer=checkpointer,
        )
        _perf_log(run, "AGENT_BUILD_END")

        graph = agent_info["graph"]
        initial_state = agent_info["initial_state"]
        config = agent_info["config"]

        # Store graph reference for resume operations
        with RUN_LOCK:
            AGENT_GRAPHS[run_id] = graph
            run.graphThreadId = config["configurable"]["thread_id"]

        _perf_log(run, "AGENT_RUN_START")

        result = graph.invoke(initial_state, config=config)

        _perf_log(run, "AGENT_RUN_END")

        # ── Process final state ────────────────────────────────

        _finalize_run(run, run_id, is_follow_up_chat)
        _do_cleanup(agent_info, run, run_id, run_t0)

    except Exception as exc:
        message = str(exc)
        logger.error(
            "execute_run_sync failed run_id=%s is_follow_up=%s error=%s",
            run_id, is_follow_up_chat, message, exc_info=True,
        )
        canceled = message == "Run canceled"
        if canceled:
            set_run_status(run, "canceled", message)
            append_event(run, "run:failed", {"runId": run_id, "reason": message})
        elif is_follow_up_chat:
            # Transient error during follow-up: let the user retry by staying in awaiting_input.
            set_run_status(run, "awaiting_input", message)
            append_event(run, "run:awaiting_input", {"runId": run_id, "reason": message})
        else:
            set_run_status(run, "failed", message)
            append_event(run, "run:failed", {"runId": run_id, "reason": message})
        append_chat_message(
            run,
            role="error",
            kind="run_status",
            content=message or "Run failed",
        )
        _do_cleanup(agent_info, run, run_id, run_t0)


def _do_cleanup(agent_info: dict | None, run: RunRecord, run_id: str, run_t0: float) -> None:
    """Clean up agent session, release project lock."""
    if agent_info is not None:
        cleanup_agent_session(agent_info)
    with RUN_LOCK:
        AGENT_GRAPHS.pop(run_id, None)
    total_elapsed = time.monotonic() - run_t0
    _perf_log(run, f"RUN_END     run_id={run_id}  total_elapsed={total_elapsed:.3f}s  status={run.status}")
    with RUN_LOCK:
        if PROJECT_LOCKS.get(run.projectId) == run_id:
            del PROJECT_LOCKS[run.projectId]



def _finalize_run(run: RunRecord, run_id: str, is_follow_up_chat: bool) -> None:
    """Process the final state after graph completes without interruption."""
    try:
        final_context = get_active_context(run_id)
    except Exception:
        set_run_status(run, "completed")
        return

    # Sync final execution state
    with RUN_LOCK:
        run.executionLog = final_context.execution_log or []
        run.executionErrors = final_context.execution_errors or []
        run.missingObjects = final_context.missing_objects or []
        run.requiresDdlUpload = bool(final_context.requires_ddl_upload)
        run.resumeFromStage = final_context.resume_from_stage or ""
        run.lastExecutedFileIndex = int(final_context.last_executed_file_index)
        run.ddlUploadPath = final_context.ddl_upload_path or ""
    RunStore.update_run_fields(
        run_id,
        missingObjects=run.missingObjects,
        requiresDdlUpload=run.requiresDdlUpload,
        resumeFromStage=run.resumeFromStage,
        lastExecutedFileIndex=run.lastExecutedFileIndex,
        ddlUploadPath=run.ddlUploadPath,
    )

    if final_context.current_stage == MigrationState.COMPLETED:
        set_run_status(run, "completed")
        append_event(run, "run:completed", {"runId": run_id})
        if not is_follow_up_chat:
            append_chat_message(
                run,
                role="system",
                kind="run_status",
                content="Migration completed successfully!",
            )
        return

    if final_context.requires_ddl_upload:
        reason = final_context.human_intervention_reason or "DDL upload required"
        set_run_status(run, "failed", reason)
        append_event(run, "run:failed", {"runId": run_id, "reason": reason})
        with RUN_LOCK:
            run.requiresDdlUpload = True
            run.missingObjects = final_context.missing_objects or []
            run.resumeFromStage = final_context.resume_from_stage or "execute_sql"
            run.lastExecutedFileIndex = int(final_context.last_executed_file_index)
        RunStore.update_run_fields(
            run_id,
            requiresDdlUpload=True,
            missingObjects=run.missingObjects,
            resumeFromStage=run.resumeFromStage,
            lastExecutedFileIndex=run.lastExecutedFileIndex,
        )
        return

    if is_follow_up_chat:
        # Follow-up conversation finished normally (agent responded).
        # Mark as awaiting_input without an error so the user can continue chatting.
        set_run_status(run, "awaiting_input")
        append_event(run, "run:awaiting_input", {"runId": run_id, "reason": ""})
    else:
        # Initial run stopped before completion (e.g. max iterations).
        reason = final_context.human_intervention_reason or "Migration stopped before completion"
        set_run_status(run, "awaiting_input", reason)
        append_event(run, "run:awaiting_input", {"runId": run_id, "reason": reason})
