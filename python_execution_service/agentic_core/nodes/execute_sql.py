"""Execute SQL workflow node."""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState
from python_execution_service.agentic_core.nodes.common import is_error_state
from python_execution_service.agentic_core.runtime.snowflake_execution import (
    build_snowflake_connection,
    close_connection,
    execute_sql_statements,
)
from python_execution_service.agentic_core.services.ewi_cleanup import clean_ewi_markers
from python_execution_service.agentic_core.utils.activity_log import log_event
from python_execution_service.agentic_core.utils.sql_files import list_sql_files

logger = logging.getLogger(__name__)


def apply_uploaded_ddl_and_resume(state: MigrationContext) -> MigrationContext:
    """Apply uploaded DDL script and prepare workflow to resume execute_sql."""
    if not state.ddl_upload_path or not os.path.exists(state.ddl_upload_path):
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.human_intervention_reason = "DDL upload is required to resolve missing objects."
        log_event(state, "warning", "DDL upload path missing for resume")
        return state

    try:
        with open(state.ddl_upload_path, "r", encoding="utf-8-sig") as ddl_file:
            ddl_sql = clean_ewi_markers(ddl_file.read())

        if not ddl_sql.strip():
            state.current_stage = MigrationState.HUMAN_REVIEW
            state.requires_human_intervention = True
            state.human_intervention_reason = "Uploaded DDL file is empty."
            log_event(state, "warning", "Uploaded DDL file is empty")
            return state

        connection = build_snowflake_connection(state)
        try:
            execute_sql_statements(connection, ddl_sql)
        finally:
            close_connection(connection)

        state.requires_ddl_upload = False
        state.ddl_upload_path = ""
        state.resume_from_stage = "execute_sql"
        state.requires_human_intervention = False
        state.human_intervention_reason = ""
        log_event(state, "info", "Uploaded DDL executed successfully, resuming SQL execution")
        return state
    except Exception as exc:
        error_msg = f"Failed to execute uploaded DDL: {exc}"
        state.errors.append(error_msg)
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.requires_ddl_upload = True
        state.human_intervention_reason = error_msg
        log_event(state, "error", error_msg)
        return state


def _write_execution_log_file(state: MigrationContext, sql_files: List[str]) -> None:
    """Write a .txt file capturing the Snowflake environment and SQL statements for the latest execution."""
    from python_execution_service.app.config.settings import OUTPUT_ROOT
    from python_execution_service.domain.runs.state import RUN_LOCK, RUNS
    from python_execution_service.infrastructure.persistence.sqlite.store import RunStore

    log_dir = None
    if state.session_id:
        with RUN_LOCK:
            run = RUNS.get(state.session_id)
            if run and run.outputDir:
                log_dir = run.outputDir
        if not log_dir:
            data = RunStore.get_run(state.session_id)
            if data and data.get("outputDir"):
                log_dir = data["outputDir"]

    if not log_dir:
        log_dir = os.path.join(state.project_path, "outputs")

    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "execution_log.txt")

    lines: List[str] = []
    lines.append("=" * 70)
    lines.append(f"SQL Execution Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("Snowflake Environment:")
    lines.append(f"  Account      : {state.sf_account or 'N/A'}")
    lines.append(f"  User         : {state.sf_user or 'N/A'}")
    lines.append(f"  Role         : {state.sf_role or 'N/A'}")
    lines.append(f"  Warehouse    : {state.sf_warehouse or 'N/A'}")
    lines.append(f"  Database     : {state.sf_database or 'N/A'}")
    lines.append(f"  Schema       : {state.sf_schema or 'N/A'}")
    lines.append(f"  Authenticator: {state.sf_authenticator or 'N/A'}")
    lines.append("")
    lines.append("-" * 70)
    lines.append("SQL Content:")
    lines.append("-" * 70)

    if sql_files:
        for sql_file in sql_files:
            lines.append("")
            lines.append(f"File: {sql_file}")
            lines.append("~" * 50)
            try:
                with open(sql_file, "r", encoding="utf-8-sig") as fh:
                    sql_text = fh.read()
                if not sql_text.strip():
                    lines.append("  (empty file - skipped)")
                    continue
                lines.append(sql_text)
            except Exception as exc:
                lines.append(f"  (error reading file: {exc})")
    elif state.converted_code.strip():
        lines.append("")
        lines.append("Source: in_memory_converted_code")
        lines.append("~" * 50)
        lines.append(state.converted_code)
    else:
        lines.append("  No SQL content found.")

    lines.append("")
    lines.append("=" * 70)

    with open(log_path, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines))

    logger.info("Execution log written to %s", log_path)


def execute_sql_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Executing converted SQL for project: %s", state.project_name)
    state.current_stage = MigrationState.EXECUTE_SQL
    state.updated_at = datetime.now()
    log_event(state, "info", "Executing converted SQL")

    if state.requires_ddl_upload:
        state = apply_uploaded_ddl_and_resume(state)
        if state.requires_ddl_upload:
            return state

    converted_dir = os.path.join(state.project_path, "snowflake")
    sql_files = list_sql_files(converted_dir)
    on_statement = getattr(state, "execution_event_sink", None)

    _write_execution_log_file(state, sql_files)

    try:
        connection = build_snowflake_connection(state)
        try:
            if sql_files:
                start_index = max(0, state.last_executed_file_index + 1)
                for index in range(start_index, len(sql_files)):
                    sql_file = sql_files[index]
                    with open(sql_file, "r", encoding="utf-8-sig") as file_handle:
                        # sql_text = clean_ewi_markers(file_handle.read())
                        sql_text = file_handle.read()
                    if not sql_text.strip():
                        state.execution_log.append({"file": sql_file, "index": index, "status": "skipped_empty"})
                        state.last_executed_file_index = index
                        continue

                    def file_statement_sink(
                        entry: Dict[str, Any],
                        file_path: str = sql_file,
                        file_index: int = index,
                    ) -> None:
                        if callable(on_statement):
                            on_statement({**entry, "file": file_path, "fileIndex": file_index})

                    statement_results = execute_sql_statements(
                        connection,
                        sql_text,
                        on_statement=file_statement_sink,
                    )
                    state.execution_log.append(
                        {
                            "file": sql_file,
                            "index": index,
                            "status": "success",
                            "statements": statement_results,
                        }
                    )
                    state.last_executed_file_index = index
            elif state.converted_code.strip():
                def mem_statement_sink(entry: Dict[str, Any]) -> None:
                    if callable(on_statement):
                        on_statement({**entry, "file": "in_memory_converted_code", "fileIndex": 0})

                statement_results = execute_sql_statements(
                    connection,
                    clean_ewi_markers(state.converted_code),
                    on_statement=mem_statement_sink,
                )
                state.execution_log.append(
                    {
                        "file": "in_memory_converted_code",
                        "index": 0,
                        "status": "success",
                        "statements": statement_results,
                    }
                )
                state.last_executed_file_index = 0
            else:
                raise ValueError("No converted SQL files or converted_code found for execution.")
        finally:
            close_connection(connection)

        state.execution_passed = True
        state.execution_errors = []
        state.missing_objects = []
        state.validation_issues = []
        state.updated_at = datetime.now()
        log_event(state, "info", "Converted SQL execution completed successfully")
        return state
    except Exception as exc:
        error_message = str(exc)
        failed_statement = ""
        failed_statement_index = -1
        partial_results: List[Dict[str, Any]] = []

        if hasattr(exc, "statement"):
            failed_statement = str(getattr(exc, "statement", ""))
            failed_statement_index = int(getattr(exc, "statement_index", -1))
            partial_results = list(getattr(exc, "partial_results", []) or [])

        state.execution_passed = False
        state.execution_errors.append(
            {
                "type": "execution_error",
                "message": error_message,
                "stage": "execute_sql",
                "statement": failed_statement,
                "statement_index": failed_statement_index,
            }
        )
        state.execution_log.append(
            {
                "file": sql_files[state.last_executed_file_index + 1]
                if sql_files and state.last_executed_file_index + 1 < len(sql_files)
                else "unknown",
                "index": state.last_executed_file_index + 1,
                "status": "failed",
                "error_type": "execution_error",
                "error_message": error_message,
                "statements": partial_results,
                "failed_statement": failed_statement,
                "failed_statement_index": failed_statement_index,
            }
        )

        state.validation_issues.append(
            {"type": "execution_error", "severity": "error", "message": error_message}
        )
        log_event(state, "error", f"Execution failed, routing to self-heal: {error_message}")
        state.updated_at = datetime.now()
        return state
