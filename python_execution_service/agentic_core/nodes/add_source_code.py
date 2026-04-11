"""Add source code workflow node."""

import logging
import os
import shutil
from datetime import datetime

from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState
from python_execution_service.agentic_core.nodes.common import is_error_state
from python_execution_service.agentic_core.services.scai_runner import run_scai_command
from python_execution_service.agentic_core.utils.activity_log import log_event
from python_execution_service.agentic_core.utils.sql_files import list_sql_files, read_sql_files

logger = logging.getLogger(__name__)


def add_source_code_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Adding source code for project: %s", state.project_name)
    log_event(state, "info", f"Adding source code for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        source_dir_abs = os.path.abspath(source_dir)

        source_input = state.source_directory or (state.source_files[0] if state.source_files else "")
        if not source_input:
            error_msg = "No source directory provided for code add"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        source_input_abs = os.path.abspath(source_input)
        if os.path.isfile(source_input_abs):
            source_input_abs = os.path.dirname(source_input_abs)

        if not os.path.isdir(source_input_abs):
            fallback_dir = source_dir_abs
            os.makedirs(fallback_dir, exist_ok=True)
            warning_msg = (
                f"Source directory does not exist: {source_input_abs}. "
                f"Using fallback directory: {fallback_dir}"
            )
            logger.warning(warning_msg)
            state.warnings.append(warning_msg)
            log_event(state, "warning", warning_msg)
            source_input_abs = fallback_dir

        if os.path.isdir(source_dir_abs):
            shutil.rmtree(source_dir_abs)

        cmd = ["scai", "code", "add", "-i", source_input_abs]
        terminal_sink = getattr(state, "terminal_output_sink", None)

        return_code, stdout, stderr = run_scai_command(
            cmd,
            state.project_path,
            terminal_callback=terminal_sink,
            run_id=state.session_id,
        )
        if stderr:
            log_event(state, "warning", "scai code add stderr", {"stderr": stderr})

        if return_code != 0:
            error_detail = stderr or stdout or "Unknown error"
            error_msg = f"Failed to add source code: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_source_added = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        # scai code add deposits files into artifacts/source_raw/ rather than
        # source/.  If source/ is still empty after the command, copy SQL files
        # from the latest artifacts/source_raw/ snapshot so that scai code
        # convert (which reads from source/) can find them.
        if not list_sql_files(source_dir_abs):
            artifacts_raw = os.path.join(state.project_path, "artifacts", "source_raw")
            if os.path.isdir(artifacts_raw):
                # Pick the most recent timestamp sub-directory
                subdirs = sorted(
                    (d for d in os.listdir(artifacts_raw)
                     if os.path.isdir(os.path.join(artifacts_raw, d))),
                    reverse=True,
                )
                if subdirs:
                    latest_raw = os.path.join(artifacts_raw, subdirs[0])
                    raw_sql = list_sql_files(latest_raw)
                    if raw_sql:
                        os.makedirs(source_dir_abs, exist_ok=True)
                        for src_file in raw_sql:
                            dest_file = os.path.join(
                                source_dir_abs, os.path.basename(src_file)
                            )
                            shutil.copy2(src_file, dest_file)
                        log_event(
                            state, "info",
                            f"Copied {len(raw_sql)} SQL file(s) from artifacts/source_raw into source/",
                        )

        # Post-condition: source/ must contain at least one SQL file
        source_files_found = list_sql_files(source_dir_abs)
        if not source_files_found:
            error_msg = (
                "No SQL files found in source/ after scai code add. "
                "The source directory is empty despite a successful ingestion command."
            )
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_source_added = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.scai_source_added = True
        state.current_stage = MigrationState.ADD_SOURCE_CODE
        state.updated_at = datetime.now()
        logger.info("Source code added successfully (%d SQL file(s))", len(source_files_found))
        log_event(state, "info", f"Source code added successfully ({len(source_files_found)} SQL file(s))")

        if not state.original_code:
            state.original_code = read_sql_files(source_dir)
    except Exception as exc:
        error_msg = f"Exception during source code addition: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_source_added = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
