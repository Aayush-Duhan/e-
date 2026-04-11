"""Apply schema mapping workflow node."""

import logging
import os
import shutil
from datetime import datetime

from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState
from python_execution_service.agentic_core.nodes.common import is_error_state
from python_execution_service.agentic_core.services.schema_mapping import process_sql_with_pandas_replace
from python_execution_service.agentic_core.utils.activity_log import log_event
from python_execution_service.agentic_core.utils.sql_files import list_sql_files, read_sql_files

logger = logging.getLogger(__name__)


def apply_schema_mapping_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Applying schema mapping for project: %s", state.project_name)
    log_event(state, "info", f"Applying schema mapping for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        mapping_path = (state.mapping_csv_path or "").strip()

        if not mapping_path:
            msg = "No schema mapping file provided; skipping schema mapping step."
            logger.info(msg)
            log_event(state, "info", msg)
            state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
            state.updated_at = datetime.now()
            state.schema_mapped_code = read_sql_files(source_dir)
            return state

        mapped_dir = os.path.join(state.project_path, "source_mapped")
        os.makedirs(mapped_dir, exist_ok=True)

        process_sql_with_pandas_replace(
            csv_file_path=mapping_path,
            sql_file_path=source_dir,
            output_dir=mapped_dir,
        )

        # Only replace source/ with source_mapped/ if the mapped dir actually
        # contains SQL files.  Previous logic deleted source/ unconditionally
        # then moved source_mapped/ in -- if source_mapped/ was empty or the
        # move failed, source/ was left deleted and convert_code would fail
        # with CVT0012 ("0 SQL files").
        mapped_sql_files = list_sql_files(mapped_dir)
        if mapped_sql_files:
            if os.path.isdir(source_dir):
                shutil.rmtree(source_dir)
            shutil.move(mapped_dir, source_dir)
            log_event(
                state, "info",
                f"Replaced source/ with {len(mapped_sql_files)} mapped file(s)",
            )
        else:
            # mapped_dir is empty -- keep original source/ intact
            shutil.rmtree(mapped_dir, ignore_errors=True)
            warning_msg = (
                "Schema mapping produced no output files; keeping original source/ intact."
            )
            state.warnings.append(warning_msg)
            logger.warning(warning_msg)
            log_event(state, "warning", warning_msg)

        # Post-condition: source/ must still contain SQL files
        source_files_found = list_sql_files(source_dir)
        if not source_files_found:
            error_msg = (
                "No SQL files found in source/ after schema mapping. "
                "The source directory was emptied during the mapping step."
            )
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
        state.updated_at = datetime.now()
        logger.info("Schema mapping applied successfully (%d SQL file(s))", len(source_files_found))
        log_event(state, "info", f"Schema mapping applied successfully ({len(source_files_found)} SQL file(s))")
        state.schema_mapped_code = read_sql_files(source_dir)
    except Exception as exc:
        error_msg = f"Exception during schema mapping: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
