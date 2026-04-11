"""Shared node helpers."""

from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState


def is_error_state(state: MigrationContext) -> bool:
    """Return True if the workflow is already in an error state."""
    return state.current_stage == MigrationState.ERROR
