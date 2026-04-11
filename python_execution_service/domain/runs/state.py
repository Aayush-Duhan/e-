"""Shared in-memory state for active runs."""

import threading

from python_execution_service.shared.models.runs import RunRecord

RUN_LOCK = threading.RLock()
RUNS: dict[str, RunRecord] = {}
PROJECT_LOCKS: dict[str, str] = {}
CANCEL_FLAGS: dict[str, threading.Event] = {}
USER_MESSAGE_QUEUES: dict[str, list[str]] = {}
AGENT_GRAPHS: dict[str, object] = {}
