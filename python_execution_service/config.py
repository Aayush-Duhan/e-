"""Global constants, shared mutable state, and configuration."""

import os
import threading
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_FILE)

from python_execution_service.models import RunRecord

EXECUTION_TOKEN = os.getenv("EXECUTION_TOKEN", "local-dev-token")
OUTPUT_ROOT = Path(os.getenv("PYTHON_EXEC_OUTPUT_ROOT", "outputs")).resolve()
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# ── Shared mutable state (guarded by RUN_LOCK) ─────────────────

RUN_LOCK = threading.RLock()
RUNS: dict[str, RunRecord] = {}
PROJECT_LOCKS: dict[str, str] = {}
CANCEL_FLAGS: dict[str, threading.Event] = {}

# ── Step / node metadata ────────────────────────────────────────

STEP_LABELS: dict[str, str] = {
    "init_project": "Initialize project",
    "add_source_code": "Ingest source SQL",
    "apply_schema_mapping": "Apply schema mapping",
    "convert_code": "Convert SQL",
    "execute_sql": "Execute SQL",
    "self_heal": "Self-heal fixes",
    "validate": "Validate output",
    "human_review": "Human review",
    "finalize": "Finalize output",
}

THINKING_STEP_IDS: set[str] = {"self_heal", "convert_code", "validate"}

# ── Agent configuration ─────────────────────────────────────────

AGENT_MODEL = os.getenv("CORTEX_MODEL", "claude-4-sonnet")

# ── Cortex REST API (OpenAI SDK) ────────────────────────────────

SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT", "")
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL", "")

# Per-run user message queues (thread-safe via RUN_LOCK)
USER_MESSAGE_QUEUES: dict[str, list[str]] = {}
# Per-run compiled graphs (thread-safe via RUN_LOCK)
AGENT_GRAPHS: dict[str, object] = {}
