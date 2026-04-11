"""Environment-backed application settings."""

import os
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = PACKAGE_ROOT / ".env"
load_dotenv(_ENV_FILE)

OUTPUT_ROOT = Path(os.getenv("PYTHON_EXEC_OUTPUT_ROOT", "outputs")).resolve()
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

AGENT_MODEL = os.getenv("CORTEX_MODEL", "claude-4-sonnet")
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT", "")
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL", "")
