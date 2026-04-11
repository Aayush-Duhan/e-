"""Python Execution Service – FastAPI application entrypoint."""

import logging
import sys
from pathlib import Path

# Allow running from inside the package directory (e.g. `uvicorn main:app`)
# by ensuring the parent directory is on sys.path so that
# `from python_execution_service import ...` works.
_parent = str(Path(__file__).resolve().parent.parent)
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from python_execution_service import sqlite_store
from python_execution_service.helpers import load_persisted_runs
from python_execution_service.routes import register_routes

logger = logging.getLogger(__name__)

app = FastAPI(title="Python Execution Service", version="0.1.0")


# ── Exception handlers ──────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/v1/runs/start":
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", errors="replace")
        logger.error(
            "Validation error on /v1/runs/start. body=%s errors=%s",
            body_text,
            exc.errors(),
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


# ── Startup ─────────────────────────────────────────────────────

sqlite_store.init_schema()
load_persisted_runs()
register_routes(app)


@app.on_event("startup")
async def _init_terminal_bridge() -> None:
    """Give the terminal bridge and cortex PTY manager a reference to the running event loop."""
    import asyncio
    from python_execution_service import terminal_bridge, cortex_pty
    loop = asyncio.get_running_loop()
    terminal_bridge.set_event_loop(loop)
    cortex_pty.set_event_loop(loop)
