"""FastAPI application entrypoint."""

import logging
import sys
import pathlib
from contextlib import asynccontextmanager

from fastapi import FastAPI

# Ensure the parent workspace directory is in sys.path so that absolute imports work regardless of cwd
workspace_root = pathlib.Path(__file__).resolve().parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

from python_execution_service.app.api.routes import register_routes
from python_execution_service.domain.runs.service import load_persisted_runs
from python_execution_service.infrastructure.persistence.sqlite import store as sqlite_store

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sqlite_store.init_schema()
    load_persisted_runs()

    import asyncio
    from python_execution_service.infrastructure.runtime import terminal_bridge

    loop = asyncio.get_running_loop()
    terminal_bridge.set_event_loop(loop)

    yield


app = FastAPI(title="Python Execution Service", version="0.1.0", lifespan=lifespan)

register_routes(app)
