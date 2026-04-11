"""FastAPI application entrypoint."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

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
