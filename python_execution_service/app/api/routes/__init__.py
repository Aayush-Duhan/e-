"""API route registration."""

from fastapi import FastAPI

from python_execution_service.app.api.routes.health import router as health_router
from python_execution_service.app.api.routes.run_actions import router as run_actions_router
from python_execution_service.app.api.routes.runs import router as runs_router
from python_execution_service.app.api.routes.streaming import router as streaming_router


def register_routes(app: FastAPI) -> None:
    """Attach all route groups to the FastAPI application."""
    app.include_router(health_router)
    app.include_router(runs_router)
    app.include_router(run_actions_router)
    app.include_router(streaming_router)


__all__ = ["register_routes"]

