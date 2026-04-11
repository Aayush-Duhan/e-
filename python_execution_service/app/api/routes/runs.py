"""Run creation and retrieval routes."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from python_execution_service.app.api.routes._shared import start_run_record
from python_execution_service.domain.runs.state import RUN_LOCK, RUNS
from python_execution_service.infrastructure.persistence.sqlite.store import RunStore
from python_execution_service.shared.models.runs import StartRunRequest, StartRunResponse

router = APIRouter(tags=["runs"])


@router.post(
    "/v1/runs/start",
    response_model=StartRunResponse,
)
def start_run(request: StartRunRequest) -> StartRunResponse:
    return start_run_record(request)


@router.get("/v1/runs")
def list_runs(
    limit: int = Query(default=100, ge=1, le=500),
    status: str | None = Query(default=None),
    projectId: str | None = Query(default=None),
) -> dict[str, Any]:
    summaries = RunStore.list_runs_summary(
        limit=limit,
        status=status,
        project_id=projectId,
    )
    return {"runs": summaries}


@router.get("/v1/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    with RUN_LOCK:
        run = RUNS.get(run_id)
    if run:
        return run.model_dump()

    data = RunStore.get_run(run_id)
    if not data:
        raise HTTPException(status_code=404, detail="Run not found")
    return data
