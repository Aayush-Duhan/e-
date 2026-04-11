"""Streaming routes for run events and terminal output."""

import asyncio
import json
import time

from fastapi import APIRouter, Header, HTTPException, Query, WebSocket, WebSocketDisconnect

from python_execution_service.domain.runs.state import RUN_LOCK, RUNS

router = APIRouter(tags=["streaming"])


@router.get("/v1/runs/{run_id}/events")
async def stream_events(
    run_id: str,
    last_event_index: int | None = Query(default=None),
    last_event_id: str | None = Header(default=None),
):
    with RUN_LOCK:
        if run_id not in RUNS:
            raise HTTPException(status_code=404, detail="Run not found")

    from fastapi.responses import StreamingResponse

    async def iterator():
        idx = 0
        if last_event_index is not None:
            idx = max(0, last_event_index)
        elif last_event_id is not None:
            try:
                idx = max(0, int(last_event_id) + 1)
            except ValueError:
                idx = 0
        heartbeat_at = time.time()
        while True:
            with RUN_LOCK:
                run = RUNS.get(run_id)
                if not run:
                    break
                events = run.events[idx:]
                status = run.status
                total_events = len(run.events)
            for event in events:
                event_id = idx
                yield f"event: {event['type']}\n".encode("utf-8")
                yield f"id: {event_id}\n".encode("utf-8")
                payload = event.get("payload", {})
                yield f"data: {json.dumps(payload)}\n\n".encode("utf-8")
                idx += 1
            now = time.time()
            if now - heartbeat_at >= 20:
                heartbeat_at = now
                yield b": heartbeat\n\n"
            if status in ("completed", "failed", "canceled", "awaiting_input") and idx >= total_events:
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(iterator(), media_type="text/event-stream")


@router.websocket("/ws/terminal/agent/{run_id}")
async def ws_agent_terminal(websocket: WebSocket, run_id: str) -> None:
    """Stream raw PTY output for a specific run to the frontend terminal."""
    from python_execution_service.infrastructure.runtime import terminal_bridge

    with RUN_LOCK:
        if run_id not in RUNS:
            await websocket.close(code=4004, reason="Run not found")
            return

    await websocket.accept()
    queue = terminal_bridge.subscribe(run_id)

    async def reader() -> None:
        try:
            while True:
                data = await queue.get()
                if data:
                    await websocket.send_text(data)
        except Exception:
            pass

    reader_task = asyncio.create_task(reader())

    try:
        while True:
            raw = await websocket.receive_text()
            if raw.startswith("{"):
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "resize":
                        continue
                except (ValueError, KeyError):
                    pass
    except WebSocketDisconnect:
        pass
    finally:
        reader_task.cancel()
        terminal_bridge.unsubscribe(run_id, queue)
