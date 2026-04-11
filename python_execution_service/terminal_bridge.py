"""Terminal broadcast bridge — per-run channels.

Bridges the synchronous PTY thread in scai_runner to async WebSocket
clients.  Each connected WebSocket subscribes to a specific run's channel
and receives only that run's raw PTY chunks in real-time.

This is the equivalent of bolt.new's PtySession tap pattern, adapted
for our architecture where scai_runner spawns its own PTY per command,
with the addition of per-run isolation so concurrent runs don't leak
terminal output to each other.
"""

import asyncio
from collections import defaultdict
from threading import Lock
from typing import Set

_channels: dict[str, Set[asyncio.Queue[str]]] = defaultdict(set)
_lock = Lock()
_loop: asyncio.AbstractEventLoop | None = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Must be called once from the async context (e.g. app startup)."""
    global _loop
    _loop = loop


def subscribe(run_id: str) -> asyncio.Queue[str]:
    """Create a new subscriber queue for a specific run."""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
    with _lock:
        _channels[run_id].add(q)
    return q


def unsubscribe(run_id: str, q: asyncio.Queue[str]) -> None:
    """Remove a subscriber queue from a run's channel."""
    with _lock:
        subs = _channels.get(run_id)
        if subs is not None:
            subs.discard(q)
            if not subs:
                del _channels[run_id]


def broadcast(run_id: str, data: str) -> None:
    """Push a raw PTY chunk to WebSocket clients subscribed to this run.

    Thread-safe — called from the sync PTY thread in scai_runner.
    Uses ``call_soon_threadsafe`` to schedule the put on the event loop.
    """
    if not data:
        return

    with _lock:
        subs = list(_channels.get(run_id, ()))

    for q in subs:
        if _loop is not None and _loop.is_running():
            _loop.call_soon_threadsafe(_safe_put, q, data)
        else:
            # Fallback: try direct put (may fail if loop not set)
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass


def _safe_put(q: asyncio.Queue[str], data: str) -> None:
    try:
        q.put_nowait(data)
    except asyncio.QueueFull:
        # Drop oldest and retry — prevents blocking the PTY
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            pass
