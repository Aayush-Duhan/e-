"""Cortex CLI PTY session manager.

Manages long-lived interactive PTY sessions for the Cortex CLI,
independent of migration runs. Each session gets a UUID and supports
bidirectional I/O (stdin writes, stdout reads) over async queues
that feed into WebSocket connections.
"""

import asyncio
import logging
import threading
import uuid
from typing import Optional

from winpty import PtyProcess  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_sessions: dict[str, "CortexPtySession"] = {}
_loop: Optional[asyncio.AbstractEventLoop] = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Must be called once from the async context (e.g. app startup)."""
    global _loop
    _loop = loop


class CortexPtySession:
    """A single interactive PTY running the cortex CLI."""

    def __init__(self, session_id: str, cols: int = 80, rows: int = 24) -> None:
        self.session_id = session_id
        self.cols = cols
        self.rows = rows
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._sub_lock = threading.Lock()
        self._proc: Optional[PtyProcess] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stopped = threading.Event()

    def start(self, command: str = "cortex") -> None:
        """Spawn the PTY process and start the reader thread."""
        self._proc = PtyProcess.spawn(
            [command],
            dimensions=(self.rows, self.cols),
        )
        self._reader_thread = threading.Thread(
            target=self._read_loop,
            daemon=True,
        )
        self._reader_thread.start()
        logger.info("Cortex PTY session %s started (pid via winpty)", self.session_id)

    def _read_loop(self) -> None:
        """Read PTY output in a background thread and broadcast to subscribers."""
        proc = self._proc
        if proc is None:
            return

        while not self._stopped.is_set() and proc.isalive():
            try:
                chunk = proc.read(4096)
            except EOFError:
                break
            except Exception:
                if self._stopped.is_set():
                    break
                continue

            if chunk:
                self._broadcast(chunk)

        # Process exited — notify subscribers
        self._broadcast("\r\n\x1b[90m[cortex] session ended\x1b[0m\r\n")
        logger.info("Cortex PTY session %s reader loop ended", self.session_id)

    def _broadcast(self, data: str) -> None:
        """Push a chunk to all subscribed async queues (thread-safe)."""
        if not data:
            return

        with self._sub_lock:
            subs = list(self._subscribers)

        for q in subs:
            if _loop is not None and _loop.is_running():
                _loop.call_soon_threadsafe(_safe_put, q, data)
            else:
                try:
                    q.put_nowait(data)
                except asyncio.QueueFull:
                    pass

    def write(self, data: str) -> None:
        """Send keystrokes to the PTY stdin."""
        if self._proc is not None and self._proc.isalive():
            self._proc.write(data)

    def resize(self, cols: int, rows: int) -> None:
        """Resize the PTY."""
        self.cols = cols
        self.rows = rows
        if self._proc is not None and self._proc.isalive():
            try:
                self._proc.setwinsize(rows, cols)
            except Exception:
                pass

    def subscribe(self) -> asyncio.Queue[str]:
        """Create a new subscriber queue for this session."""
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        with self._sub_lock:
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[str]) -> None:
        """Remove a subscriber queue."""
        with self._sub_lock:
            self._subscribers.discard(q)

    def kill(self) -> None:
        """Terminate the PTY process and clean up."""
        self._stopped.set()
        if self._proc is not None:
            try:
                self._proc.close()
            except Exception:
                pass
        logger.info("Cortex PTY session %s killed", self.session_id)

    @property
    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.isalive()


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


# ── Public API ────────────────────────────────────────────────────


def spawn_session(cols: int = 80, rows: int = 24) -> str:
    """Create and start a new Cortex PTY session. Returns the session ID."""
    session_id = str(uuid.uuid4())
    session = CortexPtySession(session_id, cols=cols, rows=rows)
    session.start()
    with _lock:
        _sessions[session_id] = session
    return session_id


def get_session(session_id: str) -> Optional[CortexPtySession]:
    """Look up an active session by ID."""
    with _lock:
        return _sessions.get(session_id)


def kill_session(session_id: str) -> bool:
    """Kill and remove a session. Returns True if found."""
    with _lock:
        session = _sessions.pop(session_id, None)
    if session is not None:
        session.kill()
        return True
    return False
