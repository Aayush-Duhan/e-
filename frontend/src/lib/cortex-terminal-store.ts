/**
 * Cortex CLI terminal store — manages a standalone interactive PTY session.
 *
 * Unlike terminal-store.ts (which is bound to migration runs), this store
 * spawns an independent Cortex CLI PTY via the backend and connects to it
 * over a bidirectional WebSocket.
 */
import type { Terminal as XTerm } from '@xterm/xterm';

const BACKEND_HOST = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
const BACKEND_PORT = '8090';

const reset = '\x1b[0m';
const esc = {
  reset,
  red: '\x1b[1;31m',
  green: '\x1b[32m',
  dim: '\x1b[90m',
};

export async function spawnCortexSession(cols: number, rows: number): Promise<string> {
  const res = await fetch('/api/cortex-terminal/spawn', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cols, rows }),
  });
  if (!res.ok) {
    throw new Error(`Failed to spawn cortex session: ${res.status}`);
  }
  const data = await res.json();
  return data.sessionId as string;
}

export function attachCortexTerminal(
  terminal: XTerm,
  sessionId: string,
): { cleanup: () => void; sendResize: (cols: number, rows: number) => void } {
  const wsUrl = `ws://${BACKEND_HOST}:${BACKEND_PORT}/ws/terminal/cortex/${sessionId}`;
  terminal.write(`${esc.dim}Connecting to Cortex CLI...${reset}\r\n`);

  const ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    terminal.write('\x1b[2J\x1b[H'); // clear screen
    terminal.write(`${esc.green}[cortex]${reset} ${esc.dim}connected${reset}\r\n\r\n`);
  };

  ws.onmessage = (event) => {
    terminal.write(event.data);
  };

  ws.onerror = () => {
    terminal.write(`${esc.red}\r\n[cortex] connection error\r\n${reset}`);
  };

  ws.onclose = (event) => {
    terminal.write(`\r\n${esc.dim}[cortex] disconnected (${event.code})${reset}\r\n`);
  };

  // Forward keystrokes from xterm → WebSocket → PTY stdin
  const onDataDisposable = terminal.onData((data) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  });

  const sendResize = (cols: number, rows: number) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'resize', cols, rows }));
    }
  };

  const cleanup = () => {
    onDataDisposable.dispose();
    ws.close();
  };

  return { cleanup, sendResize };
}
