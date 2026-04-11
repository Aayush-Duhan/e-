/**
 * Terminal store — per-run WebSocket terminal connections.
 *
 * Each terminal connects to /ws/terminal/agent/{runId} so that
 * concurrent runs don't leak PTY output to each other.
 */
import { atom, type WritableAtom } from 'nanostores';
import type { Terminal as XTerm } from '@xterm/xterm';

const BACKEND_HOST = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
const BACKEND_PORT = '8090'; // python execution service port

interface TerminalSession {
  terminal: XTerm;
  ws: WebSocket;
  runId: string;
}

const reset = '\x1b[0m';
const escapeCodes = {
  reset,
  red: '\x1b[1;31m',
  green: '\x1b[32m',
  dim: '\x1b[90m',
};

class TerminalStore {
  #agentTerminal: TerminalSession | null = null;

  showTerminal: WritableAtom<boolean> = atom(true);

  toggleTerminal(value?: boolean) {
    this.showTerminal.set(value !== undefined ? value : !this.showTerminal.get());
  }

  attachAgentTerminal(terminal: XTerm, runId?: string) {
    // If already connected to the same run, skip reconnection
    if (
      this.#agentTerminal?.terminal === terminal &&
      this.#agentTerminal?.runId === runId &&
      this.#agentTerminal?.ws.readyState === WebSocket.OPEN
    ) {
      return;
    }

    this.detachAgentTerminal(terminal);
    this.disconnectAgentTerminal();

    if (!runId) {
      terminal.write(`${escapeCodes.dim}No active session.${reset}\r\n`);
      return;
    }

    const cols = terminal.cols ?? 80;
    const rows = terminal.rows ?? 24;
    const wsUrl = `ws://${BACKEND_HOST}:${BACKEND_PORT}/ws/terminal/agent/${runId}?cols=${cols}&rows=${rows}`;

    terminal.write(`${escapeCodes.dim}Connecting to AI terminal...${reset}\r\n`);

    try {
      const ws = new WebSocket(wsUrl);
      this.#agentTerminal = { terminal, ws, runId };

      ws.onopen = () => {
        terminal.write('\x1b[2J\x1b[H'); // clear screen
        terminal.write(`${escapeCodes.green}[ai] connected${reset}\r\n\r\n`);
      };

      ws.onmessage = (event) => {
        terminal.write(event.data);
      };

      ws.onerror = () => {
        terminal.write(`${escapeCodes.red}\r\n[ai] terminal connection error\r\n${reset}`);
      };

      ws.onclose = (event) => {
        terminal.write(`\r\n${escapeCodes.dim}[ai] terminal disconnected (${event.code})${reset}\r\n`);

        if (this.#agentTerminal?.ws === ws) {
          this.#agentTerminal = null;
        }
      };
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      terminal.write(`${escapeCodes.red}Failed to connect: ${msg}\r\n${reset}`);
      this.#agentTerminal = null;
    }
  }

  onAgentTerminalResize(cols: number, rows: number) {
    const ws = this.#agentTerminal?.ws;

    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'resize', cols, rows }));
    }
  }

  disconnectAgentTerminal() {
    if (this.#agentTerminal) {
      this.#agentTerminal.ws.close();
      this.#agentTerminal = null;
    }
  }

  detachAgentTerminal(terminal: XTerm) {
    if (this.#agentTerminal?.terminal !== terminal) {
      return;
    }

    if (
      this.#agentTerminal.ws.readyState === WebSocket.OPEN ||
      this.#agentTerminal.ws.readyState === WebSocket.CONNECTING
    ) {
      this.#agentTerminal.ws.close();
    }

    this.#agentTerminal = null;
  }
}

export const terminalStore = new TerminalStore();
