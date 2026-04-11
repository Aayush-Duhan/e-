"use client";

/**
 * Cortex CLI Terminal — prototype page showcasing interactive
 * Cortex Code streaming via PTY from the browser.
 *
 * Spawns a cortex CLI session on the backend and connects to it
 * over a bidirectional WebSocket, rendering output in xterm.js.
 */

import { useEffect, useRef, useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Terminal as XTerm } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { WebLinksAddon } from "@xterm/addon-web-links";
import { Header } from "@/components/header";
import { spawnCortexSession, attachCortexTerminal } from "@/lib/cortex-terminal-store";
import "@xterm/xterm/css/xterm.css";

const TERMINAL_THEME = {
  background: "#0a0a0a",
  foreground: "#d7dde8",
  cursor: "#d7dde8",
  cursorAccent: "#0a0a0a",
  selectionBackground: "rgba(244, 211, 94, 0.22)",
  black: "#0a0a0a",
  red: "#ff7b72",
  green: "#7ee787",
  yellow: "#f4d35e",
  blue: "#79c0ff",
  magenta: "#d2a8ff",
  cyan: "#7ee7ff",
  white: "#d7dde8",
  brightBlack: "#6e7681",
  brightRed: "#ffa198",
  brightGreen: "#56d364",
  brightYellow: "#e3b341",
  brightBlue: "#a5d6ff",
  brightMagenta: "#e2c5ff",
  brightCyan: "#b3f0ff",
  brightWhite: "#f0f6fc",
};

/* ── Skeleton placeholder lines shown while the terminal loads ────── */

const SKELETON_LINES = [
  { width: "45%", delay: 0 },
  { width: "72%", delay: 0.05 },
  { width: "38%", delay: 0.1 },
  { width: "60%", delay: 0.15 },
  { width: "52%", delay: 0.2 },
  { width: "80%", delay: 0.25 },
  { width: "33%", delay: 0.3 },
  { width: "68%", delay: 0.35 },
  { width: "55%", delay: 0.4 },
  { width: "42%", delay: 0.45 },
  { width: "75%", delay: 0.5 },
  { width: "48%", delay: 0.55 },
  { width: "63%", delay: 0.6 },
  { width: "36%", delay: 0.65 },
  { width: "58%", delay: 0.7 },
];

function TerminalSkeleton() {
  return (
    <motion.div
      className="flex h-full flex-col gap-2.5 px-4 py-4 sm:px-5"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
    >
      {/* Fake prompt line */}
      <div className="flex items-center gap-2">
        <div className="h-3 w-3 rounded-sm bg-emerald-500/20" />
        <div className="h-2.5 w-16 rounded bg-white/[0.06]" />
        <motion.div
          className="h-4 w-[1px] bg-white/30"
          animate={{ opacity: [1, 1, 0, 0] }}
          transition={{ duration: 1, repeat: Infinity, times: [0, 0.49, 0.5, 1] }}
        />
      </div>

      {/* Skeleton output lines */}
      {SKELETON_LINES.map((line, i) => (
        <motion.div
          key={i}
          className="h-2.5 rounded bg-white/[0.04]"
          style={{ width: line.width }}
          initial={{ opacity: 0, x: -8 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{
            delay: line.delay,
            duration: 0.3,
            ease: "easeOut",
          }}
        >
          <motion.div
            className="h-full w-full rounded bg-gradient-to-r from-white/[0.02] via-white/[0.06] to-white/[0.02]"
            animate={{ backgroundPosition: ["200% 0", "-200% 0"] }}
            transition={{ duration: 2, repeat: Infinity, ease: "linear" }}
            style={{ backgroundSize: "200% 100%" }}
          />
        </motion.div>
      ))}

      {/* Loading message */}
      <div className="mt-4 flex items-center gap-2.5">
        <motion.div
          className="h-4 w-4 rounded-full border-2 border-blue-400/30 border-t-blue-400"
          animate={{ rotate: 360 }}
          transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
        />
        <span className="text-xs text-white/30">
          Spawning Cortex CLI session...
        </span>
      </div>
    </motion.div>
  );
}

/* ── Error state with retry ─────────────────────────────────────── */

function TerminalError({ onRetry }: { onRetry: () => void }) {
  return (
    <motion.div
      className="flex h-full flex-col items-center justify-center gap-4 px-4"
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className="flex flex-col items-center gap-3 text-center">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-red-500/10">
          <svg
            className="h-6 w-6 text-red-400"
            fill="none"
            viewBox="0 0 24 24"
            strokeWidth={1.5}
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M12 9v3.75m9-.75a9 9 0 1 1-18 0 9 9 0 0 1 18 0Zm-9 3.75h.008v.008H12v-.008Z"
            />
          </svg>
        </div>
        <div>
          <p className="text-sm font-medium text-white/70">
            Failed to connect to Cortex CLI
          </p>
          <p className="mt-1 max-w-xs text-xs text-white/30">
            Make sure the Python execution service is running on port 8090.
          </p>
        </div>
      </div>
      <button
        onClick={onRetry}
        className="rounded-md bg-white/[0.06] px-4 py-1.5 text-xs font-medium text-white/60 transition-colors hover:bg-white/[0.1] hover:text-white/80"
      >
        Retry connection
      </button>
    </motion.div>
  );
}

/* ── Main page component ────────────────────────────────────────── */

export default function CortexTerminalPage() {
  const containerRef = useRef<HTMLDivElement>(null);
  const terminalRef = useRef<XTerm | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);
  const sendResizeRef = useRef<((cols: number, rows: number) => void) | null>(null);
  const [status, setStatus] = useState<"connecting" | "connected" | "error">("connecting");
  const [retryKey, setRetryKey] = useState(0);

  const initTerminal = useCallback(async () => {
    const element = containerRef.current;
    if (!element) return;

    // Clean up any previous terminal instance on retry
    if (terminalRef.current) {
      cleanupRef.current?.();
      terminalRef.current.dispose();
      terminalRef.current = null;
      cleanupRef.current = null;
      sendResizeRef.current = null;
    }

    setStatus("connecting");

    const fitAddon = new FitAddon();
    const webLinksAddon = new WebLinksAddon((_event, uri) => {
      window.open(uri, "_blank");
    });

    const terminal = new XTerm({
      cursorBlink: true,
      convertEol: false,
      disableStdin: false,
      theme: TERMINAL_THEME,
      fontSize: 13,
      fontFamily:
        '"IBM Plex Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace',
      lineHeight: 1.45,
      letterSpacing: 0.2,
      scrollback: 10000,
    });

    terminalRef.current = terminal;
    terminal.loadAddon(fitAddon);
    terminal.loadAddon(webLinksAddon);
    terminal.open(element);

    // Fit after layout settles
    const fitTerminal = () => {
      if (element.offsetWidth > 0 && element.offsetHeight > 0) {
        try {
          fitAddon.fit();
        } catch {
          // ignore
        }
        sendResizeRef.current?.(terminal.cols, terminal.rows);
      }
    };
    setTimeout(fitTerminal, 0);

    const resizeObserver = new ResizeObserver(fitTerminal);
    resizeObserver.observe(element);

    try {
      const cols = terminal.cols ?? 80;
      const rows = terminal.rows ?? 24;
      const sessionId = await spawnCortexSession(cols, rows);
      const { cleanup, sendResize } = attachCortexTerminal(terminal, sessionId);
      cleanupRef.current = cleanup;
      sendResizeRef.current = sendResize;
      setStatus("connected");
    } catch {
      setStatus("error");
    }

    return () => {
      resizeObserver.disconnect();
      cleanupRef.current?.();
      terminal.dispose();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [retryKey]);

  useEffect(() => {
    let disposed = false;
    let disposeTerminal: (() => void) | undefined;

    initTerminal().then((dispose) => {
      if (disposed) {
        dispose?.();
      } else {
        disposeTerminal = dispose;
      }
    });

    return () => {
      disposed = true;
      disposeTerminal?.();
    };
  }, [initTerminal]);

  const handleRetry = useCallback(() => {
    setRetryKey((k) => k + 1);
  }, []);

  const statusLabel =
    status === "connected"
      ? "Connected"
      : status === "error"
        ? "Disconnected"
        : "Connecting...";

  const statusDotClass =
    status === "connected"
      ? "bg-emerald-400"
      : status === "error"
        ? "bg-red-400"
        : "bg-blue-400 animate-pulse";

  const statusBadgeClass =
    status === "connected"
      ? "bg-emerald-500/10 text-emerald-400"
      : status === "error"
        ? "bg-red-500/10 text-red-400"
        : "bg-blue-500/10 text-blue-400";

  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#07080c]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header />

      <div className="flex min-h-0 flex-1 flex-col">
        {/* Toolbar */}
        <div className="flex items-center gap-2 border-b border-white/[0.06] bg-[#0a0a0a] px-3 py-2 sm:gap-3 sm:px-4">
          <div className="flex items-center gap-2">
            <div className="flex gap-1.5">
              <div className="h-2.5 w-2.5 rounded-full bg-[#ff5f57]" />
              <div className="h-2.5 w-2.5 rounded-full bg-[#febc2e]" />
              <div className="h-2.5 w-2.5 rounded-full bg-[#28c840]" />
            </div>
            <span className="ml-1 hidden text-xs font-medium tracking-wider text-white/40 uppercase sm:ml-2 sm:inline">
              Cortex Code CLI
            </span>
          </div>

          <div className="ml-auto flex items-center gap-2">
            <span
              className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-medium sm:px-2.5 ${statusBadgeClass}`}
            >
              <span className={`h-1.5 w-1.5 rounded-full ${statusDotClass}`} />
              {statusLabel}
            </span>
          </div>
        </div>

        {/* Terminal area */}
        <div className="relative min-h-0 flex-1">
          {/* Overlay skeleton / error on top of the terminal container */}
          <AnimatePresence>
            {status === "connecting" && (
              <motion.div
                key="skeleton"
                className="absolute inset-0 z-10 bg-[#0a0a0a]"
                exit={{ opacity: 0 }}
                transition={{ duration: 0.4 }}
              >
                <TerminalSkeleton />
              </motion.div>
            )}
            {status === "error" && (
              <motion.div
                key="error"
                className="absolute inset-0 z-10 bg-[#0a0a0a]"
                exit={{ opacity: 0 }}
                transition={{ duration: 0.3 }}
              >
                <TerminalError onRetry={handleRetry} />
              </motion.div>
            )}
          </AnimatePresence>

          {/* Actual xterm container — always mounted so it can init in background */}
          <div
            ref={containerRef}
            className="h-full w-full px-2 py-2 sm:px-3"
            onWheel={(e) => e.stopPropagation()}
          />
        </div>
      </div>
    </div>
  );
}
