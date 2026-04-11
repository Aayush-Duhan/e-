import * as React from "react";

import { v4 as uuidv4 } from "uuid";
import { workbenchStore, type TerminalCommand } from "@/lib/workbench-store";
import type { StepState } from "@/lib/migration-types";
import {
  STEP_BLUEPRINT,
  type ChatMessage,
  type ExecuteStatementEvent,
  type ExecuteErrorEvent,
} from "@/lib/chat-types";
import {
  isActive,
  makeMessage,
  mergeSteps,
  buildTasks,
  flattenExecutionLog,
  makeSqlStatementMessage,
  makeSqlErrorMessage,
  buildSqlExecutionMessages,
} from "@/lib/chat-helpers";
import { getWizardState } from "@/lib/wizard-store";
import type { Task } from "@/components/ui/agent-plan";

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.id === "string" &&
    typeof row.role === "string" &&
    typeof row.kind === "string" &&
    typeof row.content === "string"
  );
}

function buildHydratedTerminalCommands(
  events: unknown,
  _messages: unknown,
  _logs: unknown,
): TerminalCommand[] {
  void _messages;
  void _logs;
  const commandMap = new Map<string, TerminalCommand>();
  const commandOrder: string[] = [];
  let autoCounter = 0;

  const getOrCreateCommand = (stepId?: string, label?: string): TerminalCommand => {
    const key = stepId || `_auto_${autoCounter++}`;
    let cmd = commandMap.get(key);
    if (!cmd) {
      cmd = {
        id: key,
        label: label || (stepId ? `$ ${stepId}` : "$ Terminal Output"),
        stepId: stepId || undefined,
        lines: [],
        isComplete: true,
        ts: Date.now() + commandOrder.length,
      };
      commandMap.set(key, cmd);
      commandOrder.push(key);
    }
    return cmd;
  };

  if (Array.isArray(events)) {
    let currentStepId: string | undefined;
    let currentLabel: string | undefined;

    for (const raw of events) {
      if (!raw || typeof raw !== "object") continue;
      const event = raw as { type?: string; payload?: Record<string, unknown> };
      const payload = event.payload && typeof event.payload === "object" ? event.payload : {};

      if (event.type === "step:started") {
        currentStepId = typeof payload.stepId === "string" ? payload.stepId : undefined;
        currentLabel = typeof payload.label === "string" ? `$ ${payload.label}` : undefined;
        if (currentStepId) {
          getOrCreateCommand(currentStepId, currentLabel);
        }
        continue;
      }

      if (event.type === "step:completed") {
        continue;
      }

      if (event.type === "terminal:output") {
        const text = typeof payload.text === "string" ? payload.text : "";
        if (text.trim().length === 0) continue;
        const stepId = typeof payload.stepId === "string" ? payload.stepId : currentStepId;
        const stepLabel = typeof payload.stepLabel === "string" ? `$ ${payload.stepLabel}` : currentLabel;
        const cmd = getOrCreateCommand(stepId, stepLabel);
        cmd.lines.push({ text, isProgress: Boolean(payload.isProgress), ts: Date.now() });
        continue;
      }
    }
  }

  if (commandOrder.length > 0) {
    return commandOrder.map((key) => commandMap.get(key)!).filter((cmd) => cmd.lines.length > 0);
  }

  return [];
}

const THINKING_STEPS = ["convert_code", "validate"];

function buildHydratedMessagesFallback(
  events: unknown,
  _logs: unknown,
  statements: ExecuteStatementEvent[],
  errors: ExecuteErrorEvent[],
): ChatMessage[] {
  void _logs;
  const timeline: ChatMessage[] = [];
  let usedEventTimeline = false;

  if (Array.isArray(events)) {
    const hasChatMessageEvents = events.some((raw) => {
      if (!raw || typeof raw !== "object") return false;
      const event = raw as { type?: string; payload?: Record<string, unknown> };
      if (event.type !== "chat:message") return false;
      return isChatMessage(event.payload);
    });

    for (const raw of events) {
      if (!raw || typeof raw !== "object") continue;
      const event = raw as { type?: string; payload?: Record<string, unknown> };
      const type = typeof event.type === "string" ? event.type : "";
      const payload = event.payload && typeof event.payload === "object" ? event.payload : {};

      if (type === "chat:message" && isChatMessage(payload)) {
        usedEventTimeline = true;
        timeline.push(payload);
        continue;
      }

      if (hasChatMessageEvents) continue;

      if (type === "step:started") {
        usedEventTimeline = true;
        const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
        const label = typeof payload.label === "string" ? payload.label : stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Starting: ${label}.`, "step_started", undefined, stepId ? { id: stepId, label } : undefined));
        }
        continue;
      }

      if (type === "step:completed") {
        usedEventTimeline = true;
        const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
        const label = typeof payload.label === "string" ? payload.label : stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Completed: ${label}.`, "step_completed", undefined, stepId ? { id: stepId, label } : undefined));
        }
        continue;
      }

      if (type === "log") continue;

      if (type === "execute_sql:statement") {
        usedEventTimeline = true;
        timeline.push(makeSqlStatementMessage(payload as ExecuteStatementEvent));
        continue;
      }

      if (type === "execute_sql:error") {
        usedEventTimeline = true;
        const executeError = payload as ExecuteErrorEvent;
        const missing =
          (executeError.errorType ?? "").toLowerCase().includes("missing") ||
          (executeError.errorMessage ?? "").toLowerCase().includes("does not exist");
        timeline.push(
          makeSqlErrorMessage(
            executeError,
            missing ? "Execution paused: missing table/object detected." : undefined,
          ),
        );
        continue;
      }

      if (type === "run:completed") {
        usedEventTimeline = true;
        timeline.push(makeMessage("system", "Migration completed.", "run_status"));
        continue;
      }

      if (type === "run:failed") {
        usedEventTimeline = true;
        const reason = typeof payload.reason === "string" ? payload.reason : "Run failed";
        timeline.push(makeMessage("error", reason, "run_status"));
      }

      if (type === "run:awaiting_input") {
        usedEventTimeline = true;
        const reason = typeof payload.reason === "string" ? payload.reason : "Agent is waiting for your input";
        timeline.push(makeMessage("system", reason, "run_status"));
      }
    }
  }

  if (!usedEventTimeline) {
    return buildSqlExecutionMessages(statements, errors);
  }

  return timeline;
}

export interface UseSessionRunReturn {
  runId: string | null;
  projectId: string | null;
  isHydratingRouteRun: boolean;
  status: string;
  error: string | null;
  isBusy: boolean;
  isCanceling: boolean;
  messages: ChatMessage[];
  tasks: Task[];
  requiresDdlUpload: boolean;
  resumeFromStage: string;
  lastExecutedFileIndex: number;
  missingObjects: string[];
  isAgentThinking: boolean;
  streamingContent: { id: string; content: string } | null;
  selectedSessionId: string | null;
  sidebarReloadKey: number;
  handleConfirm: () => Promise<void>;
  retryRun: () => Promise<void>;
  cancelRun: () => Promise<void>;
  handleResumeWithDdl: (ddlFile: File) => Promise<void>;
  sendAgentMessage: ((message: string) => Promise<void>) | undefined;
}

export function useSessionRun(routeRunId: string | null): UseSessionRunReturn {
  const [projectId, setProjectId] = React.useState<string | null>(null);
  const [sourceId, setSourceId] = React.useState<string | null>(null);
  const [schemaId, setSchemaId] = React.useState<string | null>(null);
  const [runId, setRunId] = React.useState<string | null>(null);
  const isHydratingRouteRun = Boolean(routeRunId) && runId !== routeRunId;

  const [steps, setSteps] = React.useState<StepState[]>(STEP_BLUEPRINT);
  const [status, setStatus] = React.useState("idle");
  const [error, setError] = React.useState<string | null>(null);
  const [isBusy, setIsBusy] = React.useState(false);
  const [isCanceling, setIsCanceling] = React.useState(false);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);

  const [selectedSessionId, setSelectedSessionId] = React.useState<string | null>(null);
  const [sidebarReloadKey, setSidebarReloadKey] = React.useState(0);
  const reloadSidebar = React.useCallback(() => setSidebarReloadKey((k) => k + 1), []);

  const [, setExecuteStatements] = React.useState<ExecuteStatementEvent[]>([]);
  const [, setExecuteErrors] = React.useState<ExecuteErrorEvent[]>([]);

  const [requiresDdlUpload, setRequiresDdlUpload] = React.useState(false);
  const [missingObjects, setMissingObjects] = React.useState<string[]>([]);
  const [resumeFromStage, setResumeFromStage] = React.useState("");
  const [lastExecutedFileIndex, setLastExecutedFileIndex] = React.useState(-1);

  const [isAgentThinking, setIsAgentThinking] = React.useState(false);
  const [streamingContent, setStreamingContent] = React.useState<{ id: string; content: string } | null>(null);
  const streamingContentRef = React.useRef<{ id: string; content: string } | null>(null);
  const activeStepRef = React.useRef<string | null>(null);
  const chatSchemaReadyRef = React.useRef(false);

  // Highest SSE event id we've processed. Used both as the start offset for new
  // SSE connections and as a dedup guard so replayed events are ignored.
  const lastSeenEventIdRef = React.useRef<number>(-1);

  const tasks = React.useMemo(() => buildTasks(steps, status), [steps, status]);

  const isRunActive = isActive(status);

  // Keep ref in sync so SSE event handlers can read latest streaming content
  React.useEffect(() => {
    streamingContentRef.current = streamingContent;
  }, [streamingContent]);

  /* -- Hydrate an existing run ------------------------------------------------------------------- */
  const hydrateRun = React.useCallback(async (targetRunId: string) => {
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    lastSeenEventIdRef.current = -1;
    workbenchStore.clearTerminal();
    const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
    if (!res.ok) { setError("Unable to load session"); setIsBusy(false); return; }
    const data = await res.json();

    setRunId(targetRunId);
    setSelectedSessionId(targetRunId);
    const hydratedStatus = data.status ?? "idle";
    setStatus(hydratedStatus);
    if (!isActive(hydratedStatus)) {
      setIsCanceling(false);
    }
    setSteps(mergeSteps(data.steps));
    setProjectId(data.projectId ?? null);
    setSourceId(data.sourceId ?? null);
    setSchemaId(data.schemaId ?? null);

    const s = flattenExecutionLog(data.executionLog);
    const e: ExecuteErrorEvent[] = Array.isArray(data.executionErrors) ? data.executionErrors : [];
    setExecuteStatements(s);
    setExecuteErrors(e);
    setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
    setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
    setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
    setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
    workbenchStore.replaceTerminalCommands(
      buildHydratedTerminalCommands(data.events, data.messages, data.logs),
    );

    const hydratedMessages = Array.isArray(data.messages)
      ? data.messages.filter(isChatMessage)
      : [];
    if (hydratedMessages.length > 0) {
      chatSchemaReadyRef.current = true;
      setMessages(hydratedMessages);
    } else {
      chatSchemaReadyRef.current = false;
      setMessages(buildHydratedMessagesFallback(data.events, data.logs, s, e));
    }
    if (hydratedStatus !== "awaiting_input" && typeof data.error === "string" && data.error.length) setError(data.error);
    setIsBusy(false);
  }, []);

  /* -- Reconcile snapshot ------------------------------------------------------------------------ */
  const reconcileRunSnapshot = React.useCallback(async (targetRunId: string) => {
    try {
      const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      console.log("[DEBUG-FOLLOWUP] reconcileRunSnapshot", {
        targetRunId,
        status: data.status,
        error: data.error,
        msgCount: Array.isArray(data.messages) ? data.messages.length : 0,
        eventCount: Array.isArray(data.events) ? data.events.length : 0,
      });
      const snapshotMessages = Array.isArray(data.messages)
        ? data.messages.filter(isChatMessage)
        : [];
      if (snapshotMessages.length > 0) {
        chatSchemaReadyRef.current = true;
        setMessages(snapshotMessages);
      } else if (!chatSchemaReadyRef.current) {
        const ss = flattenExecutionLog(data.executionLog);
        const se: ExecuteErrorEvent[] = Array.isArray(data.executionErrors) ? data.executionErrors : [];
        setMessages(buildHydratedMessagesFallback(data.events, data.logs, ss, se));
      }
      const nextStatus = typeof data.status === "string" ? data.status : "idle";
      console.log("[DEBUG-FOLLOWUP] reconcileRunSnapshot setting status", { nextStatus, prevError: data.error });
      if (!isActive(nextStatus)) {
        workbenchStore.replaceTerminalCommands(
          buildHydratedTerminalCommands(data.events, data.messages, data.logs),
        );
      }

      setStatus(nextStatus);
      if (!isActive(nextStatus)) {
        setIsCanceling(false);
      }
      setSteps(mergeSteps(data.steps));
      setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
      setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
      setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
      setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
      if (nextStatus === "awaiting_input") {
        // awaiting_input is not an error — the reason is stored in the error field
        // on the backend but should not be shown as an error in the UI.
        setError(null);
      } else if (typeof data.error === "string" && data.error.length > 0) {
        setError(data.error);
      } else {
        setError(null);
      }

      if (nextStatus === "completed" || nextStatus === "failed" || nextStatus === "canceled" || nextStatus === "awaiting_input") {
        setIsAgentThinking(false);
        activeStepRef.current = null;
        setIsCanceling(false);
        reloadSidebar();
      }
    } catch {
      // best-effort
    }
  }, [reloadSidebar]);

  /* -- Upload helpers ---------------------------------------------------------------------------- */
  const uploadSource = async (pid: string, f: File) => {
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", f);
    const res = await fetch(`/api/projects/${pid}/source`, { method: "POST", body: fd });
    if (!res.ok) { setError("Upload failed"); setIsBusy(false); return; }
    const data = await res.json();
    setSourceId(data.sourceId);
    setIsBusy(false);
    return data.sourceId as string;
  };

  const uploadSchema = async (pid: string, f: File) => {
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", f);
    const res = await fetch(`/api/projects/${pid}/schema`, { method: "POST", body: fd });
    if (!res.ok) { setError("Schema upload failed"); setIsBusy(false); return; }
    const data = await res.json();
    setSchemaId(data.schemaId);
    setIsBusy(false);
    return data.schemaId as string;
  };

  /* -- Start / retry run ------------------------------------------------------------------------- */
  const startRun = async (
    pid?: string,
    sid?: string,
    scid?: string,
    lang?: string,
    creds?: {
      sfAccount?: string;
      sfUser?: string;
      sfRole?: string;
      sfWarehouse?: string;
      sfDatabase?: string;
      sfSchema?: string;
      sfAuthenticator?: string;
    },
  ) => {
    const activePid = pid ?? projectId;
    const activeSid = sid ?? sourceId;
    const activeScid = scid ?? schemaId ?? undefined;
    if (!activePid || !activeSid) return;

    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    lastSeenEventIdRef.current = -1;
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    workbenchStore.clearTerminal();

    const res = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        projectId: activePid,
        sourceId: activeSid,
        schemaId: activeScid,
        sourceLanguage: lang,
        ...(creds ?? {}),
      }),
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      setError(typeof payload?.error === "string" ? payload.error : "Failed to start run");
      setIsBusy(false);
      return;
    }

    const data = await res.json();
    setRunId(data.runId);
    setSelectedSessionId(data.runId);
    setStatus("running");
    setIsCanceling(false);
    setIsBusy(false);
    reloadSidebar();
    window.history.replaceState(null, "", `/sessions/${data.runId}`);
  };

  const retryRun = React.useCallback(async () => {
    if (!runId) return;
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    lastSeenEventIdRef.current = -1;
    const res = await fetch(`/api/runs/${runId}/retry`, { method: "POST" });
    if (!res.ok) { setError("Retry failed"); setIsBusy(false); return; }

    const data = await res.json();
    setRunId(data.runId);
    setSelectedSessionId(data.runId);
    setStatus("running");
    setIsCanceling(false);
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    workbenchStore.clearTerminal();
    setRequiresDdlUpload(false);
    setMissingObjects([]);
    setResumeFromStage("");
    setLastExecutedFileIndex(-1);
    setIsBusy(false);
    reloadSidebar();
    window.history.replaceState(null, "", `/sessions/${data.runId}`);
  }, [runId, reloadSidebar]);

  /* -- DDL resume -------------------------------------------------------------------------------- */
  const handleResumeWithDdl = React.useCallback(async (ddlFile: File) => {
    if (!runId) return;
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    try {
      const fd = new FormData();
      fd.append("ddlFile", ddlFile);
      fd.append("resumeFromStage", resumeFromStage || "execute_sql");
      fd.append("lastExecutedFileIndex", String(lastExecutedFileIndex));
      fd.append("missingObjects", JSON.stringify(missingObjects));

      const res = await fetch(`/api/runs/${runId}/resume`, { method: "POST", body: fd });
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        setError(payload?.error ?? "Resume failed");
        setIsBusy(false);
        return;
      }

      const data = await res.json();
      setRunId(data.runId);
      setSelectedSessionId(data.runId);
      setStatus("running");
      setIsCanceling(false);
      setSteps(STEP_BLUEPRINT);
      setMessages((prev) => [
        ...prev,
        makeMessage("system", `Uploaded DDL (${ddlFile.name}). Resuming from checkpoint.`),
      ]);
      setExecuteStatements([]);
      setExecuteErrors([]);
      setRequiresDdlUpload(false);
      setMissingObjects([]);
      setResumeFromStage("");
      setLastExecutedFileIndex(-1);
      setIsBusy(false);
      reloadSidebar();
      window.history.replaceState(null, "", `/sessions/${data.runId}`);
    } catch {
      setError("Resume failed");
      setIsBusy(false);
    }
  }, [runId, resumeFromStage, lastExecutedFileIndex, missingObjects, reloadSidebar]);

  /* -- Confirm project creation ------------------------------------------------------------------ */
  const handleConfirm = React.useCallback(async () => {
    const wizardState = getWizardState();
    const wizardSourceFiles = wizardState.sourceFiles;
    const wizardMappingFiles = wizardState.mappingFiles;
    const wizardLanguage = wizardState.sourceLanguage;

    const sourceFilesToUpload = wizardSourceFiles.filter((f) => f.file).map((f) => f.file!);
    const mappingFileToUpload = wizardMappingFiles[0]?.file ?? null;

    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    const projectName = uuidv4();

    try {
      const res = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: projectName, sourceLanguage: wizardLanguage }),
      });
      if (!res.ok) {
        setError("Unable to create project");
        setIsBusy(false);
        return;
      }

      const data = await res.json();
      setProjectId(data.projectId);
      setMessages((prev) => [...prev, makeMessage("system", `Project created: ${projectName}`)]);

      let firstSourceId: string | null = null;
      for (const file of sourceFilesToUpload) {
        const sid = (await uploadSource(data.projectId, file)) ?? null;
        if (!firstSourceId && sid) firstSourceId = sid;
      }
      const uploadedSchemaId = mappingFileToUpload ? (await uploadSchema(data.projectId, mappingFileToUpload)) ?? null : null;

      if (firstSourceId) {
        await startRun(data.projectId, firstSourceId, uploadedSchemaId ?? undefined, wizardLanguage, {
          sfAccount: wizardState.sfAccount,
          sfUser: wizardState.sfUser,
          sfRole: wizardState.sfRole,
          sfWarehouse: wizardState.sfWarehouse,
          sfDatabase: wizardState.sfDatabase,
          sfSchema: wizardState.sfSchema,
          sfAuthenticator: wizardState.sfAuthenticator,
        });
      } else {
        setError("Uploads incomplete. Please retry attaching files.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start migration");
    } finally {
      setIsBusy(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* -- Cancel run ------------------------------------------------------------------------------- */
  const cancelRun = React.useCallback(async () => {
    if (!runId || !isActive(status) || isCanceling) return;
    setIsCanceling(true);
    setStatus("canceled");
    setIsAgentThinking(false);
    activeStepRef.current = null;
    try {
      const res = await fetch(`/api/runs/${runId}/cancel`, { method: "POST" });
      if (!res.ok) {
        setError("Unable to cancel run");
        setIsCanceling(false);
        return;
      }
      setMessages((prev) => [
        ...prev,
        makeMessage("system", "Run canceled.", "run_status"),
      ]);
      setIsCanceling(false);
      void reconcileRunSnapshot(runId);
      reloadSidebar();
    } catch {
      setError("Unable to cancel run");
      setIsCanceling(false);
    }
  }, [runId, status, isCanceling, reconcileRunSnapshot, reloadSidebar]);

  /* -- Send agent message ----------------------------------------------------------------------- */
  const sendAgentMessage = React.useMemo(() => {
    if (!runId) return undefined;
    return async (message: string) => {
      console.log("[DEBUG-FOLLOWUP] sendAgentMessage called", { runId, message: message.slice(0, 100), currentStatus: status, currentError: error });
      try {
        const response = await fetch(`/api/runs/${runId}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message }),
        });

        console.log("[DEBUG-FOLLOWUP] sendAgentMessage response", { ok: response.ok, status: response.status });

        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          console.log("[DEBUG-FOLLOWUP] sendAgentMessage error payload", payload);
          setError(typeof payload?.detail === "string" ? payload.detail : "Unable to send message");
          return;
        }

        const responseData = await response.json().catch(() => ({}));
        const eventIndex = typeof responseData?.eventIndex === "number" ? responseData.eventIndex : null;
        console.log("[DEBUG-FOLLOWUP] sendAgentMessage success, eventIndex=%d, setting queued state", eventIndex);
        // Tell the SSE effect where new events begin so old events are skipped.
        if (eventIndex != null) {
          lastSeenEventIdRef.current = eventIndex - 1;
        }
        setError(null);
        setStatus("queued");
        setIsAgentThinking(true);
        setStreamingContent(null);
      } catch (err) {
        console.error("[DEBUG-FOLLOWUP] sendAgentMessage exception", err);
        setError("Unable to send message");
      }
    };
  }, [runId]);

  /* -- Effects ---------------------------------------------------------------------------------- */
  React.useEffect(() => {
    if (!routeRunId || routeRunId === runId) return;
    void hydrateRun(routeRunId);
  }, [routeRunId, runId, hydrateRun]);

  React.useEffect(() => {
    if (!runId || !isRunActive) return;
    const timer = setInterval(() => {
      void reconcileRunSnapshot(runId);
    }, 4000);
    return () => clearInterval(timer);
  }, [runId, isRunActive, reconcileRunSnapshot]);

  /* -- SSE stream -------------------------------------------------------------------------------- */
  React.useEffect(() => {
    if (!runId || !isRunActive) return;

    // Build SSE URL. If we've already seen events, start after the last one.
    const startFrom = lastSeenEventIdRef.current >= 0 ? lastSeenEventIdRef.current + 1 : null;
    const sseUrl = startFrom != null
      ? `/api/runs/${runId}/stream?lastEventIndex=${startFrom}`
      : `/api/runs/${runId}/stream`;
    console.log("[DEBUG-SSE] opening EventSource", { runId, startFrom, sseUrl });

    const source = new EventSource(sseUrl);

    /** Guard: only process an event if its id is newer than what we've seen. */
    const accept = (event: Event): boolean => {
      const me = event as MessageEvent;
      const id = me.lastEventId != null ? parseInt(me.lastEventId, 10) : NaN;
      if (!isNaN(id)) {
        if (id <= lastSeenEventIdRef.current) return false;   // duplicate / replay
        lastSeenEventIdRef.current = id;
      }
      return true;
    };

    source.addEventListener("chat:delta", (event) => {
      if (!accept(event)) return;
      const { messageId, token } = JSON.parse((event as MessageEvent).data);
      if (!messageId) return;
      setStreamingContent((prev) =>
        prev?.id === messageId
          ? { id: messageId, content: (prev?.content ?? "") + token }
          : { id: messageId, content: token },
      );
    });

    source.addEventListener("chat:message", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data);
      if (!isChatMessage(payload)) return;
      chatSchemaReadyRef.current = true;
      setStreamingContent(null);
      setMessages((prev) => (prev.some((msg) => msg.id === payload.id) ? prev : [...prev, payload]));
      if (payload.kind === "thinking") {
        setIsAgentThinking(true);
      }
    });

    source.addEventListener("step:started", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = payload.stepId ?? null;
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "running" } : s)));
      const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
      const label = typeof payload?.label === "string" ? payload.label : stepId;
      if (!chatSchemaReadyRef.current && label) {
        setMessages((prev) => [
          ...prev,
          makeMessage("system", `Starting: ${label}.`, "step_started", undefined, stepId ? { id: stepId, label } : undefined),
        ]);
      }
      if (THINKING_STEPS.includes(payload.stepId)) {
        setIsAgentThinking(true);
      }
    });

    source.addEventListener("step:completed", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = null;
      setIsAgentThinking(false);
      const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "completed" } : s)));
      if (!chatSchemaReadyRef.current) {
        const label = typeof payload?.label === "string" ? payload.label : stepId;
        if (label) {
          setMessages((prev) => [
            ...prev,
            makeMessage("system", `Completed: ${label}.`, "step_completed", undefined, stepId ? { id: stepId, label } : undefined),
          ]);
        }
      }
    });

    source.addEventListener("run:completed", (event) => {
      if (!accept(event)) return;
      console.log("[DEBUG-SSE] run:completed received");
      setStatus("completed");
      setIsAgentThinking(false);
      activeStepRef.current = null;
      setIsCanceling(false);
      if (!chatSchemaReadyRef.current) {
        setMessages((prev) => [...prev, makeMessage("system", "Migration completed.", "run_status")]);
      }
      void reconcileRunSnapshot(runId);
      reloadSidebar();
    });

    source.addEventListener("run:awaiting_input", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data);
      const reason = payload.reason || "Agent is waiting for your input";
      console.log("[DEBUG-SSE] run:awaiting_input received", { reason });

      const partial = streamingContentRef.current;
      if (partial && partial.content.trim().length > 0) {
        setMessages((prev) => [...prev, makeMessage("agent", partial.content, "log")]);
      }
      setStreamingContent(null);

      setIsAgentThinking(false);
      activeStepRef.current = null;
      setStatus("awaiting_input");
      setError(null);
      void reconcileRunSnapshot(runId);
    });

    source.addEventListener("run:failed", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data);
      const reason = payload.reason || "Run failed";
      const canceled = String(reason).toLowerCase().includes("canceled");
      console.log("[DEBUG-SSE] run:failed received", { reason, canceled });

      const partial = streamingContentRef.current;
      if (partial && partial.content.trim().length > 0) {
        setMessages((prev) => [...prev, makeMessage("agent", partial.content, "log")]);
      }
      setStreamingContent(null);

      setError(reason);
      setIsAgentThinking(false);
      activeStepRef.current = null;
      setStatus(canceled ? "canceled" : "failed");
      setIsCanceling(false);
      const paused =
        String(reason).toLowerCase().includes("upload ddl") ||
        String(reason).toLowerCase().includes("missing object");
      if (paused) setRequiresDdlUpload(true);
      if (!chatSchemaReadyRef.current) {
        setMessages((prev) => [...prev, makeMessage("error", paused ? `Execution paused: ${reason}` : reason, "run_status")]);
      }
      void reconcileRunSnapshot(runId);
      reloadSidebar();
    });

    source.addEventListener("execute_sql:statement", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data) as ExecuteStatementEvent;
      setExecuteStatements((prev) => [...prev, payload]);
    });

    source.addEventListener("execute_sql:error", (event) => {
      if (!accept(event)) return;
      const payload = JSON.parse((event as MessageEvent).data) as ExecuteErrorEvent;
      setExecuteErrors((prev) => [...prev, payload]);
      const missing =
        (payload.errorType ?? "").toLowerCase().includes("missing") ||
        (payload.errorMessage ?? "").toLowerCase().includes("does not exist");
      if (missing) {
        setRequiresDdlUpload(true);
        const m = (payload.errorMessage ?? "").match(/['\"]([^'\"]+)['\"]/);
        if (m?.[1]) setMissingObjects((prev) => (prev.includes(m[1]) ? prev : [...prev, m[1]]));
      }
    });

    source.onerror = () => {
      console.log("[DEBUG-SSE] onerror, closing source");
      source.close();
      // Don't reconcile here — the polling effect already does it every 4s.
      // Reconciling on every SSE error was a source of status-flip loops.
    };

    return () => {
      source.close();
    };
  }, [runId, isRunActive, reloadSidebar, reconcileRunSnapshot]);

  return {
    runId,
    projectId,
    isHydratingRouteRun,
    status,
    error,
    isBusy,
    isCanceling,
    messages,
    tasks,
    requiresDdlUpload,
    resumeFromStage,
    lastExecutedFileIndex,
    missingObjects,
    isAgentThinking,
    streamingContent,
    selectedSessionId,
    sidebarReloadKey,
    handleConfirm,
    retryRun,
    cancelRun,
    handleResumeWithDdl,
    sendAgentMessage,
  };
}
