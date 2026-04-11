"use client";

import * as React from "react";
import { useParams } from "next/navigation";
import { SidebarProvider } from "@/components/ui/sidebar";
import { Header } from "@/components/header";
import { SessionSidebar } from "@/components/session-sidebar";
import { ChatPanel } from "@/components/chat-panel";
import { useSessionRun } from "@/hooks/useSessionRun";

export default function SessionsPage() {
  const params = useParams<{ id?: string }>();
  const routeRunId = typeof params?.id === "string" ? params.id : null;

  const session = useSessionRun(routeRunId);
  const ddlFileInputRef = React.useRef<HTMLInputElement>(null);

  const onPickDdlFile = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const picked = event.target.files?.[0] ?? null;
    if (!picked) return;
    await session.handleResumeWithDdl(picked);
    event.target.value = "";
  };

  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header showWorkbenchToggle={!!session.runId} />

      <input
        ref={ddlFileInputRef}
        type="file"
        accept=".sql,.ddl,.txt"
        className="hidden"
        onChange={(event) => void onPickDdlFile(event)}
      />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar
            selectedSessionId={session.selectedSessionId}
            reloadKey={session.sidebarReloadKey}
          />

          <ChatPanel
            runId={session.runId}
            projectId={session.projectId}
            isHydratingRun={session.isHydratingRouteRun}
            status={session.status}
            error={session.error}
            isBusy={session.isBusy}
            isCanceling={session.isCanceling}
            messages={session.messages}
            tasks={session.tasks}
            requiresDdlUpload={session.requiresDdlUpload}
            resumeFromStage={session.resumeFromStage}
            lastExecutedFileIndex={session.lastExecutedFileIndex}
            missingObjects={session.missingObjects}
            isAgentThinking={session.isAgentThinking}
            streamingContent={session.streamingContent}
            onCreateProject={session.handleConfirm}
            onRetryRun={session.retryRun}
            onPickDdlFile={() => ddlFileInputRef.current?.click()}
            onCancelRun={session.cancelRun}
            onSendAgentMessage={session.sendAgentMessage}
          />
        </div>
      </SidebarProvider>
    </div>
  );
}
