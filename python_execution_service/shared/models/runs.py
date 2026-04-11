"""Pydantic request/response models and internal models."""

from dataclasses import dataclass, field
from typing import Any, TypedDict

from pydantic import BaseModel, Field

from python_execution_service.agentic_core.models.context import MigrationContext


# ── Pydantic request / response models ──────────────────────────

class StartRunRequest(BaseModel):
    projectId: str
    projectName: str
    sourceId: str
    schemaId: str | None = None
    sourceLanguage: str = "teradata"
    sourcePath: str
    schemaPath: str | None = None
    sfAccount: str | None = None
    sfUser: str | None = None
    sfRole: str | None = None
    sfWarehouse: str | None = None
    sfDatabase: str | None = None
    sfSchema: str | None = None
    sfAuthenticator: str | None = None


class StartRunResponse(BaseModel):
    runId: str


# ── Internal Pydantic models ────────────────────────────────────

class RunStep(BaseModel):
    model_config = {"from_attributes": True}

    id: str
    label: str
    status: str = "pending"
    startedAt: str | None = None
    endedAt: str | None = None


class RunRecord(BaseModel):
    model_config = {"from_attributes": True}

    runId: str
    projectId: str
    projectName: str
    sourceId: str
    schemaId: str = ""
    sourceLanguage: str = "teradata"
    sourcePath: str = ""
    schemaPath: str = ""
    sfAccount: str | None = None
    sfUser: str | None = None
    sfRole: str | None = None
    sfWarehouse: str | None = None
    sfDatabase: str | None = None
    sfSchema: str | None = None
    sfAuthenticator: str | None = None
    status: str = "queued"
    createdAt: str = ""
    updatedAt: str = ""
    steps: list[RunStep] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    validationIssues: list[dict[str, Any]] = Field(default_factory=list)
    executionLog: list[dict[str, Any]] = Field(default_factory=list)
    executionErrors: list[dict[str, Any]] = Field(default_factory=list)
    missingObjects: list[str] = Field(default_factory=list)
    requiresDdlUpload: bool = False
    resumeFromStage: str = ""
    lastExecutedFileIndex: int = -1
    selfHealIteration: int = 0
    error: str | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    messages: list[dict[str, Any]] = Field(default_factory=list)
    outputDir: str = ""
    ddlUploadPath: str = ""
    executionEventCursor: int = 0
    userMessageQueue: list[str] = Field(default_factory=list)
    conversationHistory: list[dict[str, Any]] = Field(default_factory=list)
    graphThreadId: str = ""


@dataclass
class ResumeRunConfig:
    ddl_content: bytes
    ddl_filename: str
    missing_objects: list[str] = field(default_factory=list)
    resume_from_stage: str = "execute_sql"
    last_executed_file_index: int = -1


# ── LangGraph typed-dict state ──────────────────────────────────

class WorkflowState(TypedDict):
    context: MigrationContext
