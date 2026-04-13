"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { AnimatePresence, m, MotionConfig } from "framer-motion";
import useMeasure from "react-use-measure";
import {
  ChevronLeft,
  ChevronRight,
  Check,
  Database,
  FileText,
  GitBranch,
  Code2,
  Github,
  Upload,
  X,
  Snowflake,
  ArrowRight,
} from "lucide-react";
import { GitHubImportModal } from "@/components/ui/github-import-modal";
import { resetGitHubImport } from "@/lib/github-import-store";
import {
  useWizardState,
  getVisibleWizardSteps,
  SOURCE_LANGUAGES,
  SUPPORTED_SCRIPT_TYPES,
  type WizardStepId,
  type WizardFile,
  type ScriptType,
  goToNextStep,
  goToPreviousStep,
  setSourceLanguage,
  setImportSource,
  toggleScriptType,
  addSourceFiles,
  removeSourceFile,
  addMappingFiles,
  removeMappingFile,
  setCredentialField,
  setStarting,
  canProceedToNext,
  isFirstStep,
  isLastStep,
  resetWizard,
} from "@/lib/wizard-store";

function cn(...inputs: (string | boolean | undefined | null)[]): string {
  return inputs.filter(Boolean).join(" ");
}

// Step 1
const LanguageStep = React.memo(function LanguageStep() {
  const { sourceLanguage } = useWizardState();

  return (
    <div>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2.5">
        {SOURCE_LANGUAGES.map((lang) => {
          const selected = sourceLanguage === lang.id;
          return (
            <button
              key={lang.id}
              onClick={() => setSourceLanguage(lang.id)}
              className={cn(
                "group relative rounded-lg border px-4 py-3.5 text-left text-sm font-medium transition-all duration-200",
                selected
                  ? "border-[#29B5E8]/50 bg-[#29B5E8]/10 text-white shadow-[0_0_0_1px_rgba(41,181,232,0.25),0_0_12px_-3px_rgba(41,181,232,0.15)]"
                  : "border-white/8 bg-white/[0.03] text-white/60 hover:border-[#29B5E8]/30 hover:bg-white/[0.06] hover:text-white/90"
              )}
            >
              {lang.label}
            </button>
          );
        })}
      </div>
    </div>
  );
});

// Step 2
const ScriptTypeStep = React.memo(function ScriptTypeStep() {
  const { sourceLanguage, scriptTypes } = useWizardState();

  if (!sourceLanguage) {
    return null;
  }

  const supportedTypes = SUPPORTED_SCRIPT_TYPES[sourceLanguage] ?? [];
  return (
    <div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
        {supportedTypes.map((scriptType: ScriptType) => {
          const selected = scriptTypes.includes(scriptType);
          return (
            <button
              key={scriptType}
              onClick={() => toggleScriptType(scriptType)}
              className={cn(
                "flex items-center justify-between rounded-lg border px-4 py-3 text-left text-sm font-medium transition-all duration-200",
                selected
                  ? "border-[#29B5E8]/50 bg-[#29B5E8]/10 text-white shadow-[0_0_0_1px_rgba(41,181,232,0.25),0_0_12px_-3px_rgba(41,181,232,0.15)]"
                  : "border-white/8 bg-white/[0.03] text-white/60 hover:border-[#29B5E8]/30 hover:bg-white/[0.06] hover:text-white/90"
              )}
            >
              <span>{scriptType}</span>
              <span
                className={cn(
                  "flex h-5 w-5 items-center justify-center rounded border transition-all",
                  selected
                    ? "border-[#29B5E8] bg-[#29B5E8]"
                    : "border-white/20 bg-transparent"
                )}
              >
                {selected && <Check className="h-3 w-3 text-white" strokeWidth={3} />}
              </span>
            </button>
          );
        })}
      </div>
      <p className="mt-4 text-xs text-white/30">
        Select at least one script type to continue.
      </p>
    </div>
  );
});

// Step 3
const ImportSourceStep = React.memo(function ImportSourceStep() {
  const { importSource } = useWizardState();

  const options = [
    {
      id: "system" as const,
      icon: Upload,
      title: "Import from System",
      desc: "Upload files from your local machine.",
    },
    {
      id: "github" as const,
      icon: Github,
      title: "Import from GitHub",
      desc: "Select files from a GitHub Enterprise repository.",
    },
  ];

  return (
    <div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {options.map((opt) => {
          const selected = importSource === opt.id;
          const Icon = opt.icon;
          return (
            <button
              key={opt.id}
              type="button"
              onClick={() => {
                resetGitHubImport();
                setImportSource(opt.id);
              }}
              className={cn(
                "group relative flex flex-col items-start gap-3 rounded-xl border p-5 text-left transition-all duration-200",
                selected
                  ? "border-[#29B5E8]/50 bg-[#29B5E8]/10 shadow-[0_0_0_1px_rgba(41,181,232,0.2)]"
                  : "border-white/8 bg-white/[0.03] hover:border-white/15 hover:bg-white/[0.06]"
              )}
            >
              <span
                className={cn(
                  "flex h-10 w-10 items-center justify-center rounded-lg transition-colors",
                  selected
                    ? "bg-[#29B5E8]/15 text-[#29B5E8]"
                    : "bg-white/5 text-white/40 group-hover:text-white/70"
                )}
              >
                <Icon className="h-5 w-5" />
              </span>
              <div>
                <p className={cn("text-sm font-medium", selected ? "text-white" : "text-white/60 group-hover:text-white/90")}>
                  {opt.title}
                </p>
                <p className="mt-0.5 text-xs text-white/30">{opt.desc}</p>
              </div>
              {selected && (
                <span className="absolute right-3 top-3 flex h-5 w-5 items-center justify-center rounded-full bg-[#29B5E8]">
                  <Check className="h-3 w-3 text-white" strokeWidth={3} />
                </span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
});

// Shared drop zone
function FileDropZone({
  onDrop,
  onClick,
  icon: Icon,
  label,
  hint,
  isDragging,
  onDragEnter,
  onDragLeave,
}: {
  onDrop: (e: React.DragEvent) => void;
  onClick: () => void;
  icon: React.ElementType;
  label: string;
  hint: string;
  isDragging: boolean;
  onDragEnter: () => void;
  onDragLeave: () => void;
}) {
  return (
    <div
      role="button"
      tabIndex={0}
      onDrop={(e) => {
        e.preventDefault();
        onDragLeave();
        onDrop(e);
      }}
      onDragOver={(e) => {
        e.preventDefault();
        onDragEnter();
      }}
      onDragLeave={onDragLeave}
      onClick={onClick}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(); } }}
      className={cn(
        "group cursor-pointer rounded-xl border-2 border-dashed p-8 text-center transition-all duration-200",
        isDragging
          ? "border-[#29B5E8]/60 bg-[#29B5E8]/5"
          : "border-white/10 hover:border-[#29B5E8]/40 hover:bg-white/[0.02]"
      )}
    >
      <div className={cn(
        "mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-xl transition-colors",
        isDragging ? "bg-[#29B5E8]/15 text-[#29B5E8]" : "bg-white/5 text-white/40 group-hover:text-[#29B5E8]"
      )}>
        <Icon className="h-6 w-6" />
      </div>
      <p className="text-sm font-medium text-white">{label}</p>
      <p className="mt-1 text-xs text-white/40">{hint}</p>
    </div>
  );
}

// Shared file list
function FileList({
  files,
  label,
  onRemove,
}: {
  files: WizardFile[];
  label: string;
  onRemove: (key: string) => void;
}) {
  if (files.length === 0) return null;
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-xs font-medium text-white/50">{label}</p>
        <span className="rounded-full bg-[#29B5E8]/10 px-2 py-0.5 text-xs font-medium text-[#9fceff]">
          {files.length}
        </span>
      </div>
      <div className="max-h-44 space-y-0.5 overflow-y-auto rounded-lg border border-white/8 bg-black/20 p-1.5">
        {files.map((file) => {
          const key = file.relativePath ?? file.name;
          return (
            <div
              key={key}
              className="group flex items-center justify-between rounded-md px-2.5 py-1.5 transition-colors hover:bg-white/5"
            >
              <span className="flex-1 truncate text-xs text-white/50">{key}</span>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onRemove(key);
                }}
                className="ml-2 flex h-5 w-5 shrink-0 items-center justify-center rounded text-white/20 opacity-0 transition-all hover:bg-red-500/15 hover:text-red-400 group-hover:opacity-100"
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Step 4
const FilesStep = React.memo(function FilesStep() {
  const { sourceFiles, importSource } = useWizardState();
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const [repoModalOpen, setRepoModalOpen] = React.useState(false);
  const [isDragging, setIsDragging] = React.useState(false);
  const usingGitHub = importSource === "github";

  const handleFileUpload = React.useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;
    const uploadedFiles: WizardFile[] = Array.from(files).map((file) => ({
      name: file.name,
      path: file.webkitRelativePath || file.name,
      relativePath: file.webkitRelativePath || file.name,
      file: file,
    }));
    addSourceFiles(uploadedFiles);
    event.target.value = "";
  }, []);

  const handleDrop = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const items = event.dataTransfer.items;
    const uploadedFiles: WizardFile[] = [];
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          uploadedFiles.push({ name: file.name, path: file.name, relativePath: file.name, file });
        }
      }
    }
    addSourceFiles(uploadedFiles);
  }, []);

  const handleRepositoryImport = React.useCallback((files: WizardFile[]) => {
    addSourceFiles(files);
  }, []);

  return (
    <div>

      {!usingGitHub && (
        <>
          <input ref={fileInputRef} type="file" multiple accept=".sql,.ddl,.btq,.txt" onChange={handleFileUpload} className="hidden" />
          <FileDropZone
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            icon={Upload}
            label="Drop files here or click to browse"
            hint=".sql, .ddl, .btq, .txt files supported"
            isDragging={isDragging}
            onDragEnter={() => setIsDragging(true)}
            onDragLeave={() => setIsDragging(false)}
          />
        </>
      )}

      {usingGitHub && (
        <>
          <button
            type="button"
            onClick={() => setRepoModalOpen(true)}
            className="flex w-full items-center justify-center gap-2.5 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3.5 text-sm font-medium text-white/60 transition-all hover:border-[#29B5E8]/40 hover:text-white hover:bg-[#29B5E8]/5"
          >
            <Github className="h-5 w-5" />
            Select from GitHub Enterprise
          </button>
          <GitHubImportModal mode="source" open={repoModalOpen} onOpenChange={setRepoModalOpen} onImport={handleRepositoryImport} resetOnOpen={false} clearSelectionOnImport />
        </>
      )}

      <div className="mt-4">
        <FileList files={sourceFiles} label="Selected files" onRemove={removeSourceFile} />
      </div>
    </div>
  );
});

// Step 5
const MappingStep = React.memo(function MappingStep() {
  const { mappingFiles, importSource } = useWizardState();
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const [repoModalOpen, setRepoModalOpen] = React.useState(false);
  const [isDragging, setIsDragging] = React.useState(false);
  const usingGitHub = importSource === "github";

  const handleFileUpload = React.useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;
    const uploadedFiles: WizardFile[] = Array.from(files).map((file) => ({
      name: file.name, path: file.name, relativePath: file.name, file,
    }));
    addMappingFiles(uploadedFiles);
    event.target.value = "";
  }, []);

  const handleDrop = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const items = event.dataTransfer.items;
    const uploadedFiles: WizardFile[] = [];
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          uploadedFiles.push({ name: file.name, path: file.name, relativePath: file.name, file });
        }
      }
    }
    addMappingFiles(uploadedFiles);
  }, []);

  const handleRepositoryImport = React.useCallback((files: WizardFile[]) => {
    addMappingFiles(files);
  }, []);

  return (
    <div>
      <div className="mb-4 inline-flex items-center gap-1.5 rounded-full border border-[#29B5E8]/20 bg-[#29B5E8]/5 px-2.5 py-1 text-[11px] font-medium text-[#9fceff]">
        Optional *
      </div>

      {!usingGitHub && (
        <>
          <input ref={fileInputRef} type="file" accept=".csv" onChange={handleFileUpload} className="hidden" />
          <FileDropZone
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            icon={GitBranch}
            label="Drop a CSV file here or click to browse"
            hint="A single .csv mapping file for all scripts"
            isDragging={isDragging}
            onDragEnter={() => setIsDragging(true)}
            onDragLeave={() => setIsDragging(false)}
          />
        </>
      )}

      {usingGitHub && (
        <>
          <button
            type="button"
            onClick={() => setRepoModalOpen(true)}
            className="flex w-full items-center justify-center gap-2.5 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3.5 text-sm font-medium text-white/60 transition-all hover:border-[#29B5E8]/40 hover:text-white hover:bg-[#29B5E8]/5"
          >
            <Github className="h-5 w-5" />
            Select mapping file from GitHub
          </button>
          <GitHubImportModal mode="mapping" open={repoModalOpen} onOpenChange={setRepoModalOpen} onImport={handleRepositoryImport} resetOnOpen={false} clearSelectionOnImport />
        </>
      )}

      <div className="mt-4">
        <FileList files={mappingFiles} label="Mapping file" onRemove={removeMappingFile} />
      </div>

      <p className="mt-4 text-xs text-white/25">
        *Skip this step if you have already applied schema mapping.
      </p>
    </div>
  );
});

// Step 6
const CREDENTIAL_FIELDS = [
  { key: "sfAccount" as const, label: "Account", placeholder: "e.g. xy12345.us-east-1", required: true },
  { key: "sfUser" as const, label: "User", placeholder: "e.g. admin@company.com", required: false },
  { key: "sfRole" as const, label: "Role", placeholder: "e.g. SYSADMIN", required: false },
  { key: "sfWarehouse" as const, label: "Warehouse", placeholder: "e.g. COMPUTE_WH", required: false },
  { key: "sfDatabase" as const, label: "Database", placeholder: "e.g. MY_DATABASE", required: false },
  { key: "sfSchema" as const, label: "Schema", placeholder: "e.g. PUBLIC", required: false },
] as const;

const CredentialsStep = React.memo(function CredentialsStep() {
  const wizard = useWizardState();

  return (
    <div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-5">
        {CREDENTIAL_FIELDS.map(({ key, label, placeholder, required }) => (
          <div key={key} className="space-y-1.5">
            <label className="flex items-center gap-1 text-xs font-medium text-white/40">
              {label}
              {required && <span className="text-[#29B5E8]">*</span>}
            </label>
            <input
              type="text"
              value={wizard[key]}
              onChange={(e) => setCredentialField(key, e.target.value)}
              placeholder={placeholder}
              className="w-full rounded-lg border border-white/10 bg-white/[0.03] px-3.5 py-2.5 text-sm text-white placeholder-white/20 outline-none transition-all focus:border-[#29B5E8]/50 focus:bg-white/[0.05] focus:shadow-[0_0_0_3px_rgba(41,181,232,0.1)]"
            />
          </div>
        ))}
      </div>
    </div>
  );
});

// Summary row
function SummaryRow({
  icon: Icon,
  label,
  value,
  detail,
  children,
}: {
  icon: React.ElementType;
  label: string;
  value: React.ReactNode;
  detail?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-[#29B5E8]/15 bg-[#29B5E8]/[0.03] p-4">
      <div className="flex items-start gap-3">
        <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-[#29B5E8]/15 text-[#29B5E8] shadow-[0_0_10px_rgba(41,181,232,0.1)]">
          <Icon className="h-4 w-4" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-medium uppercase tracking-wider text-white/35">{label}</p>
          <p className="mt-0.5 text-sm font-medium text-white">{value}</p>
          {detail && <p className="mt-0.5 text-xs text-white/30">{detail}</p>}
          {children}
        </div>
      </div>
    </div>
  );
}

// Step 7
const SummaryStep = React.memo(function SummaryStep() {
  const { sourceLanguage, scriptTypes, sourceFiles, mappingFiles, sfAccount, sfUser, isStarting, startError } =
    useWizardState();
  const language = SOURCE_LANGUAGES.find((l) => l.id === sourceLanguage);

  return (
    <div>
      <div className="space-y-3">
        <SummaryRow icon={Database} label="Source Database" value={language?.label} />
        <SummaryRow
          icon={Code2}
          label="Script Types"
          value={scriptTypes.length > 0 ? scriptTypes.join(", ") : "None selected"}
        />
        <SummaryRow icon={FileText} label="Source Files" value={`${sourceFiles.length} file(s)`}>
          {sourceFiles.length > 0 && (
            <div className="mt-1.5 max-h-20 overflow-y-auto text-xs text-white/30">
              {sourceFiles.slice(0, 5).map((f) => (
                <p key={f.relativePath} className="truncate">{f.relativePath ?? f.name}</p>
              ))}
              {sourceFiles.length > 5 && (
                <p className="text-white/20">...and {sourceFiles.length - 5} more</p>
              )}
            </div>
          )}
        </SummaryRow>
        <SummaryRow
          icon={GitBranch}
          label="Schema Mapping"
          value={mappingFiles.length > 0 ? mappingFiles[0].relativePath ?? mappingFiles[0].name : "None (optional)"}
        />
        <SummaryRow
          icon={Snowflake}
          label="Snowflake Connection"
          value={<>{sfAccount}{sfUser ? <> &middot; {sfUser}</> : null}</>}
          detail="Auth: Browser SSO"
        />
      </div>

      {startError && (
        <div className="mt-4 rounded-lg border border-red-500/30 bg-red-500/10 p-3">
          <p className="text-sm text-red-400">{startError}</p>
        </div>
      )}

      {isStarting && (
        <div className="mt-4 flex items-center justify-center gap-2.5 py-3">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-[#29B5E8] border-t-transparent" />
          <p className="text-sm text-white/50">Starting migration...</p>
        </div>
      )}
    </div>
  );
});

const StepContent = React.memo(function StepContent({ step }: { step: WizardStepId }) {
  switch (step) {
    case "language":
      return <LanguageStep />;
    case "scriptType":
      return <ScriptTypeStep />;
    case "importSource":
      return <ImportSourceStep />;
    case "files":
      return <FilesStep />;
    case "mapping":
      return <MappingStep />;
    case "credentials":
      return <CredentialsStep />;
    case "summary":
      return <SummaryStep />;
    default:
      return null;
  }
});

const slideVariants = {
  initial: (direction: number) => ({
    x: `${110 * direction}%`,
    opacity: 0,
  }),
  animate: {
    x: "0%",
    opacity: 1,
  },
  exit: (direction: number) => ({
    x: `${-110 * direction}%`,
    opacity: 0,
  }),
};

interface SetupWizardProps {
  onStartMigration: () => void | Promise<void>;
  isBusy?: boolean;
}

export const SetupWizard = React.memo(function SetupWizard({
  onStartMigration,
  isBusy = false,
}: SetupWizardProps) {
  const { currentStep, isStarting } = useWizardState();
  const router = useRouter();
  const visibleSteps = getVisibleWizardSteps();
  const canProceed = canProceedToNext();
  const first = isFirstStep();
  const last = isLastStep();
  const currentIndex = visibleSteps.findIndex((s) => s.id === currentStep);

  const [direction, setDirection] = React.useState(1);
  const [ref, bounds] = useMeasure();

  React.useEffect(() => {
    resetWizard();
    resetGitHubImport();
    return () => {
      resetWizard();
      resetGitHubImport();
    };
  }, []);

  const handleNext = async () => {
    if (last) {
      setStarting(true);
      try {
        await onStartMigration();
        setStarting(false);
      } catch (error) {
        setStarting(false, error instanceof Error ? error.message : "Failed to start migration");
      }
    } else {
      setDirection(1);
      goToNextStep();
    }
  };

  const handleBack = () => {
    if (first) {
      router.push("/migration-toolkit");
    } else {
      setDirection(-1);
      goToPreviousStep();
    }
  };

  const currentStepInfo = visibleSteps[currentIndex];

  return (
    <MotionConfig transition={{ duration: 0.5, type: "spring", bounce: 0 }}>
      <div className="flex w-full items-center justify-center p-4">
        <div className="relative w-full max-w-2xl rounded-2xl border border-white/[0.12] bg-[#141414]/90 backdrop-blur-xl">


          <m.div layout className="relative z-10">
            {/* Header */}
            <div className="border-b border-white/[0.06] px-6 pt-5 pb-5">
              {/* Branding + step dots row */}
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2 text-white/40">
                  <Image src="/Snowflake.svg" alt="Snowflake" width={16} height={16} className="h-4 w-4 opacity-70" />
                  <span className="text-xs font-medium">Snowflake Migration</span>
                </div>
                <div className="flex items-center gap-1.5">
                  {visibleSteps.map((step, index) => (
                    <div
                      key={step.id}
                      className={cn(
                        "h-1.5 rounded-full transition-all duration-300",
                        currentIndex === index
                          ? "w-6 bg-[#29B5E8]"
                          : index < currentIndex
                            ? "w-1.5 bg-emerald-400/70"
                            : "w-1.5 bg-white/20"
                      )}
                    />
                  ))}
                </div>
              </div>
              {/* Step title + description */}
              <h2 className="text-2xl font-bold text-white tracking-tight">
                {currentStepInfo?.label ?? "Migration Setup"}
              </h2>
              <p className="mt-1 text-sm text-white/40 leading-relaxed">
                {currentStepInfo?.description ?? ""}
              </p>
            </div>

            {/* Content with animated height */}
            <m.div
              animate={{ height: bounds.height > 0 ? bounds.height : "auto" }}
              className="relative overflow-hidden"
              transition={{ type: "spring", bounce: 0, duration: 0.5 }}
            >
              <div ref={ref}>
                <div className="relative px-6 py-2">
                  <AnimatePresence
                    mode="popLayout"
                    initial={false}
                    custom={direction}
                  >
                    <m.div
                      key={currentStep}
                      variants={slideVariants}
                      initial="initial"
                      animate="animate"
                      exit="exit"
                      custom={direction}
                      className="w-full py-4"
                    >
                      <StepContent step={currentStep} />
                    </m.div>
                  </AnimatePresence>
                </div>
              </div>
            </m.div>

            {/* Footer */}
            <div className="flex items-center justify-between border-t border-white/[0.06] bg-white/[0.02] px-6 py-4 rounded-b-2xl">
              <button
                onClick={handleBack}
                className="flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-white/50 transition-all hover:bg-white/5 hover:text-white"
              >
                <ChevronLeft className="h-4 w-4" />
                Back
              </button>

              <button
                onClick={handleNext}
                disabled={!canProceed || isBusy || isStarting}
                className={cn(
                  "flex items-center gap-2 rounded-lg px-6 py-2.5 text-sm font-semibold transition-all",
                  canProceed && !isBusy && !isStarting
                    ? "bg-[#29B5E8] text-[#0a1628] shadow-[0_1px_4px_rgba(0,0,0,0.25)] hover:bg-[#24a3d4] hover:shadow-[0_2px_8px_rgba(0,0,0,0.3)]"
                    : "cursor-not-allowed bg-white/5 text-white/20"
                )}
              >
                {last ? (
                  <>
                    Start Migration
                    <ArrowRight className="h-4 w-4" />
                  </>
                ) : (
                  <>
                    Continue
                    <ChevronRight className="h-4 w-4" />
                  </>
                )}
              </button>
            </div>
          </m.div>
        </div>
      </div>
    </MotionConfig>
  );
});

export { resetWizard };
