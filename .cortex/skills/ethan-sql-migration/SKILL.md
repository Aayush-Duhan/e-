---
name: ethan-sql-migration
description: "Autonomous SQL migration from legacy databases (Teradata, Oracle) to Snowflake. Use for: SQL migration, Teradata to Snowflake, Oracle to Snowflake, code conversion, SCAI, SnowConvert, ETHAN migration, migrate SQL, convert SQL to Snowflake, legacy SQL migration, schema mapping, SQL self-healing, migration toolkit."
---

# ETHAN SQL Migration — Autonomous Snowflake Migration Workflow

Replicates the ETHAN (Efficient Thinking Autonomous Network) agentic pipeline: convert legacy SQL to Snowflake using SCAI CLI, execute on Snowflake, validate, and self-heal errors autonomously.

## Prerequisites

- **SCAI CLI** (`scai`) installed and authenticated (SnowConvert AI)
- **Snowflake connection** configured (account, user, role, warehouse, database, schema)
- **Source SQL files** ready (`.sql`, `.btq`, `.ddl`)
- **Schema mapping CSV** (optional) — see `references/schema-mapping-format.md`

## Workflow Overview

```
Step 1: Gather Inputs
  ↓
Step 2: Initialize SCAI Project (scai init)
  ↓
Step 3: Add Source Code (scai code add)
  ↓
Step 4: Apply Schema Mapping (optional, regex replace)
  ↓
Step 5: Convert Code (scai code convert + EWI cleanup)
  ↓
Step 6: Execute SQL on Snowflake
  ↓ success?
  ├─ YES → Step 7: Validate Output
  ├─ Missing objects → STOP: Ask user for DDL → execute DDL → retry Step 6
  └─ Other errors → Self-Heal: inspect → edit → retry Step 6 (max 5 retries)
  ↓
Step 7: Validate Output (line-count regression)
  ↓ passed?
  ├─ YES → Step 8: Finalize
  └─ NO → Self-Heal: inspect → edit → retry Step 6
  ↓
Step 8: Finalize Migration (copy output, generate report)
```

---

## Step 1: Gather Inputs

**Goal:** Collect all required configuration from the user.

**Ask the user for:**

```
To start the migration, I need the following:

1. **Project name** (e.g., "my_migration")
2. **Source language**: teradata | oracle
3. **Source SQL path**: Directory or file path containing the source SQL files
4. **Schema mapping CSV path** (optional): CSV file mapping SOURCE_SCHEMA → TARGET_DB_SCHEMA
5. **Snowflake connection details**:
   - Account URL (e.g., xy12345.snowflakecomputing.com)
   - User
   - Role
   - Warehouse
   - Database
   - Schema
   - Authenticator (default: externalbrowser)
```

**⚠️ INPUT CHECK**: If the user has already provided all required inputs in their initial message, confirm the values and proceed immediately — do NOT re-ask for information already given. Only prompt for **missing** inputs. Schema mapping CSV is optional — skip Step 4 if not provided.

---

## Step 2: Initialize SCAI Project

**Goal:** Create the project directory and initialize the SCAI project.

**Actions:**

1. Create project directory: `projects/<project_name>`
2. If the directory already exists and is not empty, warn the user and clear it
3. Run SCAI init:
   ```bash
   cd projects/<project_name>
   scai init -l <source_language> -n <project_name>
   ```
4. Verify exit code is 0. If non-zero, report the error and stop.

**⚠️ Windows Path Note:** On Windows (cmd.exe), paths containing spaces or special characters (e.g., `OneDrive - EY`) cause failures with `mkdir` and other shell commands even when quoted. **Always use relative paths from the current working directory** for `mkdir`, `copy`, and `cd` operations. For example:
```cmd
mkdir projects\my_project
```
Do NOT use quoted absolute paths like `mkdir "C:\Users\...\OneDrive - EY\...\projects\my_project"` — they will fail with "The filename, directory name, or volume label syntax is incorrect."

**Success criteria:** `scai init` exits with code 0, project directory created.

---

## Step 3: Add Source Code

**Goal:** Ingest source SQL files into the SCAI project.

**Actions:**

1. Identify the source SQL files/directory (from Step 1)
2. **Create the `source_input` directory** (it does not exist by default):
   ```cmd
   mkdir projects\<project_name>\source_input
   ```
3. **Copy source files locally** into `projects/<project_name>/source_input/` using relative paths:
   ```cmd
   copy C:\Users\...\source_file.sql projects\<project_name>\source_input\
   ```
   - Use **unquoted paths** when the source path contains no spaces
   - If the source path contains spaces and `copy` fails, use the **Read tool** to read the file contents, then the **Write tool** to write them into `source_input/`
   - This avoids the SCAI path concatenation bug (see note below)
4. Run SCAI code add with a **relative path**:
   ```bash
   cd projects/<project_name>
   scai code add -i source_input
   ```
5. Verify exit code is 0
6. Read the ingested source files from `projects/<project_name>/source/` to capture original code for later validation

**⚠️ SCAI Path Bug:** Do NOT pass quoted absolute paths to `scai code add -i`. SCAI concatenates the CWD with the `-i` argument, producing invalid paths like `C:\cwd\"C:\absolute\path"`. Always copy files locally first and use a relative path like `source_input`.

**Success criteria:** Source files copied into `projects/<project_name>/source/`, `scai code add` exits 0.

---

## Step 4: Apply Schema Mapping (Optional)

**Goal:** Replace source schema/table references with target Snowflake DB.SCHEMA references.

**Skip this step** if no schema mapping CSV was provided.

**Actions:**

1. **Load** `references/schema-mapping-format.md` for CSV format details
2. **Run the bundled mapping script** against the `source/` directory:
   ```bash
   python <skill_base_dir>/references/scripts/apply_schema_mapping.py <csv_path> projects/<project_name>/source/
   ```
   - The script reads the CSV, sorts mappings by length (longest first), applies regex replacements to all `.sql` files in-place, and prints a summary.
   - `<skill_base_dir>` is the base directory printed when the skill loads (e.g., `.cortex/skills/ethan-sql-migration`).
3. **Verify** the script output shows the expected replacements. Read one or two source files to spot-check.

**⚠️ Fallback (if Python is unavailable):** Use the **Read tool** to load each SQL file, apply replacements mentally, and use the **Write tool** to save the modified content. Do NOT use the Edit tool for bulk replacements — it fails on multiple identical matches.

**Success criteria:** All schema references replaced according to the CSV mapping.

---

## Step 5: Convert Code

**Goal:** Convert source SQL dialect to Snowflake SQL using SCAI.

**Actions:**

1. Run SCAI code convert:
   ```bash
   cd projects/<project_name>
   scai code convert
   ```
2. Verify exit code is 0
3. Find converted files in `projects/<project_name>/snowflake/` using the **glob tool** (`glob("**/*.sql")` in the snowflake directory). Do NOT use `dir /s` — it can hang on Windows.
4. **Auto-clean EWI and FDM markers** using the bundled cleanup script:
   ```bash
   python <skill_base_dir>/references/scripts/clean_ewi_fdm.py projects/<project_name>/snowflake/
   ```
   - The script removes full-line `--** SSC-EWI-...` markers, inline `/*EWI:...*/` comments, and inline `/*** SSC-FDM-XXXX - ... ***/` comments (Functional Difference Markers).
   - It prints a summary of markers removed per file.
   - `<skill_base_dir>` is the base directory printed when the skill loads.

**⚠️ Fallback (if Python is unavailable):** Use the **Read tool** to load each converted file, strip all EWI/FDM markers, and use the **Write tool** to save the cleaned version.

**⚠️ Do NOT use the Edit tool for EWI/FDM cleanup.** The Edit tool's `replace_all` fails when replacing identical strings with empty strings across many occurrences. Always use Read+Write or the bundled script instead.
5. Read cleaned converted code into memory for later use

**⚠️ CHECKPOINT**: Present the conversion results to the user:
- Number of files converted
- Any warnings from SCAI
- EWI markers cleaned count

**Success criteria:** Converted Snowflake SQL files in `snowflake/` directory, EWI markers removed.

---

## Step 6: Execute SQL on Snowflake

**Goal:** Execute the converted SQL on the target Snowflake environment.

**Actions:**

1. **Set Snowflake context** by executing these statements first (using the Snowflake SQL execute tool):
   ```sql
   USE ROLE <role>;
   USE WAREHOUSE <warehouse>;
   USE DATABASE <database>;
   USE SCHEMA <schema>;
   ```
2. For each converted SQL file (in order):
   - Read the file contents
   - Split into individual SQL statements (respecting quoted strings, comments, `$$` blocks)
   - Execute each statement sequentially
   - Track results per statement: success/failure, row counts, error messages
3. If **all statements succeed**: mark execution as passed, proceed to Step 7

**Error Handling — Three Scenarios:**

### Scenario A: Missing Objects (table/view does not exist)
```
⚠️ MANDATORY STOP: Missing objects detected.

The following objects are referenced but do not exist in Snowflake:
- <list of missing objects>

Please provide a DDL file that creates these objects.
Upload the DDL and I will execute it, then resume the migration.
```
Wait for user to provide DDL. Execute the DDL on Snowflake, then retry Step 6.

### Scenario B: Other Execution Errors (syntax, data type, etc.)
Enter **Self-Healing Loop** (max 5 iterations):
1. Identify the failed statement and error message
2. Inspect the converted file: find the relevant line range
3. Diagnose the issue (e.g., incompatible syntax, wrong data type, unsupported function)
4. Apply a targeted edit to fix ONLY the affected lines
5. Re-execute from Step 6
6. If the same error persists after 3 attempts on the same issue, explain to the user and stop

### Scenario C: All Statements Succeed
Proceed to Step 7.

---

## Step 7: Validate Output

**Goal:** Verify the conversion quality using the line-count regression rule.

**Actions:**

1. Count total lines in original source files (input)
2. Count total lines in converted output files (output)
3. **Rule:** Output line count must be >= input line count
   - **Pass:** Converted code preserves or adds content → proceed to Step 8
   - **Fail:** Output is shorter than input (content may have been lost)

**If validation fails:**
- Report the line counts to the user
- Enter self-healing: inspect the converted files for truncation or missing sections
- Apply fixes and re-execute from Step 6
- If still failing after 3 attempts, report to user and stop

**If validation passes:** Proceed to Step 8.

---

## Step 8: Finalize Migration

**Goal:** Produce final output and summary report.

**Actions:**

1. Create output directory: `outputs/<project_name>/converted/` (use relative paths with `mkdir`)
2. Copy all converted SQL files from `projects/<project_name>/snowflake/` to output directory (use **glob tool** to discover files, then `copy` with relative paths or the Read/Write tools)
3. Generate summary report:
   ```
   Migration Summary
   ─────────────────
   Project:           <project_name>
   Source Language:    <source_language>
   Target Platform:   Snowflake
   SCAI Initialized:  Yes/No
   Source Added:       Yes/No
   Code Converted:    Yes/No
   Self-Heal Rounds:  <count>
   Validation Passed: Yes/No
   Validation Issues: <count>
   Errors:            <count>
   Warnings:          <count>
   Output Files:      <count>
   Status:            COMPLETED
   ```

**⚠️ CHECKPOINT**: Present the summary report to the user for final review.

---

## Self-Healing Strategy (used in Steps 6 and 7)

When execution or validation fails, follow this recovery loop:

1. **Inspect**: Read the file metadata (paths, line counts, sizes)
2. **Locate**: Use the error message to identify the problematic area (file + line range)
3. **View**: Read the relevant section of the file with line numbers
4. **Diagnose**: Determine the root cause (syntax, unsupported function, data type mismatch, missing object, truncation)
5. **Fix**: Apply a targeted line-range edit to ONLY the affected lines — do not rewrite the entire file
6. **Retry**: Re-execute the SQL on Snowflake
7. **Limit**: Maximum 5 self-heal iterations total. If the same error recurs 3 times, stop and explain to the user.

Common fix patterns:
- `QUALIFY` → rewrite as subquery with `ROW_NUMBER()`
- `MULTISET TABLE` / `VOLATILE TABLE` → `CREATE TABLE` / `CREATE TEMPORARY TABLE`
- `WITH DATA` → remove clause
- Teradata `DATE` formats → Snowflake `TO_DATE()` equivalents
- `COLLECT STATISTICS` → remove (no Snowflake equivalent)
- `LOCKING` clauses → remove (not applicable in Snowflake)

---

## Stopping Points

- ✋ **Step 1**: After gathering inputs — confirm all values before proceeding
- ✋ **Step 5**: After code conversion — review conversion results
- ✋ **Step 6 (Scenario A)**: Missing objects — wait for DDL upload
- ✋ **Step 8**: After finalization — present summary report for review

**Resume rule:** Upon user approval at any checkpoint, proceed directly to the next step.

---

## Output

- Converted Snowflake SQL files in `outputs/<project_name>/converted/`
- Migration summary report
- Execution log with per-statement results
