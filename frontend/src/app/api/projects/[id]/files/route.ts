import { NextRequest, NextResponse } from 'next/server';
import * as path from 'path';
import { promises as fs } from 'fs';
import type { Dirent } from 'fs';
import { getProject } from '@/lib/storage';

export const runtime = 'nodejs';

// Workspace: ../projects/<project.name>
const PROJECTS_DIR = path.resolve(process.cwd(), '..', 'projects');

// Optional safety: avoid huge reads (set to Infinity to disable)
const MAX_FILE_BYTES = 2_000_000; // 2MB

/** Top-level directories exposed through the API. */
const ALLOWED_ROOTS = ['source', 'snowflake', 'reports', 'artifacts'];

type FileInfo =
  | { name: string; path: string; type: 'folder' }
  | { name: string; path: string; type: 'file'; content: string };

interface WritableFileInput {
  path: string;
  content: string;
}

/** Convert Windows "\" to "/" for consistent matching */
function toPosix(p: string) {
  return p.split(path.sep).join('/');
}

/** Strong path-inside check with separator boundary */
function isPathInside(rootAbs: string, targetAbs: string) {
  const root = path.resolve(rootAbs);
  const target = path.resolve(targetAbs);
  return target === root || target.startsWith(root + path.sep);
}

/**
 * Validate and normalise a relative path from the client.
 * Returns the cleaned posix-style path if it falls under an allowed root,
 * or null if it should be blocked.  An empty string means the project root listing.
 */
function validateRelativePath(rel: string): string | null {
  const clean = toPosix(rel || '').replace(/^\/+/, '');
  if (!clean) return '';

  const root = clean.split('/')[0];
  if (!ALLOWED_ROOTS.includes(root)) return null;

  return clean;
}

/** Ensure a given relative path is under one of the allowed roots */
function isAllowedRelative(rel: string): boolean {
  const p = toPosix(rel).replace(/^\/+/, '');
  if (!p) return true;
  const root = p.split('/')[0];
  return ALLOWED_ROOTS.includes(root);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isWritableFileInput(value: unknown): value is WritableFileInput {
  return (
    isRecord(value) &&
    typeof value.path === 'string' &&
    typeof value.content === 'string'
  );
}

function getWritableFilePath(value: unknown): string {
  return isRecord(value) && typeof value.path === 'string'
    ? value.path
    : '(unknown)';
}

async function readTextFileSafe(absPath: string): Promise<string> {
  try {
    const stat = await fs.stat(absPath);
    if (stat.size > MAX_FILE_BYTES) {
      return `[File too large to preview (${stat.size} bytes). Limit is ${MAX_FILE_BYTES} bytes.]`;
    }
    return await fs.readFile(absPath, 'utf-8');
  } catch {
    return '[Unable to read file]';
  }
}

/**
 * Reads a directory (non-recursive), returning only children under allowed roots.
 *
 * Uses encoding:'utf8' to ensure Dirent.name is typed as string (fixes TS NonSharedBuffer errors)
 */
async function readDirectory(absDir: string, projectRootAbs: string): Promise<FileInfo[]> {
  const items: FileInfo[] = [];

  let entries: Dirent[];
  try {
    entries = await fs.readdir(absDir, { withFileTypes: true, encoding: 'utf8' });
  } catch {
    return items;
  }

  for (const entry of entries) {
    const name = entry.name; // string
    const fullAbs = path.join(absDir, name);

    const rel = toPosix(path.relative(projectRootAbs, fullAbs));

    // Enforce allowlist (hard filter)
    if (!isAllowedRelative(rel)) continue;

    if (entry.isDirectory()) {
      items.push({ name, path: rel, type: 'folder' });
    } else if (entry.isFile()) {
      const content = await readTextFileSafe(fullAbs);
      items.push({ name, path: rel, type: 'file', content });
    }
  }

  return items;
}

async function getParamsId(
  params: { id: string } | Promise<{ id: string }>
): Promise<string> {
  const resolved = await params;
  return resolved.id;
}

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } | Promise<{ id: string }> }
) {
  const id = await getParamsId(params);

  const project = await getProject(id);
  if (!project) {
    return NextResponse.json({ error: 'Project not found in registry' }, { status: 404 });
  }

  const projectRootAbs = path.join(PROJECTS_DIR, project.name);

  // Ensure project root exists
  try {
    const stat = await fs.stat(projectRootAbs);
    if (!stat.isDirectory()) {
      return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
    }
  } catch {
    return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
  }

  const requestedPath = request.nextUrl.searchParams.get('path') || '';

  /** Root listing: show allowed directories that exist on disk. */
  if (!requestedPath) {
    const roots: FileInfo[] = [];

    for (const root of ALLOWED_ROOTS) {
      const abs = path.join(projectRootAbs, root);
      try {
        const s = await fs.stat(abs);
        if (s.isDirectory()) {
          roots.push({ name: root, path: root, type: 'folder' });
        }
      } catch {
        // If a folder doesn't exist yet, just don't show it.
      }
    }

    return NextResponse.json({
      type: 'directory',
      path: '',
      items: roots,
    });
  }

  // Validate path (block unknown roots)
  const rel = validateRelativePath(requestedPath);
  if (rel === null) {
    return NextResponse.json({ error: 'Path not allowed' }, { status: 404 });
  }

  // Absolute target path
  const targetAbs = path.join(projectRootAbs, ...toPosix(rel).split('/'));

  // Must remain inside project folder
  if (!isPathInside(projectRootAbs, targetAbs)) {
    return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
  }

  // Must remain inside allowed roots
  if (!isAllowedRelative(rel)) {
    return NextResponse.json({ error: 'Path not allowed' }, { status: 404 });
  }

  let stat;
  try {
    stat = await fs.stat(targetAbs);
  } catch {
    return NextResponse.json({ error: 'Path not found' }, { status: 404 });
  }

  if (stat.isDirectory()) {
    const items = await readDirectory(targetAbs, projectRootAbs);
    return NextResponse.json({
      type: 'directory',
      path: requestedPath,
      items,
    });
  }

  if (stat.isFile()) {
    const content = await readTextFileSafe(targetAbs);
    return NextResponse.json({
      type: 'file',
      path: requestedPath,
      name: path.basename(targetAbs),
      content,
    });
  }

  return NextResponse.json({ error: 'Unknown file type' }, { status: 400 });
}

export async function POST(
  request: NextRequest,
  { params }: { params: { id: string } | Promise<{ id: string }> }
) {
  const id = await getParamsId(params);

  const project = await getProject(id);
  if (!project) {
    return NextResponse.json({ error: 'Project not found in registry' }, { status: 404 });
  }

  const projectRootAbs = path.join(PROJECTS_DIR, project.name);

  // Ensure project root exists
  try {
    const stat = await fs.stat(projectRootAbs);
    if (!stat.isDirectory()) {
      return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
    }
  } catch {
    return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const files = isRecord(body) ? body.files : undefined;
  if (!Array.isArray(files)) {
    return NextResponse.json(
      { error: 'Body must be { files: Array<{ path, content }> }' },
      { status: 400 }
    );
  }

  const results: { path: string; success: boolean; error?: string }[] = [];

  for (const file of files) {
    try {
      if (!isWritableFileInput(file)) {
        results.push({
          path: getWritableFilePath(file),
          success: false,
          error: 'Invalid file entry. Expected { path: string; content: string }',
        });
        continue;
      }

      // Validate path (block unknown roots)
      const rel = validateRelativePath(file.path);
      if (rel === null) {
        results.push({ path: file.path, success: false, error: 'Path not allowed' });
        continue;
      }

      // Must be within allowed roots
      if (!isAllowedRelative(rel)) {
        results.push({ path: file.path, success: false, error: 'Path not allowed' });
        continue;
      }

      const targetAbs = path.join(projectRootAbs, ...toPosix(rel).split('/'));

      // Must remain inside project
      if (!isPathInside(projectRootAbs, targetAbs)) {
        results.push({ path: file.path, success: false, error: 'Invalid path' });
        continue;
      }

      await fs.mkdir(path.dirname(targetAbs), { recursive: true });
      await fs.writeFile(targetAbs, file.content, 'utf-8');

      results.push({ path: file.path, success: true });
    } catch (error) {
      results.push({
        path: getWritableFilePath(file),
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  }

  return NextResponse.json({ results });
}
