/**
 * Project File Watcher — watches project directories for changes
 * and notifies subscribed SSE clients so they can refresh the workbench.
 *
 * Watchers are ref-counted: created on first subscriber, closed when last
 * subscriber disconnects.
 */

import * as path from 'path';
import chokidar, { type FSWatcher } from 'chokidar';

export interface FileChangeEvent {
    /** Relative path from project root (e.g. "snowflake/foo.sql") */
    path: string;
    /** Type of change */
    type: 'add' | 'change' | 'unlink';
}

type Subscriber = (event: FileChangeEvent) => void;

interface WatcherEntry {
    watcher: FSWatcher;
    subscribers: Set<Subscriber>;
}

const PROJECTS_DIR = path.resolve(process.cwd(), '..', 'projects');

/** Top-level directories we watch and expose. */
const ALLOWED_ROOTS = ['source', 'snowflake', 'reports', 'artifacts'];

const watchers = new Map<string, WatcherEntry>();

function toPosix(p: string) {
    return p.split(path.sep).join('/');
}

/**
 * Convert an absolute path to a relative path under an allowed root.
 * Returns null if the path is outside allowed roots (event is dropped).
 */
function toRelativePath(absPath: string, projectRoot: string): string | null {
    const rel = toPosix(path.relative(projectRoot, absPath));
    const root = rel.split('/')[0];
    if (!ALLOWED_ROOTS.includes(root)) return null;
    return rel;
}

function createWatcher(projectName: string): WatcherEntry {
    const projectRoot = path.join(PROJECTS_DIR, projectName);

    // Watch the entire project root so we detect files even when parent
    // directories are created after the watcher starts.  The `toRelativePath`
    // filter in the notify callback ensures only files under allowed roots
    // trigger events.
    const watcher = chokidar.watch(projectRoot, {
        ignoreInitial: true,
        persistent: true,
        // Small stabilisation delay so rapid multi-file writes are batched
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
    });

    const entry: WatcherEntry = { watcher, subscribers: new Set() };

    const notify = (type: FileChangeEvent['type'], absPath: string) => {
        const relPath = toRelativePath(absPath, projectRoot);
        if (!relPath) return;
        const event: FileChangeEvent = { path: relPath, type };
        for (const sub of entry.subscribers) {
            try { sub(event); } catch { /* subscriber error – ignore */ }
        }
    };

    watcher.on('add', (p) => notify('add', p));
    watcher.on('change', (p) => notify('change', p));
    watcher.on('unlink', (p) => notify('unlink', p));

    return entry;
}

/**
 * Subscribe to file changes for a project. Returns an unsubscribe function.
 */
export function subscribeToProject(projectName: string, callback: Subscriber): () => void {
    let entry = watchers.get(projectName);
    if (!entry) {
        entry = createWatcher(projectName);
        watchers.set(projectName, entry);
    }
    entry.subscribers.add(callback);

    return () => {
        entry!.subscribers.delete(callback);
        // Close watcher when last subscriber leaves
        if (entry!.subscribers.size === 0) {
            void entry!.watcher.close();
            watchers.delete(projectName);
        }
    };
}
