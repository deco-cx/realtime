import { FilePatch, isJSONFilePatch, isTextFilePatch, isTextFileSet } from "./realtime.types.ts";

export class Mutex {
    public locked: boolean;
    public queue: Array<ReturnType<typeof Promise.withResolvers<void>>>;
    constructor() {
        this.locked = false;
        this.queue = [];
    }

    acquire(): Promise<Disposable> {
        const disposable = {
            [Symbol.dispose]: () => {
                return this.release();
            },
        };
        if (!this.locked) {
            this.locked = true;
            return Promise.resolve(disposable);
        }
        const promise = Promise.withResolvers<void>();
        this.queue.push(promise);
        return promise.promise.then(() => {
            return disposable;
        });
    }

    freeOrNext(): boolean {
        return !this.locked || this.queue.length === 0;
    }

    release() {
        if (this.queue.length > 0) {
            const next = this.queue.shift();
            next?.resolve();
        } else {
            this.locked = false;
        }
    }
}

/**
 * Dedupes an array of file patches based on path.
 * Patches coming last have priority.
 */
function dedupe(patches: FilePatch[]): FilePatch[] {
    const dedupedPatchesMap = new Map<string, FilePatch>();

    for (const patch of patches) {
        if (!dedupedPatchesMap.has(patch.path)) {
            dedupedPatchesMap.set(patch.path, patch);
        } else {
            const current = dedupedPatchesMap.get(patch.path);
            if (!current) throw Error("Error deduping file patches");

            if (isJSONFilePatch(patch) && isJSONFilePatch(current)) {
                dedupedPatchesMap.set(current.path, {
                    ...current,
                    patches: [
                        ...current.patches,
                        ...patch.patches
                    ]
                });
                continue;
            }
            
            if (isTextFileSet(patch) && isTextFileSet(current)) {
                dedupedPatchesMap.set(current.path, {
                    ...current,
                    content: patch.content,
                });
                continue;
            }
            
            if (isTextFilePatch(current) && isTextFilePatch(patch)) {
                dedupedPatchesMap.set(current.path, {
                    ...current,
                    timestamp: patch.timestamp,
                    operations: [
                        ...current.operations,
                        ...patch.operations,
                    ],
                });
                continue;
            }

            throw new Error("Error deduping file patches");
        }
    }

    return Array.from(dedupedPatchesMap.values());
}

export const FileLocker = {
    new: () => {
        const mutexes = new Map<string, Mutex>();
        const acquire = async (path: string) => {
            let mutex = mutexes.get(path);
            if (!mutex) {
                mutex = new Mutex();
                mutexes.set(path, mutex);
            }
            return mutex.acquire();
        }
        return {
            lockMany: async (patches: FilePatch[]): Promise<Disposable> => {
                /**
                 * Make sure we never try to lockMany having two references to the
                 * same path, else we're gonna be waiting forever
                 */
                const dedupedPatches = dedupe(patches);
                const locks = await Promise.all(dedupedPatches.map(patch => acquire(patch.path)));
                return {
                    [Symbol.dispose]: () => {
                        locks.forEach(lock => lock[Symbol.dispose]());
                    }
                }
            },
        };
    }
}