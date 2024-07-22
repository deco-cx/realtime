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


function dedupe(paths: string[]): string[] {
    const seenPaths = new Set<string>();

    for (const path of paths) {
        if (!seenPaths.has(path)) {
            seenPaths.add(path);
        }
    }

    return Array.from(seenPaths.values());
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
            lockMany: async (paths: string[]): Promise<Disposable> => {
                /**
                 * Make sure we never try to lockMany having two references to the
                 * same path, else we're gonna be waiting forever
                 */
                const dedupedPaths = dedupe(paths);
                const locks = await Promise.all(dedupedPaths.map(path => acquire(path)));
                return {
                    [Symbol.dispose]: () => {
                        locks.forEach(lock => lock[Symbol.dispose]());
                    }
                }
            },
        };
    }
}