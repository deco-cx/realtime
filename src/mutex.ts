import { FilePatch } from "./realtime.types.ts";

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
                const locks = await Promise.all(patches.map(patch => acquire(patch.path)));
                return {
                    [Symbol.dispose]: () => {
                        locks.forEach(lock => lock[Symbol.dispose]());
                    }
                }
            },
        };
    }
}