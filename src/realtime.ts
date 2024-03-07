import { applyReducer, type Operation } from "fast-json-patch";
import { BinaryIndexedTree } from "./bit.ts";
import { type Env } from "./index.ts";
import { createRouter, Router, Routes } from "./router.ts";

export const getObjectFor = (volume: string, ctx: { env: Env }) => {
  const object = volume.startsWith("ephemeral:")
    ? ctx.env.EPHEMERAL_REALTIME
    : ctx.env.REALTIME;
  return object.get(
    object.idFromName(volume),
  ) as unknown as Realtime;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface BaseFilePatch {
  path: string;
}
export type TextFilePatchOperation = InsertAtOperation | DeleteAtOperation;

export interface TextFilePatch extends BaseFilePatch {
  operations: TextFilePatchOperation[];
  timestamp: number;
}

export interface TextFileSet extends BaseFilePatch {
  content: string;
}

export interface TextFielPatchOperationBase {
  at: number;
}

export interface InsertAtOperation extends TextFielPatchOperationBase {
  text: string;
}

export interface DeleteAtOperation extends TextFielPatchOperationBase {
  length: number;
}

const isDeleteOperation = (
  op: TextFilePatchOperation,
): op is DeleteAtOperation => {
  return (op as DeleteAtOperation).length !== undefined;
};

export type FilePatch = JSONFilePatch | TextFilePatch | TextFileSet;

export const isJSONFilePatch = (patch: FilePatch): patch is JSONFilePatch => {
  return (patch as JSONFilePatch).patches !== undefined;
};

export const isTextFileSet = (patch: FilePatch): patch is TextFileSet => {
  return (patch as TextFileSet).content !== undefined;
};

export interface JSONFilePatch extends BaseFilePatch {
  patches: Operation[];
}

export interface VolumePatchRequest {
  patches: FilePatch[];
}

export interface FilePatchResult {
  path: string;
  accepted: boolean;
  content?: string;
  deleted?: boolean;
}

export interface VolumePatchResponse {
  results: FilePatchResult[];
  timestamp: number;
}

export interface FsEvent {
  path: string;
  deleted?: boolean;
  timestamp: number;
}

export type ServerEvent = FsEvent;

export interface File {
  content: string;
}

export type FileSystemNode = Record<string, File>;

export class RealtimeError extends Error {
  public status: number;

  constructor(
    public code: "CONFLICT" | "INTERNAL_SERVER_ERROR",
    message?: string,
  ) {
    super(message);

    this.status = code === "CONFLICT" ? 409 : 500;
  }
}

export type ErrorCode =
  | "INTERNAL_SERVER_ERROR"
  | "ENOENT"
  | "ENOTDIR"
  | "EEXIST"
  | "ENOTEMPTY"
  | "ESTALE";

export class FSError extends Error {
  constructor(
    public code: ErrorCode,
    message?: string,
  ) {
    super(message);
  }
}

const ignore = (code: ErrorCode) => (e: unknown) => {
  if (e instanceof FSError && e.code === code) {
    return null;
  }
  throw e;
};

interface MFFS {
  writeFile: (filepath: string, content: string) => Promise<void>;
  readFile: (filepath: string) => Promise<string | null>;
  readdir: (root: string) => Promise<string[]>;
  unlink: (filepath: string) => Promise<void>;
  clear: () => Promise<void>;
}

const createMemFS = (): MFFS => {
  const root = new Map<string, File>();

  return {
    readFile: async (path: string): Promise<string> => {
      const file = root.get(path);

      if (!file) {
        throw new FSError("ENOENT");
      }

      return file.content;
    },
    writeFile: async (path, content) => {
      root.set(path, { content });
    },
    unlink: async (path): Promise<void> => {
      root.delete(path);
    },
    readdir: async (path): Promise<string[]> =>
      [...root.keys()].filter((x) => x.startsWith(path)),
    clear: async () => {
      root.clear();
    },
  };
};

const createDurableFS = (state: DurableObjectState): MFFS => {
  interface Metadata {
    chunks: string[];
  }

  const MAX_CHUNK_SIZE = 131072;

  const metaKey = (filepath: string) => `meta::${filepath}`;
  const chunkKey = (filepath: string, chunk: number) =>
    `chunk::${filepath}::${chunk}`;

  return {
    readFile: async (filepath: string): Promise<string> => {
      const meta = await state.storage.get<Metadata>(metaKey(filepath));

      if (!meta) {
        throw new FSError("ENOENT");
      }

      const chunks = await state.storage.get<Uint8Array>(meta.chunks);

      const decoded = decoder.decode(
        new Uint8Array(meta.chunks.reduce(
          (acc, curr) => [...acc, ...chunks.get(curr)!],
          [] as number[],
        )),
      );

      return decoded;
    },
    writeFile: async (filepath: string, content: string) => {
      const fullData = encoder.encode(content);
      const chunks: Record<string, Uint8Array> = {};

      for (
        let chunk = 0;
        chunk * MAX_CHUNK_SIZE < fullData.length;
        chunk++
      ) {
        const key = chunkKey(filepath, chunk);

        chunks[key] = fullData.slice(
          chunk * MAX_CHUNK_SIZE,
          (chunk + 1) * MAX_CHUNK_SIZE,
        );
      }

      const metadata: Metadata = { chunks: Object.keys(chunks) };

      await state.storage.put<Uint8Array>(chunks);
      await state.storage.put<Metadata>(metaKey(filepath), metadata);
    },
    unlink: async (filepath: string) => {
      const metaLocator = metaKey(filepath);
      const meta = await state.storage.get<Metadata>(metaLocator);

      if (!meta) {
        return;
      }

      await state.storage.delete(metaLocator);
      await state.storage.delete(meta.chunks);
    },
    readdir: async (filepath: string) => {
      const match = await state.storage.list<Metadata>({
        prefix: metaKey(filepath),
      });

      const files: string[] = [];
      const base = metaKey("");

      for (const key of match.keys()) {
        files.push(key.replace(base, ""));
      }

      return files;
    },
    clear: async () => state.storage.deleteAll(),
  };
};

const tieredFS = (...fastToSlow: MFFS[]): MFFS => {
  const [fastest] = fastToSlow;

  return {
    readdir: (path) => fastest.readdir(path),
    readFile: (path) => fastest.readFile(path),
    clear: async () => {
      await Promise.all(
        fastToSlow.map((fs) => fs.clear()),
      );
    },
    unlink: async (filepath) => {
      await Promise.all(
        fastToSlow.map((fs) => fs.unlink(filepath)),
      );
    },
    writeFile: async (filepath, content) => {
      await Promise.all(
        fastToSlow.map((fs) => fs.writeFile(filepath, content)),
      );
    },
  };
};

export interface VolumeListResponse {
  fs: Record<string, { content: string | null }>;
  timestamp: number;
  volumeId: string;
}

export class Realtime implements DurableObject {
  textState: Map<number, BinaryIndexedTree>;
  state: DurableObjectState;
  sessions: Array<{ socket: WebSocket }> = [];
  fs: MFFS;

  router: Router;
  timestamp: number;

  constructor(state: DurableObjectState, ephemeral = false) {
    this.textState = new Map();
    this.state = state;
    this.timestamp = Date.now();

    const routes: Routes = {
      "/volumes/:id/files/*": {
        GET: async (req, { params }) => {
          const volumeId = params.id;
          const path = `/${params["0"]}`;
          const withContent =
            new URL(req.url).searchParams.get("content") === "true";

          const fs: Record<string, { content: string | null }> = {};
          for (const key of await this.fs.readdir(path)) {
            fs[key] = {
              content: withContent ? await this.fs.readFile(key) : null,
            };
          }

          return Response.json(
            {
              timestamp: this.timestamp,
              volumeId,
              fs,
            } satisfies VolumeListResponse,
          );
        },
      },
      "/volumes/:id/files": {
        GET: async (req: Request) => {
          if (req.headers.get("Upgrade") !== "websocket") {
            return new Response("Missing header Upgrade: websocket ", {
              status: 400,
            });
          }

          const { 0: client, 1: socket } = new WebSocketPair();

          this.sessions.push({ socket });

          socket.addEventListener(
            "close",
            () =>
              this.sessions = this.sessions.filter((s) => s.socket !== socket),
          );

          socket.accept();

          return new Response(null, { status: 101, webSocket: client });
        },
        PUT: async (req: Request) => {
          const fs = await req.json() as FileSystemNode;

          // clears filesystem before writting new files
          await this.fs.clear();

          await Promise.all(
            Object.entries(fs).map(([path, value]) =>
              this.fs.writeFile(path, value.content)
            ),
          );

          return new Response(null, { status: 204 });
        },
        PATCH: async (req: Request) => {
          const { patches } = await req.json() as VolumePatchRequest;

          const results: FilePatchResult[] = [];

          for (const patch of patches) {
            if (isJSONFilePatch(patch)) {
              const { path, patches: operations } = patch;
              const content =
                await this.fs.readFile(path).catch(ignore("ENOENT")) ?? "{}";

              try {
                const newContent = JSON.stringify(
                  operations.reduce(applyReducer, JSON.parse(content)),
                );

                results.push({
                  accepted: true,
                  path,
                  content: newContent,
                  deleted: newContent === "null",
                });
              } catch (error) {
                results.push({ accepted: false, path, content });
              }
            } else if (isTextFileSet(patch)) {
              const { path, content } = patch;
              try {
                await this.fs.writeFile(path, content);
                results.push({ accepted: true, path, content });
              } catch (error) {
                results.push({ accepted: false, path, content });
              }
            } else {
              const { path, operations, timestamp } = patch;
              const content =
                await this.fs.readFile(path).catch(ignore("ENOENT")) ?? "";
              if (!this.textState.has(timestamp)) { // durable was restarted
                results.push({ accepted: false, path, content });
                continue;
              }
              const rollbacks: Array<() => void> = [];
              const bit = this.textState.get(timestamp) ??
                new BinaryIndexedTree(2 ** 8);
              const [result, success] = operations.reduce(
                ([txt, success], op) => {
                  if (!success) {
                    return [txt, success];
                  }
                  if (isDeleteOperation(op)) {
                    const { at, length } = op;
                    const offset = bit.rangeQuery(0, at) + at;
                    if (offset < 0) {
                      return [txt, false];
                    }
                    const before = txt.slice(0, offset);
                    const after = txt.slice(offset + length);

                    // Update BIT for deletion operation
                    bit.update(at, -length); // Subtract length from the index
                    rollbacks.push(() => {
                      bit.update(at, length);
                    });
                    return [`${before}${after}`, true];
                  }
                  const { at, text } = op;
                  const offset = bit.rangeQuery(0, at) + at;
                  if (offset < 0) {
                    return [txt, false];
                  }

                  const before = txt.slice(0, offset);
                  const after = txt.slice(offset); // Use offset instead of at

                  // Update BIT for insertion operation
                  bit.update(at, text.length); // Add length of text at the index
                  rollbacks.push(() => {
                    bit.update(at, -text.length);
                  });
                  return [`${before}${text}${after}`, true];
                },
                [content, true],
              );
              if (success) {
                this.textState.set(timestamp, bit);
                results.push({
                  accepted: true,
                  path,
                  content: result,
                });
              } else {
                rollbacks.map((rollback) => rollback());
                results.push({
                  accepted: false,
                  path,
                  content,
                });
              }
            }
          }

          this.timestamp = Date.now();
          this.textState.set(this.timestamp, new BinaryIndexedTree(2 ** 8));
          const shouldWrite = results.every((r) => r.accepted);

          if (shouldWrite) {
            await Promise.all(
              results.map(async (r) => {
                try {
                  if (r.deleted) {
                    await this.fs.unlink(r.path);
                  } else {
                    await this.fs.writeFile(r.path, r.content!);
                  }
                } catch (error) {
                  console.error(error);
                  r.accepted = false;
                }
              }),
            );

            const shouldBroadcast = results.every((r) => r.accepted);
            if (shouldBroadcast) {
              for (const result of results) {
                const { path, deleted } = result;
                this.broadcast({ path, timestamp: this.timestamp, deleted });
              }
            }
          }

          return Response.json(
            {
              timestamp: this.timestamp,
              results: results.map((r) =>
                r.accepted ? { ...r, content: undefined } : r
              ),
            } satisfies VolumePatchResponse,
          );
        },
      },
      "/volumes/:id": {},
    };
    this.router = createRouter(routes);
    const memFS = createMemFS();

    if (ephemeral) {
      this.fs = memFS;
      return;
    }
    const durableFS = createDurableFS(state);
    this.fs = tieredFS(memFS, durableFS);
    // init memFS with durable content
    this.state.blockConcurrencyWhile(async () => {
      const paths = await durableFS.readdir("/");

      for (const path of paths) {
        const content = await durableFS.readFile(path);

        if (!content) {
          continue;
        }

        await memFS.writeFile(path, content);
      }
    });
  }

  broadcast(msg: ServerEvent) {
    for (const session of this.sessions) {
      session.socket.send(JSON.stringify(msg));
    }
  }

  async fetch(request: Request) {
    return this.router(request);
  }
}

export class EphemeralRealtime extends Realtime {
  constructor(state: DurableObjectState) {
    super(state, true);
  }
}
