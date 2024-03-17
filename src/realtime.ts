import { applyReducer } from "fast-json-patch";
import { BinaryIndexedTree } from "./crdt/bit.ts";
import { apply } from "./crdt/text.ts";
import { type Env } from "./index.ts";
import {
  File,
  FilePatchResult,
  FileSystemNode,
  isJSONFilePatch,
  isTextFileSet,
  ServerEvent,
  VolumePatchRequest,
  VolumePatchResponse,
} from "./realtime.types.ts";
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

  constructor(state: DurableObjectState, _env: Env, ephemeral = false) {
    this.textState = new Map();
    this.state = state;
    this.timestamp = Date.now();

    const routes: Routes = {
      "/volumes/:id/files/*": {
        GET: async (req, { params }) => {
          const volumeId = params.id;
          const path = decodeURI(`/${params["0"]}`);
          const contentQs = new URL(req.url).searchParams.get("content");
          const contentFilter: false | string = contentQs === "true"
            ? "/"
            : contentQs !== "false" && (contentQs ?? false);

          const fs: Record<string, { content: string | null }> = {};
          for (const key of await this.fs.readdir(path)) {
            const withContent = contentFilter !== false &&
              key.startsWith(contentFilter);

            console.log(
              JSON.stringify({ path, file: await this.fs.readFile(key) }),
            );

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
          const { patches, messageId } = await req.json() as VolumePatchRequest;

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
                if (!content) {
                  await this.fs.writeFile(path, "");
                } else {
                  await this.fs.writeFile(path, content);
                }
                results.push({ accepted: true, path, content: content ?? "" });
              } catch (error) {
                results.push({ accepted: false, path, content: content ?? "" });
              }
            } else {
              const { path, operations, timestamp } = patch;
              const content =
                await this.fs.readFile(path).catch(ignore("ENOENT")) ?? "";
              if (!this.textState.has(timestamp)) { // durable was restarted
                results.push({ accepted: false, path, content });
                continue;
              }
              const bit = this.textState.get(timestamp) ??
                new BinaryIndexedTree();
              const [result, success] = apply(content, operations, bit);
              if (success) {
                this.textState.set(timestamp, bit);
                results.push({
                  accepted: true,
                  path,
                  content: result,
                });
              } else {
                results.push({
                  accepted: false,
                  path,
                  content,
                });
              }
            }
          }

          this.timestamp = Date.now();
          this.textState.set(this.timestamp, new BinaryIndexedTree());
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
                this.broadcast({
                  messageId,
                  path,
                  timestamp: this.timestamp,
                  deleted,
                });
              }
            }
          }

          return Response.json(
            {
              timestamp: this.timestamp,
              results,
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
  constructor(state: DurableObjectState, env: Env) {
    super(state, env, true);
  }
}
