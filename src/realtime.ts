import type {
  DurableObject,
  DurableObjectState,
} from "@cloudflare/workers-types";
import { BinaryIndexedTree } from "./crdt/bit.ts";
import { apply } from "./crdt/text.ts";
import {
  Env,
  File,
  FilePatchResult,
  FileSystemNode,
  isJSONFilePatch,
  isTextFileSet,
  Operation,
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
  );
};

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

export interface MFFS {
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
export type RealtimeState =
  & Pick<
    DurableObjectState,
    "blockConcurrencyWhile"
  >
  & {
    storage: Pick<
      DurableObjectStorage,
      "get" | "delete" | "put" | "deleteAll" | "list"
    >;
  };

export type RealtimeDurableObjectConstructor = new (
  state: RealtimeState,
  _env: Env,
  ephemeral?: boolean,
) => DurableObject;

export const realtimeFor = (
  upgradeWebSocket: (req: Request) => { socket: WebSocket; response: Response },
  createDurableFS: (state: RealtimeState) => MFFS,
  fjp: {
    applyReducer: <T>(object: T, operation: Operation, index: number) => T;
  },
): RealtimeDurableObjectConstructor => {
  return class Realtime implements DurableObject {
    textState: Map<number, BinaryIndexedTree>;
    state: RealtimeState;
    sessions: Array<{ socket: WebSocket }> = [];
    fs: MFFS;

    router: Router;
    timestamp: number;

    constructor(
      state: RealtimeState,
      _env: Env,
      ephemeral = false,
    ) {
      this.textState = new Map();
      this.state = state;
      this.timestamp = Date.now();

      const routes: Routes = {
        "/volumes/:id/files/*": {
          GET: async (req, { params }) => {
            const volumeId = params.id;
            const path = `/${params["0"]}`;
            const contentQs = new URL(req.url).searchParams.get("content");
            const contentFilter: false | string = contentQs === "true"
              ? "/"
              : contentQs !== "false" && (contentQs ?? false);

            const fs: Record<string, { content: string | null }> = {};
            for (const key of await this.fs.readdir(path)) {
              const withContent = contentFilter !== false &&
                key.startsWith(contentFilter);
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

            const { socket, response } = upgradeWebSocket(req);

            this.sessions.push({ socket });

            socket.addEventListener(
              "close",
              (cls) => {
                this.sessions = this.sessions.filter((s) =>
                  s.socket !== socket
                );
                socket.close(cls.code, "Durable Object is closing WebSocket");
              },
            );

            return response;
          },
          PUT: async (req: Request) => {
            const fs = await req.json() as FileSystemNode;

            const ts = Date.now();
            this.timestamp = ts;
            // clears filesystem before writting new files
            await this.fs.clear();

            await Promise.all(
              Object.entries(fs).map(([path, value]) =>
                this.fs.writeFile(path, value.content).then(
                  () => {
                    // TODO (mcandeia) this is not notifying deleted files.
                    this.broadcast({ path, timestamp: ts });
                  },
                )
              ),
            );

            return new Response(null, { status: 204 });
          },
          PATCH: async (req: Request) => {
            const { patches, messageId } = await req
              .json() as VolumePatchRequest;

            const results: FilePatchResult[] = [];

            for (const patch of patches) {
              if (isJSONFilePatch(patch)) {
                const { path, patches: operations } = patch;
                const content =
                  await this.fs.readFile(path).catch(ignore("ENOENT")) ?? "{}";

                try {
                  const newContent = JSON.stringify(
                    operations.reduce(fjp.applyReducer, JSON.parse(content)),
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
                  await this.fs.writeFile(path, content ?? "");
                  results.push({
                    accepted: true,
                    path,
                    content: content ?? "",
                    deleted: content === null,
                  });
                } catch (error) {
                  results.push({
                    accepted: false,
                    path,
                    content: content ?? "",
                  });
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
  };
};
