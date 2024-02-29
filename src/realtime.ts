import { applyReducer, type Operation } from "fast-json-patch";
import { type Env } from "./index.ts";
import { createRouter, Router, Routes } from "./router.ts";

export const getObjectFor = (volume: string, ctx: { env: Env }) =>
  ctx.env.REALTIME.get(
    ctx.env.REALTIME.idFromName(volume),
  ) as unknown as Realtime;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export type FilePatch = JSONFilePatch;

export interface JSONFilePatch {
  path: string;
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
}

const createMemFS = (root: FileSystemNode = {}): MFFS => {
  return {
    readFile: async (path: string): Promise<string> => {
      const file = root[path];

      if (!file) {
        throw new FSError("ENOENT");
      }

      return file.content;
    },
    writeFile: async (path, content) => {
      root[path] = { content };
    },
    unlink: async (path): Promise<void> => {
      delete root[path];
    },
    readdir: async (path): Promise<string[]> =>
      Object.keys(root).filter((x) => x.startsWith(path)),
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
  };
};

const tieredFS = (...fastToSlow: MFFS[]): MFFS => {
  const [fastest] = fastToSlow;

  return {
    readdir: (path) => fastest.readdir(path),
    readFile: (path) => fastest.readFile(path),
    unlink: async (filepath) => {
      await Promise.all(
        fastToSlow.map((c) => c.unlink(filepath)),
      );
    },
    writeFile: async (filepath, content) => {
      await Promise.all(
        fastToSlow.map((c) => c.writeFile(filepath, content)),
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
  state: DurableObjectState;
  sessions: Array<{ socket: WebSocket }> = [];
  fs: MFFS;

  router: Router;
  timestamp: number;

  constructor(state: DurableObjectState) {
    this.state = state;

    this.timestamp = Date.now();

    const durableFS = createDurableFS(state);
    const memFS = createMemFS();

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
          }

          this.timestamp = Date.now();
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
                  this.fs;
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
