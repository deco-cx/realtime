import {
  Acked,
  ClientMessages,
  FSError,
  hash,
  ServerMessages,
} from "../client.ts";
import { type Env } from "./index.ts";
import { applyReducer, type Operation } from "fast-json-patch";

export const getObjectFor = (volume: string, ctx: { env: Env }) =>
  ctx.env.REALTIME.get(
    ctx.env.REALTIME.idFromName(volume),
  ) as unknown as Realtime;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type Context = { params: Record<string, string> };

type Handler = (req: Request, ctx: Context) => Promise<Response>;

type Route = Handler | Record<string, Handler>;

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
  ops?: Operation[];
  content?: unknown;
}

export interface VolumePatchResponse {
  results: FilePatchResult[];
}

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

interface RealtimeFS {
  fs: FileSystemNode;
}

interface MFFS {
  writeFile: (filepath: string, content: string) => Promise<void>;
  readFile: (filepath: string) => Promise<string | null>;
  readdir: (root: string) => Promise<string[]>;
}

const createMemFS = (root: FileSystemNode = {}): MFFS => {
  return {
    readFile: async (path: string): Promise<string | null> => {
      const file = root[path];

      if (!file) {
        throw new FSError("ENOENT");
      }

      return file.content;
    },
    writeFile: async (path, content) => {
      root[path] = { content };
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
    readFile: async (filepath: string): Promise<string | null> => {
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
    readdir: async (filepath: string) => {
      const match = await state.storage.list<Metadata>({
        prefix: metaKey(filepath),
      });

      const files: string[] = [];

      for (const key of match.keys()) {
        const base = key.replace(filepath, "");

        if (base.lastIndexOf("/") === 0) {
          files.push(base);
        }
      }

      return files;
    },
  };
};

const tieredFS = (fast: MFFS, slow: MFFS): MFFS => {
  return {
    readdir: fast.readdir,
    readFile: fast.readFile,
    writeFile: async (filepath, content) => {
      await Promise.all([
        slow.writeFile(filepath, content),
        fast.writeFile(filepath, content),
      ]);
    },
  };
};

interface FileSystem {
  fs: FileSystemNode;
  timestamp: number;
  volumeId: string;
}

export class Realtime implements DurableObject {
  state: DurableObjectState;
  sessions: Array<{ socket: WebSocket }> = [];
  routes: [URLPattern, Route][];

  root: FileSystemNode;
  fs: MFFS;

  constructor(state: DurableObjectState) {
    this.state = state;

    this.root = {};

    const durableFS = createDurableFS(state);
    const memFS = createMemFS(this.root);

    this.fs = tieredFS(memFS, durableFS);

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

    const routes: Record<string, Route> = {
      "/volumes/:id/files/*": {
        get: async (req, { params }) => {
          const { "0": path, id: volumeId } = params;
          const withContent = new URL(req.url).searchParams.get("content");

          const fs: Record<string, { content: string | null }> = {};
          for (const key of await this.fs.readdir(path)) {
            fs[key] = {
              content: withContent ? await this.fs.readFile(key) : null,
            };
          }

          const body: FileSystem = { fs, timestamp: Date.now(), volumeId };

          return Response.json(body);
        },
      },
      "/volumes/:id/files": {
        put: async (req: Request) => {
          const fs = await req.json() as FileSystemNode;

          await Promise.all(
            Object.entries(fs).map(([path, value]) =>
              this.fs.writeFile(path, value.content)
            ),
          );

          return new Response(null, { status: 204 });
        },
      },
      "/volumes/:id": {
        patch: async (req: Request) => {
          const { patches } = await req.json() as VolumePatchRequest;

          const patched: Record<string, unknown> = {};

          for (const patch of patches) {
            const json = await this.fs.readFile(patch.path).then((c) =>
              JSON.parse(c ?? "{}")
            );

            patched[patch.path] = patch.patches.reduce(applyReducer, json);
          }

          await Promise.all(
            Object.entries(patched).map(([path, json]) =>
              this.fs.writeFile(path, JSON.stringify(json))
            ),
          );

          const body: VolumePatchResponse = { results: [] };

          return Response.json(body);
        },
      },
    };
    this.routes = Object.entries(routes).map(([pathname, handler]) =>
      [new URLPattern({ pathname }), handler] as const
    );
  }

  broadcast(msg: ServerMessages) {
    for (const session of this.sessions) {
      session.socket.send(JSON.stringify(msg));
    }
  }

  async fetch(request: Request) {
    const route = this.routes.find(([r]) => r.test(request.url));

    if (route) {
      const [pattern, handlerOrRecord] = route;

      const handler = typeof handlerOrRecord === "function"
        ? handlerOrRecord
        : handlerOrRecord[request.method.toLowerCase()];

      const match = pattern.exec(request.url);

      if (handler) {
        return handler(request, { params: match?.pathname.groups ?? {} });
      }
    }

    return new Response(null, { status: 404 });

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Only websockets are supported", { status: 400 });
    }

    const { 0: client, 1: socket } = new WebSocketPair();

    this.sessions.push({ socket });

    const send = (msg: Acked<ServerMessages>) =>
      socket.send(JSON.stringify(msg));

    const parse = (data: string | ArrayBuffer) => {
      if (typeof data === "string") {
        return JSON.parse(data) as Acked<ClientMessages>;
      }
      throw new Error("Malformed socket event");
    };

    const messageHandler = async (event: MessageEvent) => {
      const msg = parse(event.data);

      try {
        switch (msg.type) {
          case "readFile": {
            const { ack, filepath } = msg;
            const content = await this.readFile(filepath);

            send({ type: "file-fetched", data: content, ack });

            return;
          }
          case "writeFile": {
            const { filepath, data, prevHash, ack } = msg;
            const { hash } = await this.writeFile(filepath, data, prevHash);

            send({ type: "operation-succeeded", ack });
            this.broadcast({ type: "file-updated", filepath, hash });

            return;
          }
          case "unlink": {
            const { filepath, ack } = msg;

            await this.unlink(filepath);

            send({ type: "operation-succeeded", ack });
            this.broadcast({ type: "file-unlinked", filepath });

            return;
          }
          case "du": {
            const { filepath, ack } = msg;

            const du = await this.du(filepath);

            send({ type: "operation-succeeded", data: du, ack });

            return;
          }
          case "readdir": {
            const { filepath, ack } = msg;

            const result = await this.readdir(filepath);

            send({ type: "operation-succeeded", data: result, ack });

            return;
          }
          default:
            throw new Error(`Unknown socket event ${JSON.stringify(msg)}`);
        }
      } catch (error: any) {
        send({
          type: "operation-failed",
          code: error.code ?? "INTERNAL_SERVER_ERROR",
          reason: error.message || "unknown",
          ack: msg.ack,
        });
      }
    };

    socket.addEventListener(
      "close",
      () => this.sessions = this.sessions.filter((s) => s.socket !== socket),
    );

    socket.addEventListener(
      "message",
      (e) => messageHandler(e).catch(console.error),
    );

    socket.accept();

    return new Response(null, { status: 101, webSocket: client });
  }
}
