import {
  Acked,
  ClientMessages,
  FSError,
  hash,
  ServerMessages,
} from "../client.ts";
import { type Env } from "./index.ts";

export const getObjectFor = (drivename: string, ctx: { env: Env }) =>
  ctx.env.REALTIME.get(
    ctx.env.REALTIME.idFromName(drivename),
  ) as unknown as Realtime;

const metaKey = (filepath: string) => `meta::${filepath}`;
const chunkKey = (filepath: string, chunk: number) =>
  `chunk::${filepath}::${chunk}`;

interface Metadata {
  chunks: string[];
  hash: string;
  byteLength: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class Realtime implements DurableObject {
  state: DurableObjectState;
  sessions: Array<{ socket: WebSocket }> = [];

  MAX_CHUNK_SIZE = 131072;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async assertHash(filepath: string, hash: string) {
    const meta = await this.state.storage.get<Metadata>(metaKey(filepath));

    if (meta?.hash !== hash) {
      throw new FSError("ESTALE");
    }
  }

  async du(filepath: string): Promise<number> {
    const match = await this.state.storage.list<Metadata>({
      prefix: metaKey(filepath),
    });

    return [...match.values()].reduce(
      (acc, curr: Metadata) => acc + curr.byteLength,
      0,
    );
  }

  async writeFile(filepath: string, data: string, prevHash?: string) {
    prevHash && this.assertHash(filepath, prevHash);

    const fullData = encoder.encode(data);
    const chunks: Record<string, Uint8Array> = {};

    for (
      let chunk = 0;
      chunk * this.MAX_CHUNK_SIZE < fullData.length;
      chunk++
    ) {
      const key = chunkKey(filepath, chunk);

      chunks[key] = fullData.slice(
        chunk * this.MAX_CHUNK_SIZE,
        (chunk + 1) * this.MAX_CHUNK_SIZE,
      );
    }

    const metadata: Metadata = {
      chunks: Object.keys(chunks),
      hash: await hash(data),
      byteLength: fullData.byteLength,
    };

    await this.state.storage.put<Uint8Array>(chunks);
    await this.state.storage.put<Metadata>(metaKey(filepath), metadata);

    return metadata;
  }

  async readFile(filepath: string) {
    const meta = await this.state.storage.get<Metadata>(metaKey(filepath));

    if (!meta) {
      throw new FSError("ENOENT");
    }

    const chunks = await this.state.storage.get<Uint8Array>(meta.chunks);

    const decoded = decoder.decode(
      new Uint8Array(meta.chunks.reduce(
        (acc, curr) => [...acc, ...chunks.get(curr)!],
        [] as number[],
      )),
    );

    return decoded;
  }

  async unlink(filepath: string) {
    const key = metaKey(filepath);
    const meta = await this.state.storage.get<Metadata>(key);

    if (!meta) {
      throw new FSError("ENOENT");
    }

    await this.state.storage.delete(meta.chunks);
    await this.state.storage.delete(key);
  }

  async readdir(filepath: string) {
    const match = await this.state.storage.list<Metadata>({
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
  }

  broadcast(msg: ServerMessages) {
    for (const session of this.sessions) {
      session.socket.send(JSON.stringify(msg));
    }
  }

  async fetch(request: Request) {
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

            send({ type: "operation-succeeded", data: String(du), ack });

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
