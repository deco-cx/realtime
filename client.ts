export type ClientMessages =
  | { type: "readFile"; filepath: string }
  | { type: "writeFile"; filepath: string; data: string; prevHash?: string }
  | { type: "unlink"; filepath: string; prevHash?: string }
  | { type: "du"; filepath: string }
  | { type: "readdir"; filepath: string };

export type ErrorCode =
  | "INTERNAL_SERVER_ERROR"
  | "ENOENT"
  | "ENOTDIR"
  | "EEXIST"
  | "ENOTEMPTY"
  | "ESTALE";

export type ServerMessages =
  | { type: "file-fetched"; data: string | undefined }
  | { type: "file-updated"; filepath: string; hash: string }
  | { type: "file-unlinked"; filepath: string }
  | { type: "operation-succeeded"; data?: string }
  | { type: "operation-failed"; reason?: string; code: ErrorCode };

export type Acked<T> = T & { ack: string };

export type EncodingOpts = {
  encoding?: "utf8";
};

export type StatLike = {
  type: "file" | "dir" | "symlink";
  mode: number;
  size: number;
  ino: number | string | BigInt;
  mtimeMs: number;
  ctimeMs?: number;
};

export class FSError extends Error {
  constructor(
    public code: ErrorCode,
    message?: string,
  ) {
    super(message);
  }
}

export const hash = async (data: string) => {
  const msgUint8 = new TextEncoder().encode(data); // encode as (utf-8) Uint8Array
  const hashBuffer = await crypto.subtle.digest("SHA-1", msgUint8); // hash the message
  const hashArray = Array.from(new Uint8Array(hashBuffer)); // convert buffer to byte array
  const hashHex = hashArray
    .map((b) => b.toString(16).padStart(2, "0"))
    .join(""); // convert bytes to hex string
  return hashHex;
};

export interface IBackend {
  // highly recommended - usually necessary for apps to work
  readFile(filepath: string, opts?: EncodingOpts): Promise<string>; // throws ENOENT
  writeFile(filepath: string, data: string, opts?: EncodingOpts): Promise<void>; // throws ENOENT
  unlink(filepath: string, opts?: any): void; // throws ENOENT
  readdir(filepath: string, opts?: any): Promise<string[]>; // throws ENOENT, ENOTDIR
  mkdir(filepath: string, opts?: any): void; // throws ENOENT, EEXIST
  rmdir(filepath: string, opts?: any): void; // throws ENOENT, ENOTDIR, ENOTEMPTY

  // recommended - often necessary for apps to work
  stat(filepath: string, opts?: any): Promise<StatLike>; // throws ENOENT
  lstat(filepath: string, opts?: any): Promise<StatLike>; // throws ENOENT

  // suggested - used occasionally by apps
  rename(oldFilepath: string, newFilepath: string): void; // throws ENOENT
  readlink(filepath: string, opts?: any): Promise<string>; // throws ENOENT
  symlink(target: string, filepath: string): void; // throws ENOENT

  // bonus - not part of the standard `fs` module
  backFile(filepath: string, opts: any): void;
  du(filepath: string): Promise<number>;

  // lifecycle - useful if your backend needs setup and teardown
  init?(name?: string, opts?: any): void; // passes initialization options
  activate?(): Promise<void>; // called before fs operations are started
  deactivate?(): Promise<void>; // called after fs has been idle for a while
  destroy?(): Promise<void>; // called before hotswapping backends

  /** Allow "using" keyword */
  [Symbol.dispose](): void;
}

interface Options {
  endpoint: string;
  onDisconnect?: () => void;
  onConnect?: () => void;
  onChange?: (filename: string, action: "update" | "unlink") => {};
}

export const ignore = (code: ErrorCode) => (e: Error) => {
  if (e instanceof FSError && e.code === code) {
    return undefined;
  }
  throw e;
};

export const mount = ({
  endpoint,
  onDisconnect,
  onConnect,
  onChange,
}: Options): IBackend => {
  const cache = new Map<string, string>();

  let socketPromise: Promise<WebSocket> | undefined;
  const transactions = new Map<string, string | boolean | undefined>();

  const getSocket = () => {
    socketPromise ??= init();
    return socketPromise;
  };

  const transaction = () => {
    const id = crypto.randomUUID();
    const t = Promise.withResolvers<string | boolean | undefined>();

    transactions.set(id, t);

    return { id, response: t.promise };
  };

  const init = (name?: string, retry = 0): Promise<WebSocket> => {
    const sp = Promise.withResolvers<WebSocket>();

    const ws = new WebSocket(endpoint);

    ws.addEventListener("open", () => {
      transactions.clear();
      sp.resolve(ws);
      onConnect?.();
    });

    ws.addEventListener("message", async (event: MessageEvent) => {
      if (typeof event.data !== "string") {
        return console.error(
          "Received and unparsable event. I dont know what to do",
        );
      }

      const msg = JSON.parse(event.data) as Acked<ServerMessages>;

      if (msg.type === "file-unlinked") {
        cache.delete(msg.filepath);

        return onChange?.(msg.filepath, "unlink");
      }

      if (msg.type === "file-updated") {
        const fromCache = cache.get(msg.filepath);

        const shouldUpdate = fromCache
          ? await hash(fromCache) !== msg.hash
          : false;

        if (!shouldUpdate) {
          return;
        }

        cache.delete(msg.filepath);

        return onChange?.(msg.filepath, "update");
      }

      const t = transactions.get(msg.ack);

      if (!t) {
        throw new Error(`Missing transaction for event: ${msg.type}`);
      }

      if (msg.type === "operation-failed") {
        t.reject(new FSError(msg.code, msg.reason));
      } else {
        t.resolve(msg.data);
      }

      transactions.delete(msg.ack);
    });

    // retry with a bounded exponential backoff
    ws.addEventListener("close", () => {
      setTimeout(
        () => init(name, retry + 1),
        Math.min(Math.round(Math.pow(1.5, retry) * 500), 30_000),
      );
      onDisconnect?.();
    });

    return sp.promise;
  };

  const request = async (
    msg: ClientMessages,
  ): Promise<string | boolean | undefined> => {
    const socket = await getSocket();
    const { id, response } = transaction();

    socket.send(JSON.stringify({ ...msg, ack: id }));

    return response;
  };

  const readFile = async (filepath: string) => {
    const fromCache = cache.get(filepath);

    if (fromCache) {
      return fromCache;
    }

    const response = await request({ type: "readFile", filepath });

    if (typeof response !== "string") {
      throw new Error("Unknown data type");
    }

    cache.set(filepath, response);

    return response;
  };

  const writeFile = async (filepath: string, data: string) => {
    const prevHash = await readFile(filepath)
      .then(hash)
      .catch(ignore("ENOENT"));

    cache.set(filepath, data);

    await request({ type: "writeFile", filepath, data, prevHash });
  };

  const unlink = async (filepath: string) => {
    const prevHash = await readFile(filepath)
      .then(hash)
      .catch(ignore("ENOENT"));

    cache.delete(filepath);

    await request({ type: "unlink", filepath, prevHash });
  };

  const destroy = async () => {
    const socket = await getSocket();

    socket.close();
  };

  const du = (filepath: string) =>
    request({ type: "du", filepath }).then((data) => Number(data));

  const readdir = (filepath: string) =>
    request({ type: "readdir", filepath })
      .then((data) => JSON.parse(data as string));

  const fs = {
    writeFile,
    readFile,
    unlink,

    readdir,
    mkdir: () => {
      throw new Error("Not Implemented");
    },
    rmdir: () => {
      throw new Error("Not Implemented");
    },
    stat: () => {
      throw new Error("Not Implemented");
    },

    lstat: () => {
      throw new Error("Not Implemented");
    },
    rename: () => {
      throw new Error("Not Implemented");
    },
    readlink: () => {
      throw new Error("Not Implemented");
    },
    symlink: () => {
      throw new Error("Not Implemented");
    },
    backFile: () => {
      throw new Error("Not Implemented");
    },

    du,

    init,
    destroy,

    [Symbol.dispose]: destroy,
  };

  return fs;
};
