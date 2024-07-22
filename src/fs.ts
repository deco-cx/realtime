import { FSError, MFFS, RealtimeState } from "./realtime.ts";
const encoder = new TextEncoder();
const decoder = new TextDecoder();
export const createDurableFS = (state: RealtimeState): MFFS => {
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
