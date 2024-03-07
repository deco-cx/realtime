import * as fjp from "fast-json-patch";
import type {
  FileSystemNode,
  ServerEvent,
  VolumeListResponse,
  VolumePatchRequest,
  VolumePatchResponse,
} from "./src/realtime.ts";

const jp = fjp.default;

const useProd = false;
const prod = "https://durable-realtime-fs.deco-cx.workers.dev/";
const localhost = "http://0.0.0.0:8002";
const base = useProd ? prod : localhost;
const volume = crypto.randomUUID();

const client = () => {
  const ctrl = new AbortController();
  const socket = new WebSocket(
    new URL(`/volumes/${volume}/files`, base.replace("http", "ws")).href,
  );

  const socketReady = new Promise<unknown>((resolve, reject) => {
    socket.addEventListener("open", resolve);
    socket.addEventListener("error", reject);
  });

  return {
    create: async (node: FileSystemNode): Promise<void> => {
      await fetch(new URL(`/volumes/${volume}/files`), {
        method: "PUT",
        body: JSON.stringify(node),
        headers: { "Content-Type": "application/json" },
      });
    },
    patch: (vpr: VolumePatchRequest): Promise<VolumePatchResponse> =>
      fetch(new URL(`/volumes/${volume}/files`, base), {
        method: "PATCH",
        body: JSON.stringify(vpr),
        headers: { "Content-Type": "application/json" },
      })
        .then((res) => res.json()),
    list: (
      { path, content }: { path: string; content?: boolean },
    ): Promise<VolumeListResponse> =>
      fetch(new URL(`/volumes/${volume}/files${path}?content=${content}`, base))
        .then((res) => res.json()),
    watch: async function* watch() {
      // Wait for the WebSocket connection to open
      await socketReady;

      // Infinite loop to keep listening for incoming messages
      while (!ctrl.signal.aborted) {
        try {
          // Wait for the next message
          const event = await new Promise<ServerEvent>((resolve, reject) => {
            ctrl.signal.addEventListener("abort", reject);

            socket.addEventListener(
              "message",
              (e: MessageEvent) =>
                resolve(JSON.parse(e.data as string) as ServerEvent),
              { once: true },
            );
          });

          // Yield the received message
          yield event;
        } catch {
          /**  */
        }
      }
    },
    [Symbol.dispose]: () => {
      socket.close();
      ctrl.abort();
    },
  };
};

using realtime = client();

const assertEquals = (e1: unknown, e2: unknown) => {
  if (e1 !== e2) {
    throw new Error(`Expected ${e1} to match ${e2}`);
  }
};

const assertAll = (...elems: unknown[]) => {
  if (!elems.every((e) => e)) {
    throw new Error("Expected all elements to be true");
  }
};

const tests = {
  "Should be accepted": async () => {
    const { results } = await realtime.patch({
      patches: [
        {
          path: "/home.json",
          patches: jp.compare({}, { "title": "home" }),
        },
        {
          path: "/pdp.json",
          patches: jp.compare({}, { "title": "pdp" }),
        },
        {
          path: "/sections/ProductShelf.tsx",
          content: `BC`,
        },
      ],
    });

    assertAll(...results.map((r) => r.accepted));
  },
  "Should return updated value": async () => {
    const vlr = await realtime.list({ path: "/", content: true });

    assertEquals(
      vlr.fs["/home.json"]?.content,
      JSON.stringify({ "title": "home" }),
    );
    assertEquals(
      vlr.fs["/pdp.json"]?.content,
      JSON.stringify({ "title": "pdp" }),
    );
    assertEquals(
      vlr.fs["/sections/ProductShelf.tsx"]?.content,
      "BC",
    );
  },
  "Should not return value": async () => {
    const vlr = await realtime.list({ path: "/" });

    assertEquals(
      vlr.fs["/home.json"]?.content,
      null,
    );
    assertEquals(
      vlr.fs["/pdp.json"]?.content,
      null,
    );
    assertEquals(
      vlr.fs["/sections/ProductShelf.tsx"]?.content,
      null,
    );
  },
  "should return specific listing value": async () => {
    const vlr = await realtime.list({ path: "/home.json" });

    assertAll(vlr.fs["/home.json"]);
    assertEquals(vlr.fs["/pdp.json"], undefined);
  },
  "should accept text patch": async () => {
    const shelf = "/sections/ProductShelf.tsx";
    const vlr = await realtime.list({ path: shelf, content: true });
    assertEquals(vlr.fs[shelf]?.content, "BC");
    const { results } = await realtime.patch({
      patches: [
        {
          path: shelf,
          operations: [{
            text: "A",
            at: 0,
          }],
          timestamp: vlr.timestamp,
        },
      ],
    });
    const snapshot = JSON.stringify([{
      accepted: true,
      path: shelf,
    }]);
    assertEquals(JSON.stringify(results), snapshot);

    const vlrUpdated = await realtime.list({ path: shelf, content: true });
    assertEquals(vlrUpdated.fs[shelf]?.content, "ABC");
  },
  "should accept multiple text patch": async () => {
    const shelf = "/sections/ProductShelf.tsx";
    const vlr = await realtime.list({ path: shelf, content: true });
    assertEquals(vlr.fs[shelf]?.content, "ABC");
    const { results } = await realtime.patch({
      patches: [
        {
          path: shelf,
          operations: [{
            text: "!",
            at: 0,
          }, {
            text: "Z",
            at: 0,
          }],
          timestamp: vlr.timestamp,
        },
      ],
    });
    const snapshot = JSON.stringify([{
      accepted: true,
      path: shelf,
    }]);
    assertEquals(JSON.stringify(results), snapshot);

    const vlrUpdated = await realtime.list({ path: shelf, content: true });
    assertEquals(vlrUpdated.fs[shelf]?.content, "!ZABC");

    const { results: resultsWithOldTimestamp } = await realtime.patch({
      patches: [
        {
          path: shelf,
          operations: [{
            text: "!",
            at: 3,
          }, {
            length: 1,
            at: 2,
          }],
          timestamp: vlr.timestamp, // from an old timestamp insert ! at the end, AB! as result
        },
      ],
    });
    const snapShotWithOldTimestamp = JSON.stringify([{
      accepted: true,
      path: shelf,
    }]);
    assertEquals(
      JSON.stringify(resultsWithOldTimestamp),
      snapShotWithOldTimestamp,
    );

    const vlrWithOldTimestamp = await realtime.list({
      path: shelf,
      content: true,
    });
    assertEquals(vlrWithOldTimestamp.fs[shelf]?.content, "!ZAB!");
  },
  "should not accept patch because of conflicts": async () => {
    const { results } = await realtime.patch({
      patches: [
        {
          path: "/home.json",
          patches: jp.compare(
            { "title": "not home" },
            { "title": "home" },
            true,
          ),
        },
      ],
    });

    const snapshot = JSON.stringify([{
      accepted: false,
      path: "/home.json",
      content: '{"title":"home"}',
    }]);
    assertEquals(JSON.stringify(results), snapshot);
  },
  "should accept nested patches": async () => {
    const { results } = await realtime.patch({
      patches: [
        {
          path: "/home/home.json",
          patches: jp.compare({}, { "title": "home" }, true),
        },
      ],
    });

    assertAll(...results.map((r) => r.accepted));
  },
  "should delete files": async () => {
    const { results } = await realtime.patch({
      patches: [
        {
          path: "/home/home.json",
          patches: [{ op: "remove", path: "" }],
        },
      ],
    });

    const vlr = await realtime.list({ path: "/home/home.json" });

    assertAll(...results.map((r) => r.deleted));
    assertEquals(Object.keys(vlr.fs).length, 0);
  },
  "should respect timestamps": async () => {
    const { timestamp } = await realtime.patch({
      patches: [
        {
          path: "/home/home.json",
          patches: [{ op: "add", path: "/hello", value: "world" }],
        },
      ],
    });

    const vlr = await realtime.list({ path: "/home/home.json" });

    assertEquals(timestamp, vlr.timestamp);
  },
  "should watch file changes": async () => {
    setTimeout(() =>
      realtime.patch({
        patches: [
          {
            path: "/home/home.json",
            patches: [{ op: "replace", path: "/hello", value: "deco" }],
          },
        ],
      }), 500);

    for await (const event of realtime.watch()) {
      assertEquals(event.path, "/home/home.json");
      break;
    }
  },
  "should watch timestamp file changes": async () => {
    let p: Promise<VolumePatchResponse>;

    setTimeout(() => {
      p = realtime.patch({
        patches: [
          {
            path: "/home/home.json",
            patches: [{ op: "replace", path: "/hello", value: "deco" }],
          },
        ],
      });
    }, 500);

    for await (const event of realtime.watch()) {
      // @ts-expect-error I know better
      assertEquals((await p).timestamp, event.timestamp);
      break;
    }
  },
  "should watch deleted files": async () => {
    setTimeout(() =>
      realtime.patch({
        patches: [
          {
            path: "/home/home.json",
            patches: [{ op: "remove", path: "" }],
          },
        ],
      }), 500);

    for await (const event of realtime.watch()) {
      assertEquals(event.deleted, true);
      break;
    }
  },
};

for (const test of Object.entries(tests)) {
  try {
    await test[1]();
  } catch (error) {
    console.error(test[0], "\n", error);
  }
}

// @ts-expect-error deno types are not available
Deno.exit(0);
