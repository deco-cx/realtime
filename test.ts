import type { FilePatch, ReqBody } from "./src/realtime.ts";

const volume = crypto.randomUUID();
const url = `http://0.0.0.0:8002/volumes/${volume}`;
const patch = (body: ReqBody) =>
  fetch(url, {
    method: "PATCH",
    body: JSON.stringify(body),
  });

const tests = [
  async () => {
    const p: FilePatch[] = [{ path: "/.decofile.tsx", content: "{}", sha1: "1" }];

    const res = await patch({ patches: p });

    if (res.status !== 204) {
      throw res;
    }
  },
  async () => {
    const p: FilePatch[] = [{
      path: "/.decofile.tsx",
      content: "{}",
      sha1: "1",
      prevSha1: "2",
    }];

    const res = await patch({ patches: p });

    if (res.status !== 409) {
      throw res;
    }
  },
  async () => {
    const p: FilePatch[] = [{
      path: "/.decofile.tsx",
      content: `{"hello": "world"}`,
      sha1: "2",
      prevSha1: "1",
    }];

    const res = await patch({ patches: p });

    if (res.status !== 204) {
      throw res;
    }
  },
  async () => {
    const p: FilePatch[] = [{
      path: "/.decofile.tsx",
      patches: [],
    }];

    const res = await patch({ patches: p });

    if (res.status !== 400) {
      throw res;
    }
  },
  async () => {
    const p: FilePatch[] = [
      {
        path: "/.decofile.json",
        patches: [
          {
            op: "replace",
            path: "/user/firstName",
            value: "Albert",
          },
          {
            op: "replace",
            path: "/user/lastName",
            value: "Einstein",
          },
        ],
      },
    ];
    
    const res = await patch({ patches: p });

    if (res.status !== 204) {
      throw res;
    }
  },
  async () => {
    const p: FilePatch[] = [
      {
        path: "/.decofile.json",
        patches: [
          {
            op: "test",
            path: "/user/firstName",
            value: "Albert",
          },
          {
            op: "replace",
            path: "/user/firstName",
            value: "Max",
          },
        ],
      },
    ];
    
    const res1 = await patch({ patches: p });

    if (res1.status !== 204) {
      throw res1;
    }

    const res2 = await patch({ patches: p });

    if (res2.status !== 409) {
      throw res1;
    }
  },
];

for (let it = 0; it < tests.length; it++) {
  await tests[it]().catch((error) => {
    console.log("Error while testing", it, "\n", error);
  });
}
