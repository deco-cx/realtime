import { Hono } from "hono";
import { getRouter } from "./api/router.ts";
import { setFromString } from "./security/keys.ts";

export interface Env {
  REALTIME: DurableObjectNamespace;
  WORKER_PUBLIC_KEY: string;
  WORKER_PRIVATE_KEY: string;
}

export default {
  async fetch(r: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    setFromString(env.WORKER_PUBLIC_KEY, env.WORKER_PRIVATE_KEY);

    const router = await getRouter(new Hono());
    return await router.fetch(r, env, ctx);
  },
};

export { Realtime } from "./realtime.ts";
