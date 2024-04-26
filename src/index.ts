import { getObjectFor, realtimeFor } from "./realtime.ts";
import { createRouter } from "./router.ts";
//import { wellKnownJWKSHandler } from "./security/identity.ts";
import fjp from "fast-json-patch";
import { createDurableFS } from "./fs.ts";
import { setFromString } from "./security/keys.ts";
import { upgradeWebSocket } from "./ws.ts";
import { Env } from "./realtime.types.ts";

const router = createRouter<{ env: Env }>({
  //"/.well_known/jwks.json": wellKnownJWKSHandler,
  "/volumes/:id/*": async (req, ctx) =>
    getObjectFor(ctx.params.id, ctx).fetch(req),
  "/volumes/:id": async (req, ctx) =>
    getObjectFor(ctx.params.id, ctx).fetch(req),
});

export default {
  async fetch(r: Request, env: Env): Promise<Response> {
    setFromString(env.WORKER_PUBLIC_KEY, env.WORKER_PRIVATE_KEY);

    try {
      return await router(r, { env });
    } catch (error) {
      console.error(error);
      return new Response("Internal Server Error", { status: 500 });
    }
  },
};

export const Realtime = realtimeFor(upgradeWebSocket, createDurableFS, fjp);

export class EphemeralRealtime extends Realtime {
  constructor(state: DurableObjectState, env: Env) {
    super(state, env, true);
  }
}
