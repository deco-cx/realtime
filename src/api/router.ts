import type { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { getObjectFor } from "../realtime.ts";
import { wellKnownJWKSHandler } from "../security/identity.ts";
// import { withAuth } from "./auth.ts";

export const getRouter = async (app: Hono) => {
  app.onError((err) => {
    console.log(err);
    if (err instanceof HTTPException) {
      // Get the custom response
      return err.getResponse();
    }
    throw err;
  });
  app.use("/.well_known/jwks.json", wellKnownJWKSHandler);
  // app.use("*", withAuth());

  app.all("/volumes/:id/*", async (ctx) => {
    // TODO: add JWT back
    // const canRun = get("checkIsAllowed");
    // canRun(exec.workflow);

    const volume = ctx.req.param("id");

    // @ts-expect-error somehoe tsc does not get this ctx typings
    const durable = getObjectFor(volume, ctx);

    return durable.fetch(ctx.req.raw);
  });

  return app;
};
