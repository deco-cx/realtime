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
  // TODO: add jwt back
  // app.use("*", withAuth());

  const sites = app.basePath("/mount");

  sites.all("/:drive", async (ctx) => {
    // TODO: add JWT back
    // const canRun = get("checkIsAllowed");
    // canRun(exec.workflow);

    const drivename = ctx.req.param("drive");

    // @ts-expect-error somehoe tsc does not get this ctx typings
    const object = getObjectFor(drivename, ctx);

    return object.fetch(ctx.req.raw);
  });

  return sites;
};
