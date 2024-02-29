export type BaseContext = { params: Record<string, string> };

export type Handler<TContext> = (
  req: Request,
  ctx: TContext & BaseContext,
) => Promise<Response>;

export type Route<TContext> =
  | Handler<TContext>
  | Record<string, Handler<TContext>>;

export type Routes<TContext = {}> = Record<string, Route<TContext>>;

/** O(n) router */
export const createRouter = <TContext = {}>(routes: Routes<TContext>) => {
  const compiled = Object.entries(routes).map(([pathname, handler]) =>
    [new URLPattern({ pathname }), handler] as const
  );

  return async (request: Request, ctx?: TContext): Promise<Response> => {
    const route = compiled.find(([r]) => r.test(request.url));

    if (route) {
      const [pattern, handlerOrRecord] = route;

      const handler = typeof handlerOrRecord === "function"
        ? handlerOrRecord
        : handlerOrRecord[request.method.toUpperCase()];

      const match = pattern.exec(request.url);

      if (handler) {
        // @ts-expect-error somehow TypeScript does not undersntand this works
        return handler(request, {
          ...ctx,
          params: match?.pathname.groups ?? {},
        });
      }
    }

    return new Response(null, { status: 404 });
  };
};

export type Router<TContext = {}> = (
  request: Request,
  ctx?: TContext,
) => Promise<Response>;
