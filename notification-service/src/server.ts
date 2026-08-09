import Fastify, { type FastifyInstance } from "fastify";
import type { NotificationStore } from "./store";

// The BFF surface: a UI asks this service what happened to an order
// instead of asking the orchestrator, so read traffic never touches the
// service that runs transactions.
export function buildServer(store: NotificationStore): FastifyInstance {
  const app = Fastify();

  app.get("/healthz", async () => "ok");

  app.get("/notifications", async () => ({
    count: store.size(),
    notifications: store.all(),
  }));

  app.get<{ Params: { orderId: string } }>("/notifications/:orderId", async (req) => ({
    orderId: req.params.orderId,
    notifications: store.forOrder(req.params.orderId),
  }));

  return app;
}
