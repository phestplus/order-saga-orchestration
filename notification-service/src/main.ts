import { connect, type NatsConnection } from "nats";
import { buildServer } from "./server";
import { NotificationStore } from "./store";
import { toNotification, type SagaResult } from "./notification";

const NATS_URL = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
const PORT = Number(process.env.PORT ?? 8080);

// Compose starts everything at once, so NATS is usually not accepting
// connections yet when this service boots.
async function connectWithRetry(url: string, timeoutMs = 60_000): Promise<NatsConnection> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      return await connect({ servers: url });
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 500));
    }
  }
  throw new Error(`could not connect to NATS at ${url}: ${String(lastErr)}`);
}

async function main(): Promise<void> {
  const store = new NotificationStore();
  const nats = await connectWithRetry(NATS_URL);
  console.log(`notification-service connected to ${NATS_URL}`);

  // This service is a pure consumer. order-service publishes outcomes
  // without knowing anything subscribes, which is why adding an audit or
  // analytics consumer later needs no change to the orchestrator.
  const subscription = nats.subscribe("orders.events");
  void (async () => {
    for await (const msg of subscription) {
      try {
        const result = msg.json<SagaResult>();
        const notification = toNotification(result);
        store.add(notification);
        console.log(`[${notification.severity}] ${notification.message}`);
      } catch (err) {
        // A malformed event must not kill the subscription loop and take
        // the whole feed down with it.
        console.error("could not handle orders.events message:", err);
      }
    }
  })();

  const app = buildServer(store);
  await app.listen({ port: PORT, host: "0.0.0.0" });
  console.log(`notification-service listening on :${PORT}`);

  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.on(signal, () => {
      void (async () => {
        await app.close();
        await nats.drain();
        process.exit(0);
      })();
    });
  }
}

main().catch((err) => {
  console.error("fatal startup error", err);
  process.exit(1);
});
