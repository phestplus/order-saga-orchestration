// The saga outcome as published by order-service on orders.events.
export interface SagaResult {
  sagaId: string;
  status: "completed" | "compensated" | "stuck";
  completed?: string[];
  failedStep?: string;
  failureReason?: string;
  compensated?: string[];
  compensationFailures?: { step: string; reason: string }[];
}

export interface Notification {
  orderId: string;
  severity: "info" | "warning" | "critical";
  message: string;
}

// Turning a saga result into something a human would want to read is
// pure logic, so it lives apart from the NATS subscription and the HTTP
// server and is unit tested directly.
//
// The severity mapping is the interesting part. A rolled-back order is
// only a "warning": nothing was charged, nothing shipped, and the
// customer just needs to be told it did not go through. A stuck saga is
// "critical" regardless of how ordinary the failure looked, because money
// moved and could not be moved back. Collapsing those two into one
// "order failed" notification would bury the only case that needs a
// person.
export function toNotification(result: SagaResult): Notification {
  switch (result.status) {
    case "completed":
      return {
        orderId: result.sagaId,
        severity: "info",
        message: `Order ${result.sagaId} confirmed.`,
      };

    case "compensated":
      return {
        orderId: result.sagaId,
        severity: "warning",
        message:
          `Order ${result.sagaId} could not be completed at the ${result.failedStep} step ` +
          `(${result.failureReason ?? "no reason given"}). ` +
          `Everything already applied was undone, so you have not been charged.`,
      };

    case "stuck": {
      const stuck = (result.compensationFailures ?? [])
        .map((f) => `${f.step} (${f.reason})`)
        .join(", ");
      return {
        orderId: result.sagaId,
        severity: "critical",
        message:
          `Order ${result.sagaId} failed at the ${result.failedStep} step and could not be ` +
          `fully rolled back. Needs manual intervention: ${stuck || "unknown"}.`,
      };
    }
  }
}
