import { toNotification, type SagaResult } from "../src/notification";
import { NotificationStore } from "../src/store";

describe("toNotification", () => {
  it("reports a completed saga as info", () => {
    const n = toNotification({ sagaId: "order-1", status: "completed" });
    expect(n.severity).toBe("info");
    expect(n.message).toContain("confirmed");
  });

  it("reports a rolled-back saga as a warning that reassures about billing", () => {
    const result: SagaResult = {
      sagaId: "order-2",
      status: "compensated",
      failedStep: "payment",
      failureReason: "card declined",
      compensated: ["inventory"],
    };
    const n = toNotification(result);

    expect(n.severity).toBe("warning");
    expect(n.message).toContain("payment");
    expect(n.message).toContain("card declined");
    // Nothing was charged, and saying so is the whole point of telling
    // the customer at all.
    expect(n.message).toContain("not been charged");
  });

  it("escalates a stuck saga to critical and names what could not be undone", () => {
    // The distinction that matters: this is not just another failed
    // order. Money moved and could not be moved back.
    const result: SagaResult = {
      sagaId: "order-3",
      status: "stuck",
      failedStep: "shipping",
      failureReason: "no courier available",
      compensated: ["inventory"],
      compensationFailures: [{ step: "payment", reason: "provider unavailable" }],
    };
    const n = toNotification(result);

    expect(n.severity).toBe("critical");
    expect(n.message).toContain("manual intervention");
    expect(n.message).toContain("payment");
    expect(n.message).toContain("provider unavailable");
  });

  it("does not claim a stuck order was safely undone", () => {
    const n = toNotification({
      sagaId: "order-4",
      status: "stuck",
      failedStep: "shipping",
      compensationFailures: [{ step: "payment", reason: "timeout" }],
    });
    expect(n.message).not.toContain("not been charged");
  });
});

describe("NotificationStore", () => {
  it("keeps notifications and can filter by order", () => {
    const store = new NotificationStore();
    store.add({ orderId: "a", severity: "info", message: "one" });
    store.add({ orderId: "b", severity: "info", message: "two" });
    store.add({ orderId: "a", severity: "warning", message: "three" });

    expect(store.size()).toBe(3);
    expect(store.forOrder("a")).toHaveLength(2);
    expect(store.forOrder("missing")).toHaveLength(0);
  });

  it("drops the oldest entries past its limit rather than growing forever", () => {
    const store = new NotificationStore(3);
    for (const message of ["1", "2", "3", "4", "5"]) {
      store.add({ orderId: message, severity: "info", message });
    }

    expect(store.size()).toBe(3);
    expect(store.all().map((n) => n.message)).toEqual(["3", "4", "5"]);
  });

  it("returns a copy so callers cannot mutate the feed", () => {
    const store = new NotificationStore();
    store.add({ orderId: "a", severity: "info", message: "one" });
    store.all().push({ orderId: "b", severity: "info", message: "injected" });

    expect(store.size()).toBe(1);
  });
});
