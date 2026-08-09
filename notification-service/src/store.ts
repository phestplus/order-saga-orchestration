import type { Notification } from "./notification";

// A bounded in-memory list. Bounded rather than unbounded because a
// notification feed nobody drains is a slow memory leak, and this service
// exists to be read by a UI that only ever wants the recent entries.
export class NotificationStore {
  private items: Notification[] = [];

  constructor(private readonly limit = 200) {}

  add(notification: Notification): void {
    this.items.push(notification);
    if (this.items.length > this.limit) {
      this.items = this.items.slice(-this.limit);
    }
  }

  all(): Notification[] {
    return [...this.items];
  }

  forOrder(orderId: string): Notification[] {
    return this.items.filter((n) => n.orderId === orderId);
  }

  size(): number {
    return this.items.length;
  }
}
