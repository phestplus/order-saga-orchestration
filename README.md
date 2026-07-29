# Event-Driven Order Orchestration (Saga Pattern)

**Status:** Planned (build order #8)
**Stack:** Go + Node.js, Kafka/NATS

## Problem
An order touches multiple services (inventory, payment, shipping); a distributed transaction across all of them without 2PC needs a coordinated way to fail and compensate cleanly.

## Planned deliverables
- Go services for order/inventory/payment, Node.js for a notification/BFF service
- Saga orchestration with compensating transactions on failure
- Message broker-based communication (Kafka or NATS)
- `scripts/simulate.sh` — end-to-end script placing orders including forced mid-saga failures to prove compensation logic works
- Architecture diagram + tradeoffs section in README (choreography vs orchestration)
