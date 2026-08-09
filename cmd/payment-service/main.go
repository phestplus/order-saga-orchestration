// Command payment-service charges and refunds.
package main

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/phestplus/order-saga-orchestration/internal/order"
	"github.com/phestplus/order-saga-orchestration/internal/participant"
	"github.com/phestplus/order-saga-orchestration/internal/saga"
	"github.com/phestplus/order-saga-orchestration/internal/service"
)

// ledger records captured amounts per order. A refund is recorded as a
// reversing entry rather than by deleting the charge, which is how a real
// payment system works: you never erase that money moved, you record that
// it moved back.
type ledger struct {
	mu      sync.Mutex
	entries map[string]int
}

func newLedger() *ledger { return &ledger{entries: map[string]int{}} }

func (l *ledger) charge(orderID string, cents int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[orderID] += cents
}

func (l *ledger) refund(orderID string, cents int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[orderID] -= cents
}

func (l *ledger) balance(orderID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries[orderID]
}

func main() {
	book := newLedger()

	err := service.RunParticipant("payment", "payment.*", func(svc *participant.Service) {
		svc.Handle("charge", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailAt == "payment" {
				return errors.New("card declined (injected failure)")
			}
			book.charge(o.ID, o.AmountCents)
			log.Printf("charged %d cents for %s (balance %d)", o.AmountCents, o.ID, book.balance(o.ID))
			return nil
		})

		svc.Handle("refund", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailCompensationAt == "payment" {
				// The case most saga write-ups skip: the compensation
				// itself fails, so money stays captured for an order that
				// will never ship.
				return errors.New("refund failed, payment provider unavailable (injected failure)")
			}
			book.refund(o.ID, o.AmountCents)
			log.Printf("refunded %d cents for %s (balance %d)", o.AmountCents, o.ID, book.balance(o.ID))
			return nil
		})
	})
	if err != nil {
		log.Fatalf("payment-service: %v", err)
	}
}
