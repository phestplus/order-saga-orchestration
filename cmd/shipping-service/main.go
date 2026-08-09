// Command shipping-service schedules and cancels shipments.
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

type shipments struct {
	mu        sync.Mutex
	scheduled map[string]bool
}

func newShipments() *shipments { return &shipments{scheduled: map[string]bool{}} }

func (s *shipments) schedule(orderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled[orderID] = true
}

func (s *shipments) cancel(orderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.scheduled, orderID)
}

func main() {
	ships := newShipments()

	err := service.RunParticipant("shipping", "shipping.*", func(svc *participant.Service) {
		svc.Handle("schedule", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailAt == "shipping" {
				// Failing on the last step is the most instructive case:
				// stock is already reserved and the card is already
				// charged, so both have to be undone, in reverse.
				return errors.New("no courier available (injected failure)")
			}
			ships.schedule(o.ID)
			log.Printf("scheduled shipment for %s", o.ID)
			return nil
		})

		svc.Handle("cancel", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailCompensationAt == "shipping" {
				return errors.New("shipment cancellation refused (injected failure)")
			}
			ships.cancel(o.ID)
			log.Printf("cancelled shipment for %s", o.ID)
			return nil
		})
	})
	if err != nil {
		log.Fatalf("shipping-service: %v", err)
	}
}
