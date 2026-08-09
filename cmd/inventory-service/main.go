// Command inventory-service reserves and releases stock.
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

// stock is an in-process map standing in for a real inventory database.
// The project is about the coordination between services, not about
// storage, and an in-memory map keeps the whole demo to one dependency.
type stock struct {
	mu    sync.Mutex
	units map[string]int
}

func newStock() *stock {
	return &stock{units: map[string]int{"WIDGET": 100, "GADGET": 5}}
}

func (s *stock) available(sku string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.units[sku]
}

func (s *stock) reserve(sku string, qty int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.units[sku] < qty {
		return errors.New("out of stock")
	}
	s.units[sku] -= qty
	return nil
}

func (s *stock) release(sku string, qty int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.units[sku] += qty
}

func main() {
	inv := newStock()

	err := service.RunParticipant("inventory", "inventory.*", func(svc *participant.Service) {
		svc.Handle("reserve", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailAt == "inventory" {
				return errors.New("inventory refused (injected failure)")
			}
			if err := inv.reserve(o.SKU, o.Qty); err != nil {
				return err
			}
			log.Printf("reserved %d x %s for %s (%d left)", o.Qty, o.SKU, o.ID, inv.available(o.SKU))
			return nil
		})

		svc.Handle("release", func(_ context.Context, cmd saga.Command) error {
			o, err := order.Decode(cmd.Payload)
			if err != nil {
				return err
			}
			if o.FailCompensationAt == "inventory" {
				return errors.New("inventory release refused (injected failure)")
			}
			inv.release(o.SKU, o.Qty)
			log.Printf("released %d x %s for %s (%d left)", o.Qty, o.SKU, o.ID, inv.available(o.SKU))
			return nil
		})
	})
	if err != nil {
		log.Fatalf("inventory-service: %v", err)
	}
}
