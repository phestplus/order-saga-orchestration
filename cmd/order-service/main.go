// Command order-service is the saga orchestrator and the HTTP entry
// point for placing an order.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/phestplus/order-saga-orchestration/internal/order"
	"github.com/phestplus/order-saga-orchestration/internal/saga"
	"github.com/phestplus/order-saga-orchestration/internal/service"
	"github.com/phestplus/order-saga-orchestration/internal/transport"
)

// steps is the whole workflow, declared in one place.
//
// That is the defining property of orchestration: the sequence and its
// compensations are readable as a list. Under choreography this same
// workflow exists only as an emergent consequence of which service
// happens to subscribe to which event, and no single file describes it.
var steps = []saga.Step{
	{
		Name: "inventory", Subject: "inventory.reserve", Action: "reserve",
		CompensateSubject: "inventory.release", CompensateAction: "release",
	},
	{
		Name: "payment", Subject: "payment.charge", Action: "charge",
		CompensateSubject: "payment.refund", CompensateAction: "refund",
	},
	{
		Name: "shipping", Subject: "shipping.schedule", Action: "schedule",
		CompensateSubject: "shipping.cancel", CompensateAction: "cancel",
	},
}

type server struct {
	orchestrator *saga.Orchestrator
	conn         *nats.Conn

	mu      sync.Mutex
	results map[string]saga.Result
	seq     atomic.Uint64
}

func main() {
	conn, err := transport.Connect(service.Env("NATS_URL", "nats://127.0.0.1:4222"), 60*time.Second)
	if err != nil {
		log.Fatalf("order-service: %v", err)
	}
	defer conn.Close()

	srv := &server{
		orchestrator: &saga.Orchestrator{
			Invoker: transport.NewInvoker(conn, 5*time.Second),
			Steps:   steps,
		},
		conn:    conn,
		results: map[string]saga.Result{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /orders", srv.placeOrder)
	mux.HandleFunc("GET /orders/{id}", srv.getOrder)

	addr := ":" + service.Env("PORT", "8080")
	log.Printf("order-service listening on %s", addr)
	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("order-service: %v", err)
	}
}

func (s *server) placeOrder(w http.ResponseWriter, r *http.Request) {
	var o order.Order
	if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid order: " + err.Error()})
		return
	}
	if o.SKU == "" || o.Qty <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sku and a positive qty are required"})
		return
	}
	if o.ID == "" {
		o.ID = fmt.Sprintf("order-%d", s.seq.Add(1))
	}

	result := s.orchestrator.Execute(r.Context(), o.ID, order.Encode(o))

	s.mu.Lock()
	s.results[o.ID] = result
	s.mu.Unlock()

	s.publishEvent(result)

	// A saga that failed and rolled back cleanly is a correctly handled
	// business outcome, not a server error, so it is reported as 409
	// rather than 500. A saga that could not fully roll back is a 500:
	// the system is genuinely in a bad state and someone needs to know.
	status := http.StatusCreated
	switch result.Status {
	case saga.StatusCompensated:
		status = http.StatusConflict
	case saga.StatusStuck:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, result)
}

func (s *server) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	result, ok := s.results[id]
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown order " + id})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// publishEvent broadcasts the outcome for anyone who cares. This is the
// one place the system is genuinely event-driven rather than
// request/reply: the notification service reacts to orders without the
// orchestrator knowing it exists, and adding another consumer needs no
// change here.
func (s *server) publishEvent(result saga.Result) {
	data, err := json.Marshal(result)
	if err != nil {
		return
	}
	if err := s.conn.Publish("orders.events", data); err != nil {
		log.Printf("publish event for %s: %v", result.SagaID, err)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
