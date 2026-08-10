// Package participant is the runtime each saga participant service runs.
//
// It is transport-agnostic on purpose: Dispatch takes a command and
// returns a reply, so the deduplication and error semantics that every
// participant needs are tested in memory rather than against a broker.
package participant

import (
	"context"
	"sync"

	"github.com/phestplus/order-saga-orchestration/internal/saga"
)

// Handler applies one action. Returning an error is a business refusal,
// which the orchestrator treats as a failed step.
type Handler func(ctx context.Context, cmd saga.Command) error

// entry memoizes the outcome of one (saga, action) pair.
//
// A plain seen-keys map guarded by a mutex is not enough,
// because the handler must run outside the lock (it does real work and
// can block). Two concurrent redeliveries would both find the key absent
// and both charge the card. sync.Once closes that window: the first
// caller runs the handler while the others block inside Do and then read
// the same reply.
type entry struct {
	once  sync.Once
	reply saga.Reply
}

// Service dispatches commands to handlers, with exactly-once semantics
// per (saga, action).
//
// The deduplication is not optional decoration. NATS request/reply gives
// at-least-once delivery, and the orchestrator retries on transport
// errors, so a participant will eventually see the same command twice.
// Without dedupe that means charging a card twice, or refunding twice,
// which turns the mechanism meant to restore consistency into the thing
// that destroys it. Compensations especially must be idempotent.
type Service struct {
	Name string

	mu       sync.Mutex
	handlers map[string]Handler
	entries  map[string]*entry
}

func New(name string) *Service {
	return &Service{
		Name:     name,
		handlers: map[string]Handler{},
		entries:  map[string]*entry{},
	}
}

// Handle registers the handler for an action.
func (s *Service) Handle(action string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[action] = h
}

// Dispatch applies a command, returning the previous reply if this
// (saga, action) pair has already been handled.
func (s *Service) Dispatch(ctx context.Context, cmd saga.Command) saga.Reply {
	key := cmd.SagaID + "/" + cmd.Action

	s.mu.Lock()
	h, known := s.handlers[cmd.Action]
	if !known {
		s.mu.Unlock()
		// Deliberately not memoized: an unknown action is a routing bug,
		// and replaying it after a fix should be allowed to work.
		return saga.Reply{OK: false, Error: "unknown action " + cmd.Action}
	}
	e, ok := s.entries[key]
	if !ok {
		e = &entry{}
		s.entries[key] = e
	}
	s.mu.Unlock()

	e.once.Do(func() {
		reply := saga.Reply{OK: true}
		if err := h(ctx, cmd); err != nil {
			// Refusals are memoized too. A participant that says "out of
			// stock" will say the same on redelivery, and pinning the
			// answer keeps it stable rather than letting a concurrent
			// stock change flip it after the orchestrator already acted.
			reply = saga.Reply{OK: false, Error: err.Error()}
		}
		e.reply = reply
	})

	return e.reply
}
