// Package transport wires the saga orchestrator and its participants
// onto NATS. It is the only package that imports the NATS client, which
// is what keeps the orchestration and participant logic testable without
// a broker.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/phestplus/order-saga-orchestration/internal/participant"
	"github.com/phestplus/order-saga-orchestration/internal/saga"
)

// DefaultRequestTimeout bounds a single participant call.
const DefaultRequestTimeout = 5 * time.Second

// Connect dials NATS, retrying until the deadline. Compose starts all the
// services at once, so a participant will usually come up before the
// broker is accepting connections.
func Connect(url string, wait time.Duration) (*nats.Conn, error) {
	deadline := time.Now().Add(wait)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := nats.Connect(url,
			nats.MaxReconnects(-1),
			nats.ReconnectWait(500*time.Millisecond),
		)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("connect to NATS at %s: %w", url, lastErr)
}

// Invoker implements saga.Invoker over NATS request/reply.
//
// Request/reply rather than fire-and-forget publishing is what makes this
// an orchestrated saga: the coordinator needs to know whether each step
// succeeded before deciding what to do next, and a one-way publish gives
// it nothing to decide on.
type Invoker struct {
	conn    *nats.Conn
	timeout time.Duration
}

func NewInvoker(conn *nats.Conn, timeout time.Duration) *Invoker {
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	return &Invoker{conn: conn, timeout: timeout}
}

func (i *Invoker) Invoke(ctx context.Context, subject string, cmd saga.Command) (saga.Reply, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return saga.Reply{}, fmt.Errorf("encode command: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, i.timeout)
	defer cancel()

	msg, err := i.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		// A transport failure is not a refusal. The participant may have
		// applied the action and only the reply was lost, which is
		// exactly why participants deduplicate.
		return saga.Reply{}, fmt.Errorf("request %s: %w", subject, err)
	}

	var reply saga.Reply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return saga.Reply{}, fmt.Errorf("decode reply from %s: %w", subject, err)
	}
	return reply, nil
}

// ServeParticipant subscribes a participant service to a subject pattern
// such as "inventory.*" and answers every command it receives.
//
// The subscription uses a queue group named after the service, so running
// several replicas shares the work instead of each replica handling every
// command.
func ServeParticipant(conn *nats.Conn, svc *participant.Service, pattern string) (*nats.Subscription, error) {
	sub, err := conn.QueueSubscribe(pattern, svc.Name, func(m *nats.Msg) {
		var cmd saga.Command
		if err := json.Unmarshal(m.Data, &cmd); err != nil {
			respond(m, saga.Reply{OK: false, Error: "malformed command: " + err.Error()})
			return
		}

		reply := svc.Dispatch(context.Background(), cmd)
		log.Printf("%s saga=%s action=%s ok=%t %s",
			svc.Name, cmd.SagaID, cmd.Action, reply.OK, reply.Error)
		respond(m, reply)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", pattern, err)
	}
	return sub, nil
}

func respond(m *nats.Msg, reply saga.Reply) {
	data, err := json.Marshal(reply)
	if err != nil {
		return
	}
	// A failed respond leaves the orchestrator to time out, which it
	// already handles, so there is nothing better to do here than log.
	if err := m.Respond(data); err != nil {
		log.Printf("respond to %s: %v", m.Reply, err)
	}
}
