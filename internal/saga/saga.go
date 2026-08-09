// Package saga runs a distributed transaction as an ordered list of
// steps, each paired with a compensation that undoes it, and rolls the
// completed steps back in reverse when any step fails.
//
// This package knows nothing about NATS, HTTP, or any particular
// participant. It talks to an Invoker, which is an interface, so the
// orchestration logic can be tested exhaustively in memory and the
// message broker is an implementation detail rather than a prerequisite
// for knowing whether the rollback logic is correct.
package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status is the terminal outcome of a saga.
type Status string

const (
	// StatusCompleted means every step succeeded.
	StatusCompleted Status = "completed"

	// StatusCompensated means a step failed and every completed step
	// before it was successfully undone. The business transaction did not
	// happen, but the system is consistent, which is the outcome the saga
	// pattern is designed to guarantee.
	StatusCompensated Status = "compensated"

	// StatusStuck means a step failed and at least one compensation also
	// failed, so some effects were applied and could not be undone.
	//
	// Most descriptions of the saga pattern stop at "compensate on
	// failure" and quietly assume compensation always succeeds. It does
	// not: the payment service can be down at exactly the moment you need
	// to refund. This status exists so that case is visible and
	// actionable rather than being reported as a clean rollback, because
	// it is the one outcome that genuinely needs a human or a retry loop.
	StatusStuck Status = "stuck"
)

// Command is what the orchestrator sends to a participant.
type Command struct {
	SagaID  string          `json:"sagaId"`
	Step    string          `json:"step"`
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Reply is a participant's answer. A transport-level error and OK=false
// are different things: the first means the message may not have been
// processed, the second means it was processed and refused.
type Reply struct {
	OK      bool            `json:"ok"`
	Error   string          `json:"error,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Invoker sends a command to a participant and waits for its reply.
type Invoker interface {
	Invoke(ctx context.Context, subject string, cmd Command) (Reply, error)
}

// Step is one participant call plus the call that undoes it.
type Step struct {
	Name    string
	Subject string
	Action  string

	// CompensateAction may be empty for a step that has nothing to undo,
	// such as a read or a notification. Those are skipped during rollback.
	CompensateSubject string
	CompensateAction  string
}

// CompensationFailure records a compensation that could not be applied.
type CompensationFailure struct {
	Step   string `json:"step"`
	Reason string `json:"reason"`
}

// Result is a full account of what happened, including what could not be
// undone. It is returned rather than logged so the caller (and the tests)
// can assert on the exact sequence.
type Result struct {
	SagaID               string                `json:"sagaId"`
	Status               Status                `json:"status"`
	Completed            []string              `json:"completed"`
	FailedStep           string                `json:"failedStep,omitempty"`
	FailureReason        string                `json:"failureReason,omitempty"`
	Compensated          []string              `json:"compensated,omitempty"`
	CompensationFailures []CompensationFailure `json:"compensationFailures,omitempty"`
}

// DefaultCompensationTimeout bounds the rollback path.
const DefaultCompensationTimeout = 15 * time.Second

// Orchestrator executes a fixed sequence of steps.
//
// This is the orchestration flavour of the saga pattern: one component
// holds the whole workflow and tells each participant what to do. The
// alternative, choreography, has each service react to the previous
// service's event with no central coordinator. See the README for why
// this project uses orchestration.
type Orchestrator struct {
	Invoker Invoker
	Steps   []Step

	// CompensationTimeout bounds the rollback path. Zero means
	// DefaultCompensationTimeout.
	CompensationTimeout time.Duration
}

// Execute runs the saga to a terminal state. It does not return an error:
// a failed business transaction that was correctly rolled back is a
// successful execution of the saga, and the caller distinguishes outcomes
// through Result.Status.
func (o *Orchestrator) Execute(ctx context.Context, sagaID string, payload json.RawMessage) Result {
	res := Result{SagaID: sagaID, Status: StatusCompleted}
	completed := make([]Step, 0, len(o.Steps))

	for _, step := range o.Steps {
		err := o.call(ctx, step.Subject, Command{
			SagaID:  sagaID,
			Step:    step.Name,
			Action:  step.Action,
			Payload: payload,
		})
		if err != nil {
			res.FailedStep = step.Name
			res.FailureReason = err.Error()
			o.compensate(ctx, sagaID, payload, completed, &res)
			return res
		}
		completed = append(completed, step)
		res.Completed = append(res.Completed, step.Name)
	}

	return res
}

// compensate undoes completed steps in reverse order.
func (o *Orchestrator) compensate(ctx context.Context, sagaID string, payload json.RawMessage, completed []Step, res *Result) {
	res.Status = StatusCompensated

	// Compensation deliberately does not inherit cancellation from ctx.
	//
	// The forward path often fails precisely because the caller's context
	// timed out or the client disconnected. If the rollback inherited
	// that dead context, every compensation would fail instantly and the
	// saga would abandon itself half-applied, leaving inventory reserved
	// and money captured. That is the exact failure the pattern exists to
	// prevent, so the rollback gets a fresh deadline of its own.
	timeout := o.CompensationTimeout
	if timeout <= 0 {
		timeout = DefaultCompensationTimeout
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	for i := len(completed) - 1; i >= 0; i-- {
		step := completed[i]
		if step.CompensateAction == "" {
			continue // nothing to undo for this step
		}

		err := o.call(cctx, step.CompensateSubject, Command{
			SagaID:  sagaID,
			Step:    step.Name,
			Action:  step.CompensateAction,
			Payload: payload,
		})
		if err != nil {
			// Keep going. One compensation failing must not prevent the
			// others from being attempted: undoing two of three effects
			// is strictly better than undoing none.
			res.Status = StatusStuck
			res.CompensationFailures = append(res.CompensationFailures, CompensationFailure{
				Step:   step.Name,
				Reason: err.Error(),
			})
			continue
		}
		res.Compensated = append(res.Compensated, step.Name)
	}
}

// call collapses a transport error and a refusal reply into one error,
// since the orchestrator reacts to both the same way.
func (o *Orchestrator) call(ctx context.Context, subject string, cmd Command) error {
	reply, err := o.Invoker.Invoke(ctx, subject, cmd)
	if err != nil {
		return fmt.Errorf("%s: %w", cmd.Action, err)
	}
	if !reply.OK {
		if reply.Error == "" {
			return errors.New(cmd.Action + ": refused")
		}
		return fmt.Errorf("%s: %s", cmd.Action, reply.Error)
	}
	return nil
}
