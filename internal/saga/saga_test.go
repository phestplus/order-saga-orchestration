package saga

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeInvoker records every call in order and lets a test make specific
// actions fail. Recording order matters: compensation running in the
// wrong order is a real bug that a pass/fail assertion would not catch.
type fakeInvoker struct {
	calls []string
	// failActions maps an action name to the error it should return.
	failActions map[string]error
	// refuseActions maps an action name to a business-level refusal.
	refuseActions map[string]string
}

func newFake() *fakeInvoker {
	return &fakeInvoker{
		failActions:   map[string]error{},
		refuseActions: map[string]string{},
	}
}

func (f *fakeInvoker) Invoke(ctx context.Context, subject string, cmd Command) (Reply, error) {
	if err := ctx.Err(); err != nil {
		return Reply{}, err
	}
	f.calls = append(f.calls, cmd.Action)
	if err, ok := f.failActions[cmd.Action]; ok {
		return Reply{}, err
	}
	if reason, ok := f.refuseActions[cmd.Action]; ok {
		return Reply{OK: false, Error: reason}, nil
	}
	return Reply{OK: true}, nil
}

// A three step saga mirroring the real one: reserve stock, take money,
// book a shipment.
func testSteps() []Step {
	return []Step{
		{Name: "inventory", Subject: "inventory.reserve", Action: "reserve",
			CompensateSubject: "inventory.release", CompensateAction: "release"},
		{Name: "payment", Subject: "payment.charge", Action: "charge",
			CompensateSubject: "payment.refund", CompensateAction: "refund"},
		{Name: "shipping", Subject: "shipping.schedule", Action: "schedule",
			CompensateSubject: "shipping.cancel", CompensateAction: "cancel"},
	}
}

func newOrchestrator(f *fakeInvoker) *Orchestrator {
	return &Orchestrator{Invoker: f, Steps: testSteps(), CompensationTimeout: 2 * time.Second}
}

func joined(calls []string) string { return strings.Join(calls, ",") }

func TestHappyPathRunsEveryStepAndCompensatesNothing(t *testing.T) {
	f := newFake()
	res := newOrchestrator(f).Execute(context.Background(), "order-1", nil)

	if res.Status != StatusCompleted {
		t.Fatalf("status = %q, want %q", res.Status, StatusCompleted)
	}
	if got, want := joined(f.calls), "reserve,charge,schedule"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if len(res.Compensated) != 0 {
		t.Fatalf("compensated %v, want nothing", res.Compensated)
	}
}

func TestFailureInTheMiddleCompensatesOnlyCompletedSteps(t *testing.T) {
	f := newFake()
	f.refuseActions["charge"] = "insufficient funds"

	res := newOrchestrator(f).Execute(context.Background(), "order-2", nil)

	if res.Status != StatusCompensated {
		t.Fatalf("status = %q, want %q", res.Status, StatusCompensated)
	}
	if res.FailedStep != "payment" {
		t.Fatalf("failed step = %q, want payment", res.FailedStep)
	}
	// Inventory was reserved so it must be released. Shipping never ran so
	// it must not be cancelled: compensating a step that never happened
	// is its own bug.
	if got, want := joined(f.calls), "reserve,charge,release"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if got, want := joined(res.Compensated), "inventory"; got != want {
		t.Fatalf("compensated = %q, want %q", got, want)
	}
	if !strings.Contains(res.FailureReason, "insufficient funds") {
		t.Fatalf("failure reason = %q, want it to mention the refusal", res.FailureReason)
	}
}

func TestCompensationRunsInReverseOrder(t *testing.T) {
	f := newFake()
	f.refuseActions["schedule"] = "no courier available"

	res := newOrchestrator(f).Execute(context.Background(), "order-3", nil)

	// Reverse order is not cosmetic. Later steps can depend on earlier
	// ones, so undoing them out of order can fail or corrupt state.
	if got, want := joined(f.calls), "reserve,charge,schedule,refund,release"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if got, want := joined(res.Compensated), "payment,inventory"; got != want {
		t.Fatalf("compensated = %q, want %q (reverse order)", got, want)
	}
	if res.Status != StatusCompensated {
		t.Fatalf("status = %q, want %q", res.Status, StatusCompensated)
	}
}

func TestFailureOnTheFirstStepCompensatesNothing(t *testing.T) {
	f := newFake()
	f.refuseActions["reserve"] = "out of stock"

	res := newOrchestrator(f).Execute(context.Background(), "order-4", nil)

	if got, want := joined(f.calls), "reserve"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if len(res.Compensated) != 0 {
		t.Fatalf("compensated %v, want nothing", res.Compensated)
	}
	if res.Status != StatusCompensated {
		t.Fatalf("status = %q, want %q", res.Status, StatusCompensated)
	}
}

func TestAFailedCompensationIsReportedAsStuckAndDoesNotStopTheRest(t *testing.T) {
	f := newFake()
	f.refuseActions["schedule"] = "no courier available"
	f.failActions["refund"] = errors.New("payment service unavailable")

	res := newOrchestrator(f).Execute(context.Background(), "order-5", nil)

	// The refund failed, but inventory must still be released. Undoing
	// two of three effects beats undoing none.
	if got, want := joined(f.calls), "reserve,charge,schedule,refund,release"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
	if res.Status != StatusStuck {
		t.Fatalf("status = %q, want %q when a compensation fails", res.Status, StatusStuck)
	}
	if got, want := joined(res.Compensated), "inventory"; got != want {
		t.Fatalf("compensated = %q, want %q", got, want)
	}
	if len(res.CompensationFailures) != 1 || res.CompensationFailures[0].Step != "payment" {
		t.Fatalf("compensation failures = %+v, want one for payment", res.CompensationFailures)
	}
}

func TestCompensationRunsEvenWhenTheCallerContextIsAlreadyCancelled(t *testing.T) {
	// The forward path frequently fails *because* the caller gave up. If
	// compensation inherited that cancelled context it would fail
	// instantly and strand the saga half applied, which is the exact
	// failure this pattern exists to prevent.
	f := newFake()
	f.refuseActions["charge"] = "gateway timeout"

	ctx, cancel := context.WithCancel(context.Background())
	o := newOrchestrator(f)

	// Cancel as soon as the failing step has been observed.
	original := o.Invoker
	o.Invoker = invokerFunc(func(c context.Context, subject string, cmd Command) (Reply, error) {
		reply, err := original.Invoke(c, subject, cmd)
		if cmd.Action == "charge" {
			cancel()
		}
		return reply, err
	})

	res := o.Execute(ctx, "order-6", nil)
	cancel()

	if got, want := joined(f.calls), "reserve,charge,release"; got != want {
		t.Fatalf("calls = %q, want %q (release must still run)", got, want)
	}
	if res.Status != StatusCompensated {
		t.Fatalf("status = %q, want %q", res.Status, StatusCompensated)
	}
}

func TestStepsWithNoCompensationAreSkippedDuringRollback(t *testing.T) {
	f := newFake()
	f.refuseActions["charge"] = "declined"

	o := &Orchestrator{
		Invoker: f,
		Steps: []Step{
			// A read or a notification has nothing to undo.
			{Name: "audit", Subject: "audit.record", Action: "record"},
			{Name: "payment", Subject: "payment.charge", Action: "charge",
				CompensateSubject: "payment.refund", CompensateAction: "refund"},
		},
	}

	res := o.Execute(context.Background(), "order-7", nil)

	if got, want := joined(f.calls), "record,charge"; got != want {
		t.Fatalf("calls = %q, want %q (no compensation for audit)", got, want)
	}
	if len(res.CompensationFailures) != 0 {
		t.Fatalf("compensation failures = %+v, want none", res.CompensationFailures)
	}
}

func TestPayloadReachesParticipants(t *testing.T) {
	var seen json.RawMessage
	o := &Orchestrator{
		Invoker: invokerFunc(func(_ context.Context, _ string, cmd Command) (Reply, error) {
			seen = cmd.Payload
			return Reply{OK: true}, nil
		}),
		Steps: []Step{{Name: "inventory", Subject: "inventory.reserve", Action: "reserve"}},
	}

	o.Execute(context.Background(), "order-8", json.RawMessage(`{"sku":"ABC","qty":2}`))

	if !strings.Contains(string(seen), `"sku":"ABC"`) {
		t.Fatalf("participant saw payload %q, want it to carry the order", string(seen))
	}
}

type invokerFunc func(ctx context.Context, subject string, cmd Command) (Reply, error)

func (f invokerFunc) Invoke(ctx context.Context, subject string, cmd Command) (Reply, error) {
	return f(ctx, subject, cmd)
}
