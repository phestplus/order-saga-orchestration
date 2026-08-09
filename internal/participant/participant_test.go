package participant

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/phestplus/order-saga-orchestration/internal/saga"
)

func TestDuplicateCommandIsAppliedOnlyOnce(t *testing.T) {
	// The scenario this prevents: the broker redelivers a charge, or the
	// orchestrator retries one after a transport error, and the customer
	// gets billed twice.
	calls := 0
	s := New("payment")
	s.Handle("charge", func(context.Context, saga.Command) error {
		calls++
		return nil
	})

	cmd := saga.Command{SagaID: "order-1", Action: "charge"}
	first := s.Dispatch(context.Background(), cmd)
	second := s.Dispatch(context.Background(), cmd)

	if calls != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", calls)
	}
	if !first.OK || !second.OK {
		t.Fatalf("replies = %+v / %+v, want both OK", first, second)
	}
}

func TestDuplicateCompensationDoesNotRefundTwice(t *testing.T) {
	refunds := 0
	s := New("payment")
	s.Handle("refund", func(context.Context, saga.Command) error {
		refunds++
		return nil
	})

	cmd := saga.Command{SagaID: "order-2", Action: "refund"}
	for range 3 {
		s.Dispatch(context.Background(), cmd)
	}

	if refunds != 1 {
		t.Fatalf("refund ran %d times, want exactly 1", refunds)
	}
}

func TestDifferentSagasAreIndependent(t *testing.T) {
	calls := 0
	s := New("inventory")
	s.Handle("reserve", func(context.Context, saga.Command) error {
		calls++
		return nil
	})

	s.Dispatch(context.Background(), saga.Command{SagaID: "order-a", Action: "reserve"})
	s.Dispatch(context.Background(), saga.Command{SagaID: "order-b", Action: "reserve"})

	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2 (one per saga)", calls)
	}
}

func TestRefusalIsStableAcrossRedelivery(t *testing.T) {
	// A participant that refused once must keep refusing on redelivery,
	// otherwise a retry could report success for a step the orchestrator
	// already compensated for.
	attempts := 0
	s := New("inventory")
	s.Handle("reserve", func(context.Context, saga.Command) error {
		attempts++
		if attempts == 1 {
			return errors.New("out of stock")
		}
		return nil
	})

	cmd := saga.Command{SagaID: "order-3", Action: "reserve"}
	first := s.Dispatch(context.Background(), cmd)
	second := s.Dispatch(context.Background(), cmd)

	if first.OK {
		t.Fatal("first reply should be a refusal")
	}
	if second.OK {
		t.Fatalf("second reply = %+v, want the same refusal as the first", second)
	}
	if attempts != 1 {
		t.Fatalf("handler ran %d times, want 1", attempts)
	}
}

func TestUnknownActionIsRefusedWithoutBeingRecorded(t *testing.T) {
	s := New("inventory")
	cmd := saga.Command{SagaID: "order-4", Action: "teleport"}

	if reply := s.Dispatch(context.Background(), cmd); reply.OK {
		t.Fatal("unknown action should be refused")
	}

	// Registering the handler afterwards and replaying should now work,
	// because a routing bug is not a business outcome worth memoizing.
	ran := false
	s.Handle("teleport", func(context.Context, saga.Command) error {
		ran = true
		return nil
	})
	if reply := s.Dispatch(context.Background(), cmd); !reply.OK || !ran {
		t.Fatal("replay after registering the handler should succeed")
	}
}

func TestConcurrentDuplicatesApplyOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	s := New("payment")
	s.Handle("charge", func(context.Context, saga.Command) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})

	cmd := saga.Command{SagaID: "order-5", Action: "charge"}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			s.Dispatch(context.Background(), cmd)
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("handler ran %d times under concurrency, want exactly 1", calls)
	}
}
