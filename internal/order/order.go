// Package order defines the payload that travels through the saga.
package order

import "encoding/json"

// Order is the business request the saga is trying to satisfy.
//
// FailAt and FailCompensationAt are test hooks compiled into the
// services on purpose. A saga demo is worthless unless you can force the
// failures it exists to handle, and forcing them from outside (killing a
// container mid-transaction) is both slower and less precise than asking
// a participant to refuse. They are the mechanism scripts/simulate.sh
// uses to prove compensation actually runs.
type Order struct {
	ID          string `json:"id"`
	SKU         string `json:"sku"`
	Qty         int    `json:"qty"`
	AmountCents int    `json:"amountCents"`

	// FailAt names a step whose forward action should refuse:
	// "inventory", "payment" or "shipping". Empty means none.
	FailAt string `json:"failAt,omitempty"`

	// FailCompensationAt names a step whose compensation should refuse.
	// This is how the harder case gets exercised: a saga that fails and
	// then cannot fully undo itself, which is the one outcome that leaves
	// the system inconsistent and needs a human.
	FailCompensationAt string `json:"failCompensationAt,omitempty"`
}

// Decode parses an order from a saga command payload.
func Decode(payload json.RawMessage) (Order, error) {
	var o Order
	if err := json.Unmarshal(payload, &o); err != nil {
		return Order{}, err
	}
	return o, nil
}

// Encode serializes an order for transport.
func Encode(o Order) json.RawMessage {
	b, _ := json.Marshal(o)
	return b
}
