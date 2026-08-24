package gorabbit

import "context"

// Exchange names the topic exchange that owns a group of message types. It is
// resolved from the zero value of the type, so implement it on an empty struct
// with a value receiver.
type Exchange interface {
	Name() string
}

// Msg binds a message type to the exchange E. One package per exchange, holding
// the marker as Exchange and an unexported alias for the embed, keeps the
// plumbing out of sight of whoever reads the message:
//
//	package orders
//
//	type Exchange struct{}
//	func (Exchange) Name() string { return "orders" }
//
//	type msg = gorabbit.Msg[Exchange]
//
//	type OrderCreated struct {
//	    msg
//	    OrderID string `json:"order_id"`
//	}
//
// Embedding gorabbit.Msg[orders.Exchange] directly works too, at the cost of an
// exported Msg field on the message. Either way Msg has no fields and no
// exported methods, so it stays out of the serialized payload, and a struct with
// two markers stops being a Message — the promoted methods turn ambiguous, so a
// message belongs to exactly one exchange.
type Msg[E Exchange] struct{}

func (Msg[E]) exchangeName() string {
	var e E
	return e.Name()
}

// ownedBy carries the exchange in the type system, which is what lets a client
// refuse a message from another exchange at compile time.
func (Msg[E]) ownedBy(E) {}

// Message is any type embedding Msg[E], whatever its exchange. Only the embed
// can promote the unexported method, so a type without a marker fails to compile
// instead of failing on the broker.
type Message interface {
	exchangeName() string
}

// OwnedBy is a Message that belongs to the exchange E.
type OwnedBy[E Exchange] interface {
	Message
	ownedBy(E)
}

// ExchangeOf returns the exchange bound to the message type. Drivers need it;
// application code does not.
func ExchangeOf(msg Message) string {
	return msg.exchangeName()
}

// Handler handles one concrete message type; a returned error triggers the
// driver's retry/dead-letter policy.
type Handler[T Message] func(ctx context.Context, msg T) error
