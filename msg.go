package gorabbit

import "context"

// Exchange names the topic exchange that owns a group of message types. It is
// resolved from the zero value of the type, so implement it on an empty struct
// with a value receiver.
type Exchange interface {
	Name() string
}

// Msg marks a message every bound consumer receives a copy of. It has no fields
// and no exported methods, so it stays out of the serialized payload.
type Msg[E Exchange] struct{}

func (Msg[E]) exchangeName() string {
	var e E
	return e.Name()
}

// ownedBy carries the exchange in the type system, which is what lets a client
// refuse a message from another exchange at compile time.
func (Msg[E]) ownedBy(E) {}

// RoutedMsg marks a message whose own value picks the routing key, so consumers
// can split the type between queues instead of each receiving every copy. The
// type must also implement RouteBy.
type RoutedMsg[E Exchange] struct{}

func (RoutedMsg[E]) exchangeName() string {
	var e E
	return e.Name()
}

func (RoutedMsg[E]) ownedBy(E) {}

// routedMsg keeps RoutedMessage out of reach of a plain message that happens to
// declare a RouteBy of its own.
func (RoutedMsg[E]) routedMsg() {}

// Message is any type embedding exactly one marker: the promoted method is
// unexported, so nothing else implements it and two markers make it ambiguous.
type Message interface {
	exchangeName() string
}

// OwnedBy is a Message that belongs to the exchange E.
type OwnedBy[E Exchange] interface {
	Message
	ownedBy(E)
}

// RoutedMessage is the constraint of a per-route subscription: a plain message
// cannot satisfy it, and a routed one missing RouteBy does not compile.
type RoutedMessage interface {
	Message
	routedMsg()
	RouteBy() string
}

// ExchangeOf returns the exchange bound to the message type. Drivers need it;
// application code does not.
func ExchangeOf(msg Message) string {
	return msg.exchangeName()
}

// IsRouted tells a driver which routing key shape the type uses. A type missing
// RouteBy is routed and broken, never plain, so the defect cannot pass unseen.
func IsRouted(msg Message) bool {
	_, ok := msg.(interface{ routedMsg() })
	return ok
}

// RouteOf returns the route the message picked. The false answer belongs to a
// type that embeds the routed marker and never implemented RouteBy.
func RouteOf(msg Message) (string, bool) {
	routed, ok := msg.(RoutedMessage)
	if !ok {
		return "", false
	}

	return routed.RouteBy(), true
}

// Handler handles one concrete message type; a returned error triggers the
// driver's retry/dead-letter policy.
type Handler[T Message] func(ctx context.Context, msg T) error
