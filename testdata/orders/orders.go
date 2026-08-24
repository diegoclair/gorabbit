// Package orders is the shared package of the orders exchange: the marker, the
// alias every message embeds, and the facts themselves. Applications import it
// to publish or to consume.
package orders

import "github.com/diegoclair/gorabbit"

type Exchange struct{}

func (Exchange) Name() string { return "orders" }

type msg = gorabbit.Msg[Exchange]

type OrderCreated struct {
	msg
	OrderID string `json:"order_id"`
}
