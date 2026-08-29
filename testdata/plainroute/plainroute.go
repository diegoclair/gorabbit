// Package plainroute must not compile: a per-route subscription is for messages
// that carry a route, which is what TestSubscribingARouteOfAPlainMessageDoesNotCompile
// asserts by building this package.
package plainroute

import (
	"context"

	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

func Subscribe(ctx context.Context, client *rabbitmq.Client[orders.Exchange]) error {
	return rabbitmq.SubscribeRoute(ctx, client, orders.OrderCreated{}, "any",
		func(context.Context, orders.OrderCreated) error { return nil })
}
