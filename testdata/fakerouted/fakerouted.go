// Package fakerouted must not compile: RouteBy alone does not make a message
// routed, which is what TestPlainMessageDeclaringRouteByDoesNotCompile asserts.
package fakerouted

import (
	"context"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

type orderPriced struct {
	gorabbit.Msg[orders.Exchange]
	Vendor string `json:"vendor"`
}

func (o orderPriced) RouteBy() string { return o.Vendor }

func Subscribe(ctx context.Context, client *rabbitmq.Client[orders.Exchange]) error {
	return rabbitmq.SubscribeRoute(ctx, client, orderPriced{}, "any",
		func(context.Context, orderPriced) error { return nil })
}
