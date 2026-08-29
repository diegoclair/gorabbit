// Package missingrouteby must not compile: a routed message that never says
// what routes it is only a defect, which is what
// TestRoutedMessageWithoutRouteByDoesNotCompile asserts by building this package.
package missingrouteby

import (
	"context"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

type eventRecorded struct {
	gorabbit.RoutedMsg[orders.Exchange]
	Vendor string `json:"vendor"`
}

func Subscribe(ctx context.Context, client *rabbitmq.Client[orders.Exchange]) error {
	return rabbitmq.SubscribeRoute(ctx, client, eventRecorded{}, "any",
		func(context.Context, eventRecorded) error { return nil })
}
