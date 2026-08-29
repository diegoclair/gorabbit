// Package twomarkers must not compile: a message belongs to one exchange and
// picks one routing shape, which is what TestTwoMarkersDoNotCompile asserts by
// building this package.
package twomarkers

import (
	"context"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

type orderCreated struct {
	gorabbit.Msg[orders.Exchange]
	gorabbit.RoutedMsg[orders.Exchange]
	OrderID string `json:"order_id"`
}

func (orderCreated) RouteBy() string { return "any" }

func Publish(ctx context.Context, publisher gorabbit.Publisher[orders.Exchange]) error {
	return publisher.Publish(ctx, orderCreated{})
}
