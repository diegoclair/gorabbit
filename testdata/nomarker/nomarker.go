// Package nomarker must not compile: publishing a message that does not embed
// gorabbit.Msg is a compile error, which is what
// TestMessageWithoutAMarkerDoesNotCompile asserts by building this package.
package nomarker

import (
	"context"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

type orderCreated struct {
	OrderID string `json:"order_id"`
}

func Publish(ctx context.Context, publisher gorabbit.Publisher[orders.Exchange]) error {
	return publisher.Publish(ctx, orderCreated{})
}
