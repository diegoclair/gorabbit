// Package crossexchange must not compile: a client owns one exchange and cannot
// publish a message from another, which is what
// TestPublishingAnotherExchangeDoesNotCompile asserts by building this package.
package crossexchange

import (
	"context"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

type billingExchange struct{}

func (billingExchange) Name() string { return "billing" }

type billing = gorabbit.Msg[billingExchange]

type invoiceIssued struct {
	gorabbit.Msg[billingExchange]
	InvoiceID string `json:"invoice_id"`
}

func Publish(ctx context.Context, client *rabbitmq.Client[orders.Exchange]) error {
	return client.Publish(ctx, invoiceIssued{})
}
