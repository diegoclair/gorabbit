// Package publisherconcurrency must not compile: the worker pool handles
// deliveries a publisher never receives, which is what
// TestConsumerOptionsOnAPublisherDoNotCompile asserts by building this package.
package publisherconcurrency

import (
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

func New(amqpURL string) *rabbitmq.Setup[orders.Exchange] {
	return rabbitmq.NewSetup[orders.Exchange](amqpURL, "orders-api").
		WithConcurrency(4)
}
