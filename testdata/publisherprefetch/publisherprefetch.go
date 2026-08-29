// Package publisherprefetch must not compile: a prefetch bounds deliveries a
// publisher never receives, which is what
// TestConsumerOptionsOnAPublisherDoNotCompile asserts by building this package.
package publisherprefetch

import (
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

func New(amqpURL string) *rabbitmq.Setup[orders.Exchange] {
	return rabbitmq.NewSetup[orders.Exchange](amqpURL, "orders-api").
		WithPrefetchCount(10)
}
