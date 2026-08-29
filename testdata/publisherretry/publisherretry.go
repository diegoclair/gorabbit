// Package publisherretry must not compile: retry needs the queues WithConsumer
// declares, which is what TestConsumerOptionsOnAPublisherDoNotCompile asserts by
// building this package.
package publisherretry

import (
	"time"

	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
)

func New(amqpURL string) *rabbitmq.Setup[orders.Exchange] {
	return rabbitmq.NewSetup[orders.Exchange](amqpURL, "orders-api").
		WithRetry(3, 30*time.Second, nil)
}
