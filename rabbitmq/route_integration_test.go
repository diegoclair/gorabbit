package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type routingExchange struct{}

func (routingExchange) Name() string { return "routing-events" }

type vendorEvent struct {
	gorabbit.RoutedMsg[routingExchange]
	Vendor string `json:"vendor"`
	ID     string `json:"id"`
}

func (v vendorEvent) RouteBy() string { return v.Vendor }

type dottedExchange struct{}

func (dottedExchange) Name() string { return "dotted-events" }

type dottedEvent struct {
	gorabbit.RoutedMsg[dottedExchange]
	Vendor string `json:"vendor"`
	ID     string `json:"id"`
}

func (d dottedEvent) RouteBy() string { return d.Vendor }

type retryRouteExchange struct{}

func (retryRouteExchange) Name() string { return "retry-route-events" }

type retryRouteEvent struct {
	gorabbit.RoutedMsg[retryRouteExchange]
	Vendor string `json:"vendor"`
	ID     string `json:"id"`
}

func (r retryRouteEvent) RouteBy() string { return r.Vendor }

func requireNothingElse(t *testing.T, received <-chan string, queue string) {
	t.Helper()

	select {
	case extra := <-received:
		t.Fatalf("queue %q also received %q", queue, extra)
	case <-time.After(absenceWindow):
	}
}

// A fixed routing key cannot express two queues slicing one type: they would
// share a binding and the broker would hand a copy of everything to both.
func TestIntegrationRoutedMessageReachesOnlyTheQueueOfItsRoute(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	shopee := make(chan string, 4)
	mercado := make(chan string, 4)
	everything := make(chan string, 4)

	shopeeConsumer := newConsumer[routingExchange](t, "routing-shopee-queue")
	require.NoError(t, SubscribeRoute(ctx, shopeeConsumer, vendorEvent{}, "shopee",
		collectIDs(shopee, func(m vendorEvent) string { return m.ID })))
	shopeeConsumer.Start(ctx)

	mercadoConsumer := newConsumer[routingExchange](t, "routing-mercado-queue")
	require.NoError(t, SubscribeRoute(ctx, mercadoConsumer, vendorEvent{}, "mercadolivre",
		collectIDs(mercado, func(m vendorEvent) string { return m.ID })))
	mercadoConsumer.Start(ctx)

	everythingConsumer := newConsumer[routingExchange](t, "routing-all-queue")
	require.NoError(t, Subscribe(ctx, everythingConsumer, vendorEvent{},
		collectIDs(everything, func(m vendorEvent) string { return m.ID })))
	everythingConsumer.Start(ctx)

	producer := newProducer[routingExchange](t, gorabbit.NewMemoryCache())
	require.NoError(t, producer.Publish(ctx, vendorEvent{Vendor: "shopee", ID: "s-1"}))
	require.NoError(t, producer.Publish(ctx, vendorEvent{Vendor: "mercadolivre", ID: "m-1"}))

	require.Equal(t, "s-1", waitForMessage(t, shopee))
	require.Equal(t, "m-1", waitForMessage(t, mercado))

	// Subscribe on a routed type is the whole type, so this one takes both.
	require.ElementsMatch(t, []string{"s-1", "m-1"},
		[]string{waitForMessage(t, everything), waitForMessage(t, everything)})

	requireNothingElse(t, shopee, "routing-shopee-queue")
	requireNothingElse(t, mercado, "routing-mercado-queue")
	requireNothingElse(t, everything, "routing-all-queue")
}

// A route carrying "#" or "*" would bind a queue to everything of that type,
// and one carrying the escape itself would take the messages of another route.
func TestIntegrationRouteCarryingAWildcardIsNotAWildcard(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	routes := map[string]string{"#": "hash", "*": "star", "mercado.livre": "dotted", ".": "baredot", "%2E": "escaped"}
	received := make(map[string]chan string, len(routes))
	everything := make(chan string, 8)

	for route, id := range routes {
		queue := "wildcard-" + id + "-queue"
		received[id] = make(chan string, 8)

		consumer := newConsumer[dottedExchange](t, queue)
		require.NoError(t, SubscribeRoute(ctx, consumer, dottedEvent{}, route,
			collectIDs(received[id], func(m dottedEvent) string { return m.ID })))
		consumer.Start(ctx)
	}

	allConsumer := newConsumer[dottedExchange](t, "wildcard-all-queue")
	require.NoError(t, Subscribe(ctx, allConsumer, dottedEvent{},
		collectIDs(everything, func(m dottedEvent) string { return m.ID })))
	allConsumer.Start(ctx)

	producer := newProducer[dottedExchange](t, gorabbit.NewMemoryCache())
	for route, id := range routes {
		require.NoError(t, producer.Publish(ctx, dottedEvent{Vendor: route, ID: id}))
	}

	for _, id := range routes {
		require.Equal(t, id, waitForMessage(t, received[id]))
	}

	expected := make([]string, 0, len(routes))
	for _, id := range routes {
		expected = append(expected, id)
	}
	delivered := make([]string, 0, len(expected))
	for range expected {
		delivered = append(delivered, waitForMessage(t, everything))
	}
	require.ElementsMatch(t, expected, delivered)

	for _, id := range routes {
		requireNothingElse(t, received[id], id)
	}
	requireNothingElse(t, everything, "wildcard-all-queue")
}

// The retry republishes under the routing key of the delivery: dropping the
// route leaves a key no per-route handler answers, so the message dead-letters.
func TestIntegrationRetryKeepsTheRouteOfTheMessage(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	routes := map[string]string{"mercado.livre": "dotted", "#": "hash"}

	queues := make(map[string]string, len(routes))
	delivered := make(map[string]chan string, len(routes))
	onRetry := make(map[string]<-chan string, len(routes))
	backHome := make(map[string]<-chan string, len(routes))

	for route, id := range routes {
		queue := "retry-route-" + id + "-queue"
		queues[id] = queue
		delivered[id] = make(chan string, 4)
		received := delivered[id]

		consumer := newConsumer[retryRouteExchange](t, queue,
			func(s *ConsumerSetup[retryRouteExchange]) *ConsumerSetup[retryRouteExchange] {
				return s.WithRetry(2, 300*time.Millisecond, nil)
			})

		var failedOnce atomic.Bool
		require.NoError(t, SubscribeRoute(ctx, consumer, retryRouteEvent{}, route,
			func(_ context.Context, msg retryRouteEvent) error {
				if failedOnce.CompareAndSwap(false, true) {
					return errors.New("the first attempt fails on purpose")
				}
				received <- msg.ID
				return nil
			}))

		// The key the library republishes with only exists on the wire, so the
		// two hops of the retry are read from queues of our own.
		onRetry[id] = spyRoutingKeys(t, queue+".retry")
		backHome[id] = spyRoutingKeys(t, queue)

		consumer.Start(ctx)
	}

	producer := newProducer[retryRouteExchange](t, gorabbit.NewMemoryCache())
	for route, id := range routes {
		require.NoError(t, producer.Publish(ctx, retryRouteEvent{Vendor: route, ID: id}))
	}

	for route, id := range routes {
		want := routedKey("retryRouteEvent", route)
		require.Equal(t, want, waitForMessage(t, onRetry[id]), "the retry publish dropped the route")
		require.Equal(t, want, waitForMessage(t, backHome[id]), "the message came back on another key")
		require.Equal(t, id, waitForMessage(t, delivered[id]), "the retried message never reached its handler again")
	}

	for _, id := range routes {
		requireNothingElse(t, delivered[id], queues[id])

		_, dead := consumeWithin(t, queues[id]+".dlq", absenceWindow)
		require.False(t, dead, "a message that succeeded on retry must not be dead-lettered")
	}
}

// spyRoutingKeys reads an exchange without taking the deliveries away from the
// consumer under test.
func spyRoutingKeys(t *testing.T, exchange string) <-chan string {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(queue.Name, "#", exchange, false, nil))

	deliveries, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	require.NoError(t, err)

	keys := make(chan string, 8)
	ctx := t.Context()
	go func() {
		for delivery := range deliveries {
			select {
			case keys <- delivery.RoutingKey:
			case <-ctx.Done():
				return
			}
		}
	}()

	return keys
}
