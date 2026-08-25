package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type unbindExchange struct{}

func (unbindExchange) Name() string { return "unbind-events" }

type unbindGone struct {
	gorabbit.Msg[unbindExchange]
	ID string `json:"id"`
}

type unbindKept struct {
	gorabbit.Msg[unbindExchange]
	ID string `json:"id"`
}

type strayExchange struct{}

func (strayExchange) Name() string { return "stray-events" }

type strayHandled struct {
	gorabbit.Msg[strayExchange]
	ID string `json:"id"`
}

type deadExchange struct{}

func (deadExchange) Name() string { return "dead-events" }

type deadEvent struct {
	gorabbit.Msg[deadExchange]
	ID string `json:"id"`
}

// The binding recorded by a previous run outlives the handler, so the queue
// keeps receiving what nothing handles until the cleanup unbinds it.
func TestIntegrationUnbindDropsTheBindingOfAHandlerThatIsGone(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "unbind-queue"
	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()

	previous := newSharedCacheConsumer[unbindExchange](t, queue, cache)
	require.NoError(t, RegisterHandler(ctx, previous, unbindGone{},
		func(context.Context, unbindGone) error { return nil }))
	previous.Start(ctx)
	previous.Close()

	received := make(chan string, 2)
	current := newSharedCacheConsumer[unbindExchange](t, queue, cache)
	require.NoError(t, RegisterHandler(ctx, current, unbindKept{},
		collectIDs(received, func(m unbindKept) string { return m.ID })))
	current.Start(ctx)

	require.Eventually(t, func() bool {
		keys, err := cache.GetAllKeys(ctx, fmt.Sprintf("%s:%s:*", handlerInfoCachePrefix, queue))
		return err == nil && len(keys) == 1
	}, 30*time.Second, 100*time.Millisecond, "the binding of the handler that is gone was never unbound")

	producer, err := NewSetup[unbindExchange](brokerURL, "unbind-producer").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, producer.Publish(ctx, unbindGone{ID: "gone"}))
	require.NoError(t, producer.Publish(ctx, unbindKept{ID: "kept"}))

	// The handled message proves the topology still routes, so the absence of
	// the other one is the unbind and not a slow broker.
	require.Equal(t, "kept", waitForMessage(t, received))

	_, dead := consumeWithin(t, queue+".dlq", absenceWindow)
	require.False(t, dead, "the unbound message still reached the queue and was dead-lettered")
	require.Zero(t, waitQueuesDrained(t, []string{queue}, 3*time.Second), "the unbound message is sitting in the queue")
}

func TestIntegrationDeliveryWithoutAHandlerIsDeadLetteredOnce(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		queue      = "stray-queue"
		routingKey = "strayUnhandled"
	)
	ctx := context.Background()

	consumer := newConsumer[strayExchange](t, queue)
	require.NoError(t, RegisterHandler(ctx, consumer, strayHandled{},
		func(context.Context, strayHandled) error { return nil }))
	consumer.Start(ctx)

	// The consumer's own exchange takes every routing key, which is how a
	// delivery no handler answers reaches the queue.
	rawPublish(t, queue, routingKey, []byte(`{"id":"stray"}`))

	dead, ok := consumeWithin(t, queue+".dlq", 15*time.Second)
	require.True(t, ok, "a delivery nothing handles must reach the dlq")
	require.Equal(t, routingKey, dead.RoutingKey)

	_, again := consumeWithin(t, queue+".dlq", absenceWindow)
	require.False(t, again, "the delivery reached the dlq more than once")
	require.Zero(t, waitQueuesDrained(t, []string{queue}, 3*time.Second))
}

func TestIntegrationHandlerErrorWithoutRetryIsDeadLetteredOnce(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "dead-queue"
	ctx := context.Background()
	var attempts atomic.Int32

	consumer := newConsumer[deadExchange](t, queue)
	require.NoError(t, RegisterHandler(ctx, consumer, deadEvent{}, func(context.Context, deadEvent) error {
		attempts.Add(1)
		return errors.New("always fails")
	}))
	consumer.Start(ctx)

	producer := newProducer[deadExchange](t, gorabbit.NewMemoryCache())
	require.NoError(t, producer.Publish(ctx, deadEvent{ID: "dead"}))

	dead, ok := consumeWithin(t, queue+".dlq", 15*time.Second)
	require.True(t, ok, "a failed message must reach the dlq when no retry is configured")
	require.Equal(t, "deadEvent", dead.Type)
	require.JSONEq(t, `{"id":"dead"}`, string(dead.Body))

	_, again := consumeWithin(t, queue+".dlq", absenceWindow)
	require.False(t, again, "the failed message reached the dlq more than once")
	require.Zero(t, waitQueuesDrained(t, []string{queue}, 3*time.Second))
	require.Equal(t, int32(1), attempts.Load(), "without a retry queue the handler runs once")
}

// The cache is what carries the bindings from one run to the next, so both
// clients of the unbind test must share it.
func newSharedCacheConsumer[E gorabbit.Exchange](t *testing.T, queue string, cache gorabbit.Cache) *Client[E] {
	t.Helper()

	c, err := NewSetup[E](brokerURL, queue).
		WithConsumer(queue).
		WithReconnectDelay(200 * time.Millisecond).
		Connect(cache)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func rawPublish(t *testing.T, exchange, routingKey string, body []byte) {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	require.NoError(t, ch.PublishWithContext(context.Background(), exchange, routingKey, false, false,
		amqp091.Publishing{
			ContentType:  "application/json",
			Type:         routingKey,
			Body:         body,
			DeliveryMode: amqp091.Persistent,
		}))
}
