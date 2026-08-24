package rabbitmq

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var brokerURL string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	container, err := testcontainers.Run(ctx, "rabbitmq:4-alpine",
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start rabbitmq container: %v\n", err)
		os.Exit(1)
	}

	endpoint, err := container.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get rabbitmq endpoint: %v\n", err)
		os.Exit(1)
	}
	brokerURL = fmt.Sprintf("amqp://guest:guest@%s/", endpoint)

	code := m.Run()

	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate rabbitmq container: %v\n", err)
	}

	os.Exit(code)
}

// Each test owns a message type and an exchange: every consumer binds the
// message type name as routing key, so sharing one would deliver a test's
// message to the other tests' queues.
type paymentRequested struct {
	OrderID string `json:"order_id"`
}

func (paymentRequested) ExchangeOwnerName() string { return "payments-events" }

type shipmentScheduled struct {
	OrderID string `json:"order_id"`
}

func (shipmentScheduled) ExchangeOwnerName() string { return "shipping-events" }

func skipWithoutBroker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping test that requires a rabbitmq broker")
	}
}

func TestIntegrationPublishAndConsume(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	received := make(chan orderCreated, 1)
	correlationIDs := make(chan any, 1)

	consumer := newConsumer(t, orderCreated{}.ExchangeOwnerName(), "billing-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, orderCreated{},
		func(ctx context.Context, msg orderCreated) error {
			correlationIDs <- ctx.Value(testCarrierKey)
			received <- msg
			return nil
		}))
	consumer.Start(ctx)

	producer := newProducer(t, orderCreated{}.ExchangeOwnerName(), gorabbit.NewMemoryCache())
	publishCtx := context.WithValue(ctx, testCarrierKey, "corr-1")
	require.NoError(t, producer.Publish(publishCtx, orderCreated{OrderID: "123"}))

	select {
	case msg := <-received:
		require.Equal(t, "123", msg.OrderID)
		require.Equal(t, "corr-1", <-correlationIDs)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
}

func TestIntegrationRetriesThenDeadLetters(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	var attempts atomic.Int32

	consumer := newConsumer(t, paymentRequested{}.ExchangeOwnerName(), "payments-queue",
		func(s *Setup) *Setup { return s.WithRetry(2, 200*time.Millisecond, nil) })
	require.NoError(t, RegisterHandler(ctx, consumer, paymentRequested{},
		func(context.Context, paymentRequested) error {
			attempts.Add(1)
			return fmt.Errorf("always fails")
		}))
	consumer.Start(ctx)

	producer := newProducer(t, paymentRequested{}.ExchangeOwnerName(), gorabbit.NewMemoryCache())
	require.NoError(t, producer.Publish(ctx, paymentRequested{OrderID: "456"}))

	dead := consumeOne(t, "payments-queue.dlq", 15*time.Second)
	require.Equal(t, "paymentRequested", dead.Type)
	require.JSONEq(t, `{"order_id":"456"}`, string(dead.Body))
	require.Equal(t, int32(3), attempts.Load(), "first delivery plus two retries")

	// The id assigned at publish is what makes a dead-lettered message traceable
	// back to the producer, so it must survive every retry hop.
	_, err := uuid.Parse(dead.MessageId)
	require.NoError(t, err, "message id must reach the dlq intact")
}

func TestIntegrationCachedMessagesAreFlushedOnConnect(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()
	exchange := shipmentScheduled{}.ExchangeOwnerName()
	received := make(chan shipmentScheduled, 2)

	consumer := newConsumer(t, exchange, "shipping-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, shipmentScheduled{},
		func(_ context.Context, msg shipmentScheduled) error {
			received <- msg
			return nil
		}))
	consumer.Start(ctx)

	offline := newTestClient(NewSetup(unreachableURL, exchange, "cached-producer").WithDialTimeout(200 * time.Millisecond))
	offline.cache = cache
	require.NoError(t, offline.Publish(ctx, shipmentScheduled{OrderID: "first"}))
	require.NoError(t, offline.Publish(ctx, shipmentScheduled{OrderID: "second"}))

	producer, err := NewSetup(brokerURL, exchange, "cached-producer").Connect(cache)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	for _, want := range []string{"first", "second"} {
		select {
		case msg := <-received:
			require.Equal(t, want, msg.OrderID)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for the cached message %q", want)
		}
	}

	keys, err := cache.GetAllKeys(ctx, cacheKey("cached-producer", 0)+"*")
	require.NoError(t, err)
	require.Empty(t, keys, "published messages must leave the cache")
}

func newProducer(t *testing.T, exchange string, cache gorabbit.Cache) *Client {
	t.Helper()

	c, err := NewSetup(brokerURL, exchange, "producer").WithHeaderCarrier(testCarrier{}).Connect(cache)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func newConsumer(t *testing.T, exchange, queue string, opts ...func(*Setup) *Setup) *Client {
	t.Helper()

	setup := NewSetup(brokerURL, exchange, queue).
		WithConsumer(queue).
		WithHeaderCarrier(testCarrier{}).
		WithPrefetchCount(1)

	for _, opt := range opts {
		setup = opt(setup)
	}

	c, err := setup.Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func consumeOne(t *testing.T, queue string, timeout time.Duration) amqp091.Delivery {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	require.NoError(t, err)

	select {
	case msg := <-deliveries:
		return msg
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for a message on %q", queue)
		return amqp091.Delivery{}
	}
}
