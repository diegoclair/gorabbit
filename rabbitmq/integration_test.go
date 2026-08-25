package rabbitmq

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/diegoclair/gorabbit"
	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// brokerURLEnv is duplicated in the blackbox test package, which cannot see it.
const brokerURLEnv = "GORABBIT_TEST_BROKER_URL"

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

	// The blackbox tests (package rabbitmq_test) share this binary but not this
	// package's scope, so the broker address travels through the environment.
	if err := os.Setenv(brokerURLEnv, brokerURL); err != nil {
		fmt.Fprintf(os.Stderr, "failed to export the broker url: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate rabbitmq container: %v\n", err)
	}

	os.Exit(code)
}

// Each test owns a message type and an exchange: every consumer binds the
// message type name as routing key, so sharing one would deliver a test's
// message to the other tests' queues.
type paymentsExchange struct{}

func (paymentsExchange) Name() string { return "payments-events" }

type payments = gorabbit.Msg[paymentsExchange]

type paymentRequested struct {
	payments
	OrderID string `json:"order_id"`
}

type shippingExchange struct{}

func (shippingExchange) Name() string { return "shipping-events" }

type shipping = gorabbit.Msg[shippingExchange]

type shipmentScheduled struct {
	shipping
	OrderID string `json:"order_id"`
}

type warehouseExchange struct{}

func (warehouseExchange) Name() string { return "warehouse-events" }

type warehouse = gorabbit.Msg[warehouseExchange]

type stockReserved struct {
	warehouse
	SKU string `json:"sku"`
}

type invoicesExchange struct{}

func (invoicesExchange) Name() string { return "invoices-events" }

type invoices = gorabbit.Msg[invoicesExchange]

type catalogExchange struct{}

func (catalogExchange) Name() string { return "catalog-events" }

type catalog = gorabbit.Msg[catalogExchange]

func skipWithoutBroker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping test that requires a rabbitmq broker")
	}
}

func TestIntegrationPublishAndConsume(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	received := make(chan orderCreated, 1)
	correlationIDs := make(chan any, 1)

	consumer := newConsumer[ordersExchange](t, "billing-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, orderCreated{},
		func(ctx context.Context, msg orderCreated) error {
			correlationIDs <- ctx.Value(testCarrierKey)
			received <- msg
			return nil
		}))
	consumer.Start(ctx)

	producer := newProducer[ordersExchange](t, gorabbit.NewMemoryCache())
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
	t.Parallel()

	ctx := context.Background()
	var attempts atomic.Int32

	consumer := newConsumer[paymentsExchange](t, "payments-queue",
		func(s *Setup[paymentsExchange]) *Setup[paymentsExchange] {
			return s.WithRetry(2, 200*time.Millisecond, nil)
		})
	require.NoError(t, RegisterHandler(ctx, consumer, paymentRequested{},
		func(context.Context, paymentRequested) error {
			attempts.Add(1)
			return fmt.Errorf("always fails")
		}))
	consumer.Start(ctx)

	producer := newProducer[paymentsExchange](t, gorabbit.NewMemoryCache())
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
	t.Parallel()

	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()
	received := make(chan shipmentScheduled, 2)

	consumer := newConsumer[shippingExchange](t, "shipping-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, shipmentScheduled{},
		func(_ context.Context, msg shipmentScheduled) error {
			received <- msg
			return nil
		}))
	consumer.Start(ctx)

	offline, err := NewSetup[shippingExchange](unreachableURL, "cached-producer").
		WithDialTimeout(200 * time.Millisecond).
		Connect(cache)
	require.NoError(t, err)
	require.False(t, offline.Connected())
	require.NoError(t, offline.Publish(ctx, shipmentScheduled{OrderID: "first"}))
	require.NoError(t, offline.Publish(ctx, shipmentScheduled{OrderID: "second"}))
	offline.Close()

	producer, err := NewSetup[shippingExchange](brokerURL, "cached-producer").Connect(cache)
	require.NoError(t, err)
	require.True(t, producer.Connected())
	t.Cleanup(producer.Close)

	for _, want := range []string{"first", "second"} {
		select {
		case msg := <-received:
			require.Equal(t, want, msg.OrderID)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for the cached message %q", want)
		}
	}

	keys, err := cache.GetAllKeys(ctx, cacheKey("cached-producer", "")+"*")
	require.NoError(t, err)
	require.Empty(t, keys, "published messages must leave the cache")
}

// Two message types with the same name, so the same routing key, owned by
// different exchanges: one consumer handles both and only the exchange keeps
// them apart. Each fact is published by the client that owns its exchange.
func TestIntegrationSameTypeNameInDifferentExchanges(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	fromInvoices := make(chan string, 1)
	fromCatalog := make(chan string, 1)
	var publishInvoice, publishCatalog func(t *testing.T)

	consumer := newConsumer[invoicesExchange](t, "collision-queue")
	invoicesProducer := newProducer[invoicesExchange](t, gorabbit.NewMemoryCache())
	catalogProducer := newProducer[catalogExchange](t, gorabbit.NewMemoryCache())

	{
		type itemAdded struct {
			invoices
			ID string `json:"id"`
		}

		require.NoError(t, RegisterHandler(ctx, consumer, itemAdded{},
			func(_ context.Context, msg itemAdded) error {
				fromInvoices <- msg.ID
				return nil
			}))

		publishInvoice = func(t *testing.T) {
			require.NoError(t, invoicesProducer.Publish(ctx, itemAdded{ID: "invoice-1"}))
		}
	}

	{
		type itemAdded struct {
			catalog
			ID string `json:"id"`
		}

		require.NoError(t, RegisterHandler(ctx, consumer, itemAdded{},
			func(_ context.Context, msg itemAdded) error {
				fromCatalog <- msg.ID
				return nil
			}))

		publishCatalog = func(t *testing.T) {
			require.NoError(t, catalogProducer.Publish(ctx, itemAdded{ID: "catalog-1"}))
		}
	}

	consumer.Start(ctx)
	publishInvoice(t)
	publishCatalog(t)

	require.Equal(t, "invoice-1", waitForMessage(t, fromInvoices))
	require.Equal(t, "catalog-1", waitForMessage(t, fromCatalog))
}

func TestIntegrationPublishesPointerMessages(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	received := make(chan string, 1)

	consumer := newConsumer[warehouseExchange](t, "warehouse-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, stockReserved{},
		func(_ context.Context, msg stockReserved) error {
			received <- msg.SKU
			return nil
		}))
	consumer.Start(ctx)

	producer := newProducer[warehouseExchange](t, gorabbit.NewMemoryCache())
	require.NoError(t, producer.Publish(ctx, &stockReserved{SKU: "sku-1"}))

	require.Equal(t, "sku-1", waitForMessage(t, received))
}

type notificationsExchange struct{}

func (notificationsExchange) Name() string { return "notifications-events" }

type notifications = gorabbit.Msg[notificationsExchange]

type customerNotified struct {
	notifications
	ID string `json:"id"`
}

// The broker only comes up after Connect, RegisterHandler, Start and Publish:
// the client must hold the message and the binding until the connection lands.
func TestIntegrationOfflineClientDeliversOnceTheBrokerComesUp(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	port := freeLocalPort(t)
	url := fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", port)
	received := make(chan string, 1)

	client, err := NewSetup[notificationsExchange](url, "notifications-queue").
		WithConsumer("notifications-queue").
		WithDialTimeout(2 * time.Second).
		WithReconnectDelay(200 * time.Millisecond).
		Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.False(t, client.Connected())

	require.NoError(t, RegisterHandler(ctx, client, customerNotified{},
		func(_ context.Context, msg customerNotified) error {
			received <- msg.ID
			return nil
		}))
	client.Start(ctx)
	require.NoError(t, client.Publish(ctx, customerNotified{ID: "n-1"}))

	startBrokerAt(t, port)

	select {
	case id := <-received:
		require.Equal(t, "n-1", id)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the message published while offline")
	}

	require.Eventually(t, client.Connected, 10*time.Second, 100*time.Millisecond)
}

// The kernel may hand the same ephemeral port to two callers once it is
// released, which would put two parallel tests' brokers on one address.
var (
	portsMu    sync.Mutex
	takenPorts = map[int]struct{}{}
)

func freeLocalPort(t *testing.T) int {
	t.Helper()

	portsMu.Lock()
	defer portsMu.Unlock()

	for range 50 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := l.Addr().(*net.TCPAddr).Port
		require.NoError(t, l.Close())

		if _, taken := takenPorts[port]; taken {
			continue
		}
		takenPorts[port] = struct{}{}

		return port
	}

	t.Fatal("no unused local port available")

	return 0
}

// startBrokerAt binds the broker to a fixed host port, so a client created
// before the broker exists already knows the address it will reconnect to.
func startBrokerAt(t *testing.T, port int) {
	t.Helper()
	ctx := context.Background()

	broker, err := testcontainers.Run(ctx, "rabbitmq:4-alpine",
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *mobycontainer.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("5672/tcp"): {
					{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)},
				},
			}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(broker))
	})
}

func waitForMessage(t *testing.T, messages <-chan string) string {
	t.Helper()

	select {
	case msg := <-messages:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the message")
		return ""
	}
}

func newProducer[E gorabbit.Exchange](t *testing.T, cache gorabbit.Cache) *Client[E] {
	t.Helper()

	c, err := NewSetup[E](brokerURL, "producer").WithHeaderCarrier(testCarrier{}).Connect(cache)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func newConsumer[E gorabbit.Exchange](t *testing.T, queue string, opts ...func(*Setup[E]) *Setup[E]) *Client[E] {
	t.Helper()

	setup := NewSetup[E](brokerURL, queue).
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
