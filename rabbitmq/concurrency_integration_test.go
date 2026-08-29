package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type concurrencyExchange struct{}

func (concurrencyExchange) Name() string { return "concurrency-events" }

type concurrent = gorabbit.Msg[concurrencyExchange]

// One message type per test: the type name is the binding key, so two tests
// sharing it would each receive the other's messages.
type parallelEvent struct {
	concurrent
	ID string `json:"id"`
}

type serialEvent struct {
	concurrent
	ID string `json:"id"`
}

type boundedEvent struct {
	concurrent
	ID string `json:"id"`
}

type regeneratedEvent struct {
	concurrent
	ID string `json:"id"`
}

type exactlyOnceEvent struct {
	concurrent
	ID string `json:"id"`
}

type midFlightEvent struct {
	concurrent
	ID string `json:"id"`
}

type cancelledEvent struct {
	concurrent
	ID string `json:"id"`
}

type shutdownEvent struct {
	concurrent
	ID string `json:"id"`
}

type sharedPoolEvent struct {
	concurrent
	ID string `json:"id"`
}

type sharedPoolPeerEvent struct {
	concurrent
	ID string `json:"id"`
}

const (
	handlerArrival = 30 * time.Second
	// Long enough for a pool that was never joined to be replaced by a second
	// one: a redial waits a reconnect delay and a delivery is a local hop.
	poolSettle        = 3 * time.Second
	connectedLogLine  = "gorabbit: connected to RabbitMQ"
	consumingLogLine  = "gorabbit: started consuming messages"
	consumeErrLogLine = "gorabbit: error consuming messages"
)

// inFlightTracker answers the only question these tests ask of the pool: how
// many handlers were inside it at the same moment.
type inFlightTracker struct {
	current atomic.Int64
	max     atomic.Int64
}

func (t *inFlightTracker) enter() {
	now := t.current.Add(1)
	for {
		peak := t.max.Load()
		if now <= peak || t.max.CompareAndSwap(peak, now) {
			return
		}
	}
}

func (t *inFlightTracker) leave() { t.current.Add(-1) }

func (t *inFlightTracker) peak() int64 { return t.max.Load() }

func requireNeverExceeds(t *testing.T, tracker *inFlightTracker, limit int64, window time.Duration, what string) {
	t.Helper()

	deadline := time.After(window)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return
		case <-ticker.C:
			if peak := tracker.peak(); peak > limit {
				t.Fatalf("%s: %d handlers ran at once, the configured concurrency is %d", what, peak, limit)
			}
		}
	}
}

// The failure has to name what it saw: a pool that never opened and a broker
// that never delivered look the same from here.
func waitForArrivals(t *testing.T, arrived <-chan string, n int) []string {
	t.Helper()

	var inside []string
	deadline := time.After(handlerArrival)
	for len(inside) < n {
		select {
		case id := <-arrived:
			inside = append(inside, id)
		case <-deadline:
			t.Fatalf("only %d of %d deliveries were inside a handler at once: %v — a serial consumer holds the rest behind the first",
				len(inside), n, inside)
		}
	}

	return inside
}

func requireAllHandled(t *testing.T, handled <-chan string, expected map[string]struct{}) {
	t.Helper()

	seen := make(map[string]struct{}, len(expected))
	deadline := time.After(handlerArrival)
	for len(seen) < len(expected) {
		select {
		case id := <-handled:
			seen[id] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out: %d of %d messages were handled: %v", len(seen), len(expected), seen)
		}
	}
}

// The broker's own counters are the only place that answers whether a delivery
// was really acked or a consumer really cancelled.
func requireQueueEventually(t *testing.T, queue string, reached func(amqp091.Queue) bool, what string) {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)

	deadline := time.After(20 * time.Second)
	for {
		// A passive declare asserts nothing about the arguments, so it cannot
		// redeclare the queue out from under the client.
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		require.NoError(t, err)
		if reached(q) {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s on %q: %d messages, %d consumers", what, queue, q.Messages, q.Consumers)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func purgeQueue(t *testing.T, queue string) {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueuePurge(queue, false)
	require.NoError(t, err)
}

func requireQueueDrains(t *testing.T, queue string) {
	t.Helper()

	requireQueueEventually(t, queue, func(q amqp091.Queue) bool { return q.Messages == 0 }, "the queue to drain")
}

// The app name carries the test name so two producers of one process never
// reach for the same cache key.
func newConcurrencyProducer(t *testing.T) *Client[concurrencyExchange] {
	t.Helper()

	c, err := NewSetup[concurrencyExchange](brokerURL, t.Name()+"-producer").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func TestConcurrencyWorkersRunAtTheSameTime(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const workers = 3
	ctx := context.Background()
	arrived := make(chan string, workers*2)
	allArrived := make(chan struct{})

	consumer := newConsumer[concurrencyExchange](t, "concurrency-parallel-queue",
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(workers).WithConcurrency(workers)
		})

	// Registered after the client, so it runs before Close and no handler is
	// still parked on the barrier when the client tears down.
	release := sync.OnceFunc(func() { close(allArrived) })
	t.Cleanup(release)

	require.NoError(t, Subscribe(ctx, consumer, parallelEvent{},
		func(_ context.Context, msg parallelEvent) error {
			arrived <- msg.ID
			<-allArrived
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range workers {
		require.NoError(t, producer.Publish(ctx, parallelEvent{ID: fmt.Sprintf("parallel-%d", i)}))
	}

	waitForArrivals(t, arrived, workers)

	release()
}

func TestConcurrencyDefaultsToOneDeliveryAtATime(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const published = 4
	ctx := context.Background()
	arrived := make(chan string, published*2)
	held := make(chan struct{})

	// The prefetch has to be able to hand more than one delivery over, or the
	// broker, not the consumer, is what keeps the second out of a handler.
	consumer := newConsumer[concurrencyExchange](t, "concurrency-serial-queue",
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(published)
		})

	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)

	require.NoError(t, Subscribe(ctx, consumer, serialEvent{},
		func(_ context.Context, msg serialEvent) error {
			arrived <- msg.ID
			<-held
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range published {
		require.NoError(t, producer.Publish(ctx, serialEvent{ID: fmt.Sprintf("serial-%d", i)}))
	}

	first := waitForArrivals(t, arrived, 1)[0]

	select {
	case second := <-arrived:
		t.Fatalf("%q entered a handler while %q was still running: without WithConcurrency the client handles one delivery at a time",
			second, first)
	case <-time.After(poolSettle):
	}

	release()
}

func TestConcurrencyPoolIsBounded(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		workers   = 2
		published = 8
	)
	ctx := context.Background()
	tracker := &inFlightTracker{}
	arrived := make(chan string, published*2)
	handled := make(chan string, published*2)
	held := make(chan struct{})

	consumer := newConsumer[concurrencyExchange](t, "concurrency-bounded-queue",
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(published).WithConcurrency(workers)
		})

	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)

	require.NoError(t, Subscribe(ctx, consumer, boundedEvent{},
		func(_ context.Context, msg boundedEvent) error {
			tracker.enter()
			defer tracker.leave()

			arrived <- msg.ID
			<-held
			handled <- msg.ID
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range published {
		require.NoError(t, producer.Publish(ctx, boundedEvent{ID: fmt.Sprintf("bounded-%d", i)}))
	}

	waitForArrivals(t, arrived, workers)

	requireNeverExceeds(t, tracker, workers, poolSettle,
		"more deliveries entered a handler than the pool has workers")
	release()

	requireAllHandled(t, handled, expectedIDs("bounded", published))
	require.EqualValues(t, workers, tracker.peak(),
		"the pool held %d messages at once with a prefetch of %d and a concurrency of %d", tracker.peak(), published, workers)
}

// A reconnect calls consumeMessages again; the generation it left behind must
// be gone, or two pools chew the same queue side by side.
func TestConcurrencyReconnectDoesNotDoubleThePool(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		workers   = 2
		published = 6
		queue     = "concurrency-pool-queue"
	)
	ctx := context.Background()
	tracker := &inFlightTracker{}
	logger := &recordingLogger{}
	arrived := make(chan string, published*4)
	handled := make(chan string, published*4)
	held := make(chan struct{})

	// The proxy is the only way to cut the connection under a handler without
	// taking the shared broker down.
	proxy := newBrokerProxy(t, brokerURL)
	consumer, err := NewSetup[concurrencyExchange](proxy.url(), queue).
		WithConsumer(queue).
		WithDialTimeout(testDialTimeout).
		WithReconnectDelay(testReconnectWait).
		WithPrefetchCount(published).
		WithConcurrency(workers).
		WithLogger(logger).
		Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)

	require.NoError(t, Subscribe(ctx, consumer, regeneratedEvent{},
		func(_ context.Context, msg regeneratedEvent) error {
			tracker.enter()
			defer tracker.leave()

			arrived <- msg.ID
			<-held
			handled <- msg.ID
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range published {
		require.NoError(t, producer.Publish(ctx, regeneratedEvent{ID: fmt.Sprintf("pool-%d", i)}))
	}

	waitForArrivals(t, arrived, workers)

	// Every worker is parked inside a handler, so whatever consumes next is a
	// generation this one never joined.
	proxy.cut()
	require.Eventually(t, func() bool { return logger.count(connectedLogLine) >= 2 }, handlerArrival, 50*time.Millisecond,
		"the consumer never dialled again after the connection was cut")

	requireNeverExceeds(t, tracker, workers, poolSettle,
		"a second generation of workers joined the first after the reconnect")
	release()

	requireAllHandled(t, handled, expectedIDs("pool", published))
	require.LessOrEqual(t, tracker.peak(), int64(workers),
		"the pool held %d messages at once with a concurrency of %d", tracker.peak(), workers)
	require.GreaterOrEqual(t, logger.count(consumeErrLogLine), 1,
		"a delivery channel the outage closed must be reported as a consume failure, not read as a shutdown")
}

func TestConcurrencyDeliversEveryMessageOnceAndDrainsTheQueue(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		workers   = 4
		published = 40
		queue     = "concurrency-once-queue"
	)
	ctx := context.Background()
	received := make(chan string, published*4)

	consumer := newConsumer[concurrencyExchange](t, queue,
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(workers * 2).WithConcurrency(workers)
		})
	require.NoError(t, Subscribe(ctx, consumer, exactlyOnceEvent{},
		func(_ context.Context, msg exactlyOnceEvent) error {
			received <- msg.ID
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range published {
		require.NoError(t, producer.Publish(ctx, exactlyOnceEvent{ID: fmt.Sprintf("once-%d", i)}))
	}

	requireExactlyOnce(t, received, expectedIDs("once", published))
	requireQueueDrains(t, queue)
}

// Cancelling the consume context stops the pool and nothing else: reporting it
// as a consume failure would redial a connection that was never lost.
func TestConcurrencyContextCancelStopsConsumingAndKeepsTheConnection(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "concurrency-cancel-queue"
	logger := &recordingLogger{}
	received := make(chan string, 8)
	arrived := make(chan string, 8)
	var cancelled atomic.Bool

	consumer := newConsumer[concurrencyExchange](t, queue,
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(4).WithConcurrency(2).
				WithReconnectDelay(testReconnectWait).
				WithLogger(logger)
		})
	require.NoError(t, Subscribe(context.Background(), consumer, cancelledEvent{},
		func(_ context.Context, msg cancelledEvent) error {
			if cancelled.Load() {
				arrived <- msg.ID
			}
			received <- msg.ID
			return nil
		}))

	// The message left unconsumed on purpose outlives the run in a durable
	// queue, and a later run would take it for one of its own.
	purgeQueue(t, queue)

	consumeCtx, stopConsuming := context.WithCancel(context.Background())
	consumer.Start(consumeCtx)

	producer := newConcurrencyProducer(t)
	require.NoError(t, producer.Publish(context.Background(), cancelledEvent{ID: "cancel-0"}))
	waitForArrivals(t, received, 1)

	stopConsuming()
	requireQueueEventually(t, queue, func(q amqp091.Queue) bool { return q.Consumers == 0 },
		"the consumer to be cancelled at the broker")
	cancelled.Store(true)

	require.NoError(t, producer.Publish(context.Background(), cancelledEvent{ID: "cancel-1"}))
	select {
	case id := <-arrived:
		t.Fatalf("%q reached a handler after the consume context was cancelled", id)
	case <-time.After(poolSettle):
	}

	require.True(t, consumer.Connected(), "cancelling the consume context must not drop the connection")
	require.Equal(t, 1, logger.count(connectedLogLine),
		"the client redialled a connection that was never lost: a shutdown was reported as a consume failure")
}

func TestConcurrencyCloseFinishesTheDeliveryInFlight(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "concurrency-midflight-queue"
	ctx := context.Background()
	arrived := make(chan string, 4)
	held := make(chan struct{})

	consumer := newConsumer[concurrencyExchange](t, queue,
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(2).WithConcurrency(2)
		})

	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)

	require.NoError(t, Subscribe(ctx, consumer, midFlightEvent{},
		func(_ context.Context, msg midFlightEvent) error {
			arrived <- msg.ID
			<-held
			return nil
		}))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	require.NoError(t, producer.Publish(ctx, midFlightEvent{ID: "midflight-0"}))
	waitForArrivals(t, arrived, 1)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		consumer.Close()
	}()

	// Releasing only once Close is under way is what puts the ack after the
	// cancel it has to survive.
	require.Eventually(t, consumer.closed, handlerArrival, 10*time.Millisecond, "Close never started")
	release()

	select {
	case <-closed:
	case <-time.After(closeDrainTimeout):
		t.Fatal("Close did not return once the handler finished")
	}

	requireQueueDrains(t, queue)
}

// The pool serves every subscription of one client, so a partition by key can
// keep order across the message types that share that key.
func TestConcurrencyOnePoolServesEverySubscription(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		workers   = 2
		perType   = 4
		published = perType * 2
	)
	ctx := context.Background()
	tracker := &inFlightTracker{}
	arrived := make(chan string, published*2)
	handled := make(chan string, published*2)
	held := make(chan struct{})

	consumer := newConsumer[concurrencyExchange](t, "concurrency-shared-pool-queue",
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithPrefetchCount(published).WithConcurrency(workers)
		})

	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)

	hold := func(id string) error {
		tracker.enter()
		defer tracker.leave()

		arrived <- id
		<-held
		handled <- id
		return nil
	}

	require.NoError(t, Subscribe(ctx, consumer, sharedPoolEvent{},
		func(_ context.Context, msg sharedPoolEvent) error { return hold(msg.ID) }))
	require.NoError(t, Subscribe(ctx, consumer, sharedPoolPeerEvent{},
		func(_ context.Context, msg sharedPoolPeerEvent) error { return hold(msg.ID) }))
	consumer.Start(ctx)

	producer := newConcurrencyProducer(t)
	for i := range perType {
		require.NoError(t, producer.Publish(ctx, sharedPoolEvent{ID: fmt.Sprintf("shared-%d", i)}))
		require.NoError(t, producer.Publish(ctx, sharedPoolPeerEvent{ID: fmt.Sprintf("peer-%d", i)}))
	}

	waitForArrivals(t, arrived, workers)

	requireNeverExceeds(t, tracker, workers, poolSettle,
		"the pool grew with the subscriptions instead of belonging to the client")
	release()

	expected := expectedIDs("shared", perType)
	for id := range expectedIDs("peer", perType) {
		expected[id] = struct{}{}
	}
	requireAllHandled(t, handled, expected)

	require.EqualValues(t, workers, tracker.peak(),
		"the pool held %d messages at once serving two subscriptions with a concurrency of %d", tracker.peak(), workers)
}

// A shutdown closes the delivery channel on purpose: read as an outage, every
// clean stop would log an error and mark a connection nobody lost as gone.
func TestConcurrencyCloseIsNotAConsumeFailure(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	logger := &recordingLogger{}

	consumer := newConsumer[concurrencyExchange](t, "concurrency-shutdown-queue",
		func(s *Setup[concurrencyExchange]) *Setup[concurrencyExchange] {
			return s.WithLogger(logger)
		})
	require.NoError(t, Subscribe(ctx, consumer, shutdownEvent{},
		func(context.Context, shutdownEvent) error { return nil }))
	consumer.Start(ctx)

	require.Eventually(t, func() bool { return logger.count(consumingLogLine) >= 1 }, handlerArrival, 20*time.Millisecond,
		"the consumer never started consuming, so closing it proves nothing")

	consumer.Close()

	require.Zero(t, logger.count(consumeErrLogLine),
		"the delivery channel closed by a shutdown was read as an outage: every clean stop logs an error and marks the client disconnected")
}
