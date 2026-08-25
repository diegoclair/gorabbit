package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// One exchange per test: every consumer binds the type name as routing key,
// so a shared exchange would leak messages between the audit tests.
type auditFlushExchange struct{}

func (auditFlushExchange) Name() string { return "audit-flush-events" }

type auditFlushEvent struct {
	gorabbit.Msg[auditFlushExchange]
	ID string `json:"id"`
}

type auditCloseExchange struct{}

func (auditCloseExchange) Name() string { return "audit-close-events" }

type auditCloseEvent struct {
	gorabbit.Msg[auditCloseExchange]
	ID string `json:"id"`
}

type auditCancelExchange struct{}

func (auditCancelExchange) Name() string { return "audit-cancel-events" }

type auditCancelEvent struct {
	gorabbit.Msg[auditCancelExchange]
	ID string `json:"id"`
}

type auditDrainExchange struct{}

func (auditDrainExchange) Name() string { return "audit-drain-events" }

type auditDrainEvent struct {
	gorabbit.Msg[auditDrainExchange]
	ID string `json:"id"`
}

type auditHammerExchange struct{}

func (auditHammerExchange) Name() string { return "audit-hammer-events" }

type auditHammerEvent struct {
	gorabbit.Msg[auditHammerExchange]
	ID string `json:"id"`
}

type auditCredentialsExchange struct{}

func (auditCredentialsExchange) Name() string { return "audit-credentials-events" }

type auditCredentialsEvent struct {
	gorabbit.Msg[auditCredentialsExchange]
	ID string `json:"id"`
}

type auditRouteExchange struct{}

func (auditRouteExchange) Name() string { return "audit-route-events" }

type auditRouteEvent struct {
	gorabbit.Msg[auditRouteExchange]
	ID string `json:"id"`
}

// Long enough that a local broker would have delivered whatever it was going
// to deliver, which is what makes the absence of a message an assertion.
const absenceWindow = time.Second

func collectIDs[T gorabbit.Message](received chan<- string, id func(T) string) gorabbit.Handler[T] {
	return func(_ context.Context, msg T) error {
		received <- id(msg)
		return nil
	}
}

func requireEachOnce(t *testing.T, received <-chan string, expected map[string]struct{}, settle time.Duration) {
	t.Helper()

	counts := make(map[string]int)
	deadline := time.After(30 * time.Second)
	for len(counts) < len(expected) {
		select {
		case id := <-received:
			counts[id]++
		case <-deadline:
			t.Fatalf("timed out: got %d of %d distinct messages", len(counts), len(expected))
		}
	}

	timer := time.After(settle)
	for {
		select {
		case id := <-received:
			counts[id]++
		case <-timer:
			dupes := 0
			for id, n := range counts {
				if n != 1 {
					dupes++
					t.Logf("message %q delivered %d times", id, n)
				}
			}
			require.Zero(t, dupes, "%d of %d messages were delivered more than once", dupes, len(expected))
			return
		}
	}
}

func TestAuditConcurrentFlushesPublishTheSameCachedMessageTwice(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		cached  = 100
		flushes = 3
	)
	ctx := context.Background()
	received := make(chan string, cached*flushes*2)

	c := newConsumer[auditFlushExchange](t, "audit-flush-queue")
	require.NoError(t, RegisterHandler(ctx, c, auditFlushEvent{}, collectIDs(received, func(m auditFlushEvent) string { return m.ID })))
	c.Start(ctx)

	expected := make(map[string]struct{}, cached)
	for i := range cached {
		id := fmt.Sprintf("flush-%d", i)
		expected[id] = struct{}{}
		pm, err := c.getPublishMessage(ctx, auditFlushEvent{ID: id})
		require.NoError(t, err)
		require.NoError(t, c.cacheMessage(ctx, pm))
	}

	var wg sync.WaitGroup
	for range flushes {
		wg.Go(func() { c.flushCachedMessages(ctx) })
	}
	wg.Wait()

	requireEachOnce(t, received, expected, duplicateSettle)
}

func TestAuditPublishAfterCloseDoesNotReopenAConnection(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	c, err := NewSetup[auditCloseExchange](brokerURL, "audit-close-app").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	require.True(t, c.Connected())

	c.Close()
	require.False(t, c.Connected())
	idle := runtime.NumGoroutine()

	err = c.Publish(ctx, auditCloseEvent{ID: "after-close"})
	t.Logf("Publish after Close returned err=%v, Connected()=%v, goroutines %d -> %d", err, c.Connected(), idle, runtime.NumGoroutine())

	require.False(t, c.Connected(), "Close is idempotent, so the connection Publish just dialled is never closed by anyone")
}

func TestAuditCancellingStartContextDoesNotDropConnectedState(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "audit-cancel-queue"
	// A message left by an earlier run would pass for the one published inside
	// the window below.
	deleteQueue(t, queue)

	ctx, cancel := context.WithCancel(context.Background())
	c := newConsumer[auditCancelExchange](t, queue, func(s *Setup[auditCancelExchange]) *Setup[auditCancelExchange] {
		// No redial may run during the window, or it would hide the drop by
		// healing it.
		return s.WithReconnectDelay(30 * time.Second).WithLogger(auditLogger{t})
	})
	require.NoError(t, RegisterHandler(ctx, c, auditCancelEvent{}, func(context.Context, auditCancelEvent) error { return nil }))
	c.Start(ctx)
	require.True(t, waitQueueConsumers(t, queue, 1, 10*time.Second), "the consumer never registered on the broker")

	cancel()
	require.True(t, waitQueueConsumers(t, queue, 0, 10*time.Second),
		"the cancelled context left the consumer registered, so the broker keeps handing deliveries to a loop that is gone")

	// A second client, because the pre-fix loop only notices the cancellation
	// on a delivery it has already pulled.
	producer, err := NewSetup[auditCancelExchange](brokerURL, "audit-cancel-producer").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, producer.Publish(context.Background(), auditCancelEvent{ID: "after-cancel"}))

	require.Never(t, func() bool { return !c.Connected() }, absenceWindow, 2*time.Millisecond,
		"the connection is alive, yet Connected() flips false and the monitor tears it down and redials")

	_, delivered := consumeWithin(t, queue, 5*time.Second)
	require.True(t, delivered, "the cancelled consumer pulled the message and left it unacked")

	// A caller's cancelled context is not a dead socket.
	require.NoError(t, c.Publish(ctx, auditCancelEvent{ID: "cancelled-ctx"}))
	require.True(t, c.Connected())
}

// The broker's own count: once the consume loop is gone, nothing on the client
// tells whether the broker still has a consumer to deliver to.
func waitQueueConsumers(t *testing.T, queue string, want int, timeout time.Duration) bool {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(timeout)
	for {
		ch, err := conn.Channel()
		require.NoError(t, err)
		state, err := ch.QueueInspect(queue)
		require.NoError(t, err)
		_ = ch.Close()

		if state.Consumers == want {
			return true
		}
		if time.Now().After(deadline) {
			t.Logf("queue %q has %d consumers, want %d", queue, state.Consumers, want)
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Close must return as soon as the handler does, well before closeDrainTimeout.
const closeDrainBound = 10 * time.Second

func TestAuditCloseDrainsTheInFlightHandler(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var finished atomic.Bool

	c := newConsumer[auditDrainExchange](t, "audit-drain-queue")
	require.NoError(t, RegisterHandler(ctx, c, auditDrainEvent{}, func(context.Context, auditDrainEvent) error {
		entered <- struct{}{}
		<-release
		finished.Store(true)
		return nil
	}))
	c.Start(ctx)
	require.NoError(t, c.Publish(ctx, auditDrainEvent{ID: "in-flight"}))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran")
	}

	go func() {
		time.Sleep(250 * time.Millisecond)
		close(release)
	}()
	start := time.Now()
	c.Close()
	closeTook := time.Since(start)
	closedWhileRunning := !finished.Load()
	<-release

	back, redelivered := consumeWithin(t, "audit-drain-queue", absenceWindow)
	t.Logf("Close took %s and returned while the handler was running: %v; back on the queue=%v redelivered=%v", closeTook, closedWhileRunning, redelivered, back.Redelivered)

	require.False(t, closedWhileRunning, "Close returned mid-handler; the Ack then fails and the same message is redelivered")
	require.False(t, redelivered, "the in-flight message came back: its ack did not survive Close")
	require.Less(t, closeTook, closeDrainBound, "Close waited for the drain timeout instead of the handler: the consumer was never cancelled")
}

// An empty queue is the expected outcome here, so a timeout reports absence
// instead of failing the test the way consumeOne does.
func consumeWithin(t *testing.T, queue string, timeout time.Duration) (amqp091.Delivery, bool) {
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
		return msg, true
	case <-time.After(timeout):
		return amqp091.Delivery{}, false
	}
}

// The retry/dlq republish shares the consumer channel with QueueBind; amqp091
// locks per frame, so a bind can land between the frames of a publish.
func TestAuditBindOnConsumerChannelWhileDeadLettering(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const (
		messages = 100
		hammer   = 3 * time.Second
	)
	ctx := context.Background()
	var attempts atomic.Int32
	var dropped atomic.Int32
	errs := &distinctErrors{seen: map[string]int{}}
	bindErrs := &distinctErrors{seen: map[string]int{}}
	var binds atomic.Int64

	c := newConsumer[auditHammerExchange](t, "audit-hammer-queue", func(s *Setup[auditHammerExchange]) *Setup[auditHammerExchange] {
		return s.WithRetry(1, 50*time.Millisecond, nil).WithPrefetchCount(50).WithLogger(errs)
	})
	require.NoError(t, RegisterHandler(ctx, c, auditHammerEvent{}, func(context.Context, auditHammerEvent) error {
		attempts.Add(1)
		return errors.New("always fails")
	}))
	c.Start(ctx)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Several goroutines, because one alone never puts two rpcs in flight on the
	// consumer channel — the interleaving the serialization exists to prevent.
	for g := range 4 {
		wg.Go(func() {
			exchange := auditHammerExchange{}.Name()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("hammer-%d-%d", g, i)
				bindErrs.record(c.withConsumerChannel(func(ch *amqp091.Channel) error {
					return ch.QueueBind("audit-hammer-queue", key, exchange, false, nil)
				}))
				bindErrs.record(c.withConsumerChannel(func(ch *amqp091.Channel) error {
					return ch.QueueUnbind("audit-hammer-queue", key, exchange, nil)
				}))
				binds.Add(1)
			}
		})
	}
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				if !c.Connected() {
					dropped.Add(1)
				}
			}
		}
	})

	for i := range messages {
		require.NoError(t, c.Publish(ctx, auditHammerEvent{ID: fmt.Sprintf("h-%d", i)}))
	}
	time.Sleep(hammer)
	close(stop)
	wg.Wait()

	pending := waitQueuesDrained(t, []string{"audit-hammer-queue", "audit-hammer-queue.retry"}, 30*time.Second)
	inDLQ := countQueue(t, "audit-hammer-queue.dlq", messages, 10*time.Second)

	t.Logf("attempts=%d dlq=%d/%d still-queued=%d bind-rpcs=%d connection-drop-ticks=%d",
		attempts.Load(), inDLQ, messages, pending, binds.Load(), dropped.Load())
	for msg, n := range bindErrs.snapshot() {
		t.Logf("bind rpc failed %dx: %s", n, msg)
	}
	for msg, n := range errs.snapshot() {
		if !strings.Contains(msg, "max retry count reached") {
			t.Logf("logged %dx: %s", n, msg)
		}
	}
	require.Empty(t, bindErrs.snapshot(), "binds on the consumer channel failed: an rpc took another's reply")
	require.Zero(t, dropped.Load(), "the connection dropped while binds shared the consumer channel with the retry republish")
	require.Zero(t, pending, "queues did not drain, cannot tell loss from slowness")
	require.Equal(t, messages, inDLQ, "%d messages were acked and never reached the dlq", messages-inDLQ)
}

type distinctErrors struct {
	mu   sync.Mutex
	seen map[string]int
}

func (d *distinctErrors) Debug(context.Context, string, ...any) {}
func (d *distinctErrors) Info(context.Context, string, ...any)  {}
func (d *distinctErrors) Warn(context.Context, string, ...any)  {}
func (d *distinctErrors) Error(_ context.Context, msg string, kv ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[fmt.Sprint(msg, kv)]++
}

func (d *distinctErrors) record(err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[err.Error()]++
}

func (d *distinctErrors) snapshot() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]int, len(d.seen))
	for k, v := range d.seen {
		out[k] = v
	}
	return out
}

func countQueue(t *testing.T, queue string, want int, timeout time.Duration) int {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := make(map[string]int)
	deadline := time.After(timeout)
	for len(seen) < want {
		select {
		case d := <-deliveries:
			seen[d.MessageId]++
		case <-deadline:
			return len(seen)
		}
	}
	return len(seen)
}

func waitQueuesDrained(t *testing.T, queues []string, timeout time.Duration) int {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	deadline := time.Now().Add(timeout)
	for {
		ch, err := conn.Channel()
		require.NoError(t, err)
		pending := 0
		for _, q := range queues {
			state, err := ch.QueueInspect(q)
			require.NoError(t, err)
			pending += state.Messages
		}
		_ = ch.Close()
		if pending == 0 || time.Now().After(deadline) {
			return pending
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestAuditPublishToAnExchangeWithNoBindingIsSilentlyDropped(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	// The claim only holds while nothing is bound, and a queue left over by an
	// earlier run of this test is a binding.
	deleteQueue(t, "audit-route-queue")
	p, err := NewSetup[auditRouteExchange](brokerURL, "audit-route-app").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(p.Close)

	require.NoError(t, p.Publish(ctx, auditRouteEvent{ID: "unroutable"}))

	c := newConsumer[auditRouteExchange](t, "audit-route-queue")
	received := make(chan string, 1)
	require.NoError(t, RegisterHandler(ctx, c, auditRouteEvent{}, collectIDs(received, func(m auditRouteEvent) string { return m.ID })))
	c.Start(ctx)

	select {
	case id := <-received:
		t.Fatalf("message %q published before any binding existed should have been dropped", id)
	case <-time.After(absenceWindow):
	}
}

func deleteQueue(t *testing.T, queue string) {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)

	// Absent is the state being asked for, so a queue that never existed is fine.
	_, _ = ch.QueueDelete(queue, false, false, false)
}

type auditTopologyExchange struct{}

func (auditTopologyExchange) Name() string { return "audit-topology-events" }

type auditTopologyEvent struct {
	gorabbit.Msg[auditTopologyExchange]
	ID string `json:"id"`
}

// A retry interval is baked into the queue as x-message-ttl, so a deploy that
// changes it can never declare the queue again.
func TestAuditChangedRetryIntervalIsSurfacedInsteadOfCachingForever(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	// A retry queue left behind by an interrupted run carries the ttl of that
	// run, which decides which of the two clients below is refused.
	deleteQueue(t, "audit-topology-queue.retry")

	old := newConsumer[auditTopologyExchange](t, "audit-topology-queue", func(s *Setup[auditTopologyExchange]) *Setup[auditTopologyExchange] {
		return s.WithRetry(1, time.Second, nil)
	})
	require.True(t, old.Connected())
	old.Close()

	errs := &distinctErrors{seen: map[string]int{}}
	fresh, err := NewSetup[auditTopologyExchange](brokerURL, "audit-topology-queue").
		WithConsumer("audit-topology-queue").
		WithRetry(1, 2*time.Second, nil).
		WithReconnectDelay(200 * time.Millisecond).
		WithLogger(errs).
		Connect(gorabbit.NewMemoryCache())

	for msg, n := range errs.snapshot() {
		t.Logf("logged %dx: %s", n, msg)
	}
	t.Logf("Connect err=%v client=%v", err, fresh)

	require.ErrorIs(t, err, ErrTopologyRejected,
		"the client starts disconnected forever, every publish is cached with a 24h ttl, and nothing tells the caller")
	require.Nil(t, fresh, "a client that can never declare its topology must not be handed out")
}

// A refused credential answers 403, the same code a refused declaration uses,
// and no deploy of this application can fix a password the broker rejects.
func TestAuditWrongCredentialsStartTheClientDisconnectedInsteadOfFailing(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()
	const app = "audit-credentials-app"

	c, err := NewSetup[auditCredentialsExchange](strings.Replace(brokerURL, "guest:guest@", "guest:wrong-password@", 1), app).
		WithReconnectDelay(30 * time.Second).
		WithLogger(auditLogger{t}).
		Connect(cache)
	require.NoError(t, err, "a credential the broker refuses is an outage: the client must keep retrying, not die at boot")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
	require.False(t, c.Connected())

	require.NoError(t, c.Publish(ctx, auditCredentialsEvent{ID: "cached"}))

	keys, err := cache.GetAllKeys(ctx, cacheKey(app, "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 1, "the publish was answered with the rejection instead of being cached until the broker is reachable")
}

// The same rejection met on a redial, after the client has been publishing:
// there is no boot left to fail, so Publish is what must report it.
func TestAuditTopologyRejectedOnARedialIsSurfacedInsteadOfCaching(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "audit-topology-redial-queue"
	ctx := context.Background()
	deleteQueue(t, queue+".retry")

	c := newConsumer[auditTopologyExchange](t, queue, func(s *Setup[auditTopologyExchange]) *Setup[auditTopologyExchange] {
		// Long enough that the monitor never redials during the test: the
		// assertions are about the dial Publish itself drives.
		return s.WithRetry(1, time.Second, nil).WithReconnectDelay(time.Hour).WithLogger(auditLogger{t})
	})
	require.True(t, c.Connected())
	require.NoError(t, c.Publish(ctx, auditTopologyEvent{ID: "before-the-change"}))

	redeclareRetryQueue(t, queue, 5*time.Second)
	dropLiveConnection(t, c)

	err := c.Publish(ctx, auditTopologyEvent{ID: "after-the-change"})
	t.Logf("Publish after the redial returned err=%v Connected()=%v", err, c.Connected())
	require.ErrorIs(t, err, ErrTopologyRejected,
		"the redial meets a topology the broker refuses on every attempt, yet the message is cached with a 24h ttl")
	require.False(t, c.Connected())

	deleteQueue(t, queue+".retry")
	require.Eventually(t, func() bool { return c.Publish(ctx, auditTopologyEvent{ID: "after-the-fix"}) == nil },
		10*time.Second, 200*time.Millisecond, "the client never healed once the topology matched again")
	require.True(t, c.Connected())
}

// Healing is the monitor's job: an application that only consumes never calls
// Publish, so nothing else would ever retry the rejected declaration.
func TestAuditTopologyRejectionHealsFromTheMonitorAlone(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const queue = "audit-topology-heal-queue"
	ctx := context.Background()
	deleteQueue(t, queue+".retry")

	c := newConsumer[auditTopologyExchange](t, queue, func(s *Setup[auditTopologyExchange]) *Setup[auditTopologyExchange] {
		return s.WithRetry(1, time.Second, nil).WithReconnectDelay(200 * time.Millisecond).WithLogger(auditLogger{t})
	})
	require.True(t, c.Connected())

	redeclareRetryQueue(t, queue, 5*time.Second)
	dropLiveConnection(t, c)

	require.Never(t, c.Connected, absenceWindow, 50*time.Millisecond,
		"the monitor reconnected onto a topology the broker refuses")

	deleteQueue(t, queue+".retry")
	require.Eventually(t, c.Connected, 15*time.Second, 200*time.Millisecond,
		"the monitor gave up on the rejection, so the client stays disconnected forever once the deploy fixes it")

	require.NoError(t, c.Publish(ctx, auditTopologyEvent{ID: "after-the-heal"}))
}

// redeclareRetryQueue rebuilds the retry queue with a ttl the client will not
// declare, which is what a changed WithRetry interval leaves on the broker.
func redeclareRetryQueue(t *testing.T, queue string, ttl time.Duration) {
	t.Helper()

	conn, err := amqp091.Dial(brokerURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDelete(queue+".retry", false, false, false)
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queue+".retry", true, false, false, false, amqp091.Table{
		"x-dead-letter-exchange": queue,
		"x-message-ttl":          int(ttl.Milliseconds()),
	})
	require.NoError(t, err)
}

// A broker-side close is what a redial normally follows; forcing it is the only
// way to reach the redial without stopping the broker the other tests share.
func dropLiveConnection[E gorabbit.Exchange](t *testing.T, c *Client[E]) {
	t.Helper()

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	require.NotNil(t, conn)
	require.NoError(t, conn.Close())
	require.False(t, c.Connected())
}
