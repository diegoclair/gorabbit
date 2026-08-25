package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/stretchr/testify/require"
)

type auditLogger struct{ t *testing.T }

func (l auditLogger) Debug(_ context.Context, msg string, kv ...any) { l.t.Log("DEBUG", msg, kv) }
func (l auditLogger) Info(_ context.Context, msg string, kv ...any)  { l.t.Log("INFO", msg, kv) }
func (l auditLogger) Warn(_ context.Context, msg string, kv ...any)  { l.t.Log("WARN", msg, kv) }
func (l auditLogger) Error(_ context.Context, msg string, kv ...any) { l.t.Log("ERROR", msg, kv) }

var _ gorabbit.Logger = auditLogger{}

// A listener that accepts and never speaks AMQP black-holes every dial for the
// whole timeout; counting there is what tells one dial from many.
func silentListener(t *testing.T) (url string, dials func() int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	var accepted atomic.Int32
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	return "amqp://guest:guest@" + l.Addr().String() + "/", func() int { return int(accepted.Load()) }
}

func TestAuditConcurrentPublishesSerializeBehindOneDial(t *testing.T) {
	const (
		dialTimeout = 300 * time.Millisecond
		publishers  = 4
	)
	ctx := context.Background()
	url, _ := silentListener(t)
	c := newTestClient(NewSetup[ordersExchange](url, "app").WithDialTimeout(dialTimeout))
	t.Cleanup(c.Close)

	start := time.Now()
	var wg sync.WaitGroup
	for i := range publishers {
		wg.Go(func() {
			require.NoError(t, c.Publish(ctx, orderCreated{OrderID: string(rune('a' + i))}))
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("%d concurrent publishes against a dead broker took %s (dial timeout %s)", publishers, elapsed, dialTimeout)

	require.Less(t, elapsed, 2*dialTimeout,
		"every Publish redials under c.mu, so N callers wait N dial timeouts instead of one")
}

func TestAuditConcurrentPublishesShareASingleDial(t *testing.T) {
	const (
		dialTimeout = 300 * time.Millisecond
		publishers  = 6
	)
	ctx := context.Background()
	url, dials := silentListener(t)
	c := newTestClient(NewSetup[ordersExchange](url, "app").WithDialTimeout(dialTimeout))
	t.Cleanup(c.Close)

	var wg sync.WaitGroup
	for i := range publishers {
		wg.Go(func() {
			require.NoError(t, c.Publish(ctx, orderCreated{OrderID: string(rune('a' + i))}))
		})
	}
	wg.Wait()

	t.Logf("%d concurrent publishes against a dead broker opened %d connections", publishers, dials())
	require.LessOrEqual(t, dials(), 1,
		"each Publish dials on its own, so a broker that is down gets hammered by every caller at once")
}

func TestAuditCloseStopsEveryBackgroundGoroutine(t *testing.T) {
	ctx := context.Background()
	before := runtime.NumGoroutine()

	c, err := NewSetup[ordersExchange](unreachableURL, "app").
		WithConsumer("app-queue").
		WithDialTimeout(100 * time.Millisecond).
		WithReconnectDelay(50 * time.Millisecond).
		Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	require.NoError(t, RegisterHandler(ctx, c, orderCreated{}, func(context.Context, orderCreated) error { return nil }))
	c.Start(ctx)
	require.NoError(t, c.Publish(ctx, orderCreated{OrderID: "1"}))
	time.Sleep(200 * time.Millisecond)

	c.Close()
	c.Close()

	// Eventually runs its condition on a goroutine of its own, so poll by hand.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutines leaked after Close")
	require.ErrorIs(t, c.Publish(ctx, orderCreated{OrderID: "after-close"}), ErrClientClosed)
	require.False(t, c.Connected())
}

func TestAuditRegisterHandlerTwiceForSameRoutingKeyIsRejected(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))

	first := func(context.Context, orderCreated) error { return nil }
	second := func(context.Context, orderCreated) error { return nil }
	require.NoError(t, RegisterHandler(ctx, c, orderCreated{}, first))

	err := RegisterHandler(ctx, c, orderCreated{}, second)
	require.Error(t, err, "a second handler for the same (exchange, type name) silently replaces the first")
}

func TestAuditPublishRejectsAMessageWithoutATypeName(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithDialTimeout(100 * time.Millisecond))

	anonymous := struct {
		orders
		ID string `json:"id"`
	}{ID: "1"}

	pm, err := c.getPublishMessage(ctx, anonymous)
	require.NoError(t, err)
	t.Logf("routing key for an anonymous struct: %q", pm.MsgTypeName)

	require.Error(t, c.Publish(ctx, anonymous),
		"an empty routing key can never match a binding, so the broker drops it and Publish still reports success")
}

// The entry must be identified by the message, never by the instant it was
// cached: a clock collides, and a re-cached message is still one message.
func TestAuditCacheKeySurvivesTwoMessagesInTheSameNanosecond(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	first, err := c.getPublishMessage(ctx, orderCreated{OrderID: "first"})
	require.NoError(t, err)
	second, err := c.getPublishMessage(ctx, orderCreated{OrderID: "second"})
	require.NoError(t, err)

	require.NoError(t, c.cacheMessage(ctx, first))
	require.NoError(t, c.cacheMessage(ctx, first))
	require.NoError(t, c.cacheMessage(ctx, second))

	keys, err := c.cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 2, "the key carries the caching instant, so one message occupies as many entries as it was cached times")

	require.Contains(t, keys, cacheKey("app", first.MsgID))
	require.Contains(t, keys, cacheKey("app", second.MsgID))
}

// The monitor redialing into the same rejection is what a Publish meets here:
// the answer is the rejection, not a cached message nobody will ever see.
func TestAuditPublishReportsATopologyRejectionWhileTheMonitorRedials(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	c.setupErr = fmt.Errorf("%w: %w", ErrTopologyRejected, errors.New("406 PRECONDITION_FAILED"))
	require.True(t, c.dialing.CompareAndSwap(false, true), "no dial may start, so the answer can only come from the pending one")

	require.ErrorIs(t, c.Publish(ctx, orderCreated{OrderID: "1"}), ErrTopologyRejected)

	keys, err := c.cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Empty(t, keys, "the message was cached with a 24h ttl behind an error only a deploy fixes")
}
