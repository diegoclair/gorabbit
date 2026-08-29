package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/stretchr/testify/require"
)

type leaseExchange struct{}

func (leaseExchange) Name() string { return "lease-events" }

type lease = gorabbit.Msg[leaseExchange]

type replayedEvent struct {
	lease
	Seq int `json:"seq"`
}

// The flush treats everything under the message prefix as a message, so a lease
// that the glob reaches would be published as one.
func TestPublishLeaseIsOutOfReachOfTheFlushGlob(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))
	scope := c.cacheScope()

	require.NoError(t, c.cache.Set(ctx, leaseKey(scope, "0198b7ff"), []byte("owner"), time.Minute))
	require.NoError(t, c.cache.Set(ctx, cacheKey(scope, "0198b800"), []byte(`{"MsgID":"0198b800"}`), 0))

	keys, err := c.cachedMessageKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{cacheKey(scope, "0198b800")}, keys,
		"the flush glob must reach the cached message and nothing else")

	// The harness reads the same prefix from Redis to count what is cached.
	probed, err := c.cache.GetAllKeys(ctx, fmt.Sprintf("%s:%s:*", cachedMessagePrefix, scope))
	require.NoError(t, err)
	require.Len(t, probed, 1)
}

func TestPublishLeaseOutlivesAPublishAttempt(t *testing.T) {
	short := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithPublishConfirmTimeout(time.Millisecond))
	long := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithPublishConfirmTimeout(time.Minute))

	for _, c := range []*Client[ordersExchange]{short, long} {
		require.Greater(t, c.publishLeaseTTL(), c.publishAttemptTimeout(),
			"a lease expiring mid-publish puts two processes on the same message")
		require.Greater(t, c.publishAttemptTimeout(), c.setup.confirmTimeout,
			"an attempt cut before its own confirm would be abandoned as failed")
	}

	require.Equal(t, minPublishLeaseTTL, short.publishLeaseTTL())
}

func TestReservationIsExclusiveAndReturnedOnFailure(t *testing.T) {
	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()
	first := newClient(NewSetup[ordersExchange](unreachableURL, "app"), cache)
	second := newClient(NewSetup[ordersExchange](unreachableURL, "app"), cache)

	require.True(t, first.reserveCachedMessage(ctx, "0198b7ff"))
	require.False(t, second.reserveCachedMessage(ctx, "0198b7ff"))

	first.releaseCachedMessage(ctx, "0198b7ff")
	require.True(t, second.reserveCachedMessage(ctx, "0198b7ff"))
}

// Two clients on one cache scope are two replicas of one application: the
// harness measured them draining each other's cache and delivering every
// message twice.
func TestIntegrationTwoClientsSharingACacheFlushEachMessageOnce(t *testing.T) {
	skipWithoutBroker(t)
	t.Parallel()

	const total = 300

	ctx := context.Background()
	var mu sync.Mutex
	seen := map[int]int{}

	consumer := newConsumer[leaseExchange](t, "lease-queue")
	require.NoError(t, Subscribe(ctx, consumer, replayedEvent{},
		func(_ context.Context, msg replayedEvent) error {
			mu.Lock()
			defer mu.Unlock()
			seen[msg.Seq]++
			return nil
		}))
	consumer.Start(ctx)

	deliveries := func() (unique, extra int) {
		mu.Lock()
		defer mu.Unlock()
		for _, count := range seen {
			unique++
			extra += count - 1
		}
		return unique, extra
	}

	// newClient, not Connect: the in-process guard refuses a second client on
	// one cache key, which is what two separate processes get around.
	cache := gorabbit.NewMemoryCache()
	first := newClient(NewSetup[leaseExchange](brokerURL, "lease-replica"), cache)
	second := newClient(NewSetup[leaseExchange](brokerURL, "lease-replica"), cache)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	for _, c := range []*Client[leaseExchange]{first, second} {
		established, err := c.connect(ctx)
		require.NoError(t, err)
		require.True(t, established)
	}

	for seq := range total {
		pm, err := first.getPublishMessage(ctx, replayedEvent{Seq: seq})
		require.NoError(t, err)
		require.NoError(t, first.cacheMessage(ctx, pm))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, c := range []*Client[leaseExchange]{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c.flushCachedMessages(ctx)
		}()
	}
	close(start)
	wg.Wait()

	left, err := first.cachedMessageKeys(ctx)
	require.NoError(t, err)
	require.Empty(t, left, "every cached message must have been published by exactly one of the two")

	require.Eventually(t, func() bool {
		unique, _ := deliveries()
		return unique == total
	}, 60*time.Second, 100*time.Millisecond, "not every cached message reached the queue")

	// The broker confirmed every publish before the flushes returned, so a
	// duplicate is already on its way and this window is enough to catch it.
	require.Never(t, func() bool {
		_, extra := deliveries()
		return extra > 0
	}, 3*time.Second, 100*time.Millisecond, "a message was published by both clients")
}
