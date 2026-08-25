package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type outageExchange struct{}

func (outageExchange) Name() string { return "outage-events" }

type outage = gorabbit.Msg[outageExchange]

type outageEvent struct {
	outage
	ID string `json:"id"`
}

const (
	reconnectWait     = 60 * time.Second
	duplicateSettle   = 3 * time.Second
	testDialTimeout   = time.Second
	testReconnectWait = 200 * time.Millisecond
)

// restartableBroker keeps the container handle so a test can take the broker
// down and bring it back on the same address the client already dialled.
func restartableBroker(t *testing.T, port int) *testcontainers.DockerContainer {
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

	return broker
}

func brokerURLAt(port int) string {
	return fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", port)
}

func stopBroker(t *testing.T, broker *testcontainers.DockerContainer) {
	t.Helper()
	timeout := 10 * time.Second
	require.NoError(t, broker.Stop(context.Background(), &timeout))
}

// The log wait strategy would match the previous run's output, so readiness is
// proven by a successful AMQP handshake instead.
func startBroker(t *testing.T, broker *testcontainers.DockerContainer, port int) {
	t.Helper()
	require.NoError(t, broker.Start(context.Background()))
	require.Eventually(t, func() bool {
		conn, err := amqp091.DialConfig(brokerURLAt(port), amqp091.Config{Dial: amqp091.DefaultDial(testDialTimeout)})
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, reconnectWait, 200*time.Millisecond, "broker did not come back")
}

func newOutageClient(t *testing.T, port int, consumer bool, opts ...func(*Setup[outageExchange]) *Setup[outageExchange]) *Client[outageExchange] {
	t.Helper()

	setup := NewSetup[outageExchange](brokerURLAt(port), "outage-app").
		WithDialTimeout(testDialTimeout).
		WithReconnectDelay(testReconnectWait)
	if consumer {
		setup = setup.WithConsumer("outage-queue")
	}
	for _, opt := range opts {
		setup = opt(setup)
	}

	c, err := setup.Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func registerCollector(t *testing.T, c *Client[outageExchange], received chan<- string) {
	t.Helper()
	require.NoError(t, RegisterHandler(context.Background(), c, outageEvent{},
		func(_ context.Context, msg outageEvent) error {
			received <- msg.ID
			return nil
		}))
}

func expectedIDs(prefix string, n int) map[string]struct{} {
	ids := make(map[string]struct{}, n)
	for i := range n {
		ids[fmt.Sprintf("%s-%d", prefix, i)] = struct{}{}
	}
	return ids
}

// Listening past the last expected id is what catches a replayed duplicate.
func requireExactlyOnce(t *testing.T, received <-chan string, expected map[string]struct{}) {
	t.Helper()

	counts := make(map[string]int)
	deadline := time.After(reconnectWait)
	for len(counts) < len(expected) {
		select {
		case id := <-received:
			counts[id]++
		case <-deadline:
			t.Fatalf("timed out: got %d of %d distinct messages: %v", len(counts), len(expected), counts)
		}
	}

	settle := time.After(duplicateSettle)
	for {
		select {
		case id := <-received:
			counts[id]++
		case <-settle:
			for id := range expected {
				require.Equalf(t, 1, counts[id], "message %q delivered %d times", id, counts[id])
			}
			for id := range counts {
				_, ok := expected[id]
				require.Truef(t, ok, "unexpected message %q", id)
			}
			return
		}
	}
}

func publishBatch(t *testing.T, c *Client[outageExchange], prefix string, n int) {
	t.Helper()
	for i := range n {
		require.NoError(t, c.Publish(context.Background(), outageEvent{ID: fmt.Sprintf("%s-%d", prefix, i)}))
	}
}

func TestReconnectPublishBeforeBrokerExistsIsDeliveredOnce(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	port := freeLocalPort(t)
	received := make(chan string, 16)

	client := newOutageClient(t, port, true)
	require.False(t, client.Connected())
	registerCollector(t, client, received)
	client.Start(ctx)
	require.NoError(t, client.Publish(ctx, outageEvent{ID: "single-0"}))

	startBrokerAt(t, port)

	requireExactlyOnce(t, received, expectedIDs("single", 1))
	require.Eventually(t, client.Connected, 10*time.Second, 100*time.Millisecond)
}

func TestReconnectManyPublishesWhileDownAreAllDeliveredOnce(t *testing.T) {
	skipWithoutBroker(t)

	const n = 20
	ctx := context.Background()
	port := freeLocalPort(t)
	received := make(chan string, n*4)

	client := newOutageClient(t, port, true)
	registerCollector(t, client, received)
	client.Start(ctx)
	publishBatch(t, client, "batch", n)

	startBrokerAt(t, port)

	requireExactlyOnce(t, received, expectedIDs("batch", n))
}

func TestReconnectOutageMidRunDeliversEverythingOnceAndKeepsConsuming(t *testing.T) {
	skipWithoutBroker(t)

	const n = 10
	ctx := context.Background()
	port := freeLocalPort(t)
	broker := restartableBroker(t, port)
	received := make(chan string, n*4)

	client := newOutageClient(t, port, true)
	require.True(t, client.Connected())
	registerCollector(t, client, received)
	client.Start(ctx)

	require.NoError(t, client.Publish(ctx, outageEvent{ID: "before-0"}))
	requireExactlyOnce(t, received, expectedIDs("before", 1))

	stopBroker(t, broker)
	require.Eventually(t, func() bool { return !client.Connected() }, reconnectWait, 100*time.Millisecond)
	publishBatch(t, client, "during", n)

	startBroker(t, broker, port)

	requireExactlyOnce(t, received, expectedIDs("during", n))
	require.Eventually(t, client.Connected, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, client.Publish(ctx, outageEvent{ID: "after-0"}))
	requireExactlyOnce(t, received, expectedIDs("after", 1))
}

func TestReconnectConsumerOnlyClientSurvivesBrokerRestart(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	port := freeLocalPort(t)
	broker := restartableBroker(t, port)
	received := make(chan string, 16)

	consumer := newOutageClient(t, port, true)
	registerCollector(t, consumer, received)
	consumer.Start(ctx)

	producer, err := NewSetup[outageExchange](brokerURLAt(port), "outage-producer").
		WithDialTimeout(testDialTimeout).
		WithReconnectDelay(testReconnectWait).
		Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	require.NoError(t, producer.Publish(ctx, outageEvent{ID: "pre-0"}))
	requireExactlyOnce(t, received, expectedIDs("pre", 1))

	stopBroker(t, broker)
	require.Eventually(t, func() bool { return !consumer.Connected() }, reconnectWait, 100*time.Millisecond)
	startBroker(t, broker, port)
	require.Eventually(t, consumer.Connected, reconnectWait, 100*time.Millisecond)
	require.Eventually(t, producer.Connected, reconnectWait, 100*time.Millisecond)

	require.NoError(t, producer.Publish(ctx, outageEvent{ID: "post-0"}))
	requireExactlyOnce(t, received, expectedIDs("post", 1))
}

func TestReconnectPublishOnlyClientHealsByItself(t *testing.T) {
	skipWithoutBroker(t)

	const n = 5
	ctx := context.Background()
	port := freeLocalPort(t)
	received := make(chan string, n*4)

	// A slow reconnect loop gives the consumer time to bind before the cache is
	// flushed; a topic exchange with no bound queue drops what it receives.
	producer := newOutageClient(t, port, false, func(s *Setup[outageExchange]) *Setup[outageExchange] {
		return s.WithReconnectDelay(10 * time.Second)
	})
	require.False(t, producer.Connected())
	publishBatch(t, producer, "heal", n)

	startBrokerAt(t, port)

	consumer := newOutageClient(t, port, true)
	require.True(t, consumer.Connected())
	registerCollector(t, consumer, received)
	consumer.Start(ctx)

	// Healing must not depend on a later Publish call: only the background loop
	// may bring the client back.
	require.Eventually(t, producer.Connected, reconnectWait, 100*time.Millisecond)

	requireExactlyOnce(t, received, expectedIDs("heal", n))
}

func TestReconnectHandlerRegisteredWhileDisconnectedIsBoundOnConnect(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	port := freeLocalPort(t)
	received := make(chan string, 16)

	consumer := newOutageClient(t, port, true)
	require.False(t, consumer.Connected())
	registerCollector(t, consumer, received)
	consumer.Start(ctx)

	startBrokerAt(t, port)
	require.Eventually(t, consumer.Connected, reconnectWait, 100*time.Millisecond)

	producer, err := NewSetup[outageExchange](brokerURLAt(port), "outage-producer").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, producer.Publish(ctx, outageEvent{ID: "late-0"}))

	requireExactlyOnce(t, received, expectedIDs("late", 1))
}

func TestReconnectConcurrentPublishersDuringRecoveryDeliverExactlyOnce(t *testing.T) {
	skipWithoutBroker(t)

	const (
		publishers = 10
		perWorker  = 10
	)
	ctx := context.Background()
	port := freeLocalPort(t)
	received := make(chan string, publishers*perWorker*4)

	client := newOutageClient(t, port, true)
	registerCollector(t, client, received)
	client.Start(ctx)

	expected := make(map[string]struct{}, publishers*perWorker)
	for w := range publishers {
		for i := range perWorker {
			expected[fmt.Sprintf("w%d-%d", w, i)] = struct{}{}
		}
	}

	var wg sync.WaitGroup
	publishErrs := make(chan error, publishers*perWorker)
	for w := range publishers {
		wg.Go(func() {
			for i := range perWorker {
				if err := client.Publish(ctx, outageEvent{ID: fmt.Sprintf("w%d-%d", w, i)}); err != nil {
					publishErrs <- err
				}
			}
		})
	}

	startBrokerAt(t, port)
	wg.Wait()
	close(publishErrs)
	for err := range publishErrs {
		require.NoError(t, err)
	}

	requireExactlyOnce(t, received, expected)
}

func TestReconnectCloseDuringOutageReturnsPromptly(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	port := freeLocalPort(t)
	broker := restartableBroker(t, port)

	client := newOutageClient(t, port, true)
	registerCollector(t, client, make(chan string, 16))
	client.Start(ctx)
	require.True(t, client.Connected())

	stopBroker(t, broker)
	require.Eventually(t, func() bool { return !client.Connected() }, reconnectWait, 100*time.Millisecond)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		client.Close()
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return during the outage")
	}

	published := make(chan error, 1)
	go func() { published <- client.Publish(ctx, outageEvent{ID: "after-close"}) }()
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish after Close did not return")
	}
}

func TestReconnectRetryInFlightSurvivesBrokerRestart(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	port := freeLocalPort(t)
	broker := restartableBroker(t, port)

	var attempts atomic.Int32
	firstFailed := make(chan struct{}, 1)
	handled := make(chan string, 4)

	client := newOutageClient(t, port, true, func(s *Setup[outageExchange]) *Setup[outageExchange] {
		return s.WithRetry(3, 4*time.Second, nil)
	})
	require.NoError(t, RegisterHandler(ctx, client, outageEvent{},
		func(_ context.Context, msg outageEvent) error {
			if attempts.Add(1) == 1 {
				firstFailed <- struct{}{}
				return errors.New("transient failure")
			}
			handled <- msg.ID
			return nil
		}))
	client.Start(ctx)

	require.NoError(t, client.Publish(ctx, outageEvent{ID: "retry-0"}))
	select {
	case <-firstFailed:
	case <-time.After(reconnectWait):
		t.Fatal("first delivery never happened")
	}

	// Parked in the retry queue with a ttl longer than the stop: the retry must
	// come out of a broker that restarted, not the one that parked it.
	stopBroker(t, broker)
	startBroker(t, broker, port)

	requireExactlyOnce(t, handled, expectedIDs("retry", 1))
	require.Equal(t, int32(2), attempts.Load())
}

// A client that never consumes has no failing Consume to tell it the broker
// went away; it must still notice, cache, and flush once the broker is back.
func TestReconnectPublishOnlyClientSurvivesBrokerRestart(t *testing.T) {
	skipWithoutBroker(t)

	const n = 5
	ctx := context.Background()
	port := freeLocalPort(t)
	broker := restartableBroker(t, port)
	received := make(chan string, n*4)

	consumer := newOutageClient(t, port, true)
	registerCollector(t, consumer, received)
	consumer.Start(ctx)

	producer := newOutageClient(t, port, false)
	require.True(t, producer.Connected())
	publishBatch(t, producer, "pre", 1)
	requireExactlyOnce(t, received, expectedIDs("pre", 1))

	stopBroker(t, broker)
	require.Eventually(t, func() bool { return !producer.Connected() }, reconnectWait, 100*time.Millisecond,
		"a publish-only client never noticed the broker went away")
	publishBatch(t, producer, "down", n)

	startBroker(t, broker, port)

	requireExactlyOnce(t, received, expectedIDs("down", n))
	require.Eventually(t, producer.Connected, reconnectWait, 100*time.Millisecond)
}

type confirmExchange struct{}

func (confirmExchange) Name() string { return "confirm-events" }

type confirmEvent struct {
	gorabbit.Msg[confirmExchange]
	ID string `json:"id"`
}

// brokerProxy exists to reach the window between the write of a publish and its
// confirm: it holds the bytes back and then tears the connection down.
type brokerProxy struct {
	listener   net.Listener
	target     string
	blocked    atomic.Bool
	clientSent chan struct{}
	mu         sync.Mutex
	conns      []net.Conn
}

func newBrokerProxy(t *testing.T, target string) *brokerProxy {
	t.Helper()

	u, err := url.Parse(target)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &brokerProxy{listener: listener, target: u.Host, clientSent: make(chan struct{}, 1)}
	t.Cleanup(func() {
		_ = listener.Close()
		p.cut()
	})

	go p.accept()

	return p
}

func (p *brokerProxy) url() string {
	return fmt.Sprintf("amqp://guest:guest@%s/", p.listener.Addr().String())
}

func (p *brokerProxy) accept() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		broker, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}

		p.mu.Lock()
		p.conns = append(p.conns, client, broker)
		p.mu.Unlock()

		go p.pipe(broker, client, true)
		go p.pipe(client, broker, false)
	}
}

func (p *brokerProxy) pipe(dst, src net.Conn, fromClient bool) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if p.blocked.Load() {
				// A heartbeat is 8 bytes; anything larger is the publish.
				if fromClient && n > 32 {
					select {
					case p.clientSent <- struct{}{}:
					default:
					}
				}
			} else if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *brokerProxy) block()   { p.blocked.Store(true) }
func (p *brokerProxy) unblock() { p.blocked.Store(false) }

func (p *brokerProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.conns = nil
}

func TestReconnectPublishTornDownBeforeItsConfirmIsNotLost(t *testing.T) {
	skipWithoutBroker(t)

	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()
	received := make(chan string, 4)

	consumer := newConsumer[confirmExchange](t, "confirm-queue")
	require.NoError(t, RegisterHandler(ctx, consumer, confirmEvent{},
		func(_ context.Context, msg confirmEvent) error {
			received <- msg.ID
			return nil
		}))
	consumer.Start(ctx)

	proxy := newBrokerProxy(t, brokerURL)
	producer, err := NewSetup[confirmExchange](proxy.url(), "confirm-producer").
		WithDialTimeout(testDialTimeout).
		WithReconnectDelay(testReconnectWait).
		Connect(cache)
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.True(t, producer.Connected())

	proxy.block()
	published := make(chan error, 1)
	go func() { published <- producer.Publish(ctx, confirmEvent{ID: "unconfirmed-0"}) }()

	select {
	case <-proxy.clientSent:
	case <-time.After(10 * time.Second):
		t.Fatal("the publish never reached the wire")
	}
	proxy.cut()

	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(reconnectWait):
		t.Fatal("Publish did not return after the connection was torn down")
	}

	keys, err := cache.GetAllKeys(ctx, cacheKey("confirm-producer", "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 1, "a publish the broker never confirmed must stay in the cache")

	proxy.unblock()

	requireExactlyOnce(t, received, expectedIDs("unconfirmed", 1))
	require.Eventually(t, func() bool {
		keys, err := cache.GetAllKeys(ctx, cacheKey("confirm-producer", "")+"*")
		return err == nil && len(keys) == 0
	}, reconnectWait, 100*time.Millisecond, "a delivered message must leave the cache")
}
