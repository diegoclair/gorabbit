// Command consumer is one process of the harness: it subscribes the slices it
// was given and answers what it has received, so a scenario can compare that
// with what the broker reports.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/e2e/internal/api"
	"github.com/diegoclair/gorabbit/e2e/internal/appkit"
	"github.com/diegoclair/gorabbit/e2e/internal/events"
	"github.com/diegoclair/gorabbit/e2e/internal/rediscache"
	"github.com/diegoclair/gorabbit/rabbitmq"
)

// The retry and dead-letter path must be driven by an ordinary application
// failure, never by a panic or a decode error.
var errHandlerRefused = errors.New("consumer refused the message on purpose")

type subscriptions []string

func (s *subscriptions) String() string { return strings.Join(*s, ",") }

func (s *subscriptions) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var subs subscriptions

	amqpURL := flag.String("amqp", "", "amqp url")
	redisAddr := flag.String("redis", "", "redis host:port")
	cacheKind := flag.String("cache", "redis", "redis or memory")
	appName := flag.String("app", "e2e-consumer", "application name, which also scopes the cache")
	queueName := flag.String("queue", "", "queue this consumer owns")
	failAlways := flag.Bool("fail-always", false, "every handler call fails, to drive the retry and dlq path")
	retryCount := flag.Int("retry-count", 0, "retries before the dlq; zero disables retry")
	retryInterval := flag.Duration("retry-interval", time.Second, "wait between retries")
	prefetch := flag.Int("prefetch", 0, "unacknowledged messages the broker delivers at once")
	reconnectDelay := flag.Duration("reconnect-delay", 500*time.Millisecond, "wait between reconnection attempts")
	flag.Var(&subs, "sub", "subscription: order, vendor, or vendor:<route> (repeatable)")
	flag.Parse()

	if *queueName == "" || len(subs) == 0 {
		log.Fatalf("consumer %s: -queue and at least one -sub are required", *appName)
	}

	cache, closeCache, err := openCache(*cacheKind, *redisAddr)
	if err != nil {
		log.Fatalf("consumer %s: %v", *appName, err)
	}
	defer closeCache()

	setup := rabbitmq.NewSetup[events.Exchange](*amqpURL, *appName).
		WithConsumer(*queueName).
		WithLogger(appkit.NewLogger(*appName)).
		WithReconnectDelay(*reconnectDelay).
		WithDialTimeout(3 * time.Second).
		WithPublishConfirmTimeout(3 * time.Second)

	if *retryCount > 0 {
		setup = setup.WithRetry(*retryCount, *retryInterval, nil)
	}
	if *prefetch > 0 {
		setup = setup.WithPrefetchCount(*prefetch)
	}

	client, err := setup.Connect(cache)
	if err != nil {
		log.Fatalf("consumer %s: %v", *appName, err)
	}

	c := &consumer{
		appName:    *appName,
		queueName:  *queueName,
		failAlways: *failAlways,
		subs:       subs,
		client:     client,
		seen:       map[string]*api.Item{},
	}

	ctx := context.Background()
	if err := c.subscribe(ctx); err != nil {
		log.Fatalf("consumer %s: %v", *appName, err)
	}

	client.Start(ctx)

	if err := appkit.Run(c.routes(), client.Close); err != nil {
		log.Fatalf("consumer %s: %v", *appName, err)
	}
}

func openCache(kind, redisAddr string) (gorabbit.Cache, func(), error) {
	switch kind {
	case "memory":
		return gorabbit.NewMemoryCache(), func() {}, nil
	case "redis":
		if redisAddr == "" {
			return nil, nil, fmt.Errorf("-redis is required with -cache=redis")
		}
		client := rediscache.New(redisAddr)
		if err := client.Ping(context.Background()); err != nil {
			return nil, nil, err
		}
		return client, func() { _ = client.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown cache %q", kind)
	}
}

type consumer struct {
	appName    string
	queueName  string
	failAlways bool
	subs       []string
	client     *rabbitmq.Client[events.Exchange]

	mu    sync.Mutex
	total int
	seen  map[string]*api.Item
}

func (c *consumer) subscribe(ctx context.Context) error {
	for _, sub := range c.subs {
		kind, route, hasRoute := strings.Cut(sub, ":")

		switch {
		case kind == "order":
			if err := rabbitmq.Subscribe(ctx, c.client, events.OrderPlaced{}, c.handleOrder); err != nil {
				return err
			}
		case kind == "vendor" && (!hasRoute || route == "*"):
			if err := rabbitmq.Subscribe(ctx, c.client, events.VendorEvent{}, c.handleVendor); err != nil {
				return err
			}
		case kind == "vendor":
			if err := rabbitmq.SubscribeRoute(ctx, c.client, events.VendorEvent{}, route, c.handleVendor); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown subscription %q", sub)
		}
	}

	return nil
}

func (c *consumer) handleOrder(_ context.Context, msg events.OrderPlaced) error {
	return c.record(api.Item{Kind: "order", Batch: msg.Batch, Seq: msg.Seq})
}

func (c *consumer) handleVendor(_ context.Context, msg events.VendorEvent) error {
	return c.record(api.Item{Kind: "vendor", Vendor: msg.Vendor, Batch: msg.Batch, Seq: msg.Seq})
}

func (c *consumer) record(received api.Item) error {
	key := fmt.Sprintf("%s|%s|%s|%d", received.Kind, received.Vendor, received.Batch, received.Seq)

	c.mu.Lock()
	c.total++
	known, ok := c.seen[key]
	if !ok {
		received.Deliveries = 1
		c.seen[key] = &received
	} else {
		known.Deliveries++
	}
	c.mu.Unlock()

	if c.failAlways {
		return errHandlerRefused
	}

	return nil
}

func (c *consumer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", c.health)
	mux.HandleFunc("GET /received", c.received)

	return mux
}

func (c *consumer) health(w http.ResponseWriter, _ *http.Request) {
	appkit.WriteJSON(w, http.StatusOK, api.Health{
		App:           c.appName,
		Queue:         c.queueName,
		Connected:     c.client.Connected(),
		Subscriptions: c.subs,
	})
}

func (c *consumer) received(w http.ResponseWriter, r *http.Request) {
	batch := r.URL.Query().Get("batch")

	c.mu.Lock()
	defer c.mu.Unlock()

	items := make([]api.Item, 0, len(c.seen))
	total, duplicates := 0, 0

	for _, known := range c.seen {
		if batch != "" && known.Batch != batch {
			continue
		}
		items = append(items, *known)
		total += known.Deliveries
		duplicates += known.Deliveries - 1
	}

	appkit.WriteJSON(w, http.StatusOK, api.Received{
		App:        c.appName,
		Queue:      c.queueName,
		Total:      total,
		Unique:     len(items),
		Duplicates: duplicates,
		Items:      items,
	})
}
