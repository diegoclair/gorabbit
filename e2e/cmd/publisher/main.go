// Command publisher is one process of the harness: it owns the events exchange
// and is driven over HTTP so the runner can make it work, watch it and kill it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/e2e/internal/api"
	"github.com/diegoclair/gorabbit/e2e/internal/appkit"
	"github.com/diegoclair/gorabbit/e2e/internal/events"
	"github.com/diegoclair/gorabbit/e2e/internal/rediscache"
	"github.com/diegoclair/gorabbit/rabbitmq"
)

func main() {
	amqpURL := flag.String("amqp", "", "amqp url")
	redisAddr := flag.String("redis", "", "redis host:port")
	cacheKind := flag.String("cache", "redis", "redis or memory")
	appName := flag.String("app", "e2e-publisher", "application name, which also scopes the cache")
	reconnectDelay := flag.Duration("reconnect-delay", 500*time.Millisecond, "wait between reconnection attempts")
	flag.Parse()

	cache, closeCache, err := openCache(*cacheKind, *redisAddr)
	if err != nil {
		log.Fatalf("publisher %s: %v", *appName, err)
	}
	defer closeCache()

	client, err := connect(*amqpURL, *appName, *reconnectDelay, cache)
	if err != nil {
		log.Fatalf("publisher %s: %v", *appName, err)
	}

	p := &publisher{
		client:  client,
		appName: *appName,
		amqpURL: *amqpURL,
		delay:   *reconnectDelay,
		cache:   cache,
		batches: map[string]*api.BatchStats{},
	}

	if err := appkit.Run(p.routes(), p.stop); err != nil {
		log.Fatalf("publisher %s: %v", *appName, err)
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

func connect(amqpURL, appName string, reconnectDelay time.Duration, cache gorabbit.Cache) (*rabbitmq.Client[events.Exchange], error) {
	return rabbitmq.NewSetup[events.Exchange](amqpURL, appName).
		WithLogger(appkit.NewLogger(appName)).
		WithReconnectDelay(reconnectDelay).
		WithDialTimeout(3 * time.Second).
		WithPublishConfirmTimeout(3 * time.Second).
		Connect(cache)
}

type publisher struct {
	client  *rabbitmq.Client[events.Exchange]
	appName string
	amqpURL string
	delay   time.Duration
	cache   gorabbit.Cache

	mu      sync.Mutex
	batches map[string]*api.BatchStats
	work    sync.WaitGroup
}

func (p *publisher) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", p.health)
	mux.HandleFunc("POST /publish", p.publish)
	mux.HandleFunc("GET /published", p.published)
	mux.HandleFunc("POST /claim-twin", p.claimTwin)

	return mux
}

func (p *publisher) stop() {
	p.work.Wait()
	p.client.Close()
}

func (p *publisher) health(w http.ResponseWriter, _ *http.Request) {
	appkit.WriteJSON(w, http.StatusOK, api.Health{App: p.appName, Connected: p.client.Connected()})
}

func (p *publisher) publish(w http.ResponseWriter, r *http.Request) {
	var req api.PublishRequest
	if err := appkit.ReadJSON(r, &req); err != nil {
		appkit.WriteJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.Batch == "" || req.Count <= 0 {
		appkit.WriteJSON(w, http.StatusBadRequest, map[string]any{"error": "batch and a positive count are required"})
		return
	}

	stats := p.batch(req.Batch)
	p.mu.Lock()
	stats.Requested += req.Count
	p.mu.Unlock()

	if req.Async {
		p.work.Add(1)
		go func() {
			defer p.work.Done()
			p.run(req, stats)
		}()

		appkit.WriteJSON(w, http.StatusAccepted, map[string]any{"batch": req.Batch, "requested": req.Count})

		return
	}

	p.run(req, stats)
	appkit.WriteJSON(w, http.StatusOK, p.snapshot(req.Batch))
}

func (p *publisher) run(req api.PublishRequest, stats *api.BatchStats) {
	ctx := context.Background()

	for seq := 1; seq <= req.Count; seq++ {
		err := p.client.Publish(ctx, message(req, seq))

		p.mu.Lock()
		stats.Attempted++
		if err != nil {
			stats.Failed++
			stats.Errors = append(stats.Errors, err.Error())
		} else {
			stats.OK++
		}
		p.mu.Unlock()

		if req.DelayMS > 0 {
			time.Sleep(time.Duration(req.DelayMS) * time.Millisecond)
		}
	}
}

func message(req api.PublishRequest, seq int) gorabbit.OwnedBy[events.Exchange] {
	if req.Kind == "order" {
		return events.OrderPlaced{Batch: req.Batch, Seq: seq}
	}

	return events.VendorEvent{Vendor: req.Route, Batch: req.Batch, Seq: seq}
}

func (p *publisher) published(w http.ResponseWriter, r *http.Request) {
	if batch := r.URL.Query().Get("batch"); batch != "" {
		appkit.WriteJSON(w, http.StatusOK, p.snapshot(batch))
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	all := map[string]api.BatchStats{}
	for name, stats := range p.batches {
		all[name] = *stats
	}

	appkit.WriteJSON(w, http.StatusOK, all)
}

// The in-process control for the cache-key guard, which the runner contrasts
// with what two processes do.
func (p *publisher) claimTwin(w http.ResponseWriter, _ *http.Request) {
	twin, err := connect(p.amqpURL, p.appName, p.delay, p.cache)
	if err != nil {
		appkit.WriteJSON(w, http.StatusOK, api.TwinResult{Connected: false, Error: err.Error()})
		return
	}

	connected := twin.Connected()
	twin.Close()

	appkit.WriteJSON(w, http.StatusOK, api.TwinResult{Connected: connected})
}

func (p *publisher) batch(name string) *api.BatchStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats, ok := p.batches[name]
	if !ok {
		stats = &api.BatchStats{}
		p.batches[name] = stats
	}

	return stats
}

func (p *publisher) snapshot(name string) api.BatchStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats, ok := p.batches[name]
	if !ok {
		return api.BatchStats{}
	}

	return *stats
}
