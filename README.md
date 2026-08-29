# gorabbit

<p align="center">
 <b>A typed, resilient RabbitMQ pub/sub client for Go — topic exchanges, retries, dead-letter queue and offline caching</b><br>
 <img src='./assets/gopher-rabbit.jpg' width='350'> <br>
    <a href="https://github.com/diegoclair/gorabbit/tags" alt="GitHub tag">
     <img src="https://img.shields.io/github/tag/diegoclair/gorabbit.svg" />
    </a>
    <a href='https://coveralls.io/github/diegoclair/gorabbit?branch=main'>
     <img src='https://coveralls.io/repos/github/diegoclair/gorabbit/badge.svg?branch=main' alt='Coverage Status' />
    </a>
    <a href="https://github.com/diegoclair/gorabbit/actions">
     <img src="https://github.com/diegoclair/gorabbit/actions/workflows/ci.yaml/badge.svg" alt="build status">
    </a>
    <a href="https://opensource.org/licenses/MIT">
     <img src="https://img.shields.io/badge/License-MIT-yellow.svg" />
    </a>
</p>

Publish and consume typed messages over RabbitMQ topic exchanges. The message
type decides where it goes, handlers are generic (`func(ctx, OrderCreated) error`),
and the topology — exchanges, queue, retry queue and dead-letter queue — is
declared for you on connect.

The only third-party dependency is
[`amqp091-go`](https://github.com/rabbitmq/amqp091-go). Logging, context
propagation and the cache are interfaces you implement, so the library stays
uncoupled from your application.

## Install

```sh
go get github.com/diegoclair/gorabbit
```

Requires **Go 1.27+**.

## Quickstart

One package per exchange, shared by the services that publish and consume it: the
marker names the exchange, the alias is what every message embeds, and the facts
live next to them. A message therefore carries its own destination — `Publish`
takes no exchange.

```go
// package orders
type Exchange struct{}

func (Exchange) Name() string { return "orders" }

type msg = gorabbit.Msg[Exchange] // unexported alias: the plumbing stays out of sight

type OrderCreated struct {
    msg
    OrderID string `json:"order_id"`
    Total   int64  `json:"total"`
}
```

Embedding `gorabbit.Msg[Exchange]` directly works too; the only difference is that
the `Msg` field then shows up on the message for whoever imports it. Either way
the marker has no fields and no exported methods, so it adds nothing to the JSON
payload: `{"order_id":"123","total":4990}`.

A struct without a marker is not a `gorabbit.Message` and does not compile — the
mistake is caught by the compiler, never by the broker.

### Two markers: one way, or the message picks the way

`Msg` is a fact every subscriber of that type receives in full. When the type is
instead a stream several queues want to split — one per marketplace, per tenant,
per region — the message itself says which slice it belongs to:

```go
// package events
type EventRecorded struct {
    gorabbit.RoutedMsg[Exchange]
    Vendor string `json:"vendor"`
    ID     string `json:"id"`
}

func (e EventRecorded) RouteBy() string { return e.Vendor }
```

The two markers promote different methods, so the compiler keeps them apart: a
struct embedding both is not a `Message` at all, `SubscribeRoute` refuses a `Msg`,
and a type that embeds `RoutedMsg` and forgets `RouteBy` does not compile where a
route is subscribed.

### A service owns one exchange and consumes others

The exchange a service owns is a type parameter of its client, so it is named
once, at the composition root. That client publishes the facts of that exchange
and consumes facts from any other.

```go
client, err := rabbitmq.NewSetup[orders.Exchange](amqpURL, "order-service").
    WithConsumer("order-service").             // queue name
    WithRetry(3, 30*time.Second, isRetryable). // retries before the DLQ
    WithPrefetchCount(10).                     // deliveries buffered in advance
    WithConcurrency(4).                        // handlers running at once
    Connect(gorabbit.NewMemoryCache())
if err != nil {
    log.Fatal(err) // an invalid setup or a refused topology; an outage is not an error
}
defer client.Close()

// Consuming a fact owned by another service.
err = rabbitmq.Subscribe(ctx, client, payments.PaymentConfirmed{},
    func(ctx context.Context, msg payments.PaymentConfirmed) error {
        return orderService.MarkPaid(ctx, msg.OrderID)
    })
if err != nil {
    log.Fatal(err)
}

// Consuming one slice of a routed fact: this queue takes the events of one
// vendor and no other queue receives a copy of them.
err = rabbitmq.SubscribeRoute(ctx, client, events.EventRecorded{}, "mercadolivre",
    func(ctx context.Context, msg events.EventRecorded) error {
        return orderService.Record(ctx, msg.ID)
    })
if err != nil {
    log.Fatal(err)
}

client.Start(ctx) // call after every handler is subscribed

// Publishing the fact this service owns.
err = client.Publish(ctx, orders.OrderCreated{OrderID: "123", Total: 4990})
```

`Publish` only accepts messages of the exchange the client owns:
`client.Publish(ctx, payments.PaymentConfirmed{})` does not compile, so a service
cannot forge another service's facts. Subscribing is deliberately free —
consuming what others publish is the point — and binds the queue to the exchange
owning the message, declaring it if that service has not started yet. Pointers
publish alike: `*OrderCreated` and `OrderCreated` share the exchange and the
routing key. A panic inside a handler is recovered and treated as a failure — it
never takes the consumer down. A second subscription for the same binding is
rejected instead of replacing the first, and a message type with no name
(an anonymous struct) is rejected by `Publish`: its routing key would be empty and
match no binding.

### Subscribing

| Call | What the queue receives |
| --- | --- |
| `Subscribe(ctx, client, T{}, handler)` | every message of type `T`, whatever route it carries |
| `SubscribeRoute(ctx, client, T{}, route, handler)` | the messages of type `T` carrying `route`; only a `RoutedMsg` compiles |

N pods of one service share a queue and compete for each message; two services
have two queues and each receives a copy. Two queues wanting **different slices**
of one type are the third case, and the one a fixed routing key cannot express:
they bind different routing keys instead, so neither receives the other's half.

Both ends of a route go through the same encoder, so a route is any string your
domain already uses — a dot, a space, an accent, a `#` — and neither the publisher
nor the binding can read it as anything but one route. Nothing is rejected and
nothing is rewritten behind your back; the one limit is the AMQP one, and a
routing key past 255 bytes is answered with `rabbitmq.ErrRoutingKeyTooLong`
instead of being truncated into somebody else's route.

Subscribing a type whole and one of its routes on the same client is allowed, and
the route wins for the messages that carry it.

Two message types may share a name — an `OrderCreated` in `orders` and another in
`billing` — and therefore a routing key. The exchange separates them: each binding
is `(exchange, routing key)`, and a message retried through the consumer's own
exchange carries its origin in the `x-origin-exchange` header, so it comes back to
the handler that owns it.

## Options

| Option | Effect |
| --- | --- |
| `WithConsumer(queue)` | Consume from `queue`; also declares `<queue>.dlq` and `<queue>.retry` |
| `WithRetry(count, interval, isRetryable)` | Retry failed messages before dead-lettering; `nil` retries every error |
| `WithPrefetchCount(n)` | Unacknowledged messages the broker delivers at once |
| `WithConcurrency(n)` | Deliveries this client handles at once (default 1) |
| `WithLogger(l)` | Structured logging (noop by default) |
| `WithHeaderCarrier(h)` | Propagate context values as message headers |
| `WithReconnectDelay(d)` | Wait between reconnection attempts (default 2s) |
| `WithDialTimeout(d)` | Bound each connection attempt (default: the amqp091 30s) |
| `WithPublishConfirmTimeout(d)` | Bound the wait for the broker's publish confirmation (default 5s) |

### Concurrency and prefetch

Two knobs, two questions. `WithPrefetchCount(n)` is the transport buffer: how
many unacknowledged deliveries the broker hands over in advance.
`WithConcurrency(n)` is the pool: how many of them are inside a handler at the
same time, across every subscription of the client. The default is one delivery
at a time, whatever the prefetch.

- **Above 1, the handler runs in several goroutines at the same time.** Whatever
  it touches — a map, a counter, a client of its own — has to be safe for
  concurrent use; nothing is serialised on the way in.
- **The order of the queue is not preserved, under concurrency or under retry.**
  Two deliveries in two workers finish in whatever order their handlers take, and
  a retried message comes back behind the newer ones even with a concurrency of
  one — see [Ordering](#ordering).
- **The prefetch has to be at least the concurrency**, or there are workers that
  could never hold a delivery. `Connect` refuses that setup instead of picking a
  prefetch you did not ask for. A prefetch of zero is AMQP's *unlimited* and goes
  with any pool.
- **The prefetch is what this consumer takes out of the queue.** Those messages
  are checked out and no other replica can have them, so a large prefetch behind
  a small pool is one replica hoarding work the others were free to do. Keep it
  near the concurrency and let the queue hold the rest.

The number itself comes from the ceiling downstream — the size of a database
pool, a vendor's rate limit per application — never from the queue depth.

## Design: a neutral port and a driver

The root package `gorabbit` holds the driver-neutral contracts — `Exchange`,
`Msg`, `RoutedMsg`, `Message`, `OwnedBy`, `RoutedMessage`, `Handler`,
`Publisher`, `Consumer`, `Cache`, `Logger`, `HeaderCarrier`. The
`gorabbit/rabbitmq` subpackage is the RabbitMQ driver and depends on those
contracts, never the other way around.

Your domain code therefore imports only `gorabbit` (no AMQP types leak into it),
and the broker choice stays at the composition root:

```go
type Publisher[E Exchange] interface {
    Publish(ctx context.Context, msg OwnedBy[E]) error // *rabbitmq.Client[E] satisfies this
}
```

`Message` and `OwnedBy[E]` are satisfied only by embedding a marker: the methods
it promotes are unexported, so nothing else can implement them and there is
nothing for application code to call. `Message` is any fact, whoever owns it;
`OwnedBy[E]` is a fact of the exchange `E`; `RoutedMessage` is a fact that picks
its own route.

### What a message type may be

- A named struct, exported or not, embedding one marker. The routing key is the
  type name without the package, plus the encoded route when the marker is
  `RoutedMsg`.
- A pointer to one. `Publish(ctx, &OrderCreated{})` is the same message; a typed
  nil returns an error instead of panicking.
- Not a generic type: the routing key of `Envelope[Order]` is the instantiated
  name, brackets, dots and all. Wrap the payload in a plain struct instead.

The marker itself must be a value type with a value receiver (`type Orders
struct{}`), since it is resolved from its zero value.

## Retry and dead-letter

With `WithRetry`, a handler error republishes the message to `<queue>.retry`,
which holds it for the configured interval and then returns it to the queue. The
attempt is counted in the `x-retry-count` header; past the limit the message
moves to `<queue>.dlq`. Without `WithRetry`, a failed message is dead-lettered
right away.

## Offline caching and reconnection

A client is always usable; a broker outage is a state, not an error. `Connect`
returns an error only for an invalid setup, a missing cache or a topology the
broker refuses — when the dial itself fails, whether the broker is down, the
address is wrong or the credentials are refused, it returns a live client
that starts disconnected, keeps reconnecting in background (`WithReconnectDelay`
between attempts, each one bounded by `WithDialTimeout`) and heals itself on the
first successful connection:

- `Publish` stores the message in the `Cache` and returns nil; cached messages
  are published — in order — on the next successful connection. A message no
  binding could ever match is the exception: an unnamed type or a routing key
  past the AMQP limit is answered with an error and never reaches the cache.
- `Subscribe`, `SubscribeRoute` and `Start` work while disconnected: the topology
  is declared and the queue bindings are applied when the connection lands, and
  the consumer starts consuming.
- `Connected()` reports the current state — for health checks and metrics, never
  a precondition for calling anything.
- A topology the broker refuses — a queue argument changed between deploys, say —
  is not an outage: it is refused on every attempt. At the first connect
  `Connect` fails with `rabbitmq.ErrTopologyRejected` and hands back no client;
  when the rejection appears on a later redial, `Publish` returns that same error
  instead of caching, `Connected()` stays false and the background loop keeps
  retrying until the broker (or the deploy) is fixed.
- Cancelling the `Start` context stops consuming; the client stays connected and
  keeps publishing until `Close`.
- After `Close`, `Publish` neither caches nor reconnects: it returns
  `rabbitmq.ErrClientClosed`.

Each subscription is also recorded in the cache, so on the next start bindings
whose handler no longer exists in the code are unbound.

The cache entries of a client are keyed by its app name and exchange, plus its
queue when it consumes, so two clients of one process never replay each other's
messages nor unbind each other's bindings. Reaching for a key another live client
already holds is a mistake only a deploy fixes, so `Connect` refuses it with
`rabbitmq.ErrCacheKeyTaken` and names the client that has it. Two *processes* of
the same application are the opposite case — they are meant to share the key, and
the reservation below is what keeps them from publishing the same message twice.

### A message with nowhere to go

Every publish is `mandatory`, so a routing key no queue is bound to comes back
instead of being dropped by the broker behind a confirm that says it was
delivered — which is what happens to every event published before its consumer
first started. A returned message is not a delivery: it goes into the same cache
a broker outage fills, and the client retries it with a growing delay, logging an
error on every attempt. Nothing announces a binding, so the timer is the only way
back; the message leaves the cache when it finally routes, and a message that
routes is never held back by one that does not.

`gorabbit.NewMemoryCache()` is process-local and good enough for a single
instance; a shared store (Redis) is what makes cached messages and bindings
survive a restart.

```go
type Cache interface {
    Set(ctx context.Context, key string, data []byte, ttl time.Duration) error
    SetIfAbsent(ctx context.Context, key string, data []byte, ttl time.Duration) (bool, error)
    Get(ctx context.Context, key string) ([]byte, error)
    GetAllKeys(ctx context.Context, pattern string) ([]string, error)
    Delete(ctx context.Context, keys ...string) error
}
```

A `ttl` of zero means no expiration, `Get` returns `nil, nil` when the key is
absent, and `GetAllKeys` receives a glob pattern (`*` and `?`).

`SetIfAbsent` writes the key only if it is not there and returns whether *this*
call is the one that wrote it. It is the only operation that has to be atomic,
and it must be atomic **against every other process on the same store**, not
only against the goroutines of this one — a read followed by a write is not an
implementation of it. Every store has the primitive:

| Store | Call |
| --- | --- |
| Redis | `SET key value NX PX <ttl>` — a null reply means the key was already there |
| Memcached | `ADD` |
| In-memory | a map under a mutex, expiry checked before the absence test |

An implementation that always returns `true` compiles and passes a single-instance
deployment, then delivers every offline message once per replica.

### Sharing one cache between replicas

N replicas of one application publish to one exchange under one app name, so they
share a cache scope, and after an outage they all hold the same cached messages.
Before publishing one of them, a client **reserves** it: `SetIfAbsent` on a lease
key, with a ttl derived from `WithPublishConfirmTimeout` so it outlives a whole
publish attempt, confirmation included. Whoever loses the reservation skips that
message and moves to the next, and comes back to it later if it is still cached.

The lease lives under a prefix of its own, outside the one the flush scans, so it
is never mistaken for a message. It is given back when the publish fails, and
left to expire when it succeeds.

What that buys, and what it costs:

- a replica joining during a rolling deploy is never refused, and it does not
  republish what the outgoing one is already publishing;
- a replica that is killed mid-publish holds its lease until it expires, and
  another replica then finishes the delivery — nothing is stranded;
- delivery stays **at-least-once**: a process that dies after the broker took the
  message but before the entry left the cache makes it be delivered again when
  the lease expires. The `MessageId` is stable across replays, which is what lets
  a consumer discard the duplicate.

Two clients *of one process* reaching for the same cache key is still a
configuration mistake, and `Connect` refuses it with `rabbitmq.ErrCacheKeyTaken`.
The lease is what makes sharing between processes safe; the refusal is what
catches the mistake inside one.

## Message id

Every published message carries a UUIDv7 `MessageId`, assigned before the message
reaches the cache and preserved across retries and into the dead-letter queue. The
same logical message therefore always carries the same id — which is what lets a
consumer discard a duplicate, since caching and replaying make delivery
at-least-once. It is also logged when a handler panics and when a message is
dead-lettered.

## Logging

```go
type Logger interface {
    Debug(ctx context.Context, msg string, keyvals ...any)
    Info(ctx context.Context, msg string, keyvals ...any)
    Warn(ctx context.Context, msg string, keyvals ...any)
    Error(ctx context.Context, msg string, keyvals ...any)
}
```

Every connection lifecycle line — connecting, connected, dial failure, reconnect,
close — carries `app_name` and `exchange`, so a process holding one client per
exchange can tell whose connection a line is about. The consumer's
`started consuming messages` also carries `queue`.

## Propagating context (correlation id, user, tenant)

The library does not know your context keys, so it asks you for them:

```go
type HeaderCarrier interface {
    FromContext(ctx context.Context) map[string]any
    ToContext(ctx context.Context, headers map[string]any) context.Context
}
```

`FromContext` is called when publishing and its values travel as AMQP headers;
`ToContext` is called before the handler runs, on the consumer side. Values must
be AMQP-encodable (string, int, bool, time.Time, ...).

```go
type correlationCarrier struct{}

func (correlationCarrier) FromContext(ctx context.Context) map[string]any {
    return map[string]any{"correlation_id": ctx.Value(correlationIDKey)}
}

func (correlationCarrier) ToContext(ctx context.Context, headers map[string]any) context.Context {
    id, _ := headers["correlation_id"].(string)
    if id == "" {
        id = uuid.NewV7().String()
    }
    return context.WithValue(ctx, correlationIDKey, id)
}
```

## Ordering

Ordering is not a promise of this library. Replicas sharing a queue compete for
each message, and above `WithConcurrency(1)` the workers of a single client take
several at a time, so two messages may be handled concurrently and finish out of
order. Retry breaks the order on its own: a failed message waits in the retry
queue and comes back behind everything published meanwhile, with a serial
consumer as much as with a pool.

Where order by key is what the domain needs, the recipe is one route per bucket:
publish with `hash(key) % N` as the route, and give each replica its own queue and
its own slice with `SubscribeRoute`, consumed with a concurrency of 1. Every
message of one key then lands in one queue and is handled one at a time, until a
retry puts one of them back behind the messages that followed it. It is
deliberately static: rebalancing is a deploy, and the slice of a replica that died
waits for it to come back instead of being handed to another consumer out of
order.

## Testing

`go test ./...` starts a RabbitMQ container through
[testcontainers](https://golang.testcontainers.org/) for the integration tests,
so Docker must be running. `go test -short ./...` skips them and runs only the
unit tests.

## Contributing

**Contributions are welcomed. :)**

1. Fork the repository
2. Create a new feature branch (`git checkout -b feature/<FEATURE NAME>`)
3. Make the necessary changes
4. Commit your changes (`git commit -m "Add some feature"`)
5. Push your changes to your forked repository (`git push origin feature/<FEATURE NAME>`)
6. Create a pull request to the main branch of the repository

## License

gorabbit is [MIT licensed](./LICENSE).
