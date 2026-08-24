# gorabbit

<p align="center">
 <b>A typed, resilient RabbitMQ pub/sub client for Go — topic exchanges, retries, dead-letter queue and offline caching</b><br><br>
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
    <a href='https://goreportcard.com/badge/github.com/diegoclair/gorabbit'>
     <img src='https://goreportcard.com/badge/github.com/diegoclair/gorabbit' alt='Go Report'/>
    </a>
</p>

Publish and consume typed messages over RabbitMQ topic exchanges. Each message
type is its own routing key, handlers are generic (`func(ctx, OrderCreated) error`),
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

A message is any struct that names the exchange owning it. The routing key is the
struct's type name, so producer and consumer agree on a type, not on strings.

```go
type OrderCreated struct {
    OrderID string `json:"order_id"`
    Total   int64  `json:"total"`
}

func (OrderCreated) ExchangeOwnerName() string { return "orders" }
```

### Producer

```go
client, err := rabbitmq.NewSetup(amqpURL, "orders", "orders-api").
    Connect(gorabbit.NewMemoryCache())
if err != nil {
    log.Fatal(err)
}
defer client.Close()

client.Start(ctx) // background reconnection loop

err = client.Publish(ctx, OrderCreated{OrderID: "123", Total: 4990})
```

### Consumer

```go
consumer, err := rabbitmq.NewSetup(amqpURL, "billing", "billing-worker").
    WithConsumer("billing-worker").            // queue name
    WithRetry(3, 30*time.Second, isRetryable). // retries before the DLQ
    WithPrefetchCount(10).
    Connect(myCache)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

err = rabbitmq.RegisterHandler(ctx, consumer, OrderCreated{},
    func(ctx context.Context, msg OrderCreated) error {
        return billing.Charge(ctx, msg.OrderID, msg.Total)
    })
if err != nil {
    log.Fatal(err)
}

consumer.Start(ctx) // call after every handler is registered
```

`RegisterHandler` binds the queue to the exchange owning the message type,
declaring that exchange if the producer has not started yet. A panic inside a
handler is recovered and treated as a failure — it never takes the consumer down.

## Options

| Option | Effect |
| --- | --- |
| `WithConsumer(queue)` | Consume from `queue`; also declares `<queue>.dlq` and `<queue>.retry` |
| `WithRetry(count, interval, isRetryable)` | Retry failed messages before dead-lettering; `nil` retries every error |
| `WithPrefetchCount(n)` | Unacknowledged messages the broker delivers at once |
| `WithLogger(l)` | Structured logging (noop by default) |
| `WithHeaderCarrier(h)` | Propagate context values as message headers |
| `WithReconnectDelay(d)` | Wait between reconnection attempts (default 2s) |
| `WithDialTimeout(d)` | Bound each connection attempt (default: the amqp091 30s) |

## Design: a neutral port and a driver

The root package `gorabbit` holds the driver-neutral contracts — `Message`,
`Handler`, `Publisher`, `Consumer`, `Cache`, `Logger`, `HeaderCarrier`. The
`gorabbit/rabbitmq` subpackage is the RabbitMQ driver and depends on those
contracts, never the other way around.

Your domain code therefore imports only `gorabbit` (no AMQP types leak into it),
and the broker choice stays at the composition root:

```go
type Publisher interface {
    Publish(ctx context.Context, msg gorabbit.Message) error // *rabbitmq.Client satisfies this
}
```

## Retry and dead-letter

With `WithRetry`, a handler error republishes the message to `<queue>.retry`,
which holds it for the configured interval and then returns it to the queue. The
attempt is counted in the `x-retry-count` header; past the limit the message
moves to `<queue>.dlq`. Without `WithRetry`, a failed message is dead-lettered
right away.

## Offline caching

If RabbitMQ is unreachable, `Publish` stores the message in the `Cache` and
returns nil; cached messages are published — in order — on the next successful
connection. Each registered handler is also recorded there, so on the next start
bindings whose handler no longer exists in the code are unbound.

`gorabbit.NewMemoryCache()` is process-local and good enough for a single
instance; a shared store (Redis) is what makes cached messages and bindings
survive a restart.

```go
type Cache interface {
    Set(ctx context.Context, key string, data []byte, ttl time.Duration) error
    Get(ctx context.Context, key string) ([]byte, error)
    GetAllKeys(ctx context.Context, pattern string) ([]string, error)
    Delete(ctx context.Context, keys ...string) error
}
```

A `ttl` of zero means no expiration, `Get` returns `nil, nil` when the key is
absent, and `GetAllKeys` receives a glob pattern (`*` and `?`).

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

Ordering is not handled here: with more than one consumer on a queue, two
messages may be processed concurrently and out of order. Ordering requires Single
Active Consumer plus a partitioning key, which is an application decision.

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
