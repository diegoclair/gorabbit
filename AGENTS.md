# gorabbit

A typed, resilient RabbitMQ pub/sub client for Go. The message type decides where
it goes, handlers are generic, and the topology — exchange, queue, retry queue,
dead-letter queue — is declared on connect. If the broker is down the client
caches every publish and resends it by itself once the broker is back; the cache
is an interface, so it can be process-local memory or a shared store.

Robust and simple, with no magic: nothing is rewritten behind the caller's back,
a refused topology stops the boot, and an outage is a state rather than an error.

## Layout

- `gorabbit` (root) — the driver-neutral contracts: `Exchange`, `Msg`,
  `RoutedMsg`, `Message`, `Handler`, `Publisher`, `Consumer`, `Cache`, `Logger`,
  `HeaderCarrier`, plus the in-memory cache.
- `rabbitmq` — the RabbitMQ driver: setup, connection lifecycle, publishing with
  confirms and returns, consuming, retry and dead-lettering, route encoding.
- `testdata` — packages the tests build against the library, including the ones
  that must not compile.
- `e2e` — a separate Go module holding the end-to-end harness. It is not part of
  the library's module, so `go build ./...` and `go test ./...` at the root do
  not see it and the library keeps no example dependency.

## Commands

```sh
make test        # go test -count=1 ./...   (unit + integration, TestContainers)
make build
make vet
make fmt         # gofmt -l . — must print nothing

make e2e         # the whole harness: infrastructure up, every script, then down
make e2e-list    # the script names
make e2e-run SCENARIO=dlq    # one script, infrastructure already up
make e2e-keep    # leave the broker, redis and the processes up to poke by hand
make e2e-down    # tear the infrastructure down and stop what e2e-keep left
```

`make e2e` needs Docker; nothing else does.

## The cache contract

`Cache` has exactly one atomic operation, `SetIfAbsent` (write-if-absent with a
ttl, answering whether it was the writer); everything else is plain reads and
writes. It exists because replicas of one application share a cache scope, so the
exclusivity has to belong to the **message**, not to the client: before
publishing a cached message a client reserves it under a lease key, and whoever
loses the reservation skips that message and comes back to it later.

- The lease prefix sits outside the prefix the flush globs. A lease the glob
  reaches is read back as a message and published as one, so the two must never
  nest — there is a test that holds that line.
- The lease ttl comes from the publish confirm timeout and has to outlive a whole
  publish attempt, confirmation included. A short lease is the duplicate back.
- Delivery is at-least-once and the README says so: a process dying between the
  broker taking a message and the entry leaving the cache has it delivered again
  once the lease expires. Deleting before publishing would close that window by
  trading a duplicate for a loss, which is the one thing this library must not do.
- The in-process guard in `rabbitmq/claim.go` stays: two clients of one binary on
  one cache key is a configuration mistake and is refused. The guard catches the
  mistake, the lease makes the sharing between processes safe.

## The consumer's two knobs

Prefetch and concurrency are different things and both belong to the caller.
`WithPrefetchCount` is the transport buffer — how many unacknowledged deliveries
the broker pushes in advance, and therefore how many this consumer checks out of
the queue where no other replica can reach them. `WithConcurrency` is the worker
pool — how many deliveries are inside a handler at once. Neither is derived from
the other: a pool wider than the prefetch that bounds it is refused at
`validate`, never rewritten behind the caller.

- **An option that needs a queue lives on the type `WithConsumer` returns**, so
  a client that does not consume cannot name it and the mistake is a compile
  error rather than a boot failure. That type repeats the options common to
  every setup instead of embedding, or a chain would stop being a consumer setup
  halfway. The order this imposes — `WithConsumer` first — is part of the API.
- **`validate` keeps the checks on values only.** A rule the type system already
  makes unwritable is a branch no test can reach, so it does not belong there;
  a number out of range is not something a type catches, so it does.
- **The pool belongs to the client, not to a subscription.** One set of
  goroutines reads the single delivery channel and serves every registered
  handler. Partitioning by key is the one thing that could restore order across
  message types sharing a key, and it needs the pool there — so a pool per
  subscription is not an option even where it would look equivalent today.
- **One generation of workers at a time.** `consumeMessages` runs again on every
  reconnect and must not return while a worker it started is alive, or the
  reconnect brings a second pool up beside the first.
- **Order is not a guarantee this library makes.** Above a concurrency of one,
  two messages of a queue are handled at the same time. Where order by key
  matters the answer is a route per bucket and a concurrency of one, not a lock
  in the consumer.

## When the test suite is enough, and when it is not

The suite in `rabbitmq` runs against a real RabbitMQ through TestContainers, with
separate AMQP connections, and it is the right place for almost everything:
routing, isolation between routes, retry, the dead-letter queue, route encoding,
reconnection. It runs in about thirty seconds and it is stable. **Do not rebuild
any of that with separate processes** — the broker cannot tell whether two
connections came from one binary, so a second process proves nothing there.

What that suite structurally cannot reach is **state whose scope is the process**.
A package variable, a `sync.Once`, a singleton: two clients inside one test
binary share all of it, so they are not independent instances however carefully
the test is written. This is not a theoretical gap — the cache-key guard in
`rabbitmq/claim.go` is a package-level map, so it holds inside a process and does
not exist between processes, and only a harness with real processes shows it.

Reach for `make e2e` when the question involves:

- a process dying (`kill -9`) and what survives it,
- two processes sharing a cache, a claim or any other package-level state,
- a rolling deploy, where the new process is up before the old one is gone,
- the difference between an in-memory cache and a shared one across a restart,
- what a previous *run* left behind — a stale binding a later deploy must remove.

Anything else belongs in the suite, where it is cheaper and faster.

## Rules that hold everywhere in this repo

- A test never shapes production code. If the harness or a test needs something,
  it solves it on its own side — no required dependency accepting `nil`, no field
  exported for a test to read, no `isTest` flag, no branch that only runs in tests.
- Comments state the purpose or the constraint, never the behaviour: the code
  already says what it does, and a comment describing it becomes a lie at the
  first refactor. In particular, a comment claiming *by which mechanism* something
  works has to be true — a test here once passed unnoticed while asserting a
  mechanism that was not the real one.
- Never synchronise with `time.Sleep`. Wait for a condition, with a timeout that
  fails saying what it was waiting for and what it last saw.
- A guarantee the API makes at compile time is proved by a package under
  `testdata` that must not build, and a test that builds it and asserts the
  compiler's own message. A rule with no such package is only a claim.
