# gorabbit e2e — validation with real processes

A separate Go module (`replace github.com/diegoclair/gorabbit => ../`) so the
library's own module keeps no example dependency and the root `go build ./...`
and `go test ./...` do not change.

It exists for one reason: the integration suite runs every client inside a single
test binary, so anything scoped to the process — a package variable, a
`sync.Once`, a singleton — is shared between the clients that are supposed to be
independent. Everything else belongs in that suite, which is faster and steadier.
The root `CLAUDE.md` says where the line is.

## What is in here

- `docker-compose.yml` — RabbitMQ with the management plugin, and Redis. The
  harness talks to both: the broker's management API is where queue depths,
  dead-letter contents, bindings and per-connection publish counters are read,
  and Redis is where cached messages are read. An application saying "I received
  it" proves nothing about where the message has been.
- `cmd/publisher`, `cmd/consumer` — small applications with a real `main()`, one
  process each, driven and interrogated over a stdlib HTTP server. Each one binds
  a kernel-chosen port and announces it on stdout, which is the only handshake
  with the runner.
- `cmd/runner` — runs the scripts step by step and prints the validation log.
- `internal/events` — the message types the applications share.
- `internal/rediscache` — a `gorabbit.Cache` speaking RESP over a socket, so the
  harness needs no Redis client dependency.
- `internal/mgmt` — the management API client.
- `internal/scenario` — the step machinery, the process control and the waits.

## Running it

From the repository root:

```sh
make e2e                       # everything: up, every script, down
make e2e-list                  # the script names
make e2e-up                    # just the infrastructure
make e2e-run SCENARIO=dlq      # one script (or a comma separated list)
make e2e-keep                  # run, then leave everything up
make e2e-down                  # tear down, including what e2e-keep left running
```

`make e2e-run` rebuilds the binaries first and expects the infrastructure to be
up. It also kills harness processes a previous `make e2e-keep` left behind —
those would otherwise reconnect to the vhost the next run recreates and the first
step would fail saying it found more connections than it expected.

Every script starts by deleting and recreating the `e2e` vhost and flushing
Redis, so running one alone means the same as running it inside a full pass.

### Poking at it by hand

`make e2e-keep` leaves the broker, Redis and the processes of the last script
running, and prints each process's URL and log file. From there:

```sh
curl -s localhost:PORT/health
curl -s -XPOST localhost:PORT/publish -d '{"kind":"vendor","route":"shopee","batch":"manual","count":3}'
curl -s localhost:PORT/published | jq
curl -s localhost:PORT/received | jq            # consumers
open http://127.0.0.1:15673                     # management, guest/guest
redis-cli -p 6380 --scan --pattern 'gorabbit:*'
```

Process logs are under `e2e/.logs/<script>/<process>.log` and hold the library's
own lifecycle lines.

## Reading the validation log

One line per claim, with the verdict and, underneath, the number that was
measured:

- `PASS` — the claim held; the line below is what was observed.
- `FAIL` — it did not; the line below says what went wrong, and every later step
  of that script is skipped rather than measured on a broken state.
- `OBS` — an observation. The harness measured something and states no verdict:
  it is information about the run, not a guarantee the library owes.
- `OBS?` — an observation that could not be measured; the run fails so it is
  never mistaken for a finding.
- `SKIP` — not run, because an earlier step of the script failed.
- `NOTE` — a line for the reader, never a measurement.

The exit code is non-zero if anything failed or could not be measured, and the
summary repeats each of those.

## The scripts

Every script asserts. `samekey` and `rolling` are the two about replicas sharing
one cache scope.

`samekey` drops both publisher connections in the middle of a flush and claims
that every cached message arrives and no message nobody cached does, and that the
repeats stay under a ceiling. The ceiling is there because delivery is
**at-least-once**: a message published and delivered whose confirmation dies with
the connection is read as a failed attempt, the reservation goes back and the
other process publishes it again. Claiming zero duplicates would assert
exactly-once, which the library never promised, and the test would be flaky. The
ceiling is a twentieth of the batch — well above the one or two that window has
cost when measured, and well below the defect it guards, which had both processes
republishing the whole cache.

`rolling` claims that the replica joining a deploy is never refused while the
outgoing one is still publishing, and that nothing the outgoing one confirmed is
lost when it dies mid-batch. A reservation that belonged to the client instead of
the message would refuse it and break the deploy.

What those two still only observe is information rather than a guarantee: the
broker's view of how many connections carry one application name, the in-process
guard against two clients of one binary, and what the cache scope holds when it
is over.

## Adding a script

1. Write the body in `cmd/runner/scenarios.go` as a `func(*scenario.Run)`, and
   register it in `scenarios()` with a name and a title.
2. Build it out of `run.Step` for what the harness is willing to assert and
   `run.Observe` for what it only measures. The assertion text is read by a human
   under time pressure: say what is being claimed, not which function is called.
3. Verify from outside the applications. `run.Env.Mgmt` is the broker's own view,
   `run.Cached`/`run.CachedCount` read the entries straight out of Redis.
4. Start processes with `run.Publisher` and `run.Consumer`; they are stopped for
   you unless the run was asked to keep them.
5. Never sleep to synchronise. `scenario.WaitFor` waits for a condition and fails
   saying what it wanted and what it last saw; `scenario.WaitStable` waits for a
   measurement to stop moving, which is what gives a claim about something *not*
   happening a defined moment to be made at.
6. If a script needs a change in the library to pass, stop and report it. It is a
   finding, not something the harness fixes.
