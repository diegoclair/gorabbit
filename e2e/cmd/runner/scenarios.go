package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/diegoclair/gorabbit/e2e/internal/api"
	"github.com/diegoclair/gorabbit/e2e/internal/scenario"
)

func scenarios() []scenario.Scenario {
	return []scenario.Scenario{
		{Name: "routing", Title: "who receives and who does not, counted at the broker", Body: routingScenario},
		{Name: "offline", Title: "the consumer is down: the cache holds the message and the library resends it", Body: offlineScenario},
		{Name: "dlq", Title: "retries run out: the dead-letter queue keeps the encoded route", Body: dlqScenario},
		{Name: "kill9", Title: "kill -9 in the middle of the work: another process finishes the delivery", Body: kill9Scenario},
		{Name: "samekey", Title: "two processes on one cache key split the flush instead of each repeating it", Body: sameKeyScenario},
		{Name: "rolling", Title: "rolling deploy: the new process is up before the old one dies, and nothing is lost", Body: rollingScenario},
		{Name: "memcache", Title: "the in-memory cache does not outlive the process", Body: memCacheScenario},
		{Name: "unbind", Title: "a deploy that drops a subscription unbinds what the previous run left", Body: unbindScenario},
	}
}

func routingScenario(r *scenario.Run) {
	const (
		mlQueue     = "e2e.ml"
		shopeeQueue = "e2e.shopee"
		orderQueue  = "e2e.orders"
	)

	var publisher, ml, shopee, orders *scenario.Proc

	r.Step("one publisher process and three consumer processes are up, each on its own broker connection", func() (string, error) {
		var err error
		if ml, err = r.Consumer("consumer-ml", "ml-app", mlQueue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		if shopee, err = r.Consumer("consumer-shopee", "shopee-app", shopeeQueue, "-sub", "vendor:"+routeShopee); err != nil {
			return "", err
		}
		if orders, err = r.Consumer("consumer-orders", "orders-app", orderQueue, "-sub", "order"); err != nil {
			return "", err
		}
		if publisher, err = r.Publisher("publisher", "e2e-routing-pub"); err != nil {
			return "", err
		}

		lines := make([]string, 0, 4)
		for _, process := range []*scenario.Proc{publisher, ml, shopee, orders} {
			line, err := waitConnected(process)
			if err != nil {
				return "", err
			}
			lines = append(lines, line)
		}

		return strings.Join(lines, " | "), nil
	})

	r.Step("the broker binds each queue to its own slice of the exchange and to nothing else", func() (string, error) {
		wanted := []struct {
			queue string
			keys  []string
		}{
			{mlQueue, []string{vendorRoutingKey(routeML)}},
			{shopeeQueue, []string{vendorRoutingKey(routeShopee)}},
			{orderQueue, []string{"OrderPlaced"}},
		}

		lines := make([]string, 0, len(wanted))
		for _, want := range wanted {
			line, err := waitBindings(r, want.queue, want.keys, 30*time.Second)
			if err != nil {
				return "", err
			}
			lines = append(lines, line)
		}

		return strings.Join(lines, " | "), nil
	})

	r.Step("the three consumer processes are stopped and the broker keeps their queues, all empty", func() (string, error) {
		for _, process := range []*scenario.Proc{ml, shopee, orders} {
			if err := process.Stop(); err != nil {
				return "", err
			}
		}

		observed, depths, err := depthsOf(r, mlQueue, shopeeQueue, orderQueue)
		if err != nil {
			return "", err
		}
		for queue, depth := range depths {
			if depth != 0 {
				return observed, fmt.Errorf("queue %s should be empty before the publish, it holds %d", queue, depth)
			}
		}

		return observed + " (all queues still declared)", nil
	})

	r.Step("the publisher sends 5 mercadolivre, 3 shopee and 2 orders with every consumer process down", func() (string, error) {
		sends := []api.PublishRequest{
			{Kind: "vendor", Route: routeML, Batch: "routing", Count: 5},
			{Kind: "vendor", Route: routeShopee, Batch: "routing", Count: 3},
			{Kind: "order", Batch: "routing", Count: 2},
		}

		total := 0
		for _, send := range sends {
			stats, err := publishSync(publisher, send)
			if err != nil {
				return "", err
			}
			total = stats.OK
		}

		return fmt.Sprintf("10 publishes accepted, %d recorded ok by the publisher process", total), nil
	})

	r.Step("the broker holds 5, 3 and 2 in the right queues, and no dead-letter queue holds anything", func() (string, error) {
		expected := map[string]int{
			mlQueue: 5, shopeeQueue: 3, orderQueue: 2,
			mlQueue + ".dlq": 0, shopeeQueue + ".dlq": 0, orderQueue + ".dlq": 0,
		}

		lines := make([]string, 0, len(expected))
		for _, queue := range []string{mlQueue, shopeeQueue, orderQueue, mlQueue + ".dlq", shopeeQueue + ".dlq", orderQueue + ".dlq"} {
			line, err := waitDepth(r, queue, expected[queue], 30*time.Second)
			if err != nil {
				return "", err
			}
			lines = append(lines, line)
		}

		return strings.Join(lines, " "), nil
	})

	r.Step("each consumer process, started again, takes its own slice and holds nothing that belongs to another", func() (string, error) {
		restarts := []struct {
			name, app, queue, sub, kind, vendor string
			want                                int
		}{
			{"consumer-ml-2", "ml-app", mlQueue, "vendor:" + routeML, "vendor", routeML, 5},
			{"consumer-shopee-2", "shopee-app", shopeeQueue, "vendor:" + routeShopee, "vendor", routeShopee, 3},
			{"consumer-orders-2", "orders-app", orderQueue, "order", "order", "", 2},
		}

		lines := make([]string, 0, len(restarts))
		for _, restart := range restarts {
			process, err := r.Consumer(restart.name, restart.app, restart.queue, "-sub", restart.sub)
			if err != nil {
				return "", err
			}
			if _, err := waitUnique(process, "", restart.want, 45*time.Second); err != nil {
				return "", err
			}

			answer, observed, err := settle(process, "", 2*time.Second, 30*time.Second)
			if err != nil {
				return "", err
			}
			if strangers := foreign(answer.Items, restart.kind, restart.vendor); len(strangers) > 0 {
				return observed, fmt.Errorf("%s received messages of another slice: %v", restart.name, strangers)
			}
			if answer.Unique != restart.want {
				return observed, fmt.Errorf("%s holds %d distinct messages, expected %d", restart.name, answer.Unique, restart.want)
			}

			lines = append(lines, observed+" (no foreign message)")
		}

		return strings.Join(lines, " | "), nil
	})

	r.Step("nothing was cached: every message had a queue waiting for it", func() (string, error) {
		count, err := r.CachedCount(scenario.PublisherScope("e2e-routing-pub"))
		if err != nil {
			return "", err
		}
		if count != 0 {
			return "", fmt.Errorf("the publisher cache scope holds %d entries, expected none", count)
		}

		return "the publisher cache scope in redis holds 0 entries", nil
	})
}

func offlineScenario(r *scenario.Run) {
	const (
		app   = "e2e-offline-pub"
		batch = "offline"
		queue = "e2e.offline"
	)

	var publisher, consumer *scenario.Proc
	var consumerStartedAt time.Time

	r.Step("the publisher process is up and the broker has no queue at all", func() (string, error) {
		var err error
		if publisher, err = r.Publisher("publisher", app); err != nil {
			return "", err
		}

		line, err := waitConnected(publisher)
		if err != nil {
			return "", err
		}

		queues, err := r.Env.Mgmt.Queues(bg())
		if err != nil {
			return "", err
		}
		if len(queues) != 0 {
			return "", fmt.Errorf("the broker already holds %d queue(s) before the script started", len(queues))
		}

		return line + " | the broker holds 0 queues", nil
	})

	r.Step("five publishes are accepted although no queue is bound to their route", func() (string, error) {
		stats, err := publishSync(publisher, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batch, Count: 5})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("attempted=%d ok=%d failed=%d", stats.Attempted, stats.OK, stats.Failed), nil
	})

	r.Step("redis holds the five messages, each keyed to the route it was published with", func() (string, error) {
		scope := scenario.PublisherScope(app)

		observed, err := scenario.WaitFor("redis to hold the five cached messages", 30*time.Second, func() (bool, string, error) {
			count, err := r.CachedCount(scope)
			if err != nil {
				return false, "", err
			}
			return count == 5, fmt.Sprintf("%d entries under %s", count, scope), nil
		})
		if err != nil {
			return observed, err
		}

		cached, err := r.Cached(scope)
		if err != nil {
			return observed, err
		}

		seqs, err := seqsOf(cached)
		if err != nil {
			return observed, err
		}
		for _, message := range cached {
			if message.RoutingKey != vendorRoutingKey(routeML) {
				return observed, fmt.Errorf("a cached message carries routing key %q, expected %q", message.RoutingKey, vendorRoutingKey(routeML))
			}
		}
		if len(seqs) != 5 {
			return observed, fmt.Errorf("the five cached messages are not five distinct ones: %v", seqs)
		}

		return fmt.Sprintf("%s, all with routing key %s, payload seqs %d..%d of batch %q",
			observed, vendorRoutingKey(routeML), 1, 5, batch), nil
	})

	r.Step("the broker holds none of them", func() (string, error) {
		queues, err := r.Env.Mgmt.Queues(bg())
		if err != nil {
			return "", err
		}
		if len(queues) != 0 {
			return "", fmt.Errorf("the broker holds %d queue(s), so the messages could have gone somewhere", len(queues))
		}

		return "the broker holds 0 queues, so the five messages exist only in redis", nil
	})

	r.Step("the consumer process starts and the broker shows its binding", func() (string, error) {
		var err error
		if consumer, err = r.Consumer("consumer", "offline-app", queue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		consumerStartedAt = time.Now()

		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 30*time.Second)
	})

	r.Step("the library resends the five on its own, with no nudge from the harness", func() (string, error) {
		observed, err := waitUnique(consumer, batch, 5, 150*time.Second)
		if err != nil {
			return observed, err
		}

		return fmt.Sprintf("%s, %.1fs after the consumer process came up", observed, time.Since(consumerStartedAt).Seconds()), nil
	})

	r.Step("the cache scope is empty once the messages are delivered", func() (string, error) {
		return scenario.WaitFor("the publisher cache scope to drain", 30*time.Second, func() (bool, string, error) {
			count, err := r.CachedCount(scenario.PublisherScope(app))
			if err != nil {
				return false, "", err
			}
			return count == 0, fmt.Sprintf("%d entries left in the cache scope", count), nil
		})
	})
}

func dlqScenario(r *scenario.Run) {
	const (
		queue = "e2e.dlq-demo"
		batch = "dlq"
	)

	var publisher, consumer *scenario.Proc

	r.Step("a consumer that always fails is up, bound by the encoded route and not by the bare type name", func() (string, error) {
		var err error
		consumer, err = r.Consumer("consumer-failing", "dlq-app", queue,
			"-sub", "vendor:"+routeDotted,
			"-fail-always",
			"-retry-count", "2",
			"-retry-interval", "1s")
		if err != nil {
			return "", err
		}
		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{encodedDottedKey}, 30*time.Second)
	})

	r.Step("the publisher sends one message on that route", func() (string, error) {
		var err error
		if publisher, err = r.Publisher("publisher", "e2e-dlq-pub"); err != nil {
			return "", err
		}
		if _, err := waitConnected(publisher); err != nil {
			return "", err
		}

		stats, err := publishSync(publisher, api.PublishRequest{Kind: "vendor", Route: routeDotted, Batch: batch, Count: 1})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("attempted=%d ok=%d", stats.Attempted, stats.OK), nil
	})

	r.Step("the handler runs three times: the first attempt and the two retries", func() (string, error) {
		return scenario.WaitFor("the handler to be called three times", 60*time.Second, func() (bool, string, error) {
			answer, err := received(consumer, batch)
			if err != nil {
				return false, "", err
			}
			if len(answer.Items) == 0 {
				return false, "no delivery yet", nil
			}

			return answer.Items[0].Deliveries == 3, fmt.Sprintf("handler calls=%d", answer.Items[0].Deliveries), nil
		})
	})

	r.Step("the broker holds the message in the dead-letter queue, with the main and retry queues empty", func() (string, error) {
		lines := make([]string, 0, 3)
		for _, want := range []struct {
			queue string
			depth int
		}{
			{queue + ".dlq", 1},
			{queue, 0},
			{queue + ".retry", 0},
		} {
			line, err := waitDepth(r, want.queue, want.depth, 60*time.Second)
			if err != nil {
				return "", err
			}
			lines = append(lines, line)
		}

		return strings.Join(lines, " "), nil
	})

	r.Step("the dead-lettered message still carries the encoded route, not the bare type name", func() (string, error) {
		messages, err := r.Env.Mgmt.Get(bg(), queue+".dlq", 5)
		if err != nil {
			return "", err
		}
		if len(messages) != 1 {
			return "", fmt.Errorf("the dead-letter queue answered %d messages, expected 1", len(messages))
		}
		if messages[0].RoutingKey != encodedDottedKey {
			return "", fmt.Errorf("the dead-lettered message carries routing key %q, expected %q", messages[0].RoutingKey, encodedDottedKey)
		}

		return fmt.Sprintf("routing key at the dead-letter queue = %q (published route was %q)", messages[0].RoutingKey, routeDotted), nil
	})

	r.Step("the dead-lettered message names the exchange it was published to", func() (string, error) {
		messages, err := r.Env.Mgmt.Get(bg(), queue+".dlq", 5)
		if err != nil {
			return "", err
		}
		if len(messages) != 1 {
			return "", fmt.Errorf("the dead-letter queue answered %d messages, expected 1", len(messages))
		}

		origin, _ := messages[0].Properties.Headers[originExchangeHead].(string)
		if origin != exchangeName {
			return "", fmt.Errorf("header %s is %q, expected %q", originExchangeHead, origin, exchangeName)
		}

		return fmt.Sprintf("%s=%q", originExchangeHead, origin), nil
	})
}

func kill9Scenario(r *scenario.Run) {
	const (
		app   = "e2e-killed"
		batch = "killed"
		queue = "e2e.killed"
	)

	var first, second, consumer *scenario.Proc
	var survivors map[int]string
	scope := scenario.PublisherScope(app)

	r.Step("the first publisher process is up while nothing is bound to the route", func() (string, error) {
		var err error
		if first, err = r.Publisher("publisher-a", app); err != nil {
			return "", err
		}

		return waitConnected(first)
	})

	r.Step("it starts a batch of 60 in the background and redis fills up while it works", func() (string, error) {
		err := publishAsync(first, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batch, Count: 60, DelayMS: 30})
		if err != nil {
			return "", err
		}

		return scenario.WaitFor("redis to hold at least 20 cached messages", 60*time.Second, func() (bool, string, error) {
			count, err := r.CachedCount(scope)
			if err != nil {
				return false, "", err
			}
			return count >= 20, fmt.Sprintf("%d entries cached so far", count), nil
		})
	})

	r.Step("kill -9 takes the publisher process away with no chance to close its client", func() (string, error) {
		if err := first.Kill(); err != nil {
			return "", err
		}

		return "the operating system reports: " + first.ExitStatus(), nil
	})

	r.Step("what it had cached is still in redis after its process is gone", func() (string, error) {
		cached, err := r.Cached(scope)
		if err != nil {
			return "", err
		}
		if len(cached) == 0 {
			return "", fmt.Errorf("nothing survived in the cache scope %s", scope)
		}

		survivors, err = seqsOf(cached)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%d cached messages survived the kill, all under %s", len(survivors), scope), nil
	})

	r.Step("the consumer process starts, so the route finally has a queue", func() (string, error) {
		var err error
		if consumer, err = r.Consumer("consumer", "killed-app", queue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 30*time.Second)
	})

	r.Step("a second process on the same cache finishes the delivery the dead one started", func() (string, error) {
		var err error
		if second, err = r.Publisher("publisher-b", app); err != nil {
			return "", err
		}
		if _, err := waitConnected(second); err != nil {
			return "", err
		}

		if _, err := waitUnique(consumer, batch, len(survivors), 150*time.Second); err != nil {
			return "", err
		}

		answer, observed, err := settle(consumer, batch, 2*time.Second, 60*time.Second)
		if err != nil {
			return observed, err
		}
		if missing := missingSeqs(survivors, answer.Items); len(missing) > 0 {
			return observed, fmt.Errorf("messages cached before the kill never arrived: seqs %v", missing)
		}

		return fmt.Sprintf("%s — all %d survivors delivered, and the second process published none of its own",
			observed, len(survivors)), nil
	})

	r.Step("the cache scope is empty once the survivors are delivered", func() (string, error) {
		return scenario.WaitFor("the cache scope to drain", 60*time.Second, func() (bool, string, error) {
			count, err := r.CachedCount(scope)
			if err != nil {
				return false, "", err
			}
			return count == 0, fmt.Sprintf("%d entries left", count), nil
		})
	})
}

func sameKeyScenario(r *scenario.Run) {
	const (
		app   = "e2e-shared"
		batch = "shared"
		queue = "e2e.shared"
		// Enough messages that one flush takes long enough for a second one,
		// started shortly after, to still be reading the same entries.
		total = 400
		// Delivery is at-least-once: a repeat is the contract, and only a large
		// share of the batch coming twice is the defect this script guards.
		maxDuplicates = total / 20
	)

	var first, second, consumer *scenario.Proc
	var firstConn, secondConn string
	var cachedSeqs map[int]string
	var delivered api.Received
	scope := scenario.PublisherScope(app)

	r.Step("the first publisher process is up while nothing is bound to the route", func() (string, error) {
		var err error
		if first, err = r.Publisher("publisher-a", app); err != nil {
			return "", err
		}
		line, err := waitConnected(first)
		if err != nil {
			return "", err
		}

		names, observed, err := waitConnections(r, app, 1, 30*time.Second)
		if err != nil {
			return observed, err
		}
		firstConn = names[0]

		return line + " | " + observed, nil
	})

	r.Observe("does a second client with the same cache key inside the SAME process connect?", func() (string, error) {
		var twin api.TwinResult
		if err := first.PostJSON("/claim-twin", struct{}{}, &twin); err != nil {
			return "", err
		}
		if twin.Error == "" {
			return "it connected, the library raised nothing", nil
		}

		return fmt.Sprintf("it did not connect; the library answered: %s", twin.Error), nil
	})

	r.Step(fmt.Sprintf("the first process caches %d messages nobody is bound to", total), func() (string, error) {
		if _, err := publishSync(first, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batch, Count: total}); err != nil {
			return "", err
		}

		observed, err := scenario.WaitFor("redis to hold the cached messages", 60*time.Second, func() (bool, string, error) {
			count, err := r.CachedCount(scope)
			if err != nil {
				return false, "", err
			}
			return count == total, fmt.Sprintf("%d entries under %s", count, scope), nil
		})
		if err != nil {
			return observed, err
		}

		cached, err := r.Cached(scope)
		if err != nil {
			return observed, err
		}
		if cachedSeqs, err = seqsOf(cached); err != nil {
			return observed, err
		}
		if len(cachedSeqs) != total {
			return observed, fmt.Errorf("the entries under %s are %d distinct messages, not %d", scope, len(cachedSeqs), total)
		}

		return fmt.Sprintf("%s, holding %d distinct payloads of batch %q", observed, len(cachedSeqs), batch), nil
	})

	r.Observe("does a second PROCESS with the same cache key connect while the first is alive?", func() (string, error) {
		process, err := r.Publisher("publisher-b", app)
		if err != nil {
			return "the second process did not come up: " + err.Error(), nil
		}
		second = process

		line, err := waitConnected(second)
		if err != nil {
			return "", err
		}

		names, observed, err := waitConnections(r, app, 2, 30*time.Second)
		if err != nil {
			return observed, err
		}
		for _, name := range names {
			if name != firstConn {
				secondConn = name
			}
		}

		return "it came up and connected — " + line + " | " + observed, nil
	})

	r.Observe(fmt.Sprintf("how many messages did the second process take out of the first process's cache? (the harness asked it for none, and the first for %d)", total), func() (string, error) {
		if secondConn == "" {
			return "the second connection could not be told apart, so nothing can be attributed", nil
		}

		totals, observed, err := waitPublished(r, secondConn, total, 90*time.Second)
		if err != nil {
			return observed, nil
		}

		return fmt.Sprintf("counted by the broker, per connection — first %s: published=%d unroutable=%d | second %s: published=%d unroutable=%d",
			firstConn, totals[firstConn].Publish, totals[firstConn].Unroutable,
			secondConn, totals[secondConn].Publish, totals[secondConn].Unroutable), nil
	})

	r.Step("the consumer process starts and both publisher connections are dropped at once, so both flush the same cache together", func() (string, error) {
		var err error
		if consumer, err = r.Consumer("consumer", "shared-app", queue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}
		if _, err := waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 30*time.Second); err != nil {
			return "", err
		}

		names, err := connectionsOf(r, app)
		if err != nil {
			return "", err
		}
		// Without both of them the claims below would be measured on a race
		// that never happened.
		if len(names) != 2 {
			return "", fmt.Errorf("the broker shows %d connection(s) named %q, so two processes are not flushing one cache: %v", len(names), app, names)
		}
		for _, name := range names {
			if err := r.Env.Mgmt.CloseConnection(bg(), name); err != nil {
				return "", err
			}
		}

		return fmt.Sprintf("queue %s is bound and the broker dropped both connections named %q: %v", queue, app, names), nil
	})

	r.Step(fmt.Sprintf("every one of the %d cached messages is delivered, and nothing nobody cached is", total), func() (string, error) {
		if _, err := waitUnique(consumer, batch, total, 180*time.Second); err != nil {
			return "", err
		}

		answer, observed, err := settle(consumer, batch, 5*time.Second, 120*time.Second)
		if err != nil {
			return observed, err
		}
		delivered = answer

		if missing := missingSeqs(cachedSeqs, answer.Items); len(missing) > 0 {
			return observed, fmt.Errorf("%d cached message(s) never arrived: %v", len(missing), missing)
		}
		if strangers := foreign(answer.Items, "vendor", routeML); len(strangers) > 0 {
			return observed, fmt.Errorf("%d delivered message(s) match nothing that was cached: %v", len(strangers), strangers)
		}
		if answer.Unique != len(cachedSeqs) {
			return observed, fmt.Errorf("%d distinct messages arrived for %d cached ones", answer.Unique, len(cachedSeqs))
		}

		return fmt.Sprintf("cached=%d | delivered unique=%d total=%d", len(cachedSeqs), answer.Unique, answer.Total), nil
	})

	r.Note("at-least-once is the contract: a message published and delivered whose confirmation dies with the connection is read as a failed attempt, the reservation goes back, and the other process publishes it again")
	r.Note(fmt.Sprintf("hence a ceiling instead of zero — %d is %d%% of the batch: far above the one or two that window has cost in measurement, and far below the defect it guards, which had both processes republishing the whole cache, near %d duplicates",
		maxDuplicates, 100*maxDuplicates/total, total))

	r.Step(fmt.Sprintf("at most %d of the %d arrive twice, so the processes split the flush instead of each repeating it", maxDuplicates, total), func() (string, error) {
		if delivered.Duplicates > maxDuplicates {
			return "", fmt.Errorf("%d of the %d arrived more than once (total=%d), over the ceiling of %d: the two processes are publishing the same entries instead of splitting them",
				delivered.Duplicates, total, delivered.Total, maxDuplicates)
		}

		return fmt.Sprintf("duplicates=%d against a ceiling of %d | delivered total=%d for %d cached", delivered.Duplicates, maxDuplicates, delivered.Total, total), nil
	})

	r.Observe("what is left in the shared cache scope?", func() (string, error) {
		count, err := r.CachedCount(scope)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%d entries left under %s", count, scope), nil
	})
}

func rollingScenario(r *scenario.Run) {
	const (
		app    = "e2e-roll"
		queue  = "e2e.roll"
		batchA = "roll-a"
		batchB = "roll-b"
	)

	var first, second, consumer *scenario.Proc
	attemptedByFirst, confirmedByFirst := 0, 0
	scope := scenario.PublisherScope(app)

	r.Step("the consumer process is up and bound before anything is published", func() (string, error) {
		var err error
		if consumer, err = r.Consumer("consumer", "roll-app", queue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 30*time.Second)
	})

	r.Step("the running version is up and working through a long batch", func() (string, error) {
		var err error
		if first, err = r.Publisher("publisher-old", app); err != nil {
			return "", err
		}
		if _, err := waitConnected(first); err != nil {
			return "", err
		}

		err = publishAsync(first, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batchA, Count: 400, DelayMS: 30})
		if err != nil {
			return "", err
		}

		return scenario.WaitFor("the running version to be in the middle of its batch", 60*time.Second, func() (bool, string, error) {
			stats, err := batchStats(first, batchA)
			if err != nil {
				return false, "", err
			}
			return stats.Attempted >= 30, fmt.Sprintf("attempted=%d of %d", stats.Attempted, stats.Requested), nil
		})
	})

	r.Step("the new version is not refused at the door: it connects while the running one is still publishing", func() (string, error) {
		var err error
		if second, err = r.Publisher("publisher-new", app); err != nil {
			return "", err
		}

		line, err := waitConnected(second)
		if err != nil {
			return "", err
		}

		stats, err := batchStats(first, batchA)
		if err != nil {
			return "", err
		}
		if stats.Attempted >= stats.Requested {
			return "", fmt.Errorf("the running version had already finished its batch (attempted=%d of %d), so the versions never overlapped", stats.Attempted, stats.Requested)
		}

		return fmt.Sprintf("%s | the running version is at attempted=%d of %d", line, stats.Attempted, stats.Requested), nil
	})

	r.Observe("how many broker connections carry this application name during the overlap?", func() (string, error) {
		_, observed, err := waitConnections(r, app, 2, 30*time.Second)
		if err != nil {
			current, listErr := connectionsOf(r, app)
			if listErr != nil {
				return "", listErr
			}
			return fmt.Sprintf("%d connection(s): %v", len(current), current), nil
		}

		return observed, nil
	})

	r.Step("the old version dies in the middle of its batch", func() (string, error) {
		stats, err := batchStats(first, batchA)
		if err != nil {
			return "", err
		}
		attemptedByFirst, confirmedByFirst = stats.Attempted, stats.OK

		if err := first.Kill(); err != nil {
			return "", err
		}
		if attemptedByFirst >= stats.Requested {
			return "", fmt.Errorf("it had already published its whole batch (attempted=%d of %d), so nothing was interrupted", attemptedByFirst, stats.Requested)
		}

		return fmt.Sprintf("attempted=%d ok=%d of %d at the last reading before the kill; the operating system reports: %s",
			attemptedByFirst, confirmedByFirst, stats.Requested, first.ExitStatus()), nil
	})

	r.Step("the new version keeps publishing after the old one is gone", func() (string, error) {
		stats, err := publishSync(second, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batchB, Count: 30})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("the new version published attempted=%d ok=%d failed=%d", stats.Attempted, stats.OK, stats.Failed), nil
	})

	r.Step("nothing the old version had published is lost, and everything the new one published arrives", func() (string, error) {
		if _, err := waitUnique(consumer, batchB, 30, 90*time.Second); err != nil {
			return "", err
		}

		fromOld, oldObserved, err := settle(consumer, batchA, 4*time.Second, 90*time.Second)
		if err != nil {
			return oldObserved, err
		}
		fromNew, newObserved, err := settle(consumer, batchB, 4*time.Second, 90*time.Second)
		if err != nil {
			return newObserved, err
		}

		if fromOld.Unique < confirmedByFirst {
			return oldObserved, fmt.Errorf("the old version had %d publishes confirmed before it died and only %d of them arrived", confirmedByFirst, fromOld.Unique)
		}
		if fromNew.Unique != 30 {
			return newObserved, fmt.Errorf("the new version published 30 messages and %d distinct ones arrived", fromNew.Unique)
		}

		return fmt.Sprintf("old version: confirmed>=%d, delivered total=%d unique=%d duplicates=%d | new version: delivered total=%d unique=%d duplicates=%d",
			confirmedByFirst, fromOld.Total, fromOld.Unique, fromOld.Duplicates, fromNew.Total, fromNew.Unique, fromNew.Duplicates), nil
	})

	r.Observe("what is left in the shared cache scope after the overlap?", func() (string, error) {
		count, err := r.CachedCount(scope)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%d entries left under %s", count, scope), nil
	})

	r.Note("a claim that refused the second process would have broken this sequence, which is why the reservation belongs to the message and not to the client")
}

func memCacheScenario(r *scenario.Run) {
	const (
		app     = "e2e-mem"
		queue   = "e2e.mem"
		batch   = "mem-lost"
		control = "mem-control"
	)

	var first, second, consumer *scenario.Proc

	r.Step("a publisher process with the in-memory cache is up while nothing is bound", func() (string, error) {
		var err error
		if first, err = r.Publisher("publisher-memory", app, "-cache", "memory"); err != nil {
			return "", err
		}

		return waitConnected(first)
	})

	r.Step("it caches ten messages and redis stays empty, so the cache really is inside the process", func() (string, error) {
		if _, err := publishSync(first, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: batch, Count: 10}); err != nil {
			return "", err
		}

		keys, err := r.Env.Cache.GetAllKeys(bg(), "*")
		if err != nil {
			return "", err
		}
		if len(keys) != 0 {
			return "", fmt.Errorf("redis holds %d key(s), so this process was not using the in-memory cache", len(keys))
		}

		return "10 publishes accepted, redis holds 0 keys", nil
	})

	r.Step("kill -9 takes the process and its cache away", func() (string, error) {
		if err := first.Kill(); err != nil {
			return "", err
		}

		return "the operating system reports: " + first.ExitStatus(), nil
	})

	r.Step("the consumer process starts and a replacement publisher with the same name comes up", func() (string, error) {
		var err error
		if consumer, err = r.Consumer("consumer", "mem-app", queue, "-sub", "vendor:"+routeML); err != nil {
			return "", err
		}
		if _, err := waitConnected(consumer); err != nil {
			return "", err
		}
		if _, err := waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 30*time.Second); err != nil {
			return "", err
		}

		if second, err = r.Publisher("publisher-memory-2", app, "-cache", "memory"); err != nil {
			return "", err
		}

		return waitConnected(second)
	})

	r.Step("a control batch from the replacement arrives, proving the route works now", func() (string, error) {
		if _, err := publishSync(second, api.PublishRequest{Kind: "vendor", Route: routeML, Batch: control, Count: 3}); err != nil {
			return "", err
		}

		return waitUnique(consumer, control, 3, 60*time.Second)
	})

	r.Step("none of the ten cached before the kill ever arrive", func() (string, error) {
		answer, observed, err := settle(consumer, batch, 4*time.Second, 60*time.Second)
		if err != nil {
			return observed, err
		}
		if answer.Total != 0 {
			return observed, fmt.Errorf("%d message(s) cached in memory before the kill arrived anyway", answer.Total)
		}

		return observed + " — the in-memory cache died with the process, as documented", nil
	})
}

func unbindScenario(r *scenario.Run) {
	const (
		app   = "e2e-dep"
		queue = "e2e.dep"
		batch = "dep"
	)

	var older, newer, publisher *scenario.Proc

	r.Step("the running version consumes two routes and the broker shows both bindings", func() (string, error) {
		var err error
		older, err = r.Consumer("consumer-old", app, queue,
			"-sub", "vendor:"+routeML,
			"-sub", "vendor:"+routeShopee)
		if err != nil {
			return "", err
		}
		if _, err := waitConnected(older); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{vendorRoutingKey(routeML), vendorRoutingKey(routeShopee)}, 30*time.Second)
	})

	r.Step("that process is stopped the way a deploy stops it", func() (string, error) {
		if err := older.Stop(); err != nil {
			return "", err
		}

		return "the operating system reports: " + older.ExitStatus(), nil
	})

	r.Step("the next version subscribes one route, and the binding the previous run left is removed", func() (string, error) {
		var err error
		newer, err = r.Consumer("consumer-new", app, queue, "-sub", "vendor:"+routeML)
		if err != nil {
			return "", err
		}
		if _, err := waitConnected(newer); err != nil {
			return "", err
		}

		return waitBindings(r, queue, []string{vendorRoutingKey(routeML)}, 60*time.Second)
	})

	r.Step("the kept route is delivered and the dropped one has nowhere to go", func() (string, error) {
		var err error
		if publisher, err = r.Publisher("publisher", "e2e-dep-pub"); err != nil {
			return "", err
		}
		if _, err := waitConnected(publisher); err != nil {
			return "", err
		}

		for _, route := range []string{routeML, routeShopee} {
			if _, err := publishSync(publisher, api.PublishRequest{Kind: "vendor", Route: route, Batch: batch, Count: 2}); err != nil {
				return "", err
			}
		}

		if _, err := waitUnique(newer, batch, 2, 60*time.Second); err != nil {
			return "", err
		}

		answer, observed, err := settle(newer, batch, 4*time.Second, 60*time.Second)
		if err != nil {
			return observed, err
		}
		if strangers := foreign(answer.Items, "vendor", routeML); len(strangers) > 0 {
			return observed, fmt.Errorf("the dropped route was delivered anyway: %v", strangers)
		}

		cached, err := r.Cached(scenario.PublisherScope("e2e-dep-pub"))
		if err != nil {
			return observed, err
		}
		for _, message := range cached {
			if message.RoutingKey != vendorRoutingKey(routeShopee) {
				return observed, fmt.Errorf("an unexpected message is cached with routing key %q", message.RoutingKey)
			}
		}
		if len(cached) != 2 {
			return observed, fmt.Errorf("the cache holds %d message(s) for the dropped route, expected 2", len(cached))
		}

		return fmt.Sprintf("%s | 2 messages of the dropped route sit in redis with routing key %s and no queue",
			observed, vendorRoutingKey(routeShopee)), nil
	})
}
