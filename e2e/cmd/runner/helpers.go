package main

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/diegoclair/gorabbit/e2e/internal/api"
	"github.com/diegoclair/gorabbit/e2e/internal/mgmt"
	"github.com/diegoclair/gorabbit/e2e/internal/scenario"
)

const (
	exchangeName = "e2e.events"
	routeML      = "mercadolivre"
	routeShopee  = "shopee"
	// A route carrying a character the topic exchange reads as a separator, so
	// the harness can tell an encoded key from a bare type name.
	routeDotted        = "mercado.livre"
	encodedDottedKey   = "VendorEvent.mercado%2Elivre"
	originExchangeHead = "x-origin-exchange"
)

func bg() context.Context { return context.Background() }

func waitConnected(p *scenario.Proc) (string, error) {
	return scenario.WaitFor(p.Name+" holding a live broker connection", 45*time.Second, func() (bool, string, error) {
		var health api.Health
		if err := p.GetJSON("/health", &health); err != nil {
			return false, "", err
		}

		return health.Connected, fmt.Sprintf("%s: app=%s connected=%t", p.Name, health.App, health.Connected), nil
	})
}

func received(p *scenario.Proc, batch string) (api.Received, error) {
	var answer api.Received
	path := "/received"
	if batch != "" {
		path += "?batch=" + url.QueryEscape(batch)
	}

	return answer, p.GetJSON(path, &answer)
}

func describe(p *scenario.Proc, r api.Received) string {
	return fmt.Sprintf("%s: total=%d unique=%d duplicates=%d", p.Name, r.Total, r.Unique, r.Duplicates)
}

func waitUnique(p *scenario.Proc, batch string, want int, timeout time.Duration) (string, error) {
	what := fmt.Sprintf("%s to hold %d distinct messages of batch %q", p.Name, want, batch)

	return scenario.WaitFor(what, timeout, func() (bool, string, error) {
		answer, err := received(p, batch)
		if err != nil {
			return false, "", err
		}

		return answer.Unique >= want, describe(p, answer), nil
	})
}

// settle gives a claim about something NOT arriving a defined moment to be made
// at: the counters stopped moving, so what is missing is missing.
func settle(p *scenario.Proc, batch string, quiet, timeout time.Duration) (api.Received, string, error) {
	observed, err := scenario.WaitStable(p.Name+" delivery counters", quiet, timeout, func() (string, error) {
		answer, err := received(p, batch)
		if err != nil {
			return "", err
		}

		return describe(p, answer), nil
	})
	if err != nil {
		return api.Received{}, observed, err
	}

	answer, err := received(p, batch)

	return answer, observed, err
}

func publishSync(p *scenario.Proc, req api.PublishRequest) (api.BatchStats, error) {
	var out api.BatchStats
	req.Async = false

	if err := p.PostJSON("/publish", req, &out); err != nil {
		return out, err
	}
	if out.Failed > 0 {
		return out, fmt.Errorf("%s reported %d failed publishes: %s", p.Name, out.Failed, strings.Join(out.Errors, "; "))
	}

	return out, nil
}

func publishAsync(p *scenario.Proc, req api.PublishRequest) error {
	req.Async = true
	return p.PostJSON("/publish", req, nil)
}

func batchStats(p *scenario.Proc, batch string) (api.BatchStats, error) {
	var out api.BatchStats
	return out, p.GetJSON("/published?batch="+url.QueryEscape(batch), &out)
}

func queueDepth(r *scenario.Run, name string) (int, error) {
	queue, err := r.Env.Mgmt.Queue(bg(), name)
	if err != nil {
		return 0, err
	}

	return queue.Messages, nil
}

func waitDepth(r *scenario.Run, name string, want int, timeout time.Duration) (string, error) {
	what := fmt.Sprintf("the broker to report %d message(s) in %s", want, name)

	return scenario.WaitFor(what, timeout, func() (bool, string, error) {
		depth, err := queueDepth(r, name)
		if err != nil {
			return false, "", err
		}

		return depth == want, fmt.Sprintf("%s holds %d", name, depth), nil
	})
}

func depthsOf(r *scenario.Run, names ...string) (string, map[string]int, error) {
	depths := map[string]int{}
	parts := make([]string, 0, len(names))

	for _, name := range names {
		depth, err := queueDepth(r, name)
		if err != nil {
			return "", nil, err
		}
		depths[name] = depth
		parts = append(parts, fmt.Sprintf("%s=%d", name, depth))
	}

	return strings.Join(parts, " "), depths, nil
}

// The library binds the same queue for its own retry and dead-letter plumbing,
// and those bindings are not what a routing claim is about.
func bindingsFrom(r *scenario.Run, queue, source string) ([]string, error) {
	bindings, err := r.Env.Mgmt.QueueBindings(bg(), queue)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Source == source {
			keys = append(keys, binding.RoutingKey)
		}
	}
	sort.Strings(keys)

	return keys, nil
}

func waitBindings(r *scenario.Run, queue string, want []string, timeout time.Duration) (string, error) {
	what := fmt.Sprintf("queue %s to be bound to %s by exactly %v", queue, exchangeName, want)

	return scenario.WaitFor(what, timeout, func() (bool, string, error) {
		keys, err := bindingsFrom(r, queue, exchangeName)
		if err != nil {
			return false, "", err
		}

		return equal(keys, want), fmt.Sprintf("%s <- %s: %v", queue, exchangeName, keys), nil
	})
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}

func connectionsOf(r *scenario.Run, appName string) ([]string, error) {
	connections, err := r.Env.Mgmt.Connections(bg())
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(connections))
	for _, connection := range connections {
		if mgmt.ConnectionName(connection) == appName {
			names = append(names, connection.Name)
		}
	}
	sort.Strings(names)

	return names, nil
}

func waitConnections(r *scenario.Run, appName string, want int, timeout time.Duration) ([]string, string, error) {
	var names []string

	observed, err := scenario.WaitFor(fmt.Sprintf("the broker to show %d connection(s) named %q", want, appName), timeout,
		func() (bool, string, error) {
			current, err := connectionsOf(r, appName)
			if err != nil {
				return false, "", err
			}
			names = current

			return len(current) == want, fmt.Sprintf("%d connection(s) named %q: %v", len(current), appName, current), nil
		})

	return names, observed, err
}

func vendorRoutingKey(route string) string {
	return "VendorEvent." + route
}

// A cache entry is matched to a later delivery by the application's own
// identity, never by the library's message id.
func seqsOf(messages []scenario.Cached) (map[int]string, error) {
	seqs := map[int]string{}

	for _, message := range messages {
		payload, err := message.Payload()
		if err != nil {
			return nil, err
		}
		seqs[payload.Seq] = payload.Batch
	}

	return seqs, nil
}

func missingSeqs(want map[int]string, got []api.Item) []int {
	arrived := map[int]bool{}
	for _, item := range got {
		arrived[item.Seq] = true
	}

	var missing []int
	for seq := range want {
		if !arrived[seq] {
			missing = append(missing, seq)
		}
	}
	sort.Ints(missing)

	return missing
}

func foreign(items []api.Item, kind, vendor string) []string {
	var strangers []string

	for _, item := range items {
		if item.Kind != kind || (kind == "vendor" && item.Vendor != vendor) {
			strangers = append(strangers, fmt.Sprintf("%s/%s/%s#%d", item.Kind, item.Vendor, item.Batch, item.Seq))
		}
	}

	return strangers
}

// What a process published has to come from the broker, not from the process,
// when two of them carry the same application name.
func waitPublished(r *scenario.Run, connection string, want int, timeout time.Duration) (map[string]mgmt.Totals, string, error) {
	var totals map[string]mgmt.Totals

	observed, err := scenario.WaitFor(fmt.Sprintf("the broker to credit connection %s with %d publishes", connection, want), timeout,
		func() (bool, string, error) {
			current, err := r.Env.Mgmt.PublishTotals(bg())
			if err != nil {
				return false, "", err
			}
			totals = current

			return current[connection].Publish >= want, formatTotals(current), nil
		})

	return totals, observed, err
}

func formatTotals(totals map[string]mgmt.Totals) string {
	names := make([]string, 0, len(totals))
	for name := range totals {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s: published=%d unroutable=%d", name, totals[name].Publish, totals[name].Unroutable))
	}

	return strings.Join(parts, " | ")
}
