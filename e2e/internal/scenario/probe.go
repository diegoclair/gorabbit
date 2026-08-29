package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/diegoclair/gorabbit/e2e/internal/events"
)

func ctx() context.Context { return context.Background() }

// The library writes these entries; the harness reads them straight from Redis
// so a claim about a cached message never rests on the application's word.
const cachedMessagePrefix = "gorabbit:cached_messages"

func PublisherScope(appName string) string {
	return fmt.Sprintf("publisher:%s:%s", appName, events.Exchange{}.Name())
}

func ConsumerScope(appName, queue string) string {
	return fmt.Sprintf("consumer:%s:%s:%s", appName, events.Exchange{}.Name(), queue)
}

type Cached struct {
	Key         string `json:"-"`
	MsgID       string
	MsgTypeName string
	RoutingKey  string
	Message     []byte
}

// Payload is the application's own view of a cached message, which is what lets
// the runner match a cache entry against the message it asked for.
type Payload struct {
	Vendor string `json:"vendor"`
	Batch  string `json:"batch"`
	Seq    int    `json:"seq"`
}

func (c Cached) Payload() (Payload, error) {
	var p Payload
	err := json.Unmarshal(c.Message, &p)

	return p, err
}

func (r *Run) Cached(scope string) ([]Cached, error) {
	keys, err := r.Env.Cache.GetAllKeys(ctx(), fmt.Sprintf("%s:%s:*", cachedMessagePrefix, scope))
	if err != nil {
		return nil, err
	}

	sort.Strings(keys)

	messages := make([]Cached, 0, len(keys))
	for _, key := range keys {
		raw, err := r.Env.Cache.Get(ctx(), key)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}

		var cached Cached
		if err := json.Unmarshal(raw, &cached); err != nil {
			return nil, fmt.Errorf("cache entry %s is not the envelope the harness expects: %w", key, err)
		}
		cached.Key = key

		messages = append(messages, cached)
	}

	return messages, nil
}

func (r *Run) CachedCount(scope string) (int, error) {
	keys, err := r.Env.Cache.GetAllKeys(ctx(), fmt.Sprintf("%s:%s:*", cachedMessagePrefix, scope))

	return len(keys), err
}
