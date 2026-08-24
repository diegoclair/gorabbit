package gorabbit

import (
	"context"
	"sync"
	"time"
)

// Cache is the store drivers use to survive broker downtime (messages that
// could not be published) and to track queue bindings across restarts.
// A ttl of zero means no expiration; Get returns nil data and a nil error when
// the key is absent; GetAllKeys receives a glob pattern (`*` and `?`).
type Cache interface {
	Set(ctx context.Context, key string, data []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetAllKeys(ctx context.Context, pattern string) ([]string, error)
	Delete(ctx context.Context, keys ...string) error
}

type memoryCache struct {
	mu      sync.RWMutex
	entries map[string]memoryEntry
}

type memoryEntry struct {
	data      []byte
	expiresAt time.Time
}

// NewMemoryCache returns a process-local Cache. Everything it holds is lost on
// restart, so messages cached during a broker outage do not survive one, and
// bindings are not shared between instances — use a shared store (Redis) when
// that matters.
func NewMemoryCache() Cache {
	return &memoryCache{entries: make(map[string]memoryEntry)}
}

func (c *memoryCache) Set(_ context.Context, key string, data []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := memoryEntry{data: data}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	c.entries[key] = entry

	return nil
}

func (c *memoryCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[key]
	if !ok || entry.expired() {
		return nil, nil
	}

	return entry.data, nil
}

func (c *memoryCache) GetAllKeys(_ context.Context, pattern string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var keys []string
	for key, entry := range c.entries {
		if !entry.expired() && globMatch(pattern, key) {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

func (c *memoryCache) Delete(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keys {
		delete(c.entries, key)
	}

	return nil
}

func (e memoryEntry) expired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

// globMatch matches `*` and `?` with no path-separator semantics, the way a
// Redis KEYS pattern behaves.
func globMatch(pattern, s string) bool {
	var p, i, starP, starI int
	starP = -1

	for i < len(s) {
		switch {
		case p < len(pattern) && (pattern[p] == s[i] || pattern[p] == '?'):
			p++
			i++
		case p < len(pattern) && pattern[p] == '*':
			starP, starI = p, i
			p++
		case starP >= 0:
			p = starP + 1
			starI++
			i = starI
		default:
			return false
		}
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}

	return p == len(pattern)
}
