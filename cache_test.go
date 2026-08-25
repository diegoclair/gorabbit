package gorabbit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryCache(t *testing.T) {
	ctx := context.Background()

	t.Run("get returns no data and no error for an absent key", func(t *testing.T) {
		c := NewMemoryCache()

		data, err := c.Get(ctx, "missing")
		require.NoError(t, err)
		require.Nil(t, data)
	})

	t.Run("stores and reads back", func(t *testing.T) {
		c := NewMemoryCache()
		require.NoError(t, c.Set(ctx, "key", []byte("value"), 0))

		data, err := c.Get(ctx, "key")
		require.NoError(t, err)
		require.Equal(t, []byte("value"), data)
	})

	t.Run("expired entry is invisible to get and to keys", func(t *testing.T) {
		c := NewMemoryCache()
		require.NoError(t, c.Set(ctx, "key", []byte("value"), time.Nanosecond))
		time.Sleep(time.Millisecond)

		data, err := c.Get(ctx, "key")
		require.NoError(t, err)
		require.Nil(t, data)

		keys, err := c.GetAllKeys(ctx, "*")
		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("an expired entry leaves the map behind", func(t *testing.T) {
		c := NewMemoryCache()
		entries := c.(*memoryCache)
		require.NoError(t, c.Set(ctx, "expiring", []byte("value"), 10*time.Millisecond))

		require.Eventually(t, func() bool {
			keys, err := c.GetAllKeys(ctx, "*")
			if err != nil || len(keys) > 0 {
				return false
			}

			entries.mu.RLock()
			defer entries.mu.RUnlock()
			return len(entries.entries) == 0
		}, time.Second, 10*time.Millisecond, "reading around the expired entry leaks it")
	})

	t.Run("a write evicts what expired before it", func(t *testing.T) {
		c := NewMemoryCache()
		entries := c.(*memoryCache)
		require.NoError(t, c.Set(ctx, "expiring", []byte("value"), 10*time.Millisecond))

		require.Eventually(t, func() bool {
			if err := c.Set(ctx, "kept", []byte("value"), 0); err != nil {
				return false
			}

			entries.mu.RLock()
			defer entries.mu.RUnlock()
			_, stale := entries.entries["expiring"]
			return !stale && len(entries.entries) == 1
		}, time.Second, 10*time.Millisecond, "writing around the expired entry leaks it")
	})

	t.Run("get all keys filters by pattern", func(t *testing.T) {
		c := NewMemoryCache()
		require.NoError(t, c.Set(ctx, "gorabbit:cached:app:1", nil, 0))
		require.NoError(t, c.Set(ctx, "gorabbit:cached:app:2", nil, 0))
		require.NoError(t, c.Set(ctx, "gorabbit:cached:other:1", nil, 0))

		keys, err := c.GetAllKeys(ctx, "gorabbit:cached:app:*")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"gorabbit:cached:app:1", "gorabbit:cached:app:2"}, keys)
	})

	t.Run("delete removes the given keys", func(t *testing.T) {
		c := NewMemoryCache()
		require.NoError(t, c.Set(ctx, "a", []byte("1"), 0))
		require.NoError(t, c.Set(ctx, "b", []byte("2"), 0))
		require.NoError(t, c.Delete(ctx, "a", "missing"))

		keys, err := c.GetAllKeys(ctx, "*")
		require.NoError(t, err)
		require.Equal(t, []string{"b"}, keys)
	})
}

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "anything", true},
		{"prefix:*", "prefix:123", true},
		{"prefix:*", "prefix:", true},
		{"prefix:*", "other:123", false},
		{"a:*:b", "a:x:y:b", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"exact", "exact", true},
		{"exact", "exactly", false},
		// A `*` must not stop at a separator, the way Redis KEYS behaves.
		{"a/*", "a/b/c", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.value, func(t *testing.T) {
			require.Equal(t, tt.want, globMatch(tt.pattern, tt.value))
		})
	}
}

func TestNoopHelpers(t *testing.T) {
	ctx := context.Background()

	require.Nil(t, NoopHeaderCarrier().FromContext(ctx))
	require.Equal(t, ctx, NoopHeaderCarrier().ToContext(ctx, map[string]any{"a": "b"}))

	log := NoopLogger()
	log.Debug(ctx, "msg")
	log.Info(ctx, "msg")
	log.Warn(ctx, "msg")
	log.Error(ctx, "msg", "error", "boom")
}
