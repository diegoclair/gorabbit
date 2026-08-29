package rabbitmq

import (
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/stretchr/testify/require"
)

type claimExchange struct{}

func (claimExchange) Name() string { return "claim-events" }

// Two clients on one cache key replay each other's messages, so the second one
// is stopped at the boot that can still be fixed.
func TestConnectRefusesASecondClientOnTheSameCacheKey(t *testing.T) {
	setup := func() *Setup[claimExchange] {
		return NewSetup[claimExchange](unreachableURL, "claim-app").
			WithDialTimeout(100 * time.Millisecond).
			WithReconnectDelay(time.Hour)
	}

	first, err := setup().Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)

	second, err := setup().Connect(gorabbit.NewMemoryCache())
	require.ErrorIs(t, err, ErrCacheKeyTaken)
	require.ErrorContains(t, err, "claim-app")
	require.ErrorContains(t, err, "claim-events")
	require.Nil(t, second)

	// The same client the queue separates from the first one is another key.
	consumer, err := setup().WithConsumer("claim-queue").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	// A restart has to be able to take the key back.
	first.Close()
	reopened, err := setup().Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(reopened.Close)
}
