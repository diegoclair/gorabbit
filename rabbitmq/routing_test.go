package rabbitmq

import (
	"context"
	"strings"
	"testing"

	"github.com/diegoclair/gorabbit"
	"github.com/stretchr/testify/require"
)

type vendorsExchange struct{}

func (vendorsExchange) Name() string { return "vendors" }

type eventRecorded struct {
	gorabbit.RoutedMsg[vendorsExchange]
	Vendor string `json:"vendor"`
}

func (e eventRecorded) RouteBy() string { return e.Vendor }

type routeless struct {
	gorabbit.RoutedMsg[vendorsExchange]
}

// A route is whatever the application already calls that thing, so none of it
// may be refused: a rejected route turns a typo into an event nobody receives.
func TestEncodeRouteKeepsTheTopicMetacharactersOutOfTheRoute(t *testing.T) {
	tests := []struct {
		route string
		want  string
	}{
		{"mercadolivre", "mercadolivre"},
		{"", ""},
		{"a.b", "a%2Eb"},
		{"a*b", "a%2Ab"},
		{"a#b", "a%23b"},
		{"100%", "100%25"},
		{"são paulo", "são paulo"},
		{"tenant/42", "tenant/42"},
		{"#.*", "%23%2E%2A"},
	}

	for _, tt := range tests {
		t.Run(tt.route, func(t *testing.T) {
			encoded := encodeRoute(tt.route)
			require.Equal(t, tt.want, encoded)
			require.NotContains(t, encoded, ".")
			require.NotContains(t, encoded, "*")
			require.NotContains(t, encoded, "#")
		})
	}
}

// Two routes that differ must keep differing once encoded, or the broker hands
// one queue the other's messages.
func TestEncodeRouteIsInjective(t *testing.T) {
	routes := []string{"a.b", "a%2Eb", "a", "a.", ".a", "%", "%25", "a*b", "a%2Ab"}

	seen := make(map[string]string, len(routes))
	for _, route := range routes {
		encoded := encodeRoute(route)
		previous, clash := seen[encoded]
		require.False(t, clash, "%q and %q both encode to %q", previous, route, encoded)
		seen[encoded] = route
	}
}

// Publishing and binding read the route through the same function, which is the
// only reason the two ends cannot disagree about where a message goes.
func TestPublishAndBindAgreeOnTheRoute(t *testing.T) {
	const route = "mercado.livre"

	published, err := publishRoutingKey(eventRecorded{Vendor: route}, "eventRecorded")
	require.NoError(t, err)
	require.Equal(t, routedKey("eventRecorded", route), published)
}

func TestPublishRoutingKey(t *testing.T) {
	t.Run("a plain message routes by its type name alone", func(t *testing.T) {
		key, err := publishRoutingKey(orderCreated{}, "orderCreated")
		require.NoError(t, err)
		require.Equal(t, "orderCreated", key)
	})

	t.Run("a routed message adds the route it picked", func(t *testing.T) {
		key, err := publishRoutingKey(eventRecorded{Vendor: "shopee"}, "eventRecorded")
		require.NoError(t, err)
		require.Equal(t, "eventRecorded.shopee", key)
	})

	t.Run("a routed message with no RouteBy is refused", func(t *testing.T) {
		_, err := publishRoutingKey(routeless{}, "routeless")
		require.ErrorIs(t, err, errMissingRouteBy)
	})
}

// Truncating would make two routes share a key and deliver each other's
// messages, so the length is answered to the caller instead.
func TestRoutingKeyLongerThanTheAmqpLimitIsRefused(t *testing.T) {
	require.NoError(t, checkRoutingKeyLength(strings.Repeat("a", maxRoutingKeyBytes)))
	require.ErrorIs(t, checkRoutingKeyLength(strings.Repeat("a", maxRoutingKeyBytes+1)), ErrRoutingKeyTooLong)
}

func TestPublishRefusesARoutingKeyThatDoesNotFit(t *testing.T) {
	ctx := t.Context()
	c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app"))

	err := c.Publish(ctx, eventRecorded{Vendor: strings.Repeat("v", maxRoutingKeyBytes)})
	require.ErrorIs(t, err, ErrRoutingKeyTooLong)

	keys, err := c.cache.GetAllKeys(ctx, cacheKey(c.cacheScope(), "")+"*")
	require.NoError(t, err)
	require.Empty(t, keys, "a message that can never be routed must not sit in the cache")
}

func TestSubscribeRouteRefusesARoutingKeyThatDoesNotFit(t *testing.T) {
	c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))

	err := SubscribeRoute(t.Context(), c, eventRecorded{}, strings.Repeat("v", maxRoutingKeyBytes),
		func(context.Context, eventRecorded) error { return nil })
	require.ErrorIs(t, err, ErrRoutingKeyTooLong)
}

func TestTypeNameOf(t *testing.T) {
	require.Equal(t, "orderCreated", typeNameOf("orderCreated"))
	require.Equal(t, "eventRecorded", typeNameOf("eventRecorded.shopee"))
	require.Equal(t, "eventRecorded", typeNameOf("eventRecorded."))
}
