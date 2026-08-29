package rabbitmq

import (
	"context"
	"testing"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func bindingKeys[E gorabbit.Exchange](c *Client[E]) []string {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	keys := make([]string, 0, len(c.handlers))
	for _, info := range c.handlers {
		keys = append(keys, info.BindingKey)
	}

	return keys
}

// The reason the two markers exist: two queues wanting different slices of one
// type must bind different keys, or the broker hands a copy of everything to both.
func TestSubscriptionsBindTheKeyThatMatchesTheirScope(t *testing.T) {
	ctx := context.Background()

	t.Run("a plain message binds its type name", func(t *testing.T) {
		c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))
		require.NoError(t, Subscribe(ctx, c, orderCreated{}, func(context.Context, orderCreated) error { return nil }))
		require.Equal(t, []string{"orderCreated"}, bindingKeys(c))
	})

	t.Run("every route of a routed message", func(t *testing.T) {
		c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))
		require.NoError(t, Subscribe(ctx, c, eventRecorded{}, func(context.Context, eventRecorded) error { return nil }))
		require.Equal(t, []string{"eventRecorded.#"}, bindingKeys(c))
	})

	t.Run("one route of a routed message", func(t *testing.T) {
		c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))
		require.NoError(t, SubscribeRoute(ctx, c, eventRecorded{}, "shopee",
			func(context.Context, eventRecorded) error { return nil }))
		require.Equal(t, []string{"eventRecorded.shopee"}, bindingKeys(c))
	})

	t.Run("a route carrying the topic metacharacters", func(t *testing.T) {
		c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))
		require.NoError(t, SubscribeRoute(ctx, c, eventRecorded{}, "mercado.livre",
			func(context.Context, eventRecorded) error { return nil }))
		require.Equal(t, []string{"eventRecorded.mercado%2Elivre"}, bindingKeys(c))
	})
}

// Two routes of one type on one client are two bindings and two handlers, so
// the duplicate check has to be about the binding and not about the type.
func TestSubscribeRouteAcceptsSeveralRoutesOfTheSameType(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))

	handler := func(context.Context, eventRecorded) error { return nil }
	require.NoError(t, SubscribeRoute(ctx, c, eventRecorded{}, "shopee", handler))
	require.NoError(t, SubscribeRoute(ctx, c, eventRecorded{}, "mercadolivre", handler))
	require.ErrorContains(t, SubscribeRoute(ctx, c, eventRecorded{}, "shopee", handler), "already registered")

	require.ElementsMatch(t, []string{"eventRecorded.shopee", "eventRecorded.mercadolivre"}, bindingKeys(c))
}

func TestHandlerForResolvesTheRouteOfADelivery(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[vendorsExchange](unreachableURL, "app").WithConsumer("app-queue"))

	require.NoError(t, SubscribeRoute(ctx, c, eventRecorded{}, "shopee",
		func(context.Context, eventRecorded) error { return nil }))
	require.NoError(t, Subscribe(ctx, c, orderShipped{}, func(context.Context, orderShipped) error { return nil }))

	tests := []struct {
		name       string
		routingKey string
		want       string
		wantOK     bool
	}{
		{"the route that was subscribed", "eventRecorded.shopee", "eventRecorded.shopee", true},
		{"a route nothing subscribed", "eventRecorded.magalu", "", false},
		{"any route of a type subscribed whole", "orderShipped.correios", "orderShipped.#", true},
		{"a type nobody subscribed", "invoiceIssued.shopee", "", false},
		{"a routed type reaching its wildcard with no route at all", "orderShipped", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, ok := c.handlerFor(&amqp091.Delivery{Exchange: "vendors", RoutingKey: tt.routingKey})
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.Equal(t, tt.want, info.BindingKey)
			}
		})
	}
}

type orderShipped struct {
	gorabbit.RoutedMsg[vendorsExchange]
	Carrier string `json:"carrier"`
}

func (o orderShipped) RouteBy() string { return o.Carrier }
