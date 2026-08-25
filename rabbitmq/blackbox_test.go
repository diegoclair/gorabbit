// Package rabbitmq_test uses gorabbit the way a service does: from the outside,
// owning one exchange and consuming facts from another package's exchange.
package rabbitmq_test

import (
	"context"
	"encoding/json/v2"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/diegoclair/gorabbit"
	"github.com/diegoclair/gorabbit/rabbitmq"
	"github.com/diegoclair/gorabbit/testdata/orders"
	"github.com/stretchr/testify/require"
)

// Same value as the const in the package under test, which is not visible here.
const brokerURLEnv = "GORABBIT_TEST_BROKER_URL"

type Exchange struct{}

func (Exchange) Name() string { return "support-events" }

type msg = gorabbit.Msg[Exchange]

type TicketOpened struct {
	msg
	TicketID string `json:"ticket_id"`
}

func brokerURL(t *testing.T) string {
	t.Helper()

	url := os.Getenv(brokerURLEnv)
	if url == "" {
		t.Skip("skipping test that requires a rabbitmq broker")
	}

	return url
}

// One client publishes the fact its service owns and consumes a fact owned by
// another service, which is the whole model.
func TestServiceOwnsOneExchangeAndConsumesAnother(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	url := brokerURL(t)
	tickets := make(chan TicketOpened, 1)
	created := make(chan orders.OrderCreated, 1)

	support, err := rabbitmq.NewSetup[Exchange](url, "support-worker").
		WithConsumer("support-queue").
		Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(support.Close)

	require.NoError(t, rabbitmq.RegisterHandler(ctx, support, TicketOpened{},
		func(_ context.Context, msg TicketOpened) error {
			tickets <- msg
			return nil
		}))

	// A fact from another exchange: RegisterHandler takes it, Publish would not.
	require.NoError(t, rabbitmq.RegisterHandler(ctx, support, orders.OrderCreated{},
		func(_ context.Context, msg orders.OrderCreated) error {
			created <- msg
			return nil
		}))
	support.Start(ctx)

	var publisher gorabbit.Publisher[Exchange] = support
	require.NoError(t, publisher.Publish(ctx, TicketOpened{TicketID: "t-1"}))

	ordersAPI, err := rabbitmq.NewSetup[orders.Exchange](url, "orders-api").Connect(gorabbit.NewMemoryCache())
	require.NoError(t, err)
	t.Cleanup(ordersAPI.Close)
	require.NoError(t, ordersAPI.Publish(ctx, orders.OrderCreated{OrderID: "123"}))

	select {
	case msg := <-tickets:
		require.Equal(t, "t-1", msg.TicketID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the owned message")
	}

	select {
	case msg := <-created:
		require.Equal(t, "123", msg.OrderID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the foreign message")
	}
}

func TestHiddenMarkerStaysOutOfTheStructAndThePayload(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(orders.OrderCreated{OrderID: "123"})
	require.NoError(t, err)
	require.JSONEq(t, `{"order_id":"123"}`, string(body))

	marker, ok := reflect.TypeOf(orders.OrderCreated{}).FieldByName("msg")
	require.True(t, ok)
	require.True(t, marker.Anonymous)
	require.NotEmpty(t, marker.PkgPath, "the embedded marker must be unexported")

	require.Equal(t, "orders", gorabbit.ExchangeOf(orders.OrderCreated{}))
	require.Equal(t, "support-events", gorabbit.ExchangeOf(TicketOpened{}))
}
