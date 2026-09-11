package rabbitmq

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type ackRecorder struct{ acked chan struct{} }

func (a *ackRecorder) Ack(uint64, bool) error {
	close(a.acked)
	return nil
}
func (a *ackRecorder) Nack(uint64, bool, bool) error { return nil }
func (a *ackRecorder) Reject(uint64, bool) error     { return nil }

func TestAShutdownLeavesTheDeliveryInAHandlerToFinishAndCloseWaitsForIt(t *testing.T) {
	c := newTestConsumer(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))

	inHandler := make(chan context.Context, 1)
	release := make(chan struct{})
	require.NoError(t, Subscribe(context.Background(), c, orderCreated{},
		func(ctx context.Context, _ orderCreated) error {
			inHandler <- ctx
			<-release
			return nil
		}))

	body, err := json.Marshal(orderCreated{OrderID: "1"})
	require.NoError(t, err)
	ack := &ackRecorder{acked: make(chan struct{})}
	deliveries := make(chan amqp091.Delivery, 1)
	deliveries <- amqp091.Delivery{Exchange: "orders", RoutingKey: "orderCreated", Body: body, Acknowledger: ack}

	// One worker standing in for the generation consume starts once connected.
	ctx, shutdown := context.WithCancel(context.Background())
	c.consumerWg.Add(1)
	go func() {
		defer c.consumerWg.Done()
		c.runWorker(ctx, deliveries)
	}()

	var handlerCtx context.Context
	select {
	case handlerCtx = <-inHandler:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery never reached its handler")
	}

	shutdown()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		c.Close()
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a handler still held its delivery")
	case <-ack.acked:
		t.Fatal("the delivery was acked before its handler returned")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, handlerCtx.Err(), "the shutdown cancelled the handler it should let finish")

	close(release)
	select {
	case <-ack.acked:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery that finished was never acked")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned after the handler finished")
	}
}
