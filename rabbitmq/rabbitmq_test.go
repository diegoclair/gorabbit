package rabbitmq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const unreachableURL = "amqp://guest:guest@127.0.0.1:1/"

type ordersExchange struct{}

func (ordersExchange) Name() string { return "orders" }

type orders = gorabbit.Msg[ordersExchange]

type orderCreated struct {
	orders
	OrderID string `json:"order_id"`
}

type billingExchange struct{}

func (billingExchange) Name() string { return "billing" }

type billing = gorabbit.Msg[billingExchange]

type envelope[T any] struct {
	orders
	Payload T `json:"payload"`
}

type unnamedExchange struct{}

func (unnamedExchange) Name() string { return "" }

type noExchangeName struct {
	gorabbit.Msg[unnamedExchange]
}

var (
	_ gorabbit.Publisher[ordersExchange] = (*Client[ordersExchange])(nil)
	_ gorabbit.Consumer                  = (*Client[ordersExchange])(nil)
)

func newTestClient[E gorabbit.Exchange](setup *Setup[E]) *Client[E] {
	return newClient(setup, gorabbit.NewMemoryCache())
}

func TestSetupValidate(t *testing.T) {
	// Setups of different exchanges are different types; validate is what the
	// table needs from all of them.
	tests := []struct {
		name    string
		setup   interface{ validate() error }
		wantErr string
	}{
		{
			name:  "valid producer",
			setup: NewSetup[ordersExchange](unreachableURL, "app"),
		},
		{
			name:  "valid consumer with retry",
			setup: NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue").WithRetry(3, time.Second, nil),
		},
		{
			name:    "missing amqp url",
			setup:   NewSetup[ordersExchange]("", "app"),
			wantErr: "amqp url is required",
		},
		{
			name:    "marker with an empty exchange name",
			setup:   NewSetup[unnamedExchange](unreachableURL, "app"),
			wantErr: "exchange name is required",
		},
		{
			name:    "missing app name",
			setup:   NewSetup[ordersExchange](unreachableURL, ""),
			wantErr: "app name is required",
		},
		{
			name:    "retry without consumer",
			setup:   NewSetup[ordersExchange](unreachableURL, "app").WithRetry(3, time.Second, nil),
			wantErr: "retry is only available for consumers",
		},
		{
			name:    "retry without count",
			setup:   NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("q").WithRetry(0, time.Second, nil),
			wantErr: "retry count must be greater than zero",
		},
		{
			name:    "retry without interval",
			setup:   NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("q").WithRetry(3, 0, nil),
			wantErr: "retry interval must be greater than zero",
		},
		{
			name:    "publish confirm timeout not greater than zero",
			setup:   NewSetup[ordersExchange](unreachableURL, "app").WithPublishConfirmTimeout(0),
			wantErr: "publish confirm timeout must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setup.validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSetupBuilders(t *testing.T) {
	retryable := func(error) bool { return false }

	s := NewSetup[ordersExchange](unreachableURL, "app").
		WithConsumer("app-queue").
		WithRetry(5, 2*time.Second, retryable).
		WithPrefetchCount(10).
		WithReconnectDelay(time.Minute).
		WithPublishConfirmTimeout(3 * time.Second)

	require.True(t, s.isConsumer)
	require.Equal(t, "app-queue", s.queueName)
	require.Equal(t, "app-queue.dlq", s.dlqName)
	require.Equal(t, "app-queue.retry", s.retryName)
	require.True(t, s.withRetry)
	require.Equal(t, 5, s.retryCount)
	require.Equal(t, 2*time.Second, s.retryInterval)
	require.NotNil(t, s.retryableErrorFunc)
	require.Equal(t, 10, s.preFetchCount)
	require.Equal(t, time.Minute, s.reconnectDelay)
	require.Equal(t, 3*time.Second, s.confirmTimeout)
}

func TestSetupOptionalDependenciesKeepDefaults(t *testing.T) {
	s := NewSetup[ordersExchange](unreachableURL, "app").
		WithLogger(nil).
		WithHeaderCarrier(nil).
		WithReconnectDelay(0)

	require.NotNil(t, s.logger)
	require.NotNil(t, s.headers)
	require.Equal(t, defaultReconnectDelay, s.reconnectDelay)
	require.Equal(t, defaultPublishConfirmTimeout, s.confirmTimeout)
}

func TestConnectRequiresCache(t *testing.T) {
	c, err := NewSetup[ordersExchange](unreachableURL, "app").Connect(nil)
	require.ErrorContains(t, err, "cache is required")
	require.Nil(t, c)
}

func TestConnectRejectsAnInvalidSetup(t *testing.T) {
	c, err := NewSetup[ordersExchange]("", "app").Connect(gorabbit.NewMemoryCache())
	require.ErrorContains(t, err, "amqp url is required")
	require.Nil(t, c)
}

// A broker outage is a state, not an error: Connect hands back a live client
// that caches publishes and holds handler registrations for the connection.
func TestConnectWithUnreachableBrokerReturnsUsableClient(t *testing.T) {
	ctx := context.Background()
	cache := gorabbit.NewMemoryCache()

	c, err := NewSetup[ordersExchange](unreachableURL, "app").
		WithConsumer("app-queue").
		WithDialTimeout(200 * time.Millisecond).
		Connect(cache)
	require.NoError(t, err)
	require.NotNil(t, c)
	t.Cleanup(c.Close)

	require.False(t, c.Connected())

	require.NoError(t, c.Publish(ctx, orderCreated{OrderID: "1"}))
	keys, err := cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 1, "an offline publish must land in the cache")

	require.NoError(t, RegisterHandler(ctx, c, orderCreated{},
		func(context.Context, orderCreated) error { return nil }))

	c.handlersMu.RLock()
	_, registered := c.handlers[handlersMapKey("orders", "orderCreated")]
	c.handlersMu.RUnlock()
	require.True(t, registered, "an offline registration must wait for the connection")

	info, err := cache.Get(ctx, c.handlerInfoCacheKey(handlerInfo{Exchange: "orders", RoutingKey: "orderCreated"}))
	require.NoError(t, err)
	require.NotEmpty(t, info, "the binding must be recorded even while disconnected")

	// Start while disconnected must not panic; the loops wait for the broker.
	c.Start(ctx)
}

func TestMessageTypeName(t *testing.T) {
	require.Equal(t, "orderCreated", messageTypeName(orderCreated{}))
	require.Equal(t, "orderCreated", messageTypeName(&orderCreated{}))
	require.Empty(t, messageTypeName(nil))
}

// A generic message type resolves its exchange like any other, but its routing
// key carries the instantiation — which is why the README asks for plain structs.
func TestMessageTypeNameOfAGenericTypeCarriesTheInstantiation(t *testing.T) {
	exchange, err := exchangeOf(envelope[orderCreated]{})
	require.NoError(t, err)
	require.Equal(t, "orders", exchange)

	name := messageTypeName(envelope[orderCreated]{})
	require.Equal(t, "envelope[github.com/diegoclair/gorabbit/rabbitmq.orderCreated]", name)
}

func TestExchangeOf(t *testing.T) {
	t.Run("resolves the marked exchange for values and pointers", func(t *testing.T) {
		exchange, err := exchangeOf(orderCreated{})
		require.NoError(t, err)
		require.Equal(t, "orders", exchange)

		exchange, err = exchangeOf(&orderCreated{})
		require.NoError(t, err)
		require.Equal(t, "orders", exchange)
	})

	t.Run("rejects a typed nil pointer instead of panicking", func(t *testing.T) {
		_, err := exchangeOf((*orderCreated)(nil))
		require.ErrorContains(t, err, "message is nil")
	})

	t.Run("rejects a marker with an empty name", func(t *testing.T) {
		_, err := exchangeOf(noExchangeName{})
		require.ErrorContains(t, err, "empty exchange name")
	})
}

// Two message types with the same name live in different exchanges: the routing
// key collides on purpose and only the exchange tells them apart.
func TestSameTypeNameInDifferentExchangesKeepsTheRoutingKey(t *testing.T) {
	fromOrders, err := exchangeOf(orderCreated{})
	require.NoError(t, err)

	// Shadows the package-level message: same type name, other exchange.
	type orderCreated struct {
		billing
		OrderID string `json:"order_id"`
	}

	fromBilling, err := exchangeOf(orderCreated{})
	require.NoError(t, err)

	require.Equal(t, "orderCreated", messageTypeName(orderCreated{}))
	require.NotEqual(t, fromOrders, fromBilling)
	require.NotEqual(t,
		handlersMapKey(fromOrders, "orderCreated"),
		handlersMapKey(fromBilling, "orderCreated"),
	)
}

func TestSetupTakesTheExchangeNameFromTheMarker(t *testing.T) {
	require.Equal(t, "orders", NewSetup[ordersExchange](unreachableURL, "app").exchangeName)
	require.Equal(t, "billing", NewSetup[billingExchange](unreachableURL, "app").exchangeName)
}

func TestPublishRejectsANilMessage(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	require.ErrorContains(t, c.Publish(ctx, (*orderCreated)(nil)), "message is nil")

	keys, err := c.cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Empty(t, keys, "an unpublishable message must not reach the cache")
}

func TestHandlerForUsesTheOriginExchangeOfRetriedMessages(t *testing.T) {
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))

	// Same message name in two exchanges: only the exchange tells them apart.
	fromOrders := handlerInfo{Exchange: "orders", RoutingKey: "orderCreated"}
	fromBilling := handlerInfo{Exchange: "billing", RoutingKey: "orderCreated"}
	c.handlers[handlersMapKey("orders", "orderCreated")] = fromOrders
	c.handlers[handlersMapKey("billing", "orderCreated")] = fromBilling

	tests := []struct {
		name     string
		delivery amqp091.Delivery
		want     handlerInfo
		wantOK   bool
	}{
		{
			name:     "delivered by the owning exchange",
			delivery: amqp091.Delivery{Exchange: "orders", RoutingKey: "orderCreated"},
			want:     fromOrders,
			wantOK:   true,
		},
		{
			name: "retried through the consumer exchange keeps its origin",
			delivery: amqp091.Delivery{
				Exchange:   "app-queue",
				RoutingKey: "orderCreated",
				Headers:    amqp091.Table{originExchangeHeaderKey: "orders"},
			},
			want:   fromOrders,
			wantOK: true,
		},
		{
			name: "another origin, same routing key, other handler",
			delivery: amqp091.Delivery{
				Exchange:   "app-queue",
				RoutingKey: "orderCreated",
				Headers:    amqp091.Table{originExchangeHeaderKey: "billing"},
			},
			want:   fromBilling,
			wantOK: true,
		},
		{
			name:     "retried without an origin is not handled",
			delivery: amqp091.Delivery{Exchange: "app-queue", RoutingKey: "orderCreated"},
			wantOK:   false,
		},
		{
			name: "origin without a handler is not handled",
			delivery: amqp091.Delivery{
				Exchange:   "app-queue",
				RoutingKey: "orderCreated",
				Headers:    amqp091.Table{originExchangeHeaderKey: "shipping"},
			},
			wantOK: false,
		},
		{
			name:     "unknown exchange",
			delivery: amqp091.Delivery{Exchange: "shipping", RoutingKey: "orderCreated"},
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := c.handlerFor(&tt.delivery)
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.Equal(t, tt.want.Exchange, got.Exchange)
			}
		})
	}
}

func TestStampOriginExchange(t *testing.T) {
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))

	first := amqp091.Delivery{Exchange: "orders", Headers: amqp091.Table{}}
	c.stampOriginExchange(&first)
	require.Equal(t, "orders", first.Headers[originExchangeHeaderKey])

	// A later attempt arrives from the consumer exchange and must not overwrite
	// the origin recorded on the first one.
	second := amqp091.Delivery{Exchange: "app-queue", Headers: first.Headers}
	c.stampOriginExchange(&second)
	require.Equal(t, "orders", second.Headers[originExchangeHeaderKey])
}

func TestCacheKey(t *testing.T) {
	require.Equal(t, "gorabbit:cached_messages:app:", cacheKey("app", ""))
	require.Equal(t, "gorabbit:cached_messages:app:0198b7ff", cacheKey("app", "0198b7ff"))
}

func TestHandlerKeys(t *testing.T) {
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))
	info := handlerInfo{Exchange: "orders", RoutingKey: "orderCreated"}

	require.Equal(t, "orders:orderCreated", handlersMapKey(info.Exchange, info.RoutingKey))
	require.Equal(t, "gorabbit:handler-info:app-queue:orders:orderCreated", c.handlerInfoCacheKey(info))
}

func TestPublishCachesMessageWhenBrokerIsUnreachable(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithDialTimeout(200 * time.Millisecond))

	require.NoError(t, c.Publish(ctx, orderCreated{OrderID: "123"}))

	keys, err := c.cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 1)

	data, err := c.cache.Get(ctx, keys[0])
	require.NoError(t, err)

	var cached cachedMessage
	require.NoError(t, json.Unmarshal(data, &cached))
	require.Equal(t, "orderCreated", cached.MsgTypeName)
	require.JSONEq(t, `{"order_id":"123"}`, string(cached.Message))

	// The id has to survive the cache round-trip, otherwise a replayed message
	// looks brand new to the consumer.
	require.NotEmpty(t, cached.MsgID)
	require.Equal(t, cached.MsgID, fromCacheMessage(&cached).MsgID)
}

func TestPublishMessagesGetSortableUniqueIDs(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	first, err := c.getPublishMessage(ctx, orderCreated{})
	require.NoError(t, err)
	second, err := c.getPublishMessage(ctx, orderCreated{})
	require.NoError(t, err)

	firstID, err := uuid.Parse(first.MsgID)
	require.NoError(t, err)
	secondID, err := uuid.Parse(second.MsgID)
	require.NoError(t, err)

	require.NotEqual(t, firstID, secondID)
	require.Negative(t, firstID.Compare(secondID), "v7 ids must sort by creation time")
}

func TestPublishCarriesContextHeaders(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithHeaderCarrier(testCarrier{}))

	pm, err := c.getPublishMessage(context.WithValue(ctx, testCarrierKey, "abc-123"), orderCreated{})
	require.NoError(t, err)
	require.Equal(t, amqp091.Table{"correlation_id": "abc-123"}, pm.MsgHeaders)
}

func TestFlushCachedMessagesIsANoopWhileDisconnected(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))
	require.NoError(t, c.cacheMessage(ctx, &publishMessage{MsgTypeName: "orderCreated", MsgBody: []byte(`{}`)}))

	// No channel is open, so a flush that did not check the connection would panic.
	c.flushCachedMessages(ctx)

	keys, err := c.cache.GetAllKeys(ctx, cacheKey("app", "")+"*")
	require.NoError(t, err)
	require.Len(t, keys, 1)
}

func TestGetMessagesFromCacheReplaysInPublishOrderAndSkipsCorruptEntries(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	// Stored out of order so the sort, not the insertion, is what is proven.
	first, second, third := uuid.NewV7().String(), uuid.NewV7().String(), uuid.NewV7().String()
	for _, msg := range []cachedMessage{
		{MsgID: third, MsgTypeName: "third"},
		{MsgID: first, MsgTypeName: "first"},
		{MsgID: second, MsgTypeName: "second"},
	} {
		data, err := json.Marshal(msg)
		require.NoError(t, err)
		require.NoError(t, c.cache.Set(ctx, cacheKey("app", msg.MsgID), data, 0))
	}
	require.NoError(t, c.cache.Set(ctx, cacheKey("app", "corrupt"), []byte("not json"), 0))

	messages, err := c.getMessagesFromCache(ctx)
	require.NoError(t, err)
	require.Len(t, messages, 3)
	require.Equal(t, "first", messages[0].MsgTypeName)
	require.Equal(t, "second", messages[1].MsgTypeName)
	require.Equal(t, "third", messages[2].MsgTypeName)
}

func TestRetryCountHeader(t *testing.T) {
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))
	ctx := context.Background()

	tests := []struct {
		name    string
		headers amqp091.Table
		want    int
	}{
		{"no headers", nil, 0},
		{"header absent", amqp091.Table{}, 0},
		{"int", amqp091.Table{retryCountHeaderKey: 2}, 2},
		{"int32 from the broker", amqp091.Table{retryCountHeaderKey: int32(3)}, 3},
		{"int64 from the broker", amqp091.Table{retryCountHeaderKey: int64(4)}, 4},
		{"unexpected type", amqp091.Table{retryCountHeaderKey: "5"}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, c.retryCount(ctx, &amqp091.Delivery{Headers: tt.headers}))
		})
	}
}

func TestHandleMessageSafely(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))

	t.Run("delivers the decoded payload", func(t *testing.T) {
		var got orderCreated
		err := handleMessageSafely(ctx, c, &amqp091.Delivery{Body: []byte(`{"order_id":"123"}`)},
			func(_ context.Context, msg orderCreated) error {
				got = msg
				return nil
			})

		require.NoError(t, err)
		require.Equal(t, "123", got.OrderID)
	})

	t.Run("returns the handler error", func(t *testing.T) {
		wantErr := errors.New("handler failed")
		err := handleMessageSafely(ctx, c, &amqp091.Delivery{Body: []byte(`{}`)},
			func(context.Context, orderCreated) error { return wantErr })

		require.ErrorIs(t, err, wantErr)
	})

	t.Run("turns a panic into an error", func(t *testing.T) {
		err := handleMessageSafely(ctx, c, &amqp091.Delivery{Body: []byte(`{}`)},
			func(context.Context, orderCreated) error { panic("boom") })

		require.ErrorContains(t, err, "panic recovered: boom")
	})

	t.Run("fails on an undecodable body", func(t *testing.T) {
		called := false
		err := handleMessageSafely(ctx, c, &amqp091.Delivery{Body: []byte(`not json`)},
			func(context.Context, orderCreated) error {
				called = true
				return nil
			})

		require.Error(t, err)
		require.False(t, called)
	})
}

func TestRegisterHandlerRejectsInvalidUsage(t *testing.T) {
	ctx := context.Background()

	t.Run("client is not a consumer", func(t *testing.T) {
		c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app"))
		err := RegisterHandler(ctx, c, orderCreated{}, func(context.Context, orderCreated) error { return nil })
		require.ErrorContains(t, err, "not a consumer")
	})

	t.Run("nil handler", func(t *testing.T) {
		c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))
		err := RegisterHandler[orderCreated](ctx, c, orderCreated{}, nil)
		require.ErrorContains(t, err, "handler is required")
	})

	t.Run("marker with an empty exchange name", func(t *testing.T) {
		c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))
		err := RegisterHandler(ctx, c, noExchangeName{}, func(context.Context, noExchangeName) error { return nil })
		require.ErrorContains(t, err, "empty exchange name")
	})

	t.Run("nil message", func(t *testing.T) {
		c := newTestClient(NewSetup[ordersExchange](unreachableURL, "app").WithConsumer("app-queue"))
		err := RegisterHandler(ctx, c, (*orderCreated)(nil), func(context.Context, *orderCreated) error { return nil })
		require.ErrorContains(t, err, "message is nil")
	})
}

type testCarrierKeyType struct{}

var testCarrierKey = testCarrierKeyType{}

type testCarrier struct{}

func (testCarrier) FromContext(ctx context.Context) map[string]any {
	correlationID, ok := ctx.Value(testCarrierKey).(string)
	if !ok {
		return nil
	}
	return map[string]any{"correlation_id": correlationID}
}

func (testCarrier) ToContext(ctx context.Context, headers map[string]any) context.Context {
	return context.WithValue(ctx, testCarrierKey, headers["correlation_id"])
}

// The other test loggers drop the key/values; asserting on fields needs them kept.
type recordingLogger struct {
	mu    sync.Mutex
	lines []recordedLine
}

type recordedLine struct {
	msg    string
	fields map[string]any
}

func (l *recordingLogger) Debug(_ context.Context, msg string, kv ...any) { l.record(msg, kv) }
func (l *recordingLogger) Info(_ context.Context, msg string, kv ...any)  { l.record(msg, kv) }
func (l *recordingLogger) Warn(_ context.Context, msg string, kv ...any)  { l.record(msg, kv) }
func (l *recordingLogger) Error(_ context.Context, msg string, kv ...any) { l.record(msg, kv) }

func (l *recordingLogger) record(msg string, kv []any) {
	fields := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		if key, ok := kv[i].(string); ok {
			fields[key] = kv[i+1]
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, recordedLine{msg: msg, fields: fields})
}

func (l *recordingLogger) fieldsOf(msg string) (map[string]any, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if line.msg == msg {
			return line.fields, true
		}
	}

	return nil, false
}

var _ gorabbit.Logger = (*recordingLogger)(nil)

// An application running one client per exchange needs every lifecycle line to
// say which of them it came from.
func TestConnectionLifecycleLogsIdentifyTheClient(t *testing.T) {
	logger := &recordingLogger{}
	c := newTestClient(NewSetup[ordersExchange](unreachableURL, "orders-api").
		WithLogger(logger).
		WithDialTimeout(500 * time.Millisecond))
	t.Cleanup(c.Close)

	_, err := c.connect(context.Background())
	require.Error(t, err)

	for _, msg := range []string{"gorabbit: connecting to RabbitMQ", "gorabbit: error to dial amqp"} {
		fields, ok := logger.fieldsOf(msg)
		require.True(t, ok, "missing log line %q", msg)
		require.Equal(t, "orders-api", fields["app_name"], msg)
		require.Equal(t, "orders", fields["exchange"], msg)
	}
}
