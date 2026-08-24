// Package rabbitmq implements the gorabbit contracts on top of RabbitMQ topic
// exchanges, with an optional retry exchange and a dead-letter queue.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Message ordering is not handled here: with two consumers on the same queue,
// two published messages may be processed concurrently and out of order.
// Ordering requires Single Active Consumer plus a partitioning key, which is an
// application-level decision.

const defaultReconnectDelay = 2 * time.Second

// Setup describes the topology to declare and the behaviour of the client. E is
// the exchange this application owns and publishes to.
type Setup[E gorabbit.Exchange] struct {
	amqpURL            string
	exchangeName       string
	appName            string
	queueName          string
	dlqName            string
	retryName          string
	logger             gorabbit.Logger
	headers            gorabbit.HeaderCarrier
	preFetchCount      int
	withRetry          bool
	retryCount         int
	retryInterval      time.Duration
	reconnectDelay     time.Duration
	dialTimeout        time.Duration
	isConsumer         bool
	retryableErrorFunc func(error) bool
}

// NewSetup creates a topology setup for the exchange E, which the application
// owns: it is declared on connect and is the only one this client publishes to.
// appName identifies the connection on the broker.
func NewSetup[E gorabbit.Exchange](amqpURL, appName string) *Setup[E] {
	var exchange E

	return &Setup[E]{
		amqpURL:        amqpURL,
		exchangeName:   exchange.Name(),
		appName:        appName,
		logger:         gorabbit.NoopLogger(),
		headers:        gorabbit.NoopHeaderCarrier(),
		reconnectDelay: defaultReconnectDelay,
	}
}

// WithConsumer declares the queue this application consumes from, plus its
// dead-letter queue.
func (s *Setup[E]) WithConsumer(queueName string) *Setup[E] {
	s.queueName = queueName
	s.dlqName = fmt.Sprintf("%s.dlq", queueName)
	s.retryName = fmt.Sprintf("%s.retry", queueName)
	s.isConsumer = true
	return s
}

// WithRetry sets how many times a failed message is retried before going to the
// dead-letter queue, and how long it waits between attempts. retryableErrorFunc
// decides which errors are worth retrying; nil retries every error.
func (s *Setup[E]) WithRetry(retryCount int, retryInterval time.Duration, retryableErrorFunc func(error) bool) *Setup[E] {
	s.retryCount = retryCount
	s.retryInterval = retryInterval
	s.withRetry = true
	s.retryableErrorFunc = retryableErrorFunc
	return s
}

// WithPrefetchCount limits how many unacknowledged messages the broker delivers
// to this consumer at once.
func (s *Setup[E]) WithPrefetchCount(count int) *Setup[E] {
	s.preFetchCount = count
	return s
}

func (s *Setup[E]) WithLogger(l gorabbit.Logger) *Setup[E] {
	if l != nil {
		s.logger = l
	}
	return s
}

// WithHeaderCarrier propagates ambient context values through message headers.
func (s *Setup[E]) WithHeaderCarrier(h gorabbit.HeaderCarrier) *Setup[E] {
	if h != nil {
		s.headers = h
	}
	return s
}

func (s *Setup[E]) WithReconnectDelay(d time.Duration) *Setup[E] {
	if d > 0 {
		s.reconnectDelay = d
	}
	return s
}

// WithDialTimeout bounds each connection attempt. Left unset, the amqp091
// default (30s) applies, which is how long a publish can block before falling
// back to the cache.
func (s *Setup[E]) WithDialTimeout(d time.Duration) *Setup[E] {
	if d > 0 {
		s.dialTimeout = d
	}
	return s
}

func (s *Setup[E]) validate() error {
	switch {
	case s.amqpURL == "":
		return errors.New("gorabbit: amqp url is required")
	case s.exchangeName == "":
		return errors.New("gorabbit: exchange name is required")
	case s.appName == "":
		return errors.New("gorabbit: app name is required")
	case s.withRetry && !s.isConsumer:
		return errors.New("gorabbit: retry is only available for consumers")
	case s.withRetry && s.retryCount <= 0:
		return errors.New("gorabbit: retry count must be greater than zero")
	case s.withRetry && s.retryInterval <= 0:
		return errors.New("gorabbit: retry interval must be greater than zero")
	}

	return nil
}

type handlerInfo struct {
	Exchange   string
	RoutingKey string
	handler    func(context.Context, amqp091.Delivery) error
}

// Client is a connection to RabbitMQ, used to publish the messages of the
// exchange E and to consume from any exchange.
type Client[E gorabbit.Exchange] struct {
	conn        *amqp091.Connection
	ch          *amqp091.Channel
	setup       *Setup[E]
	cache       gorabbit.Cache
	isConnected bool
	mu          sync.Mutex
	done        chan struct{}
	closeOnce   sync.Once
	handlersMu  sync.RWMutex
	handlers    map[string]handlerInfo
}

func newClient[E gorabbit.Exchange](setup *Setup[E], cache gorabbit.Cache) *Client[E] {
	return &Client[E]{
		setup:    setup,
		cache:    cache,
		done:     make(chan struct{}),
		handlers: make(map[string]handlerInfo),
	}
}

// Connect validates the setup and returns a usable Client. An error means the
// setup is invalid or the cache is missing — a broker outage is a state, not an
// error: the client then starts disconnected, caches every publish, accepts
// handler registrations and keeps reconnecting in background until the broker is
// back, when it declares the topology, applies the bindings and flushes the
// cache. The cache keeps those offline messages and tracks the queue bindings,
// so it is required — use gorabbit.NewMemoryCache when a shared store is not
// needed.
func (s *Setup[E]) Connect(cache gorabbit.Cache) (*Client[E], error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	if cache == nil {
		return nil, errors.New("gorabbit: cache is required")
	}

	ctx := context.Background()

	c := newClient(s, cache)

	if err := c.reconnect(ctx); err != nil {
		c.setup.logger.Warn(ctx, "gorabbit: broker unreachable, starting disconnected", "error", err)
	}

	// Started here, not in Start: a publish-only client must also heal.
	go c.monitorConnection(ctx)

	return c, nil
}

func (c *Client[E]) connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isConnected = false
	c.setup.logger.Info(ctx, "gorabbit: connecting to RabbitMQ")

	cfg := amqp091.Config{
		Properties: amqp091.Table{"connection_name": c.setup.appName},
	}
	if c.setup.dialTimeout > 0 {
		cfg.Dial = amqp091.DefaultDial(c.setup.dialTimeout)
	}

	var err error
	c.conn, err = amqp091.DialConfig(c.setup.amqpURL, cfg)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to dial amqp", "error", err)
		return err
	}

	c.ch, err = c.conn.Channel()
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to open channel", "error", err)
		return err
	}

	if c.setup.isConsumer {
		if c.setup.preFetchCount > 0 {
			if err = c.ch.Qos(c.setup.preFetchCount, 0, false); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: error to set prefetch count", "error", err)
				return err
			}
		}
	}

	if err := c.applyTopology(ctx); err != nil {
		return err
	}

	c.isConnected = true
	c.setup.logger.Info(ctx, "gorabbit: connected to RabbitMQ")

	return nil
}

// Start launches the consumer loops; connection monitoring runs since Connect.
// It is non-blocking and must be called after every handler has been registered.
func (c *Client[E]) Start(ctx context.Context) {
	if c.setup.isConsumer {
		go c.consume(ctx)
		go c.unbindUnusedBindings(ctx)
	}
}

// Close stops the background loops and closes the connection. It is idempotent.
func (c *Client[E]) Close() {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.ch != nil {
			if err := c.ch.Close(); err != nil {
				c.setup.logger.Error(context.Background(), "gorabbit: error closing channel", "error", err)
			}
		}

		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				c.setup.logger.Error(context.Background(), "gorabbit: error closing connection", "error", err)
			}
		}

		c.isConnected = false
	})
}

func (c *Client[E]) applyTopology(ctx context.Context) error {
	if err := c.createTopicExchanges(ctx); err != nil {
		return err
	}

	if !c.setup.isConsumer {
		return nil
	}

	if err := c.createQueues(ctx); err != nil {
		return err
	}

	if err := c.bindQueues(ctx); err != nil {
		return err
	}

	return c.bindRegisteredHandlers(ctx)
}

// bindRegisteredHandlers re-creates the binding of every registered handler,
// which is what makes a registration done while disconnected effective. Caller
// holds c.mu.
func (c *Client[E]) bindRegisteredHandlers(ctx context.Context) error {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	for _, hi := range c.handlers {
		if err := c.bindQueueToExchange(ctx, hi.Exchange, hi.RoutingKey, c.setup.queueName, true); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to bind queue to exchange", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createTopicExchanges(ctx context.Context) error {
	if err := c.createTopicExchange(c.setup.exchangeName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create topic exchange", "error", err)
		return err
	}

	if !c.setup.isConsumer {
		return nil
	}

	// Exchange the retry queue dead-letters back into, owned by this consumer.
	if err := c.createTopicExchange(c.setup.queueName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create consumer exchange", "error", err)
		return err
	}

	if err := c.createTopicExchange(c.setup.dlqName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create dlq exchange", "error", err)
		return err
	}

	if c.setup.withRetry {
		if err := c.createTopicExchange(c.setup.retryName); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create retry exchange", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createQueues(ctx context.Context) error {
	if err := c.createQueue(c.setup.dlqName, nil); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create dlq queue", "error", err)
		return err
	}

	args := amqp091.Table{"x-dead-letter-exchange": c.setup.dlqName}
	if err := c.createQueue(c.setup.queueName, args); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create queue", "error", err)
		return err
	}

	if c.setup.withRetry {
		retryArgs := amqp091.Table{
			// When the ttl expires the message dead-letters back to the
			// consumer's own exchange, which is what makes it a delayed retry.
			"x-dead-letter-exchange": c.setup.queueName,
			"x-message-ttl":          int(c.setup.retryInterval.Milliseconds()),
		}

		if err := c.createQueue(c.setup.retryName, retryArgs); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create retry queue", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) bindQueues(ctx context.Context) error {
	// "#" is the AMQP wildcard: these queues take every routing key, which is
	// what lets a republished message keep the routing key it was published
	// with instead of being flattened to an empty one.
	if err := c.bindQueueToExchange(ctx, c.setup.dlqName, "#", c.setup.dlqName, false); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to bind dlq queue", "error", err)
		return err
	}

	if err := c.bindQueueToExchange(ctx, c.setup.queueName, "#", c.setup.queueName, false); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to bind queue", "error", err)
		return err
	}

	if c.setup.withRetry {
		if err := c.bindQueueToExchange(ctx, c.setup.retryName, "#", c.setup.retryName, false); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to bind retry queue", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createTopicExchange(exchange string) error {
	return c.ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
}

func (c *Client[E]) createQueue(queue string, args amqp091.Table) error {
	_, err := c.ch.QueueDeclare(queue, true, false, false, false, args)
	return err
}

// bindQueueToExchange binds a queue to an exchange. tryCreateExchange declares
// the exchange first, needed when this consumer is not its owner and the owning
// application may not have started yet.
func (c *Client[E]) bindQueueToExchange(ctx context.Context, exchange, routingKey, queueName string, tryCreateExchange bool) error {
	if tryCreateExchange {
		if err := c.createTopicExchange(exchange); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create topic exchange", "error", err)
			return err
		}
	}

	return c.ch.QueueBind(queueName, routingKey, exchange, false, nil)
}

// channel guards the channel pointer, which a reconnection may swap while a
// publisher or a consumer is using it.
func (c *Client[E]) channel() (*amqp091.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch == nil {
		return nil, errors.New("gorabbit: not connected")
	}

	return c.ch, nil
}

// Connected reports whether the client holds a live connection right now — for
// health checks and metrics. A disconnected client is still usable: publishes
// are cached and registered handlers wait for the connection.
func (c *Client[E]) Connected() bool { return c.connected() }

func (c *Client[E]) connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isConnected
}

// bindHandler binds now when connected; while disconnected the binding is
// applied by the next successful connection.
func (c *Client[E]) bindHandler(ctx context.Context, hi handlerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isConnected || c.ch == nil {
		return
	}

	if err := c.bindQueueToExchange(ctx, hi.Exchange, hi.RoutingKey, c.setup.queueName, true); err != nil {
		// A failed bind closes the channel; dropping the connected state makes
		// the reconnect loop rebuild it and re-bind every registered handler.
		c.setup.logger.Error(ctx, "gorabbit: error to bind queue to exchange", "error", err)
		c.isConnected = false
	}
}

func (c *Client[E]) setConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isConnected = connected
}
