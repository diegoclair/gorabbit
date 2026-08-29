// Package rabbitmq implements the gorabbit contracts on top of RabbitMQ topic
// exchanges, with an optional retry exchange and a dead-letter queue.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Message ordering is not a promise of this package: replicas sharing a queue
// and the worker pool of one client both take deliveries in parallel.

const (
	defaultReconnectDelay = 2 * time.Second
	// A confirm that never arrives must not block the caller forever.
	defaultPublishConfirmTimeout = 5 * time.Second
	// A handler that never returns must not hold Close forever.
	closeDrainTimeout = 30 * time.Second
)

// ErrClientClosed is returned by Publish once Close has run: a closed client
// never reopens a connection, so the message is neither sent nor cached.
var ErrClientClosed = errors.New("gorabbit: client is closed")

// ErrTopologyRejected reports a declared topology the broker refuses on every
// attempt. Only a deploy fixes it, so Connect and Publish surface it instead of
// starting disconnected and caching until the cache expires.
var ErrTopologyRejected = errors.New("gorabbit: broker rejected the topology")

var (
	errNotConnected   = errors.New("gorabbit: not connected")
	errDialInProgress = errors.New("gorabbit: a connection attempt is already in progress")
)

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
	concurrency        int
	withRetry          bool
	retryCount         int
	retryInterval      time.Duration
	reconnectDelay     time.Duration
	dialTimeout        time.Duration
	confirmTimeout     time.Duration
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
		confirmTimeout: defaultPublishConfirmTimeout,
		concurrency:    1,
	}
}

// WithConsumer declares the queue this application consumes from, plus its
// dead-letter queue. It hands back the consumer setup, which is the only place
// the options that need a queue can be written.
func (s *Setup[E]) WithConsumer(queueName string) *ConsumerSetup[E] {
	s.queueName = queueName
	s.dlqName = fmt.Sprintf("%s.dlq", queueName)
	s.retryName = fmt.Sprintf("%s.retry", queueName)
	s.isConsumer = true
	return &ConsumerSetup[E]{setup: s}
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

// WithPublishConfirmTimeout bounds the wait for the broker's confirmation, past
// which the publish is treated as failed rather than delivered.
func (s *Setup[E]) WithPublishConfirmTimeout(d time.Duration) *Setup[E] {
	s.confirmTimeout = d
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
	case s.withRetry && s.retryCount <= 0:
		return errors.New("gorabbit: retry count must be greater than zero")
	case s.withRetry && s.retryInterval <= 0:
		return errors.New("gorabbit: retry interval must be greater than zero")
	case s.confirmTimeout <= 0:
		return errors.New("gorabbit: publish confirm timeout must be greater than zero")
	case s.concurrency < 1:
		return errors.New("gorabbit: concurrency must be greater than zero")
	// A prefetch of zero is AMQP's unlimited; anything else below the
	// concurrency leaves workers that could never hold a delivery.
	case s.preFetchCount > 0 && s.concurrency > s.preFetchCount:
		return errors.New("gorabbit: concurrency must not be greater than the prefetch count")
	}

	return nil
}

// ConsumerSetup is a Setup that has a queue. Retry, prefetch and concurrency
// are meaningless without one, so they live here and a publisher cannot name
// them.
type ConsumerSetup[E gorabbit.Exchange] struct {
	setup *Setup[E]
}

// WithRetry sets how many times a failed message is retried before going to the
// dead-letter queue, and how long it waits between attempts. retryableErrorFunc
// decides which errors are worth retrying; nil retries every error.
func (c *ConsumerSetup[E]) WithRetry(retryCount int, retryInterval time.Duration, retryableErrorFunc func(error) bool) *ConsumerSetup[E] {
	c.setup.retryCount = retryCount
	c.setup.retryInterval = retryInterval
	c.setup.withRetry = true
	c.setup.retryableErrorFunc = retryableErrorFunc
	return c
}

// WithPrefetchCount limits how many unacknowledged messages the broker delivers
// to this consumer at once.
func (c *ConsumerSetup[E]) WithPrefetchCount(count int) *ConsumerSetup[E] {
	c.setup.preFetchCount = count
	return c
}

// WithConcurrency sets how many deliveries this client handles at once, across
// every handler it registered. Above 1 the order of the queue is not preserved.
func (c *ConsumerSetup[E]) WithConcurrency(n int) *ConsumerSetup[E] {
	c.setup.concurrency = n
	return c
}

// The methods common to any setup are repeated here, and not embedded, so a
// chain that started with WithConsumer is still a consumer setup at the end.

func (c *ConsumerSetup[E]) WithLogger(l gorabbit.Logger) *ConsumerSetup[E] {
	c.setup.WithLogger(l)
	return c
}

func (c *ConsumerSetup[E]) WithHeaderCarrier(h gorabbit.HeaderCarrier) *ConsumerSetup[E] {
	c.setup.WithHeaderCarrier(h)
	return c
}

func (c *ConsumerSetup[E]) WithReconnectDelay(d time.Duration) *ConsumerSetup[E] {
	c.setup.WithReconnectDelay(d)
	return c
}

func (c *ConsumerSetup[E]) WithDialTimeout(d time.Duration) *ConsumerSetup[E] {
	c.setup.WithDialTimeout(d)
	return c
}

func (c *ConsumerSetup[E]) WithPublishConfirmTimeout(d time.Duration) *ConsumerSetup[E] {
	c.setup.WithPublishConfirmTimeout(d)
	return c
}

func (c *ConsumerSetup[E]) Connect(cache gorabbit.Cache) (*Client[E], error) {
	return c.setup.Connect(cache)
}

// The connection and its channels are swapped in as one: a client holding
// channels from two connections would publish over a socket nobody watches.
type brokerConn struct {
	conn    *amqp091.Connection
	ch      *amqp091.Channel
	pubCh   *amqp091.Channel
	returns *returnTracker
}

type handlerInfo struct {
	Exchange   string
	BindingKey string
	handler    func(context.Context, amqp091.Delivery) error
}

// Client is a connection to RabbitMQ, used to publish the messages of the
// exchange E and to consume from any exchange.
type Client[E gorabbit.Exchange] struct {
	conn *amqp091.Connection
	ch   *amqp091.Channel
	// Publishing gets its own channel: amqp091 does not serialize a multi-frame
	// publish against control methods (Consume, QueueBind) on the same channel.
	pubCh *amqp091.Channel
	// The tracker belongs to the publish channel, so it is swapped in with it.
	returns     *returnTracker
	setup       *Setup[E]
	cache       gorabbit.Cache
	isConnected bool
	claimedKey  bool
	// setupErr holds the last topology rejection, so a caller that skipped the
	// dial answers with it instead of caching behind it.
	setupErr   error
	mu         sync.Mutex
	done       chan struct{}
	closeOnce  sync.Once
	consumerWg sync.WaitGroup
	dialing    atomic.Bool
	flushing   atomic.Bool
	pending    chan struct{}
	handlersMu sync.RWMutex
	handlers   map[string]handlerInfo
}

func newClient[E gorabbit.Exchange](setup *Setup[E], cache gorabbit.Cache) *Client[E] {
	return &Client[E]{
		setup:    setup,
		cache:    cache,
		done:     make(chan struct{}),
		pending:  make(chan struct{}, 1),
		handlers: make(map[string]handlerInfo),
	}
}

// Tells apart the connection-lifecycle lines of the several clients one process
// may hold.
func (c *Client[E]) connFields(keyvals ...any) []any {
	return append([]any{"app_name", c.setup.appName, "exchange", c.setup.exchangeName}, keyvals...)
}

// Connect validates the setup and returns a usable Client. An error means the
// setup is invalid, the cache is missing or the broker rejected the topology —
// a broker outage is a state, not an error: the client then starts
// disconnected, caches every publish, accepts handler registrations and keeps
// reconnecting in background until the broker is back, when it declares the
// topology, applies the bindings and flushes the cache. The cache keeps those
// offline messages and tracks the queue bindings, so it is required — use
// gorabbit.NewMemoryCache when a shared store is not needed.
func (s *Setup[E]) Connect(cache gorabbit.Cache) (*Client[E], error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	if cache == nil {
		return nil, errors.New("gorabbit: cache is required")
	}

	ctx := context.Background()

	c := newClient(s, cache)

	if err := c.claimCacheKey(); err != nil {
		return nil, err
	}

	if err := c.reconnect(ctx); err != nil {
		// Only a deploy fixes a refused topology, so it must stop the boot
		// instead of leaving a client that can never publish.
		if errors.Is(err, ErrTopologyRejected) {
			c.setup.logger.Error(ctx, "gorabbit: broker rejected the topology", c.connFields("error", err)...)
			c.Close()
			return nil, err
		}

		c.setup.logger.Warn(ctx, "gorabbit: broker unreachable, starting disconnected", c.connFields("error", err)...)
	}

	// Started here, not in Start: a publish-only client must also heal.
	go c.monitorConnection(ctx)
	go c.retryPendingMessages(ctx)

	return c, nil
}

// The bool tells the caller it is the one that brought the connection up, so a
// single flush of the cache follows a reconnection.
func (c *Client[E]) connect(ctx context.Context) (bool, error) {
	if c.closed() {
		return false, ErrClientClosed
	}

	// Several loops notice a drop at once; only the first one may redial, or a
	// live connection gets replaced under a publish in flight.
	if c.connected() {
		return false, nil
	}

	// Whoever finds a dial in flight does not queue behind it: waiting a whole
	// dial timeout is exactly what the offline path exists to avoid.
	if !c.dialing.CompareAndSwap(false, true) {
		return false, c.pendingErr()
	}
	defer c.dialing.Store(false)

	if c.connected() {
		return false, nil
	}

	c.setup.logger.Info(ctx, "gorabbit: connecting to RabbitMQ", c.connFields()...)
	c.dropConnection()

	bc, err := c.dial(ctx)
	if err != nil {
		return false, c.failedDial(err)
	}

	if err := c.establish(ctx, bc); err != nil {
		_ = bc.conn.Close()
		return false, c.failedSetup(err)
	}

	c.setup.logger.Info(ctx, "gorabbit: connected to RabbitMQ", c.connFields()...)

	return true, nil
}

// dial runs with no lock held: a broker that swallows the handshake would
// otherwise hold Close, Connected and every publisher for a whole dial timeout.
func (c *Client[E]) dial(ctx context.Context) (*brokerConn, error) {
	cfg := amqp091.Config{
		Properties: amqp091.Table{"connection_name": c.setup.appName},
	}
	if c.setup.dialTimeout > 0 {
		cfg.Dial = amqp091.DefaultDial(c.setup.dialTimeout)
	}

	conn, err := amqp091.DialConfig(c.setup.amqpURL, cfg)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to dial amqp", c.connFields("error", err)...)
		return nil, err
	}

	bc := &brokerConn{conn: conn}

	bc.ch, err = conn.Channel()
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to open channel", c.connFields("error", err)...)
		_ = conn.Close()
		return nil, err
	}

	bc.pubCh, err = conn.Channel()
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to open publish channel", c.connFields("error", err)...)
		_ = conn.Close()
		return nil, err
	}

	// Without confirms the broker never answers a publish, so a message lost
	// between the socket and the queue would look delivered.
	if err = bc.pubCh.Confirm(false); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to put the publish channel in confirm mode", c.connFields("error", err)...)
		_ = conn.Close()
		return nil, err
	}

	bc.returns = newReturnTracker(bc.pubCh)

	if c.setup.isConsumer && c.setup.preFetchCount > 0 {
		if err = bc.ch.Qos(c.setup.preFetchCount, 0, false); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to set prefetch count", c.connFields("error", err)...)
			_ = conn.Close()
			return nil, err
		}
	}

	return bc, nil
}

// Nothing may use the connection before its topology exists, and a handler
// registered meanwhile must be bound by exactly one of the two sides.
func (c *Client[E]) establish(ctx context.Context, bc *brokerConn) error {
	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	if err := c.applyTopology(ctx, bc.ch); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed() {
		return ErrClientClosed
	}

	c.conn, c.ch, c.pubCh, c.returns = bc.conn, bc.ch, bc.pubCh, bc.returns
	c.setupErr = nil
	c.isConnected = true

	return nil
}

// dropConnection lets go of the dead connection before the redial, so nothing
// reaches for a channel that belongs to it.
func (c *Client[E]) dropConnection() {
	c.mu.Lock()
	conn := c.conn
	c.conn, c.ch, c.pubCh, c.returns = nil, nil, nil, nil
	c.isConnected = false
	c.mu.Unlock()

	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
}

// Start launches the consumer loops; connection monitoring runs since Connect.
// It is non-blocking and must be called after every handler has been registered.
func (c *Client[E]) Start(ctx context.Context) {
	if c.setup.isConsumer {
		c.consumerWg.Add(1)
		go c.consume(ctx)
		go c.unbindUnusedBindings(ctx)
	}
}

// Close stops the background loops and closes the connection. It is idempotent.
func (c *Client[E]) Close() {
	c.closeOnce.Do(func() {
		close(c.done)

		// The broker must stop delivering, and the delivery already in a
		// handler must reach its ack, before the channel goes away.
		c.cancelConsumer()
		c.waitForConsumer()

		c.mu.Lock()
		defer c.mu.Unlock()

		for _, ch := range []*amqp091.Channel{c.ch, c.pubCh} {
			if ch == nil {
				continue
			}
			if err := ch.Close(); err != nil {
				c.setup.logger.Error(context.Background(), "gorabbit: error closing channel", c.connFields("error", err)...)
			}
		}

		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				c.setup.logger.Error(context.Background(), "gorabbit: error closing connection", c.connFields("error", err)...)
			}
		}

		c.isConnected = false
		c.releaseCacheKey()
	})
}

func (c *Client[E]) applyTopology(ctx context.Context, ch *amqp091.Channel) error {
	if err := c.createTopicExchanges(ctx, ch); err != nil {
		return err
	}

	if !c.setup.isConsumer {
		return nil
	}

	if err := c.createQueues(ctx, ch); err != nil {
		return err
	}

	if err := c.bindQueues(ctx, ch); err != nil {
		return err
	}

	return c.bindRegisteredHandlers(ctx, ch)
}

// bindRegisteredHandlers re-creates the binding of every registered handler,
// which is what makes a registration done while disconnected effective. Caller
// holds c.handlersMu.
func (c *Client[E]) bindRegisteredHandlers(ctx context.Context, ch *amqp091.Channel) error {
	for _, hi := range c.handlers {
		if err := c.bindQueueToExchange(ctx, ch, hi.Exchange, hi.BindingKey, c.setup.queueName, true); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to bind queue to exchange", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createTopicExchanges(ctx context.Context, ch *amqp091.Channel) error {
	if err := c.createTopicExchange(ch, c.setup.exchangeName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create topic exchange", "error", err)
		return err
	}

	if !c.setup.isConsumer {
		return nil
	}

	// Exchange the retry queue dead-letters back into, owned by this consumer.
	if err := c.createTopicExchange(ch, c.setup.queueName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create consumer exchange", "error", err)
		return err
	}

	if err := c.createTopicExchange(ch, c.setup.dlqName); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create dlq exchange", "error", err)
		return err
	}

	if c.setup.withRetry {
		if err := c.createTopicExchange(ch, c.setup.retryName); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create retry exchange", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createQueues(ctx context.Context, ch *amqp091.Channel) error {
	if err := c.createQueue(ch, c.setup.dlqName, nil); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to create dlq queue", "error", err)
		return err
	}

	args := amqp091.Table{"x-dead-letter-exchange": c.setup.dlqName}
	if err := c.createQueue(ch, c.setup.queueName, args); err != nil {
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

		if err := c.createQueue(ch, c.setup.retryName, retryArgs); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create retry queue", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) bindQueues(ctx context.Context, ch *amqp091.Channel) error {
	// "#" is the AMQP wildcard: these queues take every routing key, which is
	// what lets a republished message keep the routing key it was published
	// with instead of being flattened to an empty one.
	if err := c.bindQueueToExchange(ctx, ch, c.setup.dlqName, "#", c.setup.dlqName, false); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to bind dlq queue", "error", err)
		return err
	}

	if err := c.bindQueueToExchange(ctx, ch, c.setup.queueName, "#", c.setup.queueName, false); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to bind queue", "error", err)
		return err
	}

	if c.setup.withRetry {
		if err := c.bindQueueToExchange(ctx, ch, c.setup.retryName, "#", c.setup.retryName, false); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to bind retry queue", "error", err)
			return err
		}
	}

	return nil
}

func (c *Client[E]) createTopicExchange(ch *amqp091.Channel, exchange string) error {
	return ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
}

func (c *Client[E]) createQueue(ch *amqp091.Channel, queue string, args amqp091.Table) error {
	_, err := ch.QueueDeclare(queue, true, false, false, false, args)
	return err
}

// bindQueueToExchange binds a queue to an exchange. tryCreateExchange declares
// the exchange first, needed when this consumer is not its owner and the owning
// application may not have started yet.
func (c *Client[E]) bindQueueToExchange(ctx context.Context, ch *amqp091.Channel, exchange, routingKey, queueName string, tryCreateExchange bool) error {
	if tryCreateExchange {
		if err := c.createTopicExchange(ch, exchange); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to create topic exchange", "error", err)
			return err
		}
	}

	return ch.QueueBind(queueName, routingKey, exchange, false, nil)
}

func (c *Client[E]) cancelConsumer() {
	if !c.setup.isConsumer {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Not live(): a client marked disconnected can still hold the socket the
	// consume loop is parked on, and only the broker's cancel releases it.
	if c.ch == nil || c.conn == nil || c.conn.IsClosed() {
		return
	}

	if err := c.ch.Cancel(c.setup.appName, false); err != nil {
		c.setup.logger.Error(context.Background(), "gorabbit: error cancelling the consumer", c.connFields("error", err)...)
	}
}

func (c *Client[E]) waitForConsumer() {
	drained := make(chan struct{})
	go func() {
		c.consumerWg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(closeDrainTimeout):
		c.setup.logger.Error(context.Background(), "gorabbit: closing while a handler is still running", c.connFields()...)
	}
}

// withConsumerChannel serializes the consumer's control methods: amqp091 does
// not, so two in-flight rpc calls on one channel can take each other's reply.
func (c *Client[E]) withConsumerChannel(fn func(*amqp091.Channel) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.live() || c.ch == nil {
		return errNotConnected
	}

	return fn(c.ch)
}

// publishChannel guards the channel pointer, which a reconnection may swap
// while a publisher is using it.
func (c *Client[E]) publishChannel() (*amqp091.Channel, *returnTracker, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.live() || c.pubCh == nil || c.returns == nil {
		return nil, nil, errNotConnected
	}

	return c.pubCh, c.returns, nil
}

// Connected reports whether the client holds a live connection right now — for
// health checks and metrics. A disconnected client is still usable: publishes
// are cached and registered handlers wait for the connection.
func (c *Client[E]) Connected() bool { return c.connected() }

func (c *Client[E]) connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live()
}

// live is the truth behind isConnected: a drop the broker side closes is
// otherwise invisible to a client that never consumes. Caller holds c.mu.
func (c *Client[E]) live() bool {
	return c.isConnected && c.conn != nil && !c.conn.IsClosed()
}

// A dial that never lands says nothing about the topology — refused credentials
// and an unknown vhost also answer 403 — so caching is the right answer to it.
func (c *Client[E]) failedDial(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setupErr = nil

	return err
}

// failedSetup tells a topology the broker refuses — which every retry meets
// again — apart from losing the connection midway, which caching answers.
func (c *Client[E]) failedSetup(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if rejectedTopology(err) {
		c.setupErr = fmt.Errorf("%w: %w", ErrTopologyRejected, err)
		return c.setupErr
	}

	c.setupErr = nil

	return err
}

// pendingErr answers whoever skipped the dial: a rejected topology is the
// caller's answer, an outage is not — the dial in flight may still land.
func (c *Client[E]) pendingErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.setupErr != nil {
		return c.setupErr
	}

	return errDialInProgress
}

// A soft channel error from the declaration phase answers a topology the broker
// will not accept, unlike a connection lost while it was being applied.
func rejectedTopology(err error) bool {
	var amqpErr *amqp091.Error
	if !errors.As(err, &amqpErr) {
		return false
	}

	switch amqpErr.Code {
	case amqp091.AccessRefused, amqp091.NotFound, amqp091.ResourceLocked, amqp091.PreconditionFailed:
		return true
	}

	return false
}

func (c *Client[E]) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// bindHandler binds now when connected; while disconnected the binding is
// applied by the next successful connection.
func (c *Client[E]) bindHandler(ctx context.Context, hi handlerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isConnected || c.ch == nil {
		return
	}

	if err := c.bindQueueToExchange(ctx, c.ch, hi.Exchange, hi.BindingKey, c.setup.queueName, true); err != nil {
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
