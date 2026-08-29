package rabbitmq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const (
	handlerInfoCachePrefix = "gorabbit:handler-info"
	// Set when a message is sent to the retry exchange: it comes back through
	// this consumer's own exchange, and the original one is what tells apart two
	// message types that share a name (and therefore a routing key).
	originExchangeHeaderKey = "x-origin-exchange"
)

// Subscribe takes the whole of type T, whoever owns it and whatever route it
// carries. It binds the consumer queue and must be called before Start.
func Subscribe[T gorabbit.Message, E gorabbit.Exchange](ctx context.Context, c *Client[E], msgType T, handler gorabbit.Handler[T]) error {
	typeName, exchange, err := subscriptionOf(c, msgType, handler)
	if err != nil {
		return err
	}

	bindingKey := typeName
	if gorabbit.IsRouted(msgType) {
		bindingKey += allRoutesSuffix
	}

	return registerHandler(ctx, c, exchange, bindingKey, handler)
}

// SubscribeRoute takes one slice of type T, so several queues can split it
// instead of each receiving every copy. Only a routed message compiles here.
func SubscribeRoute[T gorabbit.RoutedMessage, E gorabbit.Exchange](ctx context.Context, c *Client[E], msgType T, route string, handler gorabbit.Handler[T]) error {
	typeName, exchange, err := subscriptionOf(c, msgType, handler)
	if err != nil {
		return err
	}

	bindingKey := routedKey(typeName, route)
	if err := checkRoutingKeyLength(bindingKey); err != nil {
		return err
	}

	return registerHandler(ctx, c, exchange, bindingKey, handler)
}

func subscriptionOf[T gorabbit.Message, E gorabbit.Exchange](c *Client[E], msgType T, handler gorabbit.Handler[T]) (typeName, exchange string, err error) {
	if !c.setup.isConsumer {
		return "", "", errors.New("gorabbit: client is not a consumer, use WithConsumer")
	}
	if handler == nil {
		return "", "", errors.New("gorabbit: handler is required")
	}

	exchange, err = exchangeOf(msgType)
	if err != nil {
		return "", "", err
	}

	typeName = messageTypeName(msgType)
	if typeName == "" {
		return "", "", errEmptyTypeName
	}

	return typeName, exchange, nil
}

func registerHandler[T gorabbit.Message, E gorabbit.Exchange](ctx context.Context, c *Client[E], exchange, bindingKey string, handler gorabbit.Handler[T]) error {
	hi := handlerInfo{
		Exchange:   exchange,
		BindingKey: bindingKey,
		handler: func(ctx context.Context, msg amqp091.Delivery) error {
			return handleMessageSafely(ctx, c, &msg, handler)
		},
	}

	// Registered before binding: a connection landing in between re-binds every
	// registered handler, so the bind is never lost.
	key := handlersMapKey(exchange, bindingKey)
	c.handlersMu.Lock()
	if _, ok := c.handlers[key]; ok {
		c.handlersMu.Unlock()
		return fmt.Errorf("gorabbit: a handler is already registered for %s", key)
	}
	c.handlers[key] = hi
	c.handlersMu.Unlock()

	if err := c.storeHandlerInfo(ctx, hi); err != nil {
		c.handlersMu.Lock()
		delete(c.handlers, key)
		c.handlersMu.Unlock()
		return err
	}

	c.bindHandler(ctx, hi)

	return nil
}

func handlersMapKey(exchange, bindingKey string) string {
	return fmt.Sprintf("%s:%s", exchange, bindingKey)
}

// storeHandlerInfo records the binding so a later run can unbind it once the
// handler is gone from the code.
func (c *Client[E]) storeHandlerInfo(ctx context.Context, info handlerInfo) error {
	infoBytes, err := json.Marshal(info)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to marshal handler info", "error", err)
		return err
	}

	if err := c.cache.Set(ctx, c.handlerInfoCacheKey(info), infoBytes, 0); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to cache handler info", "error", err)
		return err
	}

	return nil
}

func (c *Client[E]) handlerInfoCacheKey(info handlerInfo) string {
	return fmt.Sprintf("%s:%s:%s:%s", handlerInfoCachePrefix, c.cacheScope(), info.Exchange, info.BindingKey)
}

// handleMessageSafely turns a panic inside the handler into an error, so one bad
// message cannot take the consumer down.
func handleMessageSafely[T gorabbit.Message, E gorabbit.Exchange](ctx context.Context, c *Client[E], msg *amqp091.Delivery, handler gorabbit.Handler[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("gorabbit: panic recovered: %v", r)

			c.setup.logger.Error(ctx, "gorabbit: panic recovered",
				"panic_error", r,
				"message_type", msg.Type,
				"exchange", msg.Exchange,
				"routing_key", msg.RoutingKey,
				"message_id", msg.MessageId,
			)
		}
	}()

	var payload T
	if err = json.Unmarshal(msg.Body, &payload); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to unmarshal message", "error", err)
		return err
	}

	return handler(ctx, payload)
}

func (c *Client[E]) consume(ctx context.Context) {
	defer c.consumerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		default:
			// Reconnecting is the monitor's job alone; connect explains why.
			if !c.connected() {
				select {
				case <-ctx.Done():
					return
				case <-c.done:
					return
				case <-time.After(c.setup.reconnectDelay):
				}
				continue
			}

			if err := c.consumeMessages(ctx); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: error consuming messages", c.connFields("error", err)...)
				c.setConnected(false)
			}
		}
	}
}

func (c *Client[E]) consumeMessages(ctx context.Context) error {
	var msgs <-chan amqp091.Delivery
	err := c.withConsumerChannel(func(ch *amqp091.Channel) error {
		var err error
		msgs, err = ch.Consume(c.setup.queueName, c.setup.appName, false, false, false, false, nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("gorabbit: failed to start consuming messages: %w", err)
	}

	c.setup.logger.Info(ctx, "gorabbit: started consuming messages", c.connFields("queue", c.setup.queueName)...)

	// Only consuming stops with the context: the connection is intact and still
	// publishes, so no delivery may be pulled and left unacked.
	stopCancelling := context.AfterFunc(ctx, c.cancelConsumer)
	defer stopCancelling()

	var workers sync.WaitGroup
	for range c.setup.concurrency {
		workers.Go(func() { c.runWorker(ctx, msgs) })
	}

	// A reconnect calls this again, so this generation of workers has to be gone
	// before it returns: two pools on one queue would process side by side.
	workers.Wait()

	// A channel we asked to close is a shutdown, not the outage that must make
	// the caller drop the connection and reconnect.
	if ctx.Err() != nil || c.closed() {
		return nil
	}

	return errors.New("gorabbit: consume channel closed unexpectedly")
}

func (c *Client[E]) runWorker(ctx context.Context, msgs <-chan amqp091.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: error processing message", "error", err)
			}
		}
	}
}

// handlerFor matches a retried message by the exchange it was published to, not
// the consumer's own, and an unclaimed route by the type that took them all.
func (c *Client[E]) handlerFor(msg *amqp091.Delivery) (handlerInfo, bool) {
	exchange := msg.Exchange

	if msg.Exchange == c.setup.queueName {
		if origin, ok := msg.Headers[originExchangeHeaderKey].(string); ok && origin != "" {
			exchange = origin
		}
	}

	c.handlersMu.RLock()
	defer c.handlersMu.RUnlock()

	if info, ok := c.handlers[handlersMapKey(exchange, msg.RoutingKey)]; ok {
		return info, true
	}

	typeName := typeNameOf(msg.RoutingKey)
	if typeName == msg.RoutingKey {
		return handlerInfo{}, false
	}

	info, ok := c.handlers[handlersMapKey(exchange, typeName+allRoutesSuffix)]

	return info, ok
}

func (c *Client[E]) processMessage(ctx context.Context, msg amqp091.Delivery) error {
	handlerInfo, ok := c.handlerFor(&msg)
	if !ok {
		c.setup.logger.Error(ctx, "gorabbit: no handler for message", "message_type", msg.Type, "routing_key", msg.RoutingKey)
		return msg.Nack(false, false) // no handler, no requeue
	}

	ctx = c.setup.headers.ToContext(ctx, msg.Headers)

	if err := handlerInfo.handler(ctx, msg); err != nil {
		if c.setup.withRetry && (c.setup.retryableErrorFunc == nil || c.setup.retryableErrorFunc(err)) {
			return c.republishMessageToRetryExchange(ctx, &msg)
		}
		return msg.Nack(false, false)
	}

	return msg.Ack(false)
}

func (c *Client[E]) republishMessageToRetryExchange(ctx context.Context, msg *amqp091.Delivery) error {
	retryCount := c.retryCount(ctx, msg) + 1

	if retryCount > c.setup.retryCount {
		c.setup.logger.Error(ctx, "gorabbit: max retry count reached, sending to dlq",
			"app_name", c.setup.appName,
			"message_type", msg.Type,
			"message_id", msg.MessageId,
		)

		// The queue's dead-letter exchange routes it, so no publish of ours can
		// die after the delivery is already acked.
		return msg.Nack(false, false)
	}

	if msg.Headers == nil {
		msg.Headers = amqp091.Table{}
	}
	msg.Headers[retryCountHeaderKey] = retryCount
	c.stampOriginExchange(msg)

	c.setup.logger.Warn(ctx, "gorabbit: republishing message to retry exchange",
		"message_type", msg.Type,
		"retry_exchange", c.setup.retryName,
		"retry_count", retryCount,
	)

	if err := c.republishMessage(ctx, c.setup.retryName, msg.RoutingKey, msg); err != nil {
		return err
	}

	// The original delivery is only acked once the copy is safely on the retry
	// exchange, otherwise it would be dead-lettered on every redelivery.
	return msg.Ack(false)
}

// stampOriginExchange records where the message came from before it starts
// bouncing through the retry topology. Later attempts arrive from this
// consumer's own exchange, so only the first one carries the real origin.
func (c *Client[E]) stampOriginExchange(msg *amqp091.Delivery) {
	if msg.Exchange == c.setup.queueName {
		return
	}
	if origin, ok := msg.Headers[originExchangeHeaderKey].(string); ok && origin != "" {
		return
	}

	msg.Headers[originExchangeHeaderKey] = msg.Exchange
}

// republishMessage keeps the original routing key so a message coming back from
// the retry exchange still reaches its handler.
func (c *Client[E]) republishMessage(ctx context.Context, exchange, routingKey string, msg *amqp091.Delivery) error {
	err := c.publishConfirmed(ctx, exchange, routingKey, amqp091.Publishing{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		Type:            msg.Type,
		Body:            msg.Body,
		Headers:         msg.Headers,
		DeliveryMode:    amqp091.Persistent,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		MessageId:       msg.MessageId,
		Timestamp:       time.Now(),
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Expiration:      msg.Expiration,
	})
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to republish message", "exchange", exchange, "error", err)
		return err
	}

	return nil
}

func (c *Client[E]) retryCount(ctx context.Context, msg *amqp091.Delivery) int {
	val, ok := msg.Headers[retryCountHeaderKey]
	if !ok {
		return 0
	}

	switch v := val.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		c.setup.logger.Error(ctx, "gorabbit: unexpected type for retry count", "type", fmt.Sprintf("%T", v))
		return 0
	}
}

// unbindUnusedBindings removes bindings recorded by previous runs whose handler
// no longer exists, so the queue stops receiving messages nothing handles.
func (c *Client[E]) unbindUnusedBindings(ctx context.Context) {
	// Start may run while disconnected; the cleanup waits for a live channel.
	ticker := time.NewTicker(c.setup.reconnectDelay)
	defer ticker.Stop()

	for !c.connected() {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
		}
	}

	keys, err := c.cache.GetAllKeys(ctx, fmt.Sprintf("%s:%s:*", handlerInfoCachePrefix, c.cacheScope()))
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to get handler info keys", "error", err)
		return
	}

	for _, key := range keys {
		infoBytes, err := c.cache.Get(ctx, key)
		if err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to get handler info", "key", key, "error", err)
			continue
		}
		if len(infoBytes) == 0 {
			continue
		}

		var info handlerInfo
		if err := json.Unmarshal(infoBytes, &info); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to unmarshal handler info", "key", key, "error", err)
			continue
		}

		c.handlersMu.RLock()
		_, registered := c.handlers[handlersMapKey(info.Exchange, info.BindingKey)]
		c.handlersMu.RUnlock()
		if registered {
			continue
		}

		c.setup.logger.Debug(ctx, "gorabbit: unbinding unused binding",
			"exchange", info.Exchange,
			"binding_key", info.BindingKey,
			"queue", c.setup.queueName,
		)

		err = c.withConsumerChannel(func(ch *amqp091.Channel) error {
			return ch.QueueUnbind(c.setup.queueName, info.BindingKey, info.Exchange, nil)
		})
		if err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to unbind queue", "error", err)
			continue
		}

		if err := c.cache.Delete(ctx, key); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to delete handler info", "error", err)
		}
	}
}
