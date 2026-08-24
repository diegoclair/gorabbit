package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const handlerInfoCachePrefix = "gorabbit:handler-info"

// RegisterHandler routes every message of type T to handler. It binds the
// consumer queue to the exchange that owns the message type, so it must be
// called before Start.
func RegisterHandler[T gorabbit.Message](ctx context.Context, c *Client, msgType T, handler gorabbit.Handler[T]) error {
	if !c.setup.isConsumer {
		return errors.New("gorabbit: client is not a consumer, use WithConsumer")
	}
	if handler == nil {
		return errors.New("gorabbit: handler is required")
	}

	msgName := messageTypeName(msgType)
	exchangeOwnerName := msgType.ExchangeOwnerName()
	if exchangeOwnerName == "" {
		return fmt.Errorf("gorabbit: message %s has an empty exchange owner name", msgName)
	}

	if err := c.bindQueueToExchange(ctx, exchangeOwnerName, msgName, c.setup.queueName, true); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to bind queue to exchange", "error", err)
		return err
	}

	hi := handlerInfo{
		Exchange:   exchangeOwnerName,
		RoutingKey: msgName,
		handler: func(ctx context.Context, msg amqp091.Delivery) error {
			return handleMessageSafely(ctx, c, &msg, handler)
		},
	}

	if err := c.storeHandlerInfo(ctx, hi); err != nil {
		return err
	}

	c.handlers[handlersMapKey(exchangeOwnerName, msgName)] = hi
	// A retried message comes back through this consumer's own exchange, so the
	// same handler has to be reachable under it.
	c.handlers[handlersMapKey(c.setup.queueName, msgName)] = hi

	return nil
}

func handlersMapKey(exchange, routingKey string) string {
	return fmt.Sprintf("%s:%s", exchange, routingKey)
}

// storeHandlerInfo records the binding so a later run can unbind it once the
// handler is gone from the code.
func (c *Client) storeHandlerInfo(ctx context.Context, info handlerInfo) error {
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

func (c *Client) handlerInfoCacheKey(info handlerInfo) string {
	return fmt.Sprintf("%s:%s:%s:%s", handlerInfoCachePrefix, c.setup.queueName, info.Exchange, info.RoutingKey)
}

// handleMessageSafely turns a panic inside the handler into an error, so one bad
// message cannot take the consumer down.
func handleMessageSafely[T gorabbit.Message](ctx context.Context, c *Client, msg *amqp091.Delivery, handler gorabbit.Handler[T]) (err error) {
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

func (c *Client) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		default:
			if !c.connected() {
				c.setup.logger.Info(ctx, "gorabbit: consumer not connected, attempting to reconnect")
				if err := c.connect(ctx); err != nil {
					c.setup.logger.Error(ctx, "gorabbit: failed to reconnect", "error", err)
					time.Sleep(c.setup.reconnectDelay)
				}
				continue
			}

			if err := c.consumeMessages(ctx); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: error consuming messages", "error", err)
				c.setConnected(false)
			}
		}
	}
}

func (c *Client) consumeMessages(ctx context.Context) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(c.setup.queueName, c.setup.appName, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("gorabbit: failed to start consuming messages: %w", err)
	}

	c.setup.logger.Info(ctx, "gorabbit: started consuming messages", "queue", c.setup.queueName)

	for msg := range msgs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return nil
		default:
			if err := c.processMessage(ctx, msg); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: error processing message", "error", err)
			}
		}
	}

	return errors.New("gorabbit: consume channel closed unexpectedly")
}

func (c *Client) processMessage(ctx context.Context, msg amqp091.Delivery) error {
	handlerInfo, ok := c.handlers[handlersMapKey(msg.Exchange, msg.RoutingKey)]
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

func (c *Client) republishMessageToRetryExchange(ctx context.Context, msg *amqp091.Delivery) error {
	retryCount := c.retryCount(ctx, msg) + 1

	if retryCount > c.setup.retryCount {
		c.setup.logger.Error(ctx, "gorabbit: max retry count reached, sending to dlq",
			"app_name", c.setup.appName,
			"message_type", msg.Type,
			"message_id", msg.MessageId,
		)

		// Ack first so the broker does not also dead-letter it on its own.
		if err := msg.Ack(false); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to ack message", "error", err)
		}

		return c.republishMessage(ctx, c.setup.dlqName, msg.RoutingKey, msg)
	}

	if msg.Headers == nil {
		msg.Headers = amqp091.Table{}
	}
	msg.Headers[retryCountHeaderKey] = retryCount

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

// republishMessage keeps the original routing key so a message coming back from
// the retry exchange still reaches its handler.
func (c *Client) republishMessage(ctx context.Context, exchange, routingKey string, msg *amqp091.Delivery) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	err = ch.Publish(exchange, routingKey, false, false, amqp091.Publishing{
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

func (c *Client) retryCount(ctx context.Context, msg *amqp091.Delivery) int {
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
func (c *Client) unbindUnusedBindings(ctx context.Context) {
	ch, err := c.channel()
	if err != nil {
		return
	}

	keys, err := c.cache.GetAllKeys(ctx, fmt.Sprintf("%s:%s:*", handlerInfoCachePrefix, c.setup.queueName))
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

		if _, ok := c.handlers[handlersMapKey(info.Exchange, info.RoutingKey)]; ok {
			continue
		}

		c.setup.logger.Debug(ctx, "gorabbit: unbinding unused binding",
			"exchange", info.Exchange,
			"routing_key", info.RoutingKey,
			"queue", c.setup.queueName,
		)

		if err := ch.QueueUnbind(c.setup.queueName, info.RoutingKey, info.Exchange, nil); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to unbind queue", "error", err)
			continue
		}

		if err := c.cache.Delete(ctx, key); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error to delete handler info", "error", err)
		}
	}
}
