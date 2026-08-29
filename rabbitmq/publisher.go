package rabbitmq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"maps"
	"uuid"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const retryCountHeaderKey = "x-retry-count"

var (
	errNotConfirmed = errors.New("gorabbit: message was not confirmed by the broker")
	// The type name opens the routing key, so an unnamed type can match no
	// binding.
	errEmptyTypeName = errors.New("gorabbit: message type has no name, use a named struct")
	// errUnroutable reports a message the broker gave back for want of a queue.
	// It is not a delivery, so the caller must keep its copy.
	errUnroutable = errors.New("gorabbit: no queue is bound to the routing key")
)

type publishMessage struct {
	MsgID       string
	MsgTypeName string
	RoutingKey  string
	MsgBody     []byte
	MsgHeaders  amqp091.Table
}

func (pm *publishMessage) toCacheMessage() cachedMessage {
	return cachedMessage{
		MsgID:       pm.MsgID,
		MsgTypeName: pm.MsgTypeName,
		RoutingKey:  pm.RoutingKey,
		Message:     pm.MsgBody,
		MsgHeaders:  pm.MsgHeaders,
	}
}

func fromCacheMessage(cm *cachedMessage) publishMessage {
	return publishMessage{
		MsgID:       cm.MsgID,
		MsgTypeName: cm.MsgTypeName,
		RoutingKey:  cm.RoutingKey,
		MsgBody:     cm.Message,
		MsgHeaders:  cm.MsgHeaders,
	}
}

func (c *Client[E]) getPublishMessage(ctx context.Context, msg gorabbit.OwnedBy[E]) (*publishMessage, error) {
	if isNil(msg) {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "error", errNilMessage)
		return nil, errNilMessage
	}

	typeName := messageTypeName(msg)
	if typeName == "" {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "error", errEmptyTypeName)
		return nil, errEmptyTypeName
	}

	routingKey, err := publishRoutingKey(msg, typeName)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "error", err)
		return nil, err
	}

	if err := checkRoutingKeyLength(routingKey); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "error", err)
		return nil, err
	}

	body, err := json.Marshal(msg)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to marshal message", "error", err)
		return nil, err
	}

	headers := amqp091.Table{}
	maps.Copy(headers, c.setup.headers.FromContext(ctx))

	return &publishMessage{
		// Assigned before caching so a message replayed from the cache keeps the
		// same id, which is what lets a consumer detect the duplicate. v7 keeps
		// ids sortable by creation time.
		MsgID:       uuid.NewV7().String(),
		MsgTypeName: typeName,
		RoutingKey:  routingKey,
		MsgBody:     body,
		MsgHeaders:  headers,
	}, nil
}

// Publish sends a message to the exchange this client owns; another exchange
// does not compile. An unreachable broker and a missing binding both cache.
func (c *Client[E]) Publish(ctx context.Context, msg gorabbit.OwnedBy[E]) error {
	pm, err := c.getPublishMessage(ctx, msg)
	if err != nil {
		return err
	}

	if !c.connected() {
		c.setup.logger.Info(ctx, "gorabbit: not connected, attempting to reconnect before publishing", c.connFields()...)
		if err := c.reconnect(ctx); err != nil {
			if errors.Is(err, ErrClientClosed) || errors.Is(err, ErrTopologyRejected) {
				return err
			}

			c.setup.logger.Error(ctx, "gorabbit: failed to reconnect, caching message", c.connFields("error", err)...)
			return c.cacheMessage(ctx, pm)
		}
	}

	err = c.publish(ctx, pm)
	if err == nil {
		return nil
	}

	if errors.Is(err, errUnroutable) {
		c.setup.logger.Error(ctx, "gorabbit: message has no destination, caching it until a queue is bound",
			"message_type", pm.MsgTypeName,
			"routing_key", pm.RoutingKey,
			"message_id", pm.MsgID,
		)
		if err := c.cacheMessage(ctx, pm); err != nil {
			return err
		}

		c.signalPending()

		return nil
	}

	c.setup.logger.Error(ctx, "gorabbit: failed to publish, caching message", "error", err)

	return c.cacheMessage(ctx, pm)
}

func (c *Client[E]) publish(ctx context.Context, pm *publishMessage) error {
	err := c.publishConfirmed(ctx, c.setup.exchangeName, pm.RoutingKey, amqp091.Publishing{
		ContentType:  "application/json",
		MessageId:    pm.MsgID,
		Type:         pm.MsgTypeName,
		Body:         pm.MsgBody,
		DeliveryMode: amqp091.Persistent,
		Headers:      pm.MsgHeaders,
	})
	if err != nil && !errors.Is(err, errUnroutable) {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "message_type", pm.MsgTypeName, "error", err)
	}

	return err
}

// Callers drop their own copy of the message on success, so a nil error must
// mean the broker owns it.
func (c *Client[E]) publishConfirmed(ctx context.Context, exchange, routingKey string, msg amqp091.Publishing) error {
	ch, returns, err := c.publishChannel()
	if err != nil {
		return err
	}

	// mandatory, so a routing key nothing is bound to comes back instead of
	// being dropped by the broker behind a confirm that says it was delivered.
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, msg)
	if err != nil {
		// A caller giving up says nothing about the socket, and dropping the
		// state here would tear a healthy connection down.
		if ctx.Err() == nil {
			c.setConnected(false)
		}
		return err
	}
	if confirmation == nil {
		return errNotConfirmed
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.setup.confirmTimeout)
	defer cancel()

	acked, waitErr := confirmation.WaitContext(waitCtx)

	// Asked after the confirm on purpose: the broker returns a message before it
	// confirms it, so this is the first moment the answer can be complete.
	if returns.wasReturned(msg.MessageId) {
		return errUnroutable
	}

	if waitErr != nil {
		return waitErr
	}
	if !acked {
		return errNotConfirmed
	}

	return nil
}
