package rabbitmq

import (
	"context"
	"encoding/json"
	"maps"
	"time"
	"uuid"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const retryCountHeaderKey = "x-retry-count"

type publishMessage struct {
	MsgID       string
	MsgTypeName string
	MsgBody     []byte
	MsgHeaders  amqp091.Table
}

func (pm *publishMessage) toCacheMessage() cachedMessage {
	return cachedMessage{
		MsgID:       pm.MsgID,
		MsgTypeName: pm.MsgTypeName,
		Message:     pm.MsgBody,
		MsgHeaders:  pm.MsgHeaders,
		Timestamp:   time.Now(),
	}
}

func fromCacheMessage(cm *cachedMessage) publishMessage {
	return publishMessage{
		MsgID:       cm.MsgID,
		MsgTypeName: cm.MsgTypeName,
		MsgBody:     cm.Message,
		MsgHeaders:  cm.MsgHeaders,
	}
}

func (c *Client) getPublishMessage(ctx context.Context, msg gorabbit.Message) (*publishMessage, error) {
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
		MsgTypeName: messageTypeName(msg),
		MsgBody:     body,
		MsgHeaders:  headers,
	}, nil
}

// Publish sends a message to the configured exchange using the message type
// name as routing key. When RabbitMQ is unreachable the message is cached and
// published on the next successful connection.
func (c *Client) Publish(ctx context.Context, msg gorabbit.Message) error {
	pm, err := c.getPublishMessage(ctx, msg)
	if err != nil {
		return err
	}

	if !c.connected() {
		c.setup.logger.Info(ctx, "gorabbit: not connected, attempting to reconnect before publishing")
		if err := c.connect(ctx); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: failed to reconnect, caching message", "error", err)
			return c.cacheMessage(ctx, pm)
		}
		c.flushCachedMessages(ctx)
	}

	if err := c.publish(ctx, pm); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: failed to publish, caching message", "error", err)
		return c.cacheMessage(ctx, pm)
	}

	return nil
}

func (c *Client) publish(ctx context.Context, pm *publishMessage) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}

	err = ch.Publish(
		c.setup.exchangeName,
		pm.MsgTypeName, // routing key is always the message type
		false,          // mandatory
		false,          // immediate
		amqp091.Publishing{
			ContentType:  "application/json",
			MessageId:    pm.MsgID,
			Type:         pm.MsgTypeName,
			Body:         pm.MsgBody,
			DeliveryMode: amqp091.Persistent,
			Headers:      pm.MsgHeaders,
		},
	)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to publish message", "message_type", pm.MsgTypeName, "error", err)
		c.setConnected(false)
		return err
	}

	return nil
}
