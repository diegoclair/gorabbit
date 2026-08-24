package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/diegoclair/gorabbit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const (
	cachedMessagePrefix = "gorabbit:cached_messages"
	cachedMessageTTL    = 24 * time.Hour
	// Cached messages are flushed once more after this delay, so a message
	// cached by a concurrent publisher while the first flush was running does
	// not sit in the cache until the next reconnection.
	delayedFlushDelay = 10 * time.Second
)

type cachedMessage struct {
	MsgID       string
	MsgTypeName string
	Message     []byte
	MsgHeaders  amqp091.Table
	Timestamp   time.Time
}

func (c *Client[E]) cacheMessage(ctx context.Context, pm *publishMessage) error {
	cachedMsg := pm.toCacheMessage()

	jsonMsg, err := json.Marshal(cachedMsg)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to marshal message for caching", "error", err)
		return err
	}

	key := cacheKey(c.setup.appName, cachedMsg.Timestamp.UnixNano())
	if err := c.cache.Set(ctx, key, jsonMsg, cachedMessageTTL); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to cache message", "error", err)
		return err
	}

	c.setup.logger.Debug(ctx, "gorabbit: message cached",
		"message_type", cachedMsg.MsgTypeName,
		"message_id", cachedMsg.MsgID,
	)

	return nil
}

func (c *Client[E]) flushCachedMessages(ctx context.Context) {
	if !c.connected() {
		return
	}

	messages, err := c.getMessagesFromCache(ctx)
	if err != nil || len(messages) == 0 {
		return
	}

	for _, msg := range messages {
		pm := fromCacheMessage(&msg)
		if err := c.publish(ctx, &pm); err != nil {
			// Stop on the first failure: the connection is likely gone and the
			// remaining messages must keep their order.
			c.setup.logger.Error(ctx, "gorabbit: error publishing cached message", "error", err)
			return
		}

		key := cacheKey(c.setup.appName, msg.Timestamp.UnixNano())
		if err := c.cache.Delete(ctx, key); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error deleting cached message", "error", err)
		}
	}
}

func (c *Client[E]) getMessagesFromCache(ctx context.Context) ([]cachedMessage, error) {
	keys, err := c.cache.GetAllKeys(ctx, cacheKey(c.setup.appName, 0)+"*")
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error retrieving cached message keys", "error", err)
		return nil, err
	}

	var messages []cachedMessage
	for _, key := range keys {
		jsonMsg, err := c.cache.Get(ctx, key)
		if err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error retrieving cached message", "key", key, "error", err)
			continue
		}
		if len(jsonMsg) == 0 {
			continue
		}

		var cachedMsg cachedMessage
		if err := json.Unmarshal(jsonMsg, &cachedMsg); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error unmarshalling cached message", "key", key, "error", err)
			continue
		}

		messages = append(messages, cachedMsg)
	}

	// Publish in the order the messages were cached.
	slices.SortFunc(messages, func(a, b cachedMessage) int {
		return a.Timestamp.Compare(b.Timestamp)
	})

	return messages, nil
}

func cacheKey(appName string, timestamp int64) string {
	key := fmt.Sprintf("%s:%s:", cachedMessagePrefix, appName)
	if timestamp > 0 {
		key = fmt.Sprintf("%s%d", key, timestamp)
	}

	return key
}

// errNilMessage guards the typed nil: it marshals to "null" and panics on the
// promoted marker method.
var errNilMessage = errors.New("gorabbit: message is nil")

func isNil(msg any) bool {
	v := reflect.ValueOf(msg)
	return !v.IsValid() || (v.Kind() == reflect.Pointer && v.IsNil())
}

// exchangeOf resolves the exchange a message is bound to — any exchange, since a
// consumer handles messages it does not own.
func exchangeOf(msg gorabbit.Message) (string, error) {
	if isNil(msg) {
		return "", errNilMessage
	}

	exchange := gorabbit.ExchangeOf(msg)
	if exchange == "" {
		return "", fmt.Errorf("gorabbit: message %s has an empty exchange name", messageTypeName(msg))
	}

	return exchange, nil
}

// messageTypeName is the routing key of a message: its concrete type name,
// pointers dereferenced so *OrderCreated and OrderCreated route alike.
func messageTypeName(msg any) string {
	t := reflect.TypeOf(msg)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}

	return t.Name()
}

func (c *Client[E]) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(c.setup.reconnectDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
			if c.connected() {
				continue
			}

			c.setup.logger.Info(ctx, "gorabbit: not connected, attempting to reconnect")
			if err := c.reconnect(ctx); err != nil {
				c.setup.logger.Error(ctx, "gorabbit: failed to reconnect", "error", err)
				continue
			}

			c.setup.logger.Info(ctx, "gorabbit: reconnected")
		}
	}
}

// reconnect delivers whatever the cache holds as soon as the connection lands;
// every path that connects goes through it so no flush is missed.
func (c *Client[E]) reconnect(ctx context.Context) error {
	if err := c.connect(ctx); err != nil {
		return err
	}

	c.flushCachedMessages(ctx)
	c.scheduleDelayedFlush(ctx)

	return nil
}

func (c *Client[E]) scheduleDelayedFlush(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		case <-time.After(delayedFlushDelay):
			c.flushCachedMessages(ctx)
		}
	}()
}
