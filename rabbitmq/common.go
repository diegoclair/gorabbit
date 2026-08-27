package rabbitmq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
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
}

func (c *Client[E]) cacheMessage(ctx context.Context, pm *publishMessage) error {
	cachedMsg := pm.toCacheMessage()

	jsonMsg, err := json.Marshal(cachedMsg)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error to marshal message for caching", "error", err)
		return err
	}

	key := cacheKey(c.setup.appName, cachedMsg.MsgID)
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

	// A message leaves the cache only once it is published, so two flushes
	// racing over the same snapshot publish it twice.
	if !c.flushing.CompareAndSwap(false, true) {
		return
	}
	defer c.flushing.Store(false)

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

		key := cacheKey(c.setup.appName, msg.MsgID)
		if err := c.cache.Delete(ctx, key); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error deleting cached message", "error", err)
		}
	}
}

func (c *Client[E]) getMessagesFromCache(ctx context.Context) ([]cachedMessage, error) {
	keys, err := c.cache.GetAllKeys(ctx, cacheKey(c.setup.appName, "")+"*")
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

	// Replay in publish order: the id is a UUIDv7, so it already sorts by time.
	slices.SortFunc(messages, func(a, b cachedMessage) int {
		return strings.Compare(a.MsgID, b.MsgID)
	})

	return messages, nil
}

// The message id keys the entry: a timestamp collides whenever two messages
// share an instant, and the loser is overwritten.
func cacheKey(appName, msgID string) string {
	return fmt.Sprintf("%s:%s:%s", cachedMessagePrefix, appName, msgID)
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

			c.setup.logger.Info(ctx, "gorabbit: not connected, attempting to reconnect", c.connFields()...)
			if err := c.reconnect(ctx); err != nil {
				// The dial already in flight is the one that will heal it.
				if !errors.Is(err, errDialInProgress) {
					c.setup.logger.Error(ctx, "gorabbit: failed to reconnect", c.connFields("error", err)...)
				}
				continue
			}

			c.setup.logger.Info(ctx, "gorabbit: reconnected", c.connFields()...)
		}
	}
}

// reconnect delivers whatever the cache holds as soon as the connection lands;
// every path that connects goes through it so no flush is missed.
func (c *Client[E]) reconnect(ctx context.Context) error {
	established, err := c.connect(ctx)
	if err != nil {
		return err
	}

	// A caller that found the connection already up did not bring it back, and
	// flushing from there only adds contenders to the one flush that matters.
	if !established {
		return nil
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
