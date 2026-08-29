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
	// A binding that does not exist yet has no event to announce it, and neither
	// does a reservation another process is holding, so the only way back to a
	// message still in the cache is to try it again.
	pendingRetryDelay    = 5 * time.Second
	maxPendingRetryDelay = 5 * time.Minute
)

type cachedMessage struct {
	MsgID       string
	MsgTypeName string
	RoutingKey  string
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

	key := cacheKey(c.cacheScope(), cachedMsg.MsgID)
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

// The count of messages left behind is what tells the retry loop there is still
// something to chase — a missing binding, or another process's reservation.
func (c *Client[E]) flushCachedMessages(ctx context.Context) int {
	if !c.connected() {
		return 0
	}

	// One flush per client is enough; a second only contends for the same
	// reservations.
	if !c.flushing.CompareAndSwap(false, true) {
		return 0
	}
	defer c.flushing.Store(false)

	keys, err := c.cachedMessageKeys(ctx)
	if err != nil || len(keys) == 0 {
		return 0
	}

	pending := 0

	for _, key := range keys {
		msgID := msgIDFromCacheKey(key)

		if !c.reserveCachedMessage(ctx, msgID) {
			pending++
			continue
		}

		msg, ok := c.readCachedMessage(ctx, key)
		if !ok {
			continue
		}

		pm := fromCacheMessage(&msg)

		attemptCtx, cancel := context.WithTimeout(ctx, c.publishAttemptTimeout())
		err := c.publish(attemptCtx, &pm)
		cancel()

		if err != nil {
			c.releaseCachedMessage(ctx, msgID)

			// A queue nobody bound yet must not hold back the messages that do
			// have somewhere to go, so only a broken connection stops the flush.
			if errors.Is(err, errUnroutable) {
				pending++
				continue
			}

			c.setup.logger.Error(ctx, "gorabbit: error publishing cached message", "error", err)
			break
		}

		if err := c.cache.Delete(ctx, key); err != nil {
			c.setup.logger.Error(ctx, "gorabbit: error deleting cached message", "error", err)
		}
	}

	if pending > 0 {
		c.signalPending()
	}

	return pending
}

func (c *Client[E]) cachedMessageKeys(ctx context.Context) ([]string, error) {
	keys, err := c.cache.GetAllKeys(ctx, cacheKey(c.cacheScope(), "")+"*")
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error retrieving cached message keys", "error", err)
		return nil, err
	}

	// Replay in publish order: every key here shares the scope prefix and ends
	// in a UUIDv7, so sorting the keys sorts by publish time.
	slices.Sort(keys)

	return keys, nil
}

// Read after the reservation and never before: an entry another process
// delivered meanwhile is gone, and a snapshot taken earlier would replay it.
func (c *Client[E]) readCachedMessage(ctx context.Context, key string) (cachedMessage, bool) {
	jsonMsg, err := c.cache.Get(ctx, key)
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error retrieving cached message", "key", key, "error", err)
		return cachedMessage{}, false
	}
	if len(jsonMsg) == 0 {
		return cachedMessage{}, false
	}

	var cachedMsg cachedMessage
	if err := json.Unmarshal(jsonMsg, &cachedMsg); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error unmarshalling cached message", "key", key, "error", err)
		return cachedMessage{}, false
	}

	return cachedMsg, true
}

// cacheScope separates one client's entries from every other client sharing the
// store, the role first so a publisher glob cannot reach a consumer's keys.
func (c *Client[E]) cacheScope() string {
	if c.setup.isConsumer {
		return fmt.Sprintf("consumer:%s:%s:%s", c.setup.appName, c.setup.exchangeName, c.setup.queueName)
	}

	return fmt.Sprintf("publisher:%s:%s", c.setup.appName, c.setup.exchangeName)
}

// The message id keys the entry: a timestamp collides whenever two messages
// share an instant, and the loser is overwritten.
func cacheKey(scope, msgID string) string {
	return fmt.Sprintf("%s:%s:%s", cachedMessagePrefix, scope, msgID)
}

// The id is the last segment, since the scope cacheKey puts before it carries
// separators of its own.
func msgIDFromCacheKey(key string) string {
	return key[strings.LastIndex(key, ":")+1:]
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

// messageTypeName opens the routing key of a message: its concrete type name,
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

func (c *Client[E]) signalPending() {
	select {
	case c.pending <- struct{}{}:
	default:
	}
}

// retryPendingMessages stands in for the event neither a new binding nor an
// expiring reservation announces. The delay grows so a permanent mistake cannot
// turn into a hot loop.
func (c *Client[E]) retryPendingMessages(ctx context.Context) {
	timer := time.NewTimer(pendingRetryDelay)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	delay := pendingRetryDelay
	waiting := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-c.pending:
			if waiting {
				continue
			}
			waiting, delay = true, pendingRetryDelay
			timer.Reset(delay)
		case <-timer.C:
			left := c.flushCachedMessages(ctx)
			if left == 0 {
				waiting = false
				continue
			}

			delay = min(2*delay, maxPendingRetryDelay)
			c.setup.logger.Error(ctx, "gorabbit: cached messages are still waiting to be delivered",
				c.connFields("messages", left, "next_attempt_in", delay)...)
			timer.Reset(delay)
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
