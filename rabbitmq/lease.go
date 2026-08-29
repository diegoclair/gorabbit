package rabbitmq

import (
	"context"
	"fmt"
	"time"
)

const (
	// Deliberately outside cachedMessagePrefix: the flush globs that prefix, so
	// a lease living under it would be read back as a message and published.
	publishLeasePrefix = "gorabbit:publish_lease"
	// A cached publish is bounded so the lease guarding it outlives the attempt
	// it protects — an expired lease is two processes on one message.
	publishAttemptFactor = 2
	// Floor for a confirm timeout short enough that the lease would expire while
	// the frames are still on the wire.
	minPublishLeaseTTL = 30 * time.Second
)

func leaseKey(scope, msgID string) string {
	return fmt.Sprintf("%s:%s:%s", publishLeasePrefix, scope, msgID)
}

func (c *Client[E]) publishAttemptTimeout() time.Duration {
	return publishAttemptFactor * c.setup.confirmTimeout
}

func (c *Client[E]) publishLeaseTTL() time.Duration {
	return max(2*c.publishAttemptTimeout(), minPublishLeaseTTL)
}

// Exclusivity belongs to the message, not to the client: replicas sharing a
// cache scope split the flush instead of each repeating it.
func (c *Client[E]) reserveCachedMessage(ctx context.Context, msgID string) bool {
	acquired, err := c.cache.SetIfAbsent(ctx, leaseKey(c.cacheScope(), msgID), []byte(c.clientDescription()), c.publishLeaseTTL())
	if err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error reserving a cached message", "message_id", msgID, "error", err)
		return false
	}

	return acquired
}

// A failed attempt gives the reservation back so the next flush is not held off
// until it expires; a delivered one leaves it to expire on its own.
func (c *Client[E]) releaseCachedMessage(ctx context.Context, msgID string) {
	if err := c.cache.Delete(ctx, leaseKey(c.cacheScope(), msgID)); err != nil {
		c.setup.logger.Error(ctx, "gorabbit: error releasing a cached message reservation", "message_id", msgID, "error", err)
	}
}
