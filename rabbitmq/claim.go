package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCacheKeyTaken guards two clients of one process from replaying each
// other's messages and unbinding each other's bindings.
var ErrCacheKeyTaken = errors.New("gorabbit: another client already holds this cache key")

var (
	claimedKeysMu sync.Mutex
	claimedKeys   = map[string]string{}
)

func (c *Client[E]) claimCacheKey() error {
	key := c.cacheScope()
	owner := c.clientDescription()

	claimedKeysMu.Lock()
	defer claimedKeysMu.Unlock()

	if holder, taken := claimedKeys[key]; taken {
		return fmt.Errorf("%w: %s already holds %q", ErrCacheKeyTaken, holder, key)
	}

	claimedKeys[key] = owner
	c.claimedKey = true

	return nil
}

func (c *Client[E]) releaseCacheKey() {
	if !c.claimedKey {
		return
	}

	claimedKeysMu.Lock()
	defer claimedKeysMu.Unlock()

	delete(claimedKeys, c.cacheScope())
	c.claimedKey = false
}

func (c *Client[E]) clientDescription() string {
	if c.setup.isConsumer {
		return fmt.Sprintf("the client of app %q consuming %q on exchange %q",
			c.setup.appName, c.setup.queueName, c.setup.exchangeName)
	}

	return fmt.Sprintf("the client of app %q publishing to exchange %q", c.setup.appName, c.setup.exchangeName)
}
