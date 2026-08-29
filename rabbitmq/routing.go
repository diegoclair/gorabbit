package rabbitmq

import (
	"errors"
	"fmt"
	"strings"

	"github.com/diegoclair/gorabbit"
)

const (
	// An AMQP routing key is a shortstr, so it cannot carry more than this.
	maxRoutingKeyBytes = 255
	// The topic exchange reads these as the word separator and the two
	// wildcards, so a route carrying them would bind or route where it was
	// never meant to.
	reservedRouteChars = ".*#%"
	upperhex           = "0123456789ABCDEF"
	// "#" matches zero or more words, so it takes every route of the type.
	allRoutesSuffix = ".#"
)

// ErrRoutingKeyTooLong is answered to the caller rather than truncated: a
// truncated key collides with another route and reaches the wrong queue.
var ErrRoutingKeyTooLong = errors.New("gorabbit: routing key is longer than the amqp limit")

// errMissingRouteBy guards the routed type that never said what it routes by.
var errMissingRouteBy = errors.New("gorabbit: routed message does not implement RouteBy() string")

// encodeRoute is the single encoder of both the publish and the bind, which is
// what keeps the two ends from disagreeing on what a route means.
func encodeRoute(route string) string {
	if !strings.ContainsAny(route, reservedRouteChars) {
		return route
	}

	var encoded strings.Builder
	encoded.Grow(len(route) + 8)

	for i := range len(route) {
		char := route[i]
		if strings.IndexByte(reservedRouteChars, char) < 0 {
			encoded.WriteByte(char)
			continue
		}

		encoded.WriteByte('%')
		encoded.WriteByte(upperhex[char>>4])
		encoded.WriteByte(upperhex[char&0xf])
	}

	return encoded.String()
}

func routedKey(typeName, route string) string {
	return typeName + "." + encodeRoute(route)
}

func checkRoutingKeyLength(key string) error {
	if len(key) > maxRoutingKeyBytes {
		return fmt.Errorf("%w: %d bytes, %s", ErrRoutingKeyTooLong, len(key), key[:maxRoutingKeyBytes])
	}

	return nil
}

func publishRoutingKey(msg gorabbit.Message, typeName string) (string, error) {
	if !gorabbit.IsRouted(msg) {
		return typeName, nil
	}

	route, ok := gorabbit.RouteOf(msg)
	if !ok {
		return "", fmt.Errorf("%w: %s", errMissingRouteBy, typeName)
	}

	return routedKey(typeName, route), nil
}

func typeNameOf(routingKey string) string {
	name, _, _ := strings.Cut(routingKey, ".")
	return name
}
