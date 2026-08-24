// Package gorabbit holds the driver-neutral messaging contracts. Drivers live
// in subpackages (rabbitmq) and depend on these types — never the other way
// around, so application code can depend on the contracts without pulling a
// broker client in.
package gorabbit

import "context"

// Publisher publishes the messages of the exchange E it owns.
type Publisher[E Exchange] interface {
	Publish(ctx context.Context, msg OwnedBy[E]) error
}

// Consumer runs the background message handling; Start is non-blocking.
type Consumer interface {
	Start(ctx context.Context)
	Close()
}

// HeaderCarrier moves ambient values (correlation id, user, tenant) between the
// application context and the message headers. Values must be AMQP-encodable
// (string, int, bool, time.Time, ...).
type HeaderCarrier interface {
	FromContext(ctx context.Context) map[string]any
	ToContext(ctx context.Context, headers map[string]any) context.Context
}

type noopHeaderCarrier struct{}

func (noopHeaderCarrier) FromContext(context.Context) map[string]any { return nil }

func (noopHeaderCarrier) ToContext(ctx context.Context, _ map[string]any) context.Context {
	return ctx
}

// NoopHeaderCarrier propagates nothing.
func NoopHeaderCarrier() HeaderCarrier { return noopHeaderCarrier{} }

// Logger receives fields as alternating key/value pairs.
type Logger interface {
	Debug(ctx context.Context, msg string, keyvals ...any)
	Info(ctx context.Context, msg string, keyvals ...any)
	Warn(ctx context.Context, msg string, keyvals ...any)
	Error(ctx context.Context, msg string, keyvals ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, ...any) {}
func (noopLogger) Info(context.Context, string, ...any)  {}
func (noopLogger) Warn(context.Context, string, ...any)  {}
func (noopLogger) Error(context.Context, string, ...any) {}

// NoopLogger returns a Logger that discards everything.
func NoopLogger() Logger { return noopLogger{} }
