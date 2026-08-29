// Package appkit gives every harness application the same handshake with the
// runner, so nothing in the harness has to reserve a port.
package appkit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diegoclair/gorabbit"
)

// ListenAnnouncement opens the runner's only handshake with an application: the
// port is chosen by the kernel, so nothing in the harness reserves one.
const ListenAnnouncement = "E2E-LISTEN"

// shutdown runs only on an orderly stop, which is what leaves a killed process
// indistinguishable from a crashed one.
func Run(mux *http.ServeMux, shutdown func()) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("http server stopped", "error", err)
		}
	}()

	fmt.Printf("%s %s\n", ListenAnnouncement, listener.Addr().String())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)

	shutdown()

	return nil
}

func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func ReadJSON(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

type slogLogger struct{ log *slog.Logger }

// stderr is what the runner files per process, and where the operator looks
// after a failed step.
func NewLogger(app string) gorabbit.Logger {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})

	return slogLogger{log: slog.New(handler).With("app", app)}
}

func (l slogLogger) Debug(_ context.Context, msg string, keyvals ...any) {
	l.log.Debug(msg, keyvals...)
}

func (l slogLogger) Info(_ context.Context, msg string, keyvals ...any) {
	l.log.Info(msg, keyvals...)
}

func (l slogLogger) Warn(_ context.Context, msg string, keyvals ...any) {
	l.log.Warn(msg, keyvals...)
}

func (l slogLogger) Error(_ context.Context, msg string, keyvals ...any) {
	l.log.Error(msg, keyvals...)
}
