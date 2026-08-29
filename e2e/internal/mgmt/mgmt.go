// Package mgmt reads the broker's own view through the RabbitMQ management API,
// so a claim about a queue never rests on what an application said.
package mgmt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ErrNotFound reports a resource the broker does not have, which a scenario
// asserting absence needs to tell apart from a failed request.
var ErrNotFound = errors.New("mgmt: not found")

type Client struct {
	baseURL  string
	user     string
	password string
	vhost    string
	http     *http.Client
}

func New(baseURL, user, password, vhost string) *Client {
	return &Client{
		baseURL:  baseURL,
		user:     user,
		password: password,
		vhost:    vhost,
		http:     &http.Client{Timeout: 15 * time.Second},
	}
}

type Queue struct {
	Name                   string `json:"name"`
	Messages               int    `json:"messages"`
	MessagesReady          int    `json:"messages_ready"`
	MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	Consumers              int    `json:"consumers"`
}

type Binding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

type Message struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
	Payload    string `json:"payload"`
	Properties struct {
		Headers map[string]any `json:"headers"`
	} `json:"properties"`
}

type Connection struct {
	Name             string         `json:"name"`
	Vhost            string         `json:"vhost"`
	ClientProperties map[string]any `json:"client_properties"`
}

func (c *Client) Ready(ctx context.Context) error {
	return c.call(ctx, http.MethodGet, "/api/overview", nil, nil)
}

// ResetVhost gives every scenario a broker with no topology of its own, which is
// what makes running one scenario alone equal to running it after the others.
func (c *Client) ResetVhost(ctx context.Context) error {
	err := c.call(ctx, http.MethodDelete, "/api/vhosts/"+url.PathEscape(c.vhost), nil, nil)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	if err := c.call(ctx, http.MethodPut, "/api/vhosts/"+url.PathEscape(c.vhost), map[string]any{}, nil); err != nil {
		return err
	}

	permissions := map[string]string{"configure": ".*", "write": ".*", "read": ".*"}
	path := fmt.Sprintf("/api/permissions/%s/%s", url.PathEscape(c.vhost), url.PathEscape(c.user))

	return c.call(ctx, http.MethodPut, path, permissions, nil)
}

func (c *Client) Queues(ctx context.Context) ([]Queue, error) {
	var queues []Queue
	err := c.call(ctx, http.MethodGet, "/api/queues/"+url.PathEscape(c.vhost), nil, &queues)

	return queues, err
}

func (c *Client) Queue(ctx context.Context, name string) (Queue, error) {
	var queue Queue
	path := fmt.Sprintf("/api/queues/%s/%s", url.PathEscape(c.vhost), url.PathEscape(name))
	err := c.call(ctx, http.MethodGet, path, nil, &queue)

	return queue, err
}

func (c *Client) QueueBindings(ctx context.Context, name string) ([]Binding, error) {
	var bindings []Binding
	path := fmt.Sprintf("/api/queues/%s/%s/bindings", url.PathEscape(c.vhost), url.PathEscape(name))
	err := c.call(ctx, http.MethodGet, path, nil, &bindings)

	return bindings, err
}

// Read and requeued: the step that reads this queue next must still find the
// message.
func (c *Client) Get(ctx context.Context, queue string, count int) ([]Message, error) {
	body := map[string]any{
		"count":    count,
		"ackmode":  "ack_requeue_true",
		"encoding": "auto",
		"truncate": 50000,
	}

	var messages []Message
	path := fmt.Sprintf("/api/queues/%s/%s/get", url.PathEscape(c.vhost), url.PathEscape(queue))
	err := c.call(ctx, http.MethodPost, path, body, &messages)

	return messages, err
}

func (c *Client) Connections(ctx context.Context) ([]Connection, error) {
	var connections []Connection
	if err := c.call(ctx, http.MethodGet, "/api/connections", nil, &connections); err != nil {
		return nil, err
	}

	scoped := make([]Connection, 0, len(connections))
	for _, conn := range connections {
		if conn.Vhost == c.vhost {
			scoped = append(scoped, conn)
		}
	}

	return scoped, nil
}

// A connection's own name is a socket pair; the application name lives in the
// client properties.
func ConnectionName(conn Connection) string {
	name, _ := conn.ClientProperties["connection_name"].(string)
	return name
}

func (c *Client) CloseConnection(ctx context.Context, name string) error {
	return c.call(ctx, http.MethodDelete, "/api/connections/"+url.PathEscape(name), nil, nil)
}

func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, payload)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.user, c.password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("mgmt: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	answer, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s %s", ErrNotFound, method, path)
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("mgmt: %s %s: status %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(answer))
	}

	if out == nil {
		return nil
	}

	return json.Unmarshal(answer, out)
}

type Channel struct {
	Name              string `json:"name"`
	Vhost             string `json:"vhost"`
	ConnectionDetails struct {
		Name string `json:"name"`
	} `json:"connection_details"`
	MessageStats struct {
		Publish          int `json:"publish"`
		ReturnUnroutable int `json:"return_unroutable"`
	} `json:"message_stats"`
}

type Totals struct {
	Publish    int
	Unroutable int
}

// The only view from outside that tells two processes carrying the same
// application name apart.
func (c *Client) PublishTotals(ctx context.Context) (map[string]Totals, error) {
	var channels []Channel
	if err := c.call(ctx, http.MethodGet, "/api/channels", nil, &channels); err != nil {
		return nil, err
	}

	totals := map[string]Totals{}
	for _, channel := range channels {
		if channel.Vhost != c.vhost {
			continue
		}

		current := totals[channel.ConnectionDetails.Name]
		current.Publish += channel.MessageStats.Publish
		current.Unroutable += channel.MessageStats.ReturnUnroutable
		totals[channel.ConnectionDetails.Name] = current
	}

	return totals, nil
}
