// Package rediscache is the shared gorabbit.Cache the harness applications use,
// speaking RESP over a socket so the harness carries no client dependency.
package rediscache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	addr string

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

func New(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.dropLocked()
}

func (c *Client) Ping(context.Context) error {
	_, err := c.do("PING")
	return err
}

func (c *Client) FlushAll(context.Context) error {
	_, err := c.do("FLUSHALL")
	return err
}

func (c *Client) Set(_ context.Context, key string, data []byte, ttl time.Duration) error {
	args := [][]byte{[]byte("SET"), []byte(key), data}
	if ttl > 0 {
		args = append(args, []byte("PX"), []byte(strconv.FormatInt(ttl.Milliseconds(), 10)))
	}

	_, err := c.doRaw(args)

	return err
}

func (c *Client) SetIfAbsent(_ context.Context, key string, data []byte, ttl time.Duration) (bool, error) {
	args := [][]byte{[]byte("SET"), []byte(key), data, []byte("NX")}
	if ttl > 0 {
		args = append(args, []byte("PX"), []byte(strconv.FormatInt(ttl.Milliseconds(), 10)))
	}

	rep, err := c.doRaw(args)
	if err != nil {
		return false, err
	}

	// SET NX answers a null bulk when the key was already there.
	return !rep.null, nil
}

func (c *Client) Get(_ context.Context, key string) ([]byte, error) {
	rep, err := c.do("GET", key)
	if err != nil {
		return nil, err
	}
	if rep.null {
		return nil, nil
	}

	return rep.str, nil
}

func (c *Client) GetAllKeys(_ context.Context, pattern string) ([]string, error) {
	rep, err := c.do("KEYS", pattern)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(rep.arr))
	for _, item := range rep.arr {
		keys = append(keys, string(item.str))
	}

	return keys, nil
}

func (c *Client) Delete(_ context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	args := append([]string{"DEL"}, keys...)
	_, err := c.do(args...)

	return err
}

type reply struct {
	str  []byte
	num  int64
	arr  []reply
	null bool
}

func (c *Client) do(args ...string) (reply, error) {
	raw := make([][]byte, 0, len(args))
	for _, arg := range args {
		raw = append(raw, []byte(arg))
	}

	return c.doRaw(raw)
}

// A dropped socket is retried once: the harness kills connections on purpose and
// a stale one must not be reported as a Redis failure.
func (c *Client) doRaw(args [][]byte) (reply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	rep, err := c.attemptLocked(args)
	if err == nil {
		return rep, nil
	}

	var protoErr *protocolError
	if errors.As(err, &protoErr) {
		return reply{}, err
	}

	_ = c.dropLocked()

	return c.attemptLocked(args)
}

func (c *Client) attemptLocked(args [][]byte) (reply, error) {
	if err := c.connectLocked(); err != nil {
		return reply{}, err
	}

	if err := c.conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return reply{}, err
	}

	if _, err := c.conn.Write(encode(args)); err != nil {
		return reply{}, err
	}

	return readReply(c.r)
}

func (c *Client) connectLocked() error {
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("redis: dial %s: %w", c.addr, err)
	}

	c.conn, c.r = conn, bufio.NewReader(conn)

	return nil
}

func (c *Client) dropLocked() error {
	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn, c.r = nil, nil

	return err
}

func encode(args [][]byte) []byte {
	var buf []byte
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')

	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}

	return buf
}

// protocolError is an answer Redis gave, so retrying on another socket would
// only repeat it.
type protocolError struct{ msg string }

func (e *protocolError) Error() string { return "redis: " + e.msg }

func readReply(r *bufio.Reader) (reply, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return reply{}, err
	}

	line, err := readLine(r)
	if err != nil {
		return reply{}, err
	}

	switch prefix {
	case '+':
		return reply{str: line}, nil
	case '-':
		return reply{}, &protocolError{msg: string(line)}
	case ':':
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return reply{}, &protocolError{msg: "unparsable integer reply: " + string(line)}
		}
		return reply{num: n}, nil
	case '$':
		return readBulk(r, line)
	case '*':
		return readArray(r, line)
	default:
		return reply{}, &protocolError{msg: "unknown reply prefix " + string(prefix)}
	}
}

func readBulk(r *bufio.Reader, line []byte) (reply, error) {
	size, err := strconv.Atoi(string(line))
	if err != nil {
		return reply{}, &protocolError{msg: "unparsable bulk length: " + string(line)}
	}
	if size < 0 {
		return reply{null: true}, nil
	}

	buf := make([]byte, size+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return reply{}, err
	}

	return reply{str: buf[:size]}, nil
}

func readArray(r *bufio.Reader, line []byte) (reply, error) {
	count, err := strconv.Atoi(string(line))
	if err != nil {
		return reply{}, &protocolError{msg: "unparsable array length: " + string(line)}
	}
	if count < 0 {
		return reply{null: true}, nil
	}

	items := make([]reply, 0, count)
	for range count {
		item, err := readReply(r)
		if err != nil {
			return reply{}, err
		}
		items = append(items, item)
	}

	return reply{arr: items}, nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 {
		return nil, &protocolError{msg: "short reply line"}
	}

	return line[:len(line)-2], nil
}
