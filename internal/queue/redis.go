package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a [Backend] over a Redis list: RPUSH to enqueue, BLPOP to dequeue.
//
// A list is the smallest thing that is still a real queue — no consumer groups,
// no redelivery. That is a deliberate limitation, recorded in the README: the
// subject of this project is trace propagation, and a list exercises exactly the
// same propagation path that a Kafka or SQS client would.
type Redis struct {
	client *redis.Client
	prefix string
}

// RedisOptions configures [NewRedis].
type RedisOptions struct {
	// Addr is host:port.
	Addr string
	// Prefix namespaces the list keys. Defaults to "otelq".
	Prefix string
}

// NewRedis connects to Redis. It does not ping: the services retry on first use
// so that compose start-up ordering does not have to be perfect.
func NewRedis(opts RedisOptions) *Redis {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "otelq"
	}
	return &Redis{
		client: redis.NewClient(&redis.Options{Addr: opts.Addr}),
		prefix: prefix,
	}
}

var _ Backend = (*Redis)(nil)

func (r *Redis) key(topic string) string { return r.prefix + ":" + topic }

// Enqueue implements [Backend].
func (r *Redis) Enqueue(ctx context.Context, topic string, msg Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue/redis: marshal message: %w", err)
	}
	return r.client.RPush(ctx, r.key(topic), payload).Err()
}

// Dequeue implements [Backend].
//
// BLPOP's timeout has one-second granularity and 0 means "block forever", so a
// sub-second wait is rounded up rather than silently becoming an unbounded
// block — a worker that never returns from Dequeue cannot be shut down.
func (r *Redis) Dequeue(ctx context.Context, topic string, wait time.Duration) (Message, error) {
	if wait < time.Second {
		wait = time.Second
	}
	res, err := r.client.BLPop(ctx, wait, r.key(topic)).Result()
	if errors.Is(err, redis.Nil) {
		return Message{}, ErrNoMessage
	}
	if err != nil {
		return Message{}, err
	}
	if len(res) != 2 {
		return Message{}, fmt.Errorf("queue/redis: unexpected BLPOP reply of %d elements", len(res))
	}

	var msg Message
	if err := json.Unmarshal([]byte(res[1]), &msg); err != nil {
		return Message{}, fmt.Errorf("queue/redis: unmarshal message: %w", err)
	}
	if msg.Topic == "" {
		msg.Topic = topic
	}
	return msg, nil
}

// Depth implements [Backend].
func (r *Redis) Depth(ctx context.Context, topic string) (int64, error) {
	return r.client.LLen(ctx, r.key(topic)).Result()
}

// Close implements [Backend].
func (r *Redis) Close() error { return r.client.Close() }

// Ping reports whether Redis is reachable, for the services' readiness probes.
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }
