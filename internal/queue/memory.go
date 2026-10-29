package queue

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-process [Backend]. It is what the propagation tests run
// against: the tracing behaviour under test is identical to Redis's, so the
// assertions about parent spans and surviving trace ids hold without a broker.
type Memory struct {
	mu     sync.Mutex
	queues map[string][]Message
	signal chan struct{}
}

// NewMemory builds an empty in-memory backend.
func NewMemory() *Memory {
	return &Memory{
		queues: make(map[string][]Message),
		// Buffered by one so an Enqueue never blocks on an absent consumer.
		signal: make(chan struct{}, 1),
	}
}

var _ Backend = (*Memory)(nil)

// Enqueue implements [Backend].
func (m *Memory) Enqueue(_ context.Context, topic string, msg Message) error {
	m.mu.Lock()
	m.queues[topic] = append(m.queues[topic], msg)
	m.mu.Unlock()

	select {
	case m.signal <- struct{}{}:
	default:
	}
	return nil
}

// Dequeue implements [Backend]. It returns [ErrNoMessage] once wait elapses.
func (m *Memory) Dequeue(ctx context.Context, topic string, wait time.Duration) (Message, error) {
	deadline := time.Now().Add(wait)
	for {
		m.mu.Lock()
		q := m.queues[topic]
		if len(q) > 0 {
			msg := q[0]
			m.queues[topic] = q[1:]
			m.mu.Unlock()
			return msg, nil
		}
		m.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return Message{}, ErrNoMessage
		}
		timer := time.NewTimer(min(remaining, 25*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Message{}, ctx.Err()
		case <-m.signal:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Depth implements [Backend].
func (m *Memory) Depth(_ context.Context, topic string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.queues[topic])), nil
}

// Close implements [Backend]. There is nothing to release.
func (m *Memory) Close() error { return nil }

// Len reports the queue depth without a context. Test helper.
func (m *Memory) Len(topic string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queues[topic])
}
