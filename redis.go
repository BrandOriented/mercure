package mercure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const RedisDefaultCleanupFrequency = 0.3

func init() { //nolint:gochecknoinits
	RegisterTransportFactory("redis", NewRedisTransport)
}

const defaultRedisStreamName = "updates"

// RedisTransport implements the TransportInterface using Redis.
type RedisTransport struct {
	sync.RWMutex
	subscribers      *SubscriberList
	logger           Logger
	client           *redis.Client
	streamName       string
	size             uint64
	cleanupFrequency float64
	closed           chan struct{}
	closedOnce       sync.Once
	lastSeq          uint64
	lastEventID      string
}

// NewRedisTransport creates a new RedisTransport.
func NewRedisTransport(u *url.URL, l Logger) (Transport, error) {
	var err error
	q := u.Query()
	streamName := defaultRedisStreamName
	if q.Get("stream_name") != "" {
		streamName = q.Get("stream_name")
	}

	size := uint64(0)
	if sizeParameter := q.Get("size"); sizeParameter != "" {
		size, err = strconv.ParseUint(sizeParameter, 10, 64)
		if err != nil {
			return nil, &TransportError{u.Redacted(), fmt.Sprintf(`invalid "size" parameter %q`, sizeParameter), err}
		}
	}

	cleanupFrequency := RedisDefaultCleanupFrequency
	cleanupFrequencyParameter := q.Get("cleanup_frequency")
	if cleanupFrequencyParameter != "" {
		cleanupFrequency, err = strconv.ParseFloat(cleanupFrequencyParameter, 64)
		if err != nil {
			return nil, &TransportError{u.Redacted(), fmt.Sprintf(`invalid "cleanup_frequency" parameter %q`, cleanupFrequencyParameter), err}
		}
	}

	address := u.Host
	if address == "" {
		return nil, &TransportError{u.Redacted(), "missing host", err}
	}

	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	return NewRedisTransportInstance(l, client, streamName, size, cleanupFrequency)
}

// NewRedisTransportInstance creates a new RedisTransport instance.
func NewRedisTransportInstance(
	logger Logger,
	client *redis.Client,
	streamName string,
	size uint64,
	cleanupFrequency float64,
) (*RedisTransport, error) {
	lastEventID, err := getRedisLastEventID(client, streamName)
	if err != nil {
		return nil, &TransportError{err: err}
	}

	return &RedisTransport{
		logger:           logger,
		client:           client,
		streamName:       streamName,
		size:             size,
		cleanupFrequency: cleanupFrequency,
		subscribers:      NewSubscriberList(1e5),
		closed:           make(chan struct{}),
		lastEventID:      lastEventID,
	}, nil
}

func getRedisLastEventID(client *redis.Client, streamName string) (string, error) {
	lastEventID := EarliestLastEventID

	// Check the latest event from the Redis stream
	xReadResp, err := client.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{streamName, ">"},
		Count:   1,
		Block:   0,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("unable to get lastEventID from Redis: %w", err)
	}

	if len(xReadResp) > 0 && len(xReadResp[0].Messages) > 0 {
		lastEventID = xReadResp[0].Messages[0].ID
	}

	return lastEventID, nil
}

// Dispatch dispatches an update to all subscribers and persists it in Redis.
func (t *RedisTransport) Dispatch(update *Update) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	AssignUUID(update)
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	// We cannot use RLock() because Redis allows concurrent writes
	t.Lock()
	defer t.Unlock()

	if err := t.persist(update.ID, updateJSON); err != nil {
		return err
	}

	for _, s := range t.subscribers.MatchAny(update) {
		s.Dispatch(update, false)
	}

	return nil
}

// persist stores update in Redis.
func (t *RedisTransport) persist(updateID string, updateJSON []byte) error {
	// Store update in Redis stream
	_, err := t.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: t.streamName,
		Values: map[string]interface{}{"updateID": updateID, "updateData": updateJSON},
	}).Result()
	if err != nil {
		return fmt.Errorf("error when adding to Redis stream: %w", err)
	}

	// Handle cleanup
	if err := t.cleanup(); err != nil {
		return fmt.Errorf("cleanup error: %w", err)
	}

	return nil
}

// AddSubscriber adds a new subscriber to the transport.
func (t *RedisTransport) AddSubscriber(s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	t.subscribers.Add(s)
	t.Unlock()

	if s.RequestLastEventID != "" {
		if err := t.dispatchHistory(s); err != nil {
			return err
		}
	}

	s.Ready()

	return nil
}

// RemoveSubscriber removes a new subscriber from the transport.
func (t *RedisTransport) RemoveSubscriber(s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()
	t.subscribers.Remove(s)

	return nil
}

// GetSubscribers get the list of active subscribers.
func (t *RedisTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	return t.lastEventID, getSubscribers(t.subscribers), nil
}

func (t *RedisTransport) dispatchHistory(s *LocalSubscriber) error {
	// Read history from Redis
	xReadResp, err := t.client.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{t.streamName, s.RequestLastEventID},
		Count:   1000, // Adjust as needed
		Block:   0,
	}).Result()
	if err != nil {
		return fmt.Errorf("unable to retrieve history from Redis: %w", err)
	}

	for _, message := range xReadResp {
		for _, msg := range message.Messages {
			var update *Update
			if err := json.Unmarshal([]byte(msg.Values["updateData"].(string)), &update); err != nil {
				if c := t.logger.Check(zap.ErrorLevel, "Unable to unmarshal update coming from Redis"); c != nil {
					c.Write(zap.Error(err))
				}

				return fmt.Errorf("unable to unmarshal update: %w", err)
			}

			if !s.Dispatch(update, true) {
				break
			}
		}
	}

	return nil
}

// Close closes the Transport.
func (t *RedisTransport) Close() (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)

		t.Lock()
		defer t.Unlock()

		t.subscribers.Walk(0, func(s *LocalSubscriber) bool {
			s.Disconnect()

			return true
		})
	})

	return nil
}

// cleanup removes entries in the history above the size limit, triggered probabilistically.
func (t *RedisTransport) cleanup() error {
	if t.size == 0 ||
		t.cleanupFrequency == 0 ||
		t.size >= t.lastSeq ||
		(t.cleanupFrequency != 1 && rand.Float64() < t.cleanupFrequency) {
		return nil
	}

	// Truncate Redis stream
	_, err := t.client.XTrim(context.Background(), t.streamName, &redis.XTrimArgs{MinID: fmt.Sprintf("%d", t.lastSeq)}).Result()
	if err != nil {
		return fmt.Errorf("unable to trim Redis stream: %w", err)
	}

	return nil
}

// Interface guards.
var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)
