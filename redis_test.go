package mercure

import (
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

func createRedisTransport(dsn string) *RedisTransport {
	u, _ := url.Parse(dsn)
	transport, _ := NewRedisTransport(u, zap.NewNop())

	return transport.(*RedisTransport)
}

func TestRedisTransportHistory(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewLocalSubscriber("8", transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)

	require.NoError(t, transport.AddSubscriber(s))

	var count int
	for {
		u := <-s.Receive()
		// the reading loop must read the #9 and #10 messages
		assert.Equal(t, strconv.Itoa(9+count), u.ID)
		count++
		if count == 2 {
			return
		}
	}
}

func TestRedisTransportLogsBogusLastEventID(t *testing.T) {
	sink, logger := newTestLogger(t)
	defer sink.Reset()

	u, _ := url.Parse("redis://localhost:6379")
	transport, _ := NewRedisTransport(u, logger)
	defer transport.Close()

	// make sure the db is not empty
	topics := []string{"https://example.com/foo"}
	transport.Dispatch(&Update{
		Event:  Event{ID: "1"},
		Topics: topics,
	})

	s := NewLocalSubscriber("711131", logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)

	require.NoError(t, transport.AddSubscriber(s))

	log := sink.String()
	assert.Contains(t, log, `"LastEventID":"711131"`)
}

func TestRedisTopicSelectorHistory(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed"}, Event: Event{ID: "1"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/not-subscribed"}, Event: Event{ID: "2"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed-public-only"}, Private: true, Event: Event{ID: "3"}})
	transport.Dispatch(&Update{Topics: []string{"http://example.com/subscribed-public-only"}, Event: Event{ID: "4"}})

	s := NewLocalSubscriber(EarliestLastEventID, transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"http://example.com/subscribed", "http://example.com/subscribed-public-only"}, []string{"http://example.com/subscribed"})

	require.NoError(t, transport.AddSubscriber(s))

	assert.Equal(t, "1", (<-s.Receive()).ID)
	assert.Equal(t, "4", (<-s.Receive()).ID)
}

func TestRedisTransportRetrieveAllHistory(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewLocalSubscriber(EarliestLastEventID, transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)
	require.NoError(t, transport.AddSubscriber(s))

	var count int
	for {
		u := <-s.Receive()
		// the reading loop must read all messages
		count++
		assert.Equal(t, strconv.Itoa(count), u.ID)
		if count == 10 {
			break
		}
	}
	assert.Equal(t, 10, count)
}

func TestRedisTransportHistoryAndLive(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Topics: topics,
			Event:  Event{ID: strconv.Itoa(i)},
		})
	}

	s := NewLocalSubscriber("8", transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)
	require.NoError(t, transport.AddSubscriber(s))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var count int
		for {
			u := <-s.Receive()

			// the reading loop must read the #9, #10 and #11 messages
			assert.Equal(t, strconv.Itoa(9+count), u.ID)
			count++
			if count == 3 {
				return
			}
		}
	}()

	transport.Dispatch(&Update{
		Event:  Event{ID: "11"},
		Topics: topics,
	})

	wg.Wait()
}

func TestRedisTransportPurgeHistory(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379?size=5&cleanup_frequency=1")
	defer transport.Close()

	for i := 0; i < 12; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: []string{"https://example.com/foo"},
		})
	}

	// Check the Redis stream size
	xInfoResp, err := transport.client.XInfoGroups(context.Background(), transport.streamName).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(xInfoResp), 5)
}

func TestNewRedisTransport(t *testing.T) {
	u, _ := url.Parse("redis://localhost:6379?stream_name=demo")
	transport, err := NewRedisTransport(u, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, transport)
	transport.Close()

	u, _ = url.Parse("redis://")
	_, err = NewRedisTransport(u, zap.NewNop())
	require.EqualError(t, err, `"redis://": invalid transport: missing host`)

	u, _ = url.Parse("redis://localhost:6379?cleanup_frequency=invalid")
	_, err = NewRedisTransport(u, zap.NewNop())
	require.EqualError(t, err, `"redis://localhost:6379?cleanup_frequency=invalid": invalid "cleanup_frequency" parameter "invalid": invalid transport: strconv.ParseFloat: parsing "invalid": invalid syntax`)

	u, _ = url.Parse("redis://localhost:6379?size=invalid")
	_, err = NewRedisTransport(u, zap.NewNop())
	require.EqualError(t, err, `"redis://localhost:6379?size=invalid": invalid "size" parameter "invalid": invalid transport: strconv.ParseUint: parsing "invalid": invalid syntax`)
}

func TestRedisTransportDoNotDispatchUntilListen(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	assert.Implements(t, (*Transport)(nil), transport)

	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	require.NoError(t, transport.AddSubscriber(s))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for range s.Receive() {
			t.Fail()
		}

		wg.Done()
	}()

	s.Disconnect()

	wg.Wait()
}

func TestRedisTransportDispatch(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	defer transport.Close()

	assert.Implements(t, (*Transport)(nil), transport)

	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/foo", "https://example.com/private"}, []string{"https://example.com/private"})

	require.NoError(t, transport.AddSubscriber(s))

	notSubscribed := &Update{Topics: []string{"not-subscribed"}}
	require.NoError(t, transport.Dispatch(notSubscribed))

	subscribedNotAuthorized := &Update{Topics: []string{"https://example.com/foo"}, Private: true}
	require.NoError(t, transport.Dispatch(subscribedNotAuthorized))

	public := &Update{Topics: s.SubscribedTopics}
	require.NoError(t, transport.Dispatch(public))

	assert.Equal(t, public, <-s.Receive())

	private := &Update{Topics: s.AllowedPrivateTopics, Private: true}
	require.NoError(t, transport.Dispatch(private))

	assert.Equal(t, private, <-s.Receive())
}

func TestRedisTransportClosed(t *testing.T) {
	transport := createRedisTransport("redis://localhost:6379")
	require.NotNil(t, transport)
	defer transport.Close()

	assert.Implements(t, (*Transport)(nil), transport)

	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/foo"}, nil)
	require.NoError(t, transport.AddSubscriber(s))

	require.NoError(t, transport.Close())
	require.Error(t, transport.AddSubscriber(s))

	assert.Equal(t, transport.Dispatch(&Update{Topics: s.SubscribedTopics}), ErrClosedTransport)

	_, ok := <-s.out
	assert.False(t, ok)
}
