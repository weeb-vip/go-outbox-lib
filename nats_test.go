package outbox

import (
	"context"
	"testing"
	"time"

	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"
	"github.com/google/uuid"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startNats runs a JetStream-enabled server in-process, so this test needs no
// Docker and no network.
func startNats(t *testing.T) *server.Server {
	t.Helper()

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	require.NoError(t, err)
	srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second), "nats server did not start")
	t.Cleanup(srv.Shutdown)

	return srv
}

func TestNatsPublisher_PublishesWithMessageIDAndDeduplicates(t *testing.T) {
	srv := startNats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	driver := epnats.NewNatsDriver(&epnats.Config{URL: srv.ClientURL()})
	t.Cleanup(func() { _ = driver.Close() })

	publisher := NewNatsPublisher(driver)
	event := &Event{ID: uuid.New(), Subject: "outbox-test", Payload: JSON(`{"hello":"world"}`)}

	require.NoError(t, publisher.Publish(ctx, event))
	// A relay that crashed between publishing and marking would send this
	// exact message again. The server drops the duplicate.
	require.NoError(t, publisher.Publish(ctx, event))

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	stream, err := js.Stream(ctx, epnats.StreamNameForSubject("outbox-test"))
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, info.State.Msgs, "duplicate publish must be dropped by the server")

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "outbox-test",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	require.NoError(t, err)

	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, event.ID.String(), msg.Headers().Get(MessageIDHeader))
	assert.JSONEq(t, `{"hello":"world"}`, string(msg.Data()))
	require.NoError(t, msg.Ack())
}
