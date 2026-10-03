package outbox

import (
	"context"

	"github.com/ThatCatDev/ep/v2/drivers"
	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"
)

// MessageIDHeader is the header JetStream deduplicates on. Set to the event id,
// so a republished row is dropped by the server inside the stream's duplicate
// window (two minutes by default).
const MessageIDHeader = "Nats-Msg-Id"

// Publisher delivers one event to the bus. The relay treats any error as
// "not delivered" and leaves the row unpublished for the next tick.
type Publisher interface {
	Publish(ctx context.Context, event *Event) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, event *Event) error

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, event *Event) error { return f(ctx, event) }

// NewNatsPublisher publishes through an ep NATS driver, one JetStream subject
// per event subject. The driver creates a stream for a subject it has not
// seen, so a service needs no stream provisioning beyond the size limits kept
// in weeb-argocd's nats-streams values.
func NewNatsPublisher(driver drivers.Driver[*epnats.Message]) Publisher {
	return PublisherFunc(func(ctx context.Context, event *Event) error {
		return driver.Produce(ctx, event.Subject, &epnats.Message{
			Subject: event.Subject,
			Data:    []byte(event.Payload),
			Headers: map[string]string{MessageIDHeader: event.ID.String()},
		})
	})
}
