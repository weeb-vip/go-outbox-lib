package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Write records an event inside tx. Callers pass the transaction that holds
// their own change, so the event commits or rolls back with it; passing a
// non-transactional *gorm.DB works but gives no atomicity, which defeats the
// purpose.
//
// payload is marshalled with encoding/json unless it is already JSON bytes
// (JSON, json.RawMessage or []byte), which are stored as given after a
// validity check.
func Write(ctx context.Context, tx *gorm.DB, subject string, payload any) (*Event, error) {
	if tx == nil {
		return nil, errors.New("outbox: nil transaction")
	}
	if subject == "" {
		return nil, errors.New("outbox: empty subject")
	}

	body, err := marshal(payload)
	if err != nil {
		return nil, err
	}

	event := &Event{
		Subject: subject,
		Payload: body,
	}
	if err := WriteEvent(ctx, tx, event); err != nil {
		return nil, err
	}

	return event, nil
}

// WriteEvent records event inside tx, filling in a zero ID and CreatedAt.
//
// Callers that need the event id inside the payload -- so a consumer can be
// idempotent on the body alone, without the message header -- generate the id
// first, put it in both places and use this form.
func WriteEvent(ctx context.Context, tx *gorm.DB, event *Event) error {
	if tx == nil {
		return errors.New("outbox: nil transaction")
	}
	if event == nil {
		return errors.New("outbox: nil event")
	}
	if event.Subject == "" {
		return errors.New("outbox: empty subject")
	}
	if !json.Valid(event.Payload) {
		return errors.New("outbox: payload is not valid JSON")
	}
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.PublishedAt = nil

	if err := tx.WithContext(ctx).Create(event).Error; err != nil {
		return fmt.Errorf("outbox: write event: %w", err)
	}

	return nil
}

func marshal(payload any) (JSON, error) {
	var raw []byte

	switch v := payload.(type) {
	case nil:
		return nil, errors.New("outbox: nil payload")
	case JSON:
		raw = v
	case json.RawMessage:
		raw = v
	case []byte:
		raw = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("outbox: marshal payload: %w", err)
		}

		return JSON(b), nil
	}

	if !json.Valid(raw) {
		return nil, errors.New("outbox: payload is not valid JSON")
	}

	return append(JSON(nil), raw...), nil
}
