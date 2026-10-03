package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestWrite_CommitsWithTransaction(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	type payload struct {
		UserID string `json:"user_id"`
		N      int    `json:"n"`
	}

	var written *Event
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		written, err = Write(ctx, tx, "user-follow", payload{UserID: "user_1", N: 2})

		return err
	})
	require.NoError(t, err)
	require.NotNil(t, written)

	var stored Event
	require.NoError(t, db.First(&stored, "id = ?", written.ID).Error)
	assert.Equal(t, "user-follow", stored.Subject)
	assert.JSONEq(t, `{"user_id":"user_1","n":2}`, string(stored.Payload))
	assert.Nil(t, stored.PublishedAt)
	assert.False(t, stored.CreatedAt.IsZero())
}

func TestWrite_RollsBackWithTransaction(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	boom := errors.New("boom")
	err := db.Transaction(func(tx *gorm.DB) error {
		if _, err := Write(ctx, tx, "user-follow", map[string]string{"k": "v"}); err != nil {
			return err
		}

		return boom
	})
	require.ErrorIs(t, err, boom)

	assert.EqualValues(t, 0, countUnpublished(t, db))
}

func TestWrite_AcceptsRawJSON(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	cases := map[string]any{
		"json.RawMessage": json.RawMessage(`{"a":1}`),
		"JSON":            JSON(`{"a":1}`),
		"[]byte":          []byte(`{"a":1}`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			ev, err := Write(ctx, db, "s", payload)
			require.NoError(t, err)

			var stored Event
			require.NoError(t, db.First(&stored, "id = ?", ev.ID).Error)
			assert.JSONEq(t, `{"a":1}`, string(stored.Payload))
		})
	}
}

func TestWrite_RejectsBadInput(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	_, err := Write(ctx, nil, "s", map[string]int{})
	assert.Error(t, err)

	_, err = Write(ctx, db, "", map[string]int{})
	assert.Error(t, err)

	_, err = Write(ctx, db, "s", nil)
	assert.Error(t, err)

	_, err = Write(ctx, db, "s", []byte(`{not json`))
	assert.Error(t, err)

	_, err = Write(ctx, db, "s", make(chan int))
	assert.Error(t, err)

	assert.EqualValues(t, 0, countUnpublished(t, db))
}

func TestWriteEvent_KeepsCallerID(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	id := uuid.New()
	event := &Event{ID: id, Subject: "s", Payload: JSON(`{"id":"` + id.String() + `"}`)}
	require.NoError(t, WriteEvent(ctx, db, event))
	assert.Equal(t, id, event.ID)
	assert.False(t, event.CreatedAt.IsZero())

	var stored Event
	require.NoError(t, db.First(&stored, "id = ?", id).Error)
	assert.Equal(t, "s", stored.Subject)

	assert.Error(t, WriteEvent(ctx, db, nil))
	assert.Error(t, WriteEvent(ctx, db, &Event{Subject: "", Payload: JSON(`{}`)}))
	assert.Error(t, WriteEvent(ctx, db, &Event{Subject: "s", Payload: JSON(`nope`)}))
}
