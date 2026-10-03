package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func seed(t *testing.T, db *gorm.DB, subject string, n int) []uuid.UUID {
	t.Helper()

	ids := make([]uuid.UUID, 0, n)
	base := time.Now().UTC().Add(-time.Duration(n) * time.Millisecond)
	events := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		ev := Event{
			ID:        uuid.New(),
			Subject:   subject,
			Payload:   JSON(fmt.Sprintf(`{"seq":%d}`, i)),
			CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
		}
		ids = append(ids, ev.ID)
		events = append(events, ev)
	}
	require.NoError(t, db.CreateInBatches(events, 500).Error)

	return ids
}

func TestRelayOnce_PublishesInOrderAndMarks(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	ids := seed(t, db, "user-activity", 5)

	pub := &fakePublisher{}
	metrics := newRecordingMetrics()
	relay := NewRelay(db, pub, Config{BatchSize: 10}, WithMetrics(metrics))

	n, err := relay.RelayOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	got := pub.published()
	require.Len(t, got, 5)
	for i, ev := range got {
		assert.Equal(t, ids[i], ev.ID, "event %d out of order", i)
		assert.Equal(t, "user-activity", ev.Subject)
	}

	assert.EqualValues(t, 0, countUnpublished(t, db))
	assert.Equal(t, 5, metrics.published["user-activity"])

	// A second pass finds nothing.
	n, err = relay.RelayOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRelayOnce_RespectsBatchSize(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "s", 7)

	pub := &fakePublisher{}
	relay := NewRelay(db, pub, Config{BatchSize: 3})

	n, err := relay.RelayOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.EqualValues(t, 4, countUnpublished(t, db))
}

func TestRelayOnce_FailureLeavesRowAndLaterOnesUnpublished(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	ids := seed(t, db, "s", 3)

	boom := errors.New("nats down")
	pub := &fakePublisher{failOn: func(ev *Event) error {
		if ev.ID == ids[1] {
			return boom
		}

		return nil
	}}
	metrics := newRecordingMetrics()
	relay := NewRelay(db, pub, Config{BatchSize: 10}, WithMetrics(metrics))

	n, err := relay.RelayOnce(ctx)
	require.ErrorIs(t, err, boom)
	assert.Equal(t, 1, n, "the row before the failure is still marked")
	assert.EqualValues(t, 2, countUnpublished(t, db))
	assert.Equal(t, 1, metrics.failed["s"])

	var first Event
	require.NoError(t, db.First(&first, "id = ?", ids[0]).Error)
	assert.NotNil(t, first.PublishedAt)

	// Once the publisher recovers the next pass drains the rest, in order.
	pub.failOn = nil
	n, err = relay.RelayOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := pub.published()
	require.Len(t, got, 3)
	assert.Equal(t, ids[1], got[1].ID)
	assert.Equal(t, ids[2], got[2].ID)
	assert.EqualValues(t, 0, countUnpublished(t, db))
}

func TestRelayOnce_ConcurrentRelaysNeverPublishTwice(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const total = 1000
	seed(t, db, "s", total)

	pub := &fakePublisher{}
	const workers = 3
	var wg sync.WaitGroup
	errs := make(chan error, workers*8)
	for w := 0; w < workers; w++ {
		relay := NewRelay(db, pub, Config{BatchSize: 50})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := relay.RelayOnce(ctx)
				if err != nil {
					errs <- err

					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	got := pub.published()
	assert.Len(t, got, total)
	seen := make(map[uuid.UUID]struct{}, total)
	for _, ev := range got {
		_, dup := seen[ev.ID]
		assert.False(t, dup, "event %s published twice", ev.ID)
		seen[ev.ID] = struct{}{}
	}
	assert.EqualValues(t, 0, countUnpublished(t, db))
}

func TestCleanup_DeletesOnlyExpiredPublishedRows(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	old := time.Now().UTC().Add(-48 * time.Hour)
	recent := time.Now().UTC().Add(-time.Hour)
	rows := []Event{
		{ID: uuid.New(), Subject: "s", Payload: JSON(`{}`), CreatedAt: old, PublishedAt: &old},
		{ID: uuid.New(), Subject: "s", Payload: JSON(`{}`), CreatedAt: old, PublishedAt: &recent},
		{ID: uuid.New(), Subject: "s", Payload: JSON(`{}`), CreatedAt: old},
	}
	require.NoError(t, db.Create(&rows).Error)

	relay := NewRelay(db, &fakePublisher{}, Config{Retention: 24 * time.Hour})
	deleted, err := relay.Cleanup(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted)

	var remaining int64
	require.NoError(t, db.Model(&Event{}).Count(&remaining).Error)
	assert.EqualValues(t, 2, remaining, "the recent published row and the unpublished row survive")
}

func TestBacklog(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	seed(t, db, "s", 4)

	relay := NewRelay(db, &fakePublisher{}, Config{})
	n, err := relay.Backlog(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 4, n)
}

func TestRun_DrainsAndStopsOnCancel(t *testing.T) {
	db := testDB(t)
	seed(t, db, "s", 120)

	pub := &fakePublisher{}
	metrics := newRecordingMetrics()
	relay := NewRelay(db, pub, Config{
		PollInterval:    20 * time.Millisecond,
		BatchSize:       50,
		BacklogInterval: 30 * time.Millisecond,
	}, WithMetrics(metrics))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	require.Eventually(t, func() bool { return len(pub.published()) == 120 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()

		return len(metrics.backlog) > 0
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	assert.EqualValues(t, 0, countUnpublished(t, db))
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	assert.Equal(t, 250*time.Millisecond, cfg.PollInterval)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, 7*24*time.Hour, cfg.Retention)
	assert.Equal(t, time.Hour, cfg.CleanupInterval)
	assert.Equal(t, 10*time.Second, cfg.BacklogInterval)

	custom := Config{PollInterval: time.Second, BatchSize: 5}.withDefaults()
	assert.Equal(t, time.Second, custom.PollInterval)
	assert.Equal(t, 5, custom.BatchSize)
}
