package outbox

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testDB connects to the Postgres named by TEST_DATABASE_URL, applies the
// schema and empties the table. Tests share one table and so must not run in
// parallel with each other.
func testDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; run `make db-up` and `make test`")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	for _, stmt := range SchemaStatements {
		require.NoError(t, db.Exec(stmt).Error)
	}
	require.NoError(t, db.Exec("TRUNCATE "+TableName).Error)

	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	return db
}

// fakePublisher records what it was handed and can be told to fail.
type fakePublisher struct {
	mu     sync.Mutex
	events []Event
	failOn func(event *Event) error
}

func (f *fakePublisher) Publish(_ context.Context, event *Event) error {
	if f.failOn != nil {
		if err := f.failOn(event); err != nil {
			return err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, *event)

	return nil
}

func (f *fakePublisher) published() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]Event(nil), f.events...)
}

type recordingMetrics struct {
	mu        sync.Mutex
	published map[string]int
	failed    map[string]int
	backlog   []int64
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{published: map[string]int{}, failed: map[string]int{}}
}

func (m *recordingMetrics) Published(subject string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published[subject]++
}

func (m *recordingMetrics) PublishFailed(subject string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed[subject]++
}

func (m *recordingMetrics) Backlog(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backlog = append(m.backlog, n)
}

func countUnpublished(t *testing.T, db *gorm.DB) int64 {
	t.Helper()

	var n int64
	require.NoError(t, db.Model(&Event{}).Where("published_at IS NULL").Count(&n).Error)

	return n
}
