package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Config tunes the relay. Zero values take the defaults documented on each
// field.
type Config struct {
	// PollInterval is how long the relay waits after an empty or failed pass.
	// Default 250ms: an event reaches the bus well under a second after commit
	// while an idle relay costs one cheap index probe four times a second.
	PollInterval time.Duration
	// BatchSize is the number of rows locked and published per pass. Default 100.
	BatchSize int
	// Retention is how long published rows are kept before Cleanup deletes
	// them. Default 7 days: long enough to investigate a consumer problem by
	// re-marking rows, short enough that the table never grows unbounded.
	Retention time.Duration
	// CleanupInterval is how often Run deletes expired rows. Default 1 hour.
	CleanupInterval time.Duration
	// BacklogInterval is how often Run reports the unpublished count to
	// Metrics. Default 10 seconds.
	BacklogInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Hour
	}
	if c.BacklogInterval <= 0 {
		c.BacklogInterval = 10 * time.Second
	}

	return c
}

// Metrics receives relay observations. Services adapt it to go-metrics-lib;
// the library itself has no opinion about the metrics backend.
type Metrics interface {
	// Published is called once per event handed to the Publisher successfully.
	Published(subject string)
	// PublishFailed is called once per Publish error.
	PublishFailed(subject string)
	// Backlog reports the number of unpublished rows.
	Backlog(unpublished int64)
}

type noopMetrics struct{}

func (noopMetrics) Published(string)     {}
func (noopMetrics) PublishFailed(string) {}
func (noopMetrics) Backlog(int64)        {}

// Relay moves unpublished rows to a Publisher.
//
// Rows are claimed with FOR UPDATE SKIP LOCKED, so several relays can run
// against one table without publishing a row twice: each pass sees only rows
// no other pass holds. Within a pass rows go out in creation order and the
// pass stops at the first failure, so a subject's events never overtake one
// another through the relay.
type Relay struct {
	db        *gorm.DB
	publisher Publisher
	cfg       Config
	metrics   Metrics
	log       zerolog.Logger
}

// Option configures a Relay.
type Option func(*Relay)

// WithMetrics sets the Metrics sink.
func WithMetrics(m Metrics) Option {
	return func(r *Relay) {
		if m != nil {
			r.metrics = m
		}
	}
}

// WithLogger sets the logger Run reports through.
func WithLogger(l zerolog.Logger) Option {
	return func(r *Relay) { r.log = l }
}

// NewRelay builds a relay over db that publishes through publisher.
func NewRelay(db *gorm.DB, publisher Publisher, cfg Config, opts ...Option) *Relay {
	r := &Relay{
		db:        db,
		publisher: publisher,
		cfg:       cfg.withDefaults(),
		metrics:   noopMetrics{},
		log:       zerolog.Nop(),
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// RelayOnce publishes up to BatchSize rows and returns how many it marked
// published. A Publish error stops the pass; rows published before it are
// still marked, and the error is returned after the transaction commits so
// that a partial pass is never undone.
//
// If marking fails after a publish succeeded, those rows are published again
// on a later pass. That duplicate is what MessageIDHeader and the consumer's
// event_id index exist for.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	var (
		published  int
		publishErr error
	)

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var events []Event
		err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("published_at IS NULL").
			Order("created_at ASC, id ASC").
			Limit(r.cfg.BatchSize).
			Find(&events).Error
		if err != nil {
			return fmt.Errorf("outbox: select unpublished: %w", err)
		}
		if len(events) == 0 {
			return nil
		}

		ids := make([]uuid.UUID, 0, len(events))
		for i := range events {
			event := &events[i]
			if err := r.publisher.Publish(ctx, event); err != nil {
				r.metrics.PublishFailed(event.Subject)
				publishErr = fmt.Errorf("outbox: publish %s on %q: %w", event.ID, event.Subject, err)

				break
			}
			r.metrics.Published(event.Subject)
			ids = append(ids, event.ID)
		}
		if len(ids) == 0 {
			return nil
		}

		now := time.Now().UTC()
		err = tx.Model(&Event{}).Where("id IN ?", ids).Update("published_at", now).Error
		if err != nil {
			return fmt.Errorf("outbox: mark published: %w", err)
		}
		published = len(ids)

		return nil
	})
	if err != nil {
		return 0, err
	}

	return published, publishErr
}

// Cleanup deletes rows published longer ago than Retention and returns how
// many it removed.
func (r *Relay) Cleanup(ctx context.Context) (int64, error) {
	cutoff := time.Now().UTC().Add(-r.cfg.Retention)
	result := r.db.WithContext(ctx).
		Where("published_at IS NOT NULL AND published_at < ?", cutoff).
		Delete(&Event{})
	if result.Error != nil {
		return 0, fmt.Errorf("outbox: cleanup: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// Backlog returns the number of unpublished rows.
func (r *Relay) Backlog(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&Event{}).Where("published_at IS NULL").Count(&n).Error
	if err != nil {
		return 0, fmt.Errorf("outbox: count backlog: %w", err)
	}

	return n, nil
}

// Run relays until ctx is cancelled, which is the only way it returns nil.
// Errors from a pass are logged and retried on the next tick rather than
// returned: the relay's job is to keep trying, and a transient bus or database
// error must not take the process down and lose the backlog with it.
func (r *Relay) Run(ctx context.Context) error {
	poll := time.NewTicker(r.cfg.PollInterval)
	defer poll.Stop()
	cleanup := time.NewTicker(r.cfg.CleanupInterval)
	defer cleanup.Stop()
	backlog := time.NewTicker(r.cfg.BacklogInterval)
	defer backlog.Stop()

	r.log.Info().
		Dur("poll_interval", r.cfg.PollInterval).
		Int("batch_size", r.cfg.BatchSize).
		Msg("outbox relay started")

	for {
		select {
		case <-ctx.Done():
			r.log.Info().Msg("outbox relay stopped")

			return nil
		case <-poll.C:
			r.drain(ctx)
		case <-cleanup.C:
			if n, err := r.Cleanup(ctx); err != nil {
				r.log.Error().Err(err).Msg("outbox cleanup failed")
			} else if n > 0 {
				r.log.Info().Int64("deleted", n).Msg("outbox cleanup")
			}
		case <-backlog.C:
			if n, err := r.Backlog(ctx); err != nil {
				r.log.Error().Err(err).Msg("outbox backlog count failed")
			} else {
				r.metrics.Backlog(n)
			}
		}
	}
}

// drain runs passes back to back while they come back full, so a burst is
// cleared at publish speed rather than one batch per poll interval.
func (r *Relay) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.RelayOnce(ctx)
		if err != nil {
			r.log.Error().Err(err).Int("published", n).Msg("outbox relay pass failed")

			return
		}
		if n < r.cfg.BatchSize {
			return
		}
	}
}
