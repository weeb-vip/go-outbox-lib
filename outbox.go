// Package outbox implements the transactional outbox pattern for weeb-vip
// services: a service writes an event row in the same database transaction as
// the state change it describes, and a Relay publishes those rows to NATS
// JetStream afterwards. An event therefore exists if and only if the change
// that caused it was committed.
//
// Services own the table. They copy Schema into a migration of their own so
// the migrations workflow can verify it like any other, and pass a *gorm.DB
// transaction to Write. The Relay runs as a separate command of the same
// binary, deployed on its own, so a relay outage never blocks a request.
package outbox

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TableName is the table every service keeps its outbox in. One name across
// services keeps the relay, the dashboards and the migrations identical.
const TableName = "outbox_events"

// SchemaStatements creates the outbox table. They are separate statements
// rather than one string because the extended query protocol runs exactly one
// statement per Exec. Schema joins them for a migration file.
//
// The partial index is the whole read path of the relay: unpublished rows are
// the only rows it ever looks for, and once published a row is dead weight
// until cleanup removes it.
var SchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS ` + TableName + ` (
    id           uuid PRIMARY KEY,
    subject      text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
)`,
	`CREATE INDEX IF NOT EXISTS idx_` + TableName + `_unpublished ON ` + TableName + ` (created_at, id) WHERE published_at IS NULL`,
}

// Schema is SchemaStatements as one migration body.
var Schema = strings.Join(SchemaStatements, ";\n") + ";\n"

// SchemaDown reverses Schema.
const SchemaDown = `DROP TABLE IF EXISTS ` + TableName + `;`

// JSON is a jsonb column value. It is its own type rather than
// json.RawMessage because database/sql hands a []byte to the driver as bytea,
// and Postgres refuses bytea for a jsonb column; sending text lets the server
// cast it.
type JSON []byte

// Value implements driver.Valuer.
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, fmt.Errorf("outbox: empty JSON payload")
	}

	return string(j), nil
}

// Scan implements sql.Scanner.
func (j *JSON) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*j = nil
	case []byte:
		*j = append(JSON(nil), v...)
	case string:
		*j = JSON(v)
	default:
		return fmt.Errorf("outbox: cannot scan %T into JSON", src)
	}

	return nil
}

// MarshalJSON returns the stored document unchanged.
func (j JSON) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}

	return j, nil
}

// UnmarshalJSON stores the document unchanged.
func (j *JSON) UnmarshalJSON(b []byte) error {
	*j = append(JSON(nil), b...)

	return nil
}

// Event is one outbox row.
//
// ID doubles as the JetStream message id, so a row the relay publishes twice --
// after a crash between publishing and marking, say -- is deduplicated by the
// server inside its duplicate window, and by the consumer's own event_id
// unique index outside it.
type Event struct {
	ID          uuid.UUID  `gorm:"column:id;type:uuid;primaryKey"`
	Subject     string     `gorm:"column:subject;not null"`
	Payload     JSON       `gorm:"column:payload;type:jsonb;not null"`
	CreatedAt   time.Time  `gorm:"column:created_at;not null"`
	PublishedAt *time.Time `gorm:"column:published_at"`
}

// TableName implements gorm's Tabler.
func (Event) TableName() string { return TableName }
