# go-outbox-lib

Transactional outbox for weeb-vip Go services. A service writes an event row
in the same Postgres transaction as the change it describes; a relay publishes
the rows to NATS JetStream afterwards. Events therefore exist if and only if
the change they describe was committed, and no request ever waits on the bus.

## Use in a service

1. Add the table with a migration. Copy `outbox.Schema` into
   `db/migrations/<ts>_outbox_events.up.sql` (and `outbox.SchemaDown` into the
   `.down.sql`) so the migrations workflow verifies it like any other.

2. Write events inside your transaction:

   ```go
   err := db.Transaction(func(tx *gorm.DB) error {
       if err := tx.Create(&follow).Error; err != nil {
           return err
       }
       _, err := outbox.Write(ctx, tx, "user-follow", FollowEvent{...})
       return err
   })
   ```

3. Run the relay as its own command (deployed with weeb-argocd's event-chart):

   ```go
   driver := epnats.NewNatsDriver(&epnats.Config{URL: cfg.NatsURL})
   relay := outbox.NewRelay(db, outbox.NewNatsPublisher(driver), outbox.Config{},
       outbox.WithLogger(log), outbox.WithMetrics(myMetrics))
   return relay.Run(ctx) // until ctx is cancelled
   ```

## Guarantees

- **At-least-once.** A row is marked published only after the publish call
  returned. A crash in between republishes it; the message carries
  `Nats-Msg-Id` = event id, so JetStream drops the duplicate inside its
  duplicate window, and consumers must also keep an `event_id` unique index.
- **Ordered per relay pass.** Rows go out in `created_at` order and a pass
  stops at the first failure.
- **Safe to scale.** Rows are claimed with `FOR UPDATE SKIP LOCKED`; several
  relays never publish the same row twice.

## Tests

```
make db-up     # Postgres 16.4 on :5433
make test
make db-down
```

NATS tests run an in-process JetStream server and need nothing else.
