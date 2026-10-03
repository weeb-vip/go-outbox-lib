# go-outbox-lib

Shared library: transactional outbox (Postgres + GORM) with a relay that
publishes to NATS JetStream via `github.com/ThatCatDev/ep/v2`.

## Commands
- `make db-up` / `make db-down`: Postgres 16.4 for tests on port 5433.
- `make test`: runs all tests (sets `TEST_DATABASE_URL`). Tests skip without it.
- `go vet ./...`

## Layout
- `outbox.go`: `Event`, `JSON` column type, `Schema`/`SchemaStatements`/`SchemaDown`.
- `writer.go`: `Write(ctx, tx, subject, payload)`.
- `publisher.go`: `Publisher` interface, `NewNatsPublisher`, `MessageIDHeader`.
- `relay.go`: `Relay` (`RelayOnce`, `Cleanup`, `Backlog`, `Run`), `Config`, `Metrics`.

## Conventions
- Services copy `Schema` into their own migration files; the library never
  migrates for them.
- Keep the library free of logging/metrics backends beyond zerolog and the
  `Metrics` interface; services adapt go-metrics-lib themselves.
- Tests share one table and must not use `t.Parallel()`.
