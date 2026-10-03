# TEST_DATABASE_URL points the tests at a Postgres. `make db-up` starts one on
# port 5433 so it does not collide with a service's own local database.
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5433/outbox_test?sslmode=disable

generate:

db-up:
	docker compose up -d --wait

db-down:
	docker compose down -v

test:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) go test -race -count=1 ./...

lint:
	go vet ./...
