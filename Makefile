.PHONY: run test bench load up
run:
	go run ./cmd/server -addr :8080
test:
	go test -race ./...
bench:
	go test -run x -bench . -benchmem ./internal/...
load:
	go run ./cmd/loadgen -url http://localhost:8080 -c 128 -d 20s
up:
	docker compose up --build
