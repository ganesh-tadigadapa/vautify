.PHONY: build test test-race test-unit test-integration demo demo-selfheal demo-corruption demo-all cluster docker-up docker-smoke docker-down docker-selfheal-up docker-selfheal-smoke docker-selfheal-down docker-partition-up docker-partition-smoke clean

build:
	go build -o bin/vault ./cmd/vault

test-unit:
	go test ./internal/...

test-integration:
	go test -count=1 ./test/integration/

test:
	go vet ./...
	go test -count=1 ./...

# Race detector for unit tests AND the spawned node/api processes.
test-race:
	go test -race -count=1 ./...

demo: build
	./bin/vault demo

demo-selfheal: build
	./bin/vault demo self-heal

demo-corruption: build
	./bin/vault demo corruption-repair

demo-all: build
	./bin/vault demo all

cluster: build
	./bin/vault cluster --data ./data

docker-up:
	docker compose up --build -d

docker-smoke:
	./scripts/docker-smoke.sh

docker-down:
	docker compose down

SELFHEAL_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.selfheal.yml

docker-selfheal-up:
	$(SELFHEAL_COMPOSE) up --build -d

docker-selfheal-smoke:
	./scripts/docker-selfheal-smoke.sh

docker-selfheal-down:
	$(SELFHEAL_COMPOSE) down

# Phase 4: partition/quorum/healing smoke test on the 5-node deployment,
# from fresh volumes.
docker-partition-up:
	$(SELFHEAL_COMPOSE) down -v
	$(SELFHEAL_COMPOSE) up --build -d

docker-partition-smoke:
	./scripts/docker-partition-smoke.sh

clean:
	rm -rf bin data
