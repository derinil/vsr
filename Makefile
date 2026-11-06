BIN     := build/bin
CMDS    := vsrd vsrctl vsrmon vsrdemo vsrmaelstrom
CLUSTER ?= 127.0.0.1:10188,127.0.0.1:10189,127.0.0.1:10190,127.0.0.1:10191,127.0.0.1:10192
ARGS    ?=

.PHONY: help all build test race vet fmt check demo $(CMDS) up down logs e2e chaos docs docs-gen clean

help:
	@echo "build        build all commands into $(BIN)"
	@echo "test         go test ./..."
	@echo "race         go test -race ./..."
	@echo "vet fmt      go vet, gofmt check"
	@echo "check        fmt, vet, test"
	@echo "demo         run the local SQLite cluster with load, chaos and dashboard (ARGS=...)"
	@echo "vsrd vsrctl vsrmon vsrmaelstrom"
	@echo "             run a command with ARGS; vsrctl uses CLUSTER, e.g. make vsrctl ARGS='get a'"
	@echo "up down logs docker compose cluster"
	@echo "e2e          Maelstrom lin-kv against the compose cluster (scripts/e2e.sh)"
	@echo "chaos        chaos against the compose cluster (scripts/chaos.sh)"
	@echo "docs         browse package docs; docs-gen writes them to build/docs"
	@echo "clean        remove $(BIN)"

all: check build

build:
	@mkdir -p $(BIN)
	go build -o $(BIN)/ $(addprefix ./cmd/,$(CMDS))

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt vet test

demo:
	go run ./cmd/vsrdemo $(ARGS)

vsrd vsrmon vsrmaelstrom:
	go run ./cmd/$@ $(ARGS)

vsrctl:
	go run ./cmd/vsrctl -cluster $(CLUSTER) $(ARGS)

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

e2e:
	scripts/e2e.sh

chaos:
	scripts/chaos.sh

docs:
	scripts/docs.sh serve

docs-gen:
	scripts/docs.sh gen

clean:
	rm -rf $(BIN)
