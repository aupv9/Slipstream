BINARY  := bin/slipstream
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test test-integration vet fmt clean

build:
	go build -ldflags "-X main.version=$(VERSION)" -o $(BINARY) ./cmd/slipstream

# Unit tests only; the integration tests skip without their DSN env vars.
test:
	go test ./...

# Integration tests against a real Postgres (wal_level = logical required).
#   make test-integration \
#     CP_DSN=postgres://postgres@127.0.0.1:5432/slipstream_cp \
#     SRC_DSN=postgres://postgres@127.0.0.1:5432/app
test-integration:
	SLIPSTREAM_TEST_CP_DSN=$(CP_DSN) SLIPSTREAM_TEST_SOURCE_DSN=$(SRC_DSN) go test ./... -count=1

vet:
	go vet ./...

fmt:
	gofmt -l -w .

clean:
	rm -rf bin
