GO ?= go
BINARY ?= bin/open-aspm

.PHONY: build check fmt format test test-integration test-race test-s3-integration vet

build:
	mkdir -p $(dir $(BINARY))
	$(GO) build -trimpath -o $(BINARY) ./cmd/open-aspm

fmt:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following Go files are not formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

format:
	gofmt -w .

test:
	$(GO) test -count=1 ./...

test-integration:
	$(GO) test -count=1 -tags=integration ./internal/database ./internal/ingestion ./internal/jobqueue

test-s3-integration:
	$(GO) test -count=1 -tags=integration ./internal/blobstore/s3store

test-race:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

check: fmt vet test build
