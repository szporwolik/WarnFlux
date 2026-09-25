# WarnFlux development helpers.
# `make check` replicates the CI pipeline locally — run it before every push.
# `make fmt` rewrites all Go sources with gofmt (use it when `make check`
# complains about unformatted files).

GO ?= go

.PHONY: fmt check test race build

## fmt: rewrite all Go sources with gofmt (in place)
fmt:
	$(GO)fmt -w $$(find . -name '*.go' -not -path './vendor/*')

## check: everything the CI workflow gates on, in the same order
check:
	@unformatted=$$($(GO)fmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "unformatted files (run 'make fmt'):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	@v="$$(tr -d '[:space:]' < VERSION)"; \
	if ! echo "$$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "VERSION must be a semantic version like 0.1.0, got '$$v'"; \
		exit 1; \
	fi
	$(GO) vet ./...
	$(GO) test -count=1 ./...
	$(GO) test -race -count=1 ./...
	$(GO) build ./cmd/warnflux

## test: plain (non-race) test run
test:
	$(GO) test -count=1 ./...

## race: tests under the race detector
race:
	$(GO) test -race -count=1 ./...

## build: local binary into build/warnflux
build:
	mkdir -p build
	$(GO) build -o build/warnflux ./cmd/warnflux
