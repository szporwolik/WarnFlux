# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
FROM golang:1.26-alpine AS build

# Version metadata, injected by CI (or defaults for local builds).
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Fetch dependencies first for better layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/warnflux ./cmd/warnflux

# Prepare empty /data (SQLite) and /logs (optional log files) owned by
# the runtime user so mounted volumes inherit writable permissions for
# the non-root container.
RUN mkdir -p /out/data /out/logs && chown 65532:65532 /out/data /out/logs

# ---- Runtime stage ---------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

# Re-declared so the runtime stage can use them in labels.
ARG VERSION=dev
ARG COMMIT=unknown

# Standard OCI labels; version metadata is injected at build time.
LABEL org.opencontainers.image.title="WarnFlux" \
      org.opencontainers.image.description="Hazard event aggregation, normalization and at-least-once delivery daemon" \
      org.opencontainers.image.source="https://github.com/szporwolik/WarnFlux" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"

COPY --from=build /out/warnflux /warnflux
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/logs /logs

# The base image already provides a non-root user.
USER nonroot:nonroot

# WarnFlux acts only as an MQTT client, so no ports are exposed.
ENTRYPOINT ["/warnflux"]
