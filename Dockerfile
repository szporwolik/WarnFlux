# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Fetch dependencies first for better layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/warnflux ./cmd/warnflux

# ---- Runtime stage ---------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/warnflux /warnflux

# The base image already provides a non-root user.
USER nonroot:nonroot

# WarnFlux acts only as an MQTT client, so no ports are exposed.
ENTRYPOINT ["/warnflux"]
