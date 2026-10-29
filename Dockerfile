# One multi-stage Dockerfile for all three Go services, selected by --target.
# A single build stage shared by the three binaries means the module download
# and the dependency compile are cached once rather than three times.
#
# CGO_ENABLED=0 here is about producing a static binary for a distroless base,
# not about the broken local macOS SDK. CI must not set it globally, because
# `go test -race` needs cgo.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, so a code change does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG SERVICE_VERSION=0.0.0-dev
ENV CGO_ENABLED=0 GOOS=linux

RUN go build -trimpath \
      -ldflags="-s -w -X main.version=${SERVICE_VERSION}" \
      -o /out/gateway ./cmd/gateway \
 && go build -trimpath \
      -ldflags="-s -w -X main.version=${SERVICE_VERSION}" \
      -o /out/orders ./cmd/orders \
 && go build -trimpath \
      -ldflags="-s -w -X main.version=${SERVICE_VERSION}" \
      -o /out/inventory ./cmd/inventory

# wget comes from busybox in the alpine runtime stage and is what the compose
# healthchecks use; a distroless base would need a Go-based probe instead.
FROM alpine:3.22 AS runtime
RUN adduser -D -u 10001 app
USER app

FROM runtime AS gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gateway"]

FROM runtime AS orders
COPY --from=build /out/orders /usr/local/bin/orders
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/orders"]

FROM runtime AS inventory
COPY --from=build /out/inventory /usr/local/bin/inventory
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/inventory"]
