# syntax=docker/dockerfile:1.7

FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY pkg ./pkg

ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=
ARG BUILD_DATE=

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-X github.com/gerinsp/rivus/pkg/version.Version=${VERSION} -X github.com/gerinsp/rivus/pkg/version.Commit=${COMMIT} -X github.com/gerinsp/rivus/pkg/version.BuildDate=${BUILD_DATE}" \
    -o rivus ./cmd/rivus

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache tzdata

COPY --from=builder /app/rivus /app/rivus
COPY ui /app/ui

EXPOSE 8080

CMD ["/app/rivus", "-addr", ":8080", "-ui-dir", "./ui"]
