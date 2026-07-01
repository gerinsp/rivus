<p align="center">
  <img src="ui/rivus-logo.png" alt="Rivus logo" width="96" height="96">
</p>

<h1 align="center">Rivus</h1>

<p align="center">
  A lightweight streaming data engine for MySQL snapshots, binlog CDC, Doris, and Apache Iceberg.
</p>

<p align="center">
  <a href="https://github.com/gerinsp/rivus/actions/workflows/ci.yml"><img src="https://github.com/gerinsp/rivus/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/gerinsp/rivus/actions/workflows/publish-container.yml"><img src="https://github.com/gerinsp/rivus/actions/workflows/publish-container.yml/badge.svg" alt="Publish Container"></a>
  <a href="https://github.com/gerinsp/rivus/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gerinsp/rivus" alt="License"></a>
  <a href="https://github.com/gerinsp/rivus/pkgs/container/rivus"><img src="https://img.shields.io/badge/ghcr.io-rivus-blue" alt="GHCR"></a>
</p>

Rivus is a small streaming data engine for moving table data from MySQL into analytical stores. It supports initial snapshots, MySQL binlog CDC, resumable job state, and a lightweight web UI for submitting and monitoring jobs.

## Features

- Chunked initial snapshots from MySQL.
- `snapshot-only` mode for one-time loads without CDC.
- MySQL binlog CDC using `go-mysql`.
- Doris sink with table creation, DDL forwarding, batching, retries, and stream-load support.
- Native Iceberg REST catalog sink for object storage-backed tables.
- Persistent offsets and job registry in MySQL metadata storage.
- Multi-job REST API and dashboard UI.
- Pause/resume behavior that drains buffered events before checkpointing.
- Optional UI and API protection using environment variables.
- YAML/JSON job configs with `${ENV_VAR}` placeholder expansion.

## Quick Start

Prerequisites:

- Docker, if you want to run Rivus from the published container image.
- Go 1.25 or newer, if you want to develop from source.

Run Rivus with the published GHCR image:

```sh
docker pull ghcr.io/gerinsp/rivus:latest
docker run --rm -p 8080:8080 ghcr.io/gerinsp/rivus:latest
```

Then open `http://localhost:8080`.

To run with a local metadata MySQL:

```sh
cp .env.example .env
docker compose pull
docker compose up -d
```

To update an existing Compose deployment:

```sh
docker compose pull rivus
docker compose up -d rivus
```

Run from source during development:

```sh
go run ./cmd/rivus -addr :8080 -ui-dir ./ui
```

## Configuration

Rivus jobs are submitted as YAML or JSON through the UI or API. A job defines a source, a sink, retry policy, buffer size, and metadata storage.

Generic examples are available in:

- `examples/mysql-to-doris.yaml`
- `examples/mysql-to-iceberg.yaml`

Submit a job through the API:

```sh
curl -X POST \
  -H 'Content-Type: application/x-yaml' \
  --data-binary @examples/mysql-to-doris.yaml \
  http://localhost:8080/api/jobs
```

If `RIVUS_API_TOKEN` is set, pass one of these headers:

```text
X-Rivus-Token: <token>
Authorization: Bearer <token>
```

## Important Environment Variables

```env
RIVUS_META_MYSQL_DSN=rivus:change-me@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
RIVUS_UI_LOGIN_ENABLED=false
RIVUS_UI_LOGIN_USERNAME=admin
RIVUS_UI_LOGIN_PASSWORD=change-me
RIVUS_UI_SESSION_SECRET=change-me
RIVUS_API_TOKEN=
RIVUS_LOG_DIR=/app/logs
RIVUS_LOG_STDERR=true
```

Iceberg sink integrations can also use:

```env
ICEBERG_REST_URI=http://iceberg-rest:8181
ICEBERG_WAREHOUSE=warehouse
ICEBERG_REST_AUTH_HEADER=
ICEBERG_REST_BASIC_USERNAME=
ICEBERG_REST_BASIC_PASSWORD=
ICEBERG_S3_ENDPOINT=http://minio:9000
ICEBERG_S3_PATH_STYLE=true
AWS_ACCESS_KEY_ID=change-me
AWS_SECRET_ACCESS_KEY=change-me
AWS_DEFAULT_REGION=us-east-1
```

## Docker

Published images are distributed through GitHub Container Registry:

```sh
docker pull ghcr.io/gerinsp/rivus:latest
docker run --rm -p 8080:8080 ghcr.io/gerinsp/rivus:latest
```

The publish workflow pushes images on `main`, version tags such as `v1.0.0`, and manual workflow runs.

Common tags:

- `latest`: latest build from the default branch.
- `main`: latest build from the `main` branch.
- `sha-<commit>`: exact build for a commit.

Run the local Compose stack from GHCR:

```sh
cp .env.example .env
docker compose pull
docker compose up -d
```

Build an image locally for development:

```sh
docker build \
  --build-arg VERSION=dev \
  --build-arg COMMIT="$(git rev-parse --short HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t rivus:dev .
```

Or use the development Compose override:

```sh
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

## Development

Useful commands:

```sh
go test ./...
go test ./pkg/connectors/mysql
go run ./cmd/rivus -addr :8080 -ui-dir ./ui
```

Please see `CONTRIBUTING.md` before opening a pull request.

## Security

Do not commit real database credentials, object storage credentials, API tokens, logs, or production job configs. Use `.env` locally and keep publishable configs under `examples/` with placeholder values only.

To report a vulnerability, see `SECURITY.md`.

## License

Apache License 2.0. See `LICENSE`.
