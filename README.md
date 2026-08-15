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
- Hybrid Iceberg maintenance with a durable MySQL scheduler, native `iceberg-go` operations, and Spark fallback for heavy compaction.
- Persistent offsets and job registry in MySQL metadata storage.
- Multi-job REST API and dashboard UI.
- Pause/resume behavior that drains buffered events before checkpointing.
- Optional UI and API protection using environment variables.
- YAML/JSON job configs with `${ENV_VAR}` placeholder expansion.

## Quick Start

Prerequisites:

- Docker for the published image.
- Go 1.25 or newer for source development.

Run the normal CDC/API server:

```sh
docker pull ghcr.io/gerinsp/rivus:latest
docker run --rm -p 8080:8080 ghcr.io/gerinsp/rivus:latest
```

Then open `http://localhost:8080`.

Run from source:

```sh
go run ./cmd/rivus -addr :8080 -ui-dir ./ui
```

## Two-container Iceberg maintenance

The same Rivus image supports two independent process modes. Do not run both processes in one container.

```text
                    same rivus image
                          |
             +------------+------------+
             |                         |
             v                         v
      rivus-streaming            rivus-maintenance
      /app/rivus                 /app/rivus
        -addr :8080                maintenance-worker --queue
             |                         |
      MySQL CDC -> Iceberg       durable MySQL queue
                                       |
                                +------+------+
                                |             |
                             native          Spark
                          tiny/cleanup       heavy
```

Example Compose service split:

```yaml
services:
  rivus-streaming:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "-addr", ":8080", "-ui-dir", "./ui"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}

  rivus-maintenance:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "maintenance-worker", "--queue"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_MAINTENANCE_GOMAXPROCS: "1"
      GOMEMLIMIT: 256MiB
```

Both containers need the same Iceberg REST/object-storage credentials. The maintenance worker must also be able to reach runner-app/Spark when heavy compaction fallback is configured.

The default Docker command is still the normal Rivus server; the image does not start a maintenance process automatically.

## Hybrid Iceberg maintenance

Native maintenance is explicitly opt-in per Iceberg job. During the first rollout, use `native_enabled: true` **without** legacy `enabled: true`; the legacy flag starts the old CDC-side automatic Spark monitor and should remain disabled when the separate worker owns scheduling.

```yaml
sink:
  type: iceberg_native
  config:
    # normal Iceberg REST/S3 settings...
    table_maintenance:
      native_enabled: true
      native_signal_delay_seconds: 300
      native_idle_check_interval_seconds: 604800

      # existing runner/Spark integration is retained for heavy compaction
      runner_uri: http://runner-app:8001
      runner_api_token: ${RUNNER_API_TOKEN}
      runner_resource_profile: small
      catalog_name: rivus

      # native tiny-compaction boundary
      small_file_size_bytes: 67108864
      small_files_min_count: 10
      small_files_min_total_bytes: 268435456
      native_max_selected_input_bytes: 536870912
      native_max_selected_files: 100
      native_target_file_size_bytes: 134217728
      native_scan_concurrency: 1
      native_timeout_seconds: 600

      # metadata cleanup
      native_expire_interval_seconds: 86400
      native_snapshot_max_age_hours: 168
      native_snapshot_retain_last: 10
      native_orphan_interval_seconds: 2592000
      native_orphan_min_age_hours: 168
      native_orphan_dry_run: true
      worker_temp_directory: /tmp/rivus-maintenance
```

The worker plans compaction with `iceberg-go` before choosing an executor. Total table size is not the routing metric: Rivus sums only selected rewrite groups. By default, selected work up to 512 MiB and 100 files can run natively. Larger rewrites, position-delete workloads, and multiple substantial groups route to the existing Spark maintenance integration.

Native compaction uses atomic `RewriteDataFiles`, applies equality deletes while reading, and uses Iceberg's dead-equality-delete logic before removing equality-delete files. Snapshot expiration is native and keeps at least ten snapshots with a seven-day default maximum age. Orphan cleanup is native and disk-bucketed so the complete referenced/candidate sets do not need to live in memory at once.

CDC is not paused for maintenance. The worker checks the initial snapshot barrier before scheduling a table; commit conflicts are maintenance retries, so CDC wins concurrent writes.

For the complete scheduler schema, routing rules, safety model, API and rollout guide, see [`docs/iceberg-maintenance.md`](docs/iceberg-maintenance.md).

## Durable scheduler

Maintenance state is stored in metadata MySQL rather than a 6,000-table in-memory priority queue. The worker creates:

- `iceberg_maintenance_state`
- `iceberg_maintenance_tasks`
- `iceberg_maintenance_runs`
- `iceberg_maintenance_results`

Due state is read in bounded pages, tasks use idempotency keys, MySQL 8 leases use `FOR UPDATE SKIP LOCKED`, expired leases are reclaimable, and deterministic jitter prevents every table from waking at the same time.

Useful worker environment variables:

```env
RIVUS_META_MYSQL_DSN=rivus:change-me@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
RIVUS_MAINTENANCE_GOMAXPROCS=1
RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS=30
RIVUS_MAINTENANCE_LEASE_SECONDS=900
RIVUS_MAINTENANCE_TASK_PAGE_SIZE=50
RIVUS_MAINTENANCE_DUE_PAGE_SIZE=100
GOMEMLIMIT=256MiB
```

One-shot and queue modes:

```sh
/app/rivus maintenance-worker
/app/rivus maintenance-worker --queue
```

## Maintenance API

The API is protected by the same Rivus API authentication as other protected endpoints:

```text
GET /api/iceberg/maintenance/summary
GET /api/iceberg/maintenance/runs?limit=50&offset=0
GET /api/iceberg/maintenance/runs/{id}?limit=100
GET /api/iceberg/maintenance/tables/{catalog.namespace.table}
```

The existing per-job manual maintenance endpoint remains available for compatibility:

```sh
curl -X POST -H 'Content-Type: application/json'   -d '{"tables":["analytics.orders"],"operations":[{"type":"rewrite_data_files"}]}'   http://localhost:8080/api/jobs/example-mysql-to-iceberg/iceberg/maintenance
```

## Configuration

Rivus jobs are submitted as YAML or JSON through the UI or API. Generic examples are available in:

- `examples/mysql-to-doris.yaml`
- `examples/mysql-to-iceberg.yaml`

Submit a job:

```sh
curl -X POST   -H 'Content-Type: application/x-yaml'   --data-binary @examples/mysql-to-doris.yaml   http://localhost:8080/api/jobs
```

If `RIVUS_API_TOKEN` is set, pass either:

```text
X-Rivus-Token: <token>
Authorization: Bearer <token>
```

Important server environment variables:

```env
RIVUS_META_MYSQL_DSN=rivus:change-me@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
RIVUS_UI_LOGIN_ENABLED=false
RIVUS_UI_LOGIN_USERNAME=admin
RIVUS_UI_LOGIN_PASSWORD=change-me
RIVUS_UI_SESSION_SECRET=change-me
RIVUS_API_TOKEN=
RIVUS_AUTO_RESUME=false
RIVUS_SHUTDOWN_TIMEOUT_SECONDS=90
RIVUS_LOG_DIR=/app/logs
RIVUS_LOG_STDERR=true
```

Iceberg integrations can use:

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

## Snapshots and shutdown

On `SIGTERM` or `SIGINT`, the server stops starting new work, drains active jobs to committed checkpoints, and preserves their desired state as `RUNNING`. Set `RIVUS_AUTO_RESUME=true` with `RIVUS_META_MYSQL_DSN` to resume saved running jobs after replacement.

Initial MySQL snapshots for `iceberg_native` use a disk-backed rolling writer by default. Each source batch is spooled locally and acknowledged without advancing durable table progress; at table completion Rivus streams the spool into Iceberg Parquet files and then advances snapshot progress.

```yaml
sink:
  type: iceberg_native
  config:
    snapshot_rolling_enabled: true
    snapshot_target_file_size_bytes: 134217728
    snapshot_parquet_row_group_rows: 50000
    snapshot_spool_directory: /tmp/rivus-snapshot-spool
    snapshot_spool_max_bytes: 21474836480
```

## Docker

Published images are distributed through GitHub Container Registry:

```sh
docker pull ghcr.io/gerinsp/rivus:latest
docker run --rm -p 8080:8080 ghcr.io/gerinsp/rivus:latest
```

The publish workflow pushes images on `main` and version tags. The default `CMD` remains:

```text
/app/rivus -addr :8080 -ui-dir ./ui
```

## Development

```sh
go test ./...
go test ./pkg/connectors/iceberg
go run ./cmd/rivus -addr :8080 -ui-dir ./ui
```

Please see `CONTRIBUTING.md` before opening a pull request.

## Security

Do not commit real database credentials, object-storage credentials, API tokens, logs, or production job configs. Use environment placeholders in publishable examples.

To report a vulnerability, see `SECURITY.md`.

## License

Apache License 2.0. See `LICENSE`.
