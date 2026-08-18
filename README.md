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
                         maintenance       compaction
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

Both containers need the same Iceberg REST/object-storage credentials. The maintenance worker must also be able to reach runner-app/Spark when `executor: hybrid` may route compaction to Spark or when `executor: spark` is configured.

The default Docker command is still the normal Rivus server; the image does not start a maintenance process automatically.

## Hybrid Iceberg maintenance

Automatic maintenance is enabled with one master switch. Compaction behavior is selected independently with `executor`:

```yaml
sink:
  type: iceberg_native
  config:
    # normal Iceberg REST/S3 settings...
    table_maintenance:
      enabled: true
      executor: hybrid
      native_signal_delay_seconds: 300
      native_idle_check_interval_seconds: 604800

      # runner/Spark integration used by hybrid fallback or executor: spark
      runner_uri: http://runner-app:8001
      runner_api_token: ${RUNNER_API_TOKEN}
      runner_resource_profile: small
      catalog_name: rivus

      # hybrid native-compaction boundary. A table becomes eligible after
      # 200 active small files, 50 active equality-delete files, or 10 small
      # files totaling 256 MiB.
      small_file_size_bytes: 67108864
      small_files_min_count: 10
      small_files_min_total_bytes: 268435456
      native_max_selected_input_bytes: 536870912
      native_max_selected_files: 250
      native_max_equality_delete_files: 100
      position_delete_files_threshold: 25 # Spark compaction after this many active position deletes
      native_target_file_size_bytes: 134217728
      native_scan_concurrency: 1
      native_timeout_seconds: 600

      # metadata cleanup
      native_expire_interval_seconds: 86400
      native_snapshot_max_age_hours: 168
      native_snapshot_retain_last: 10
      native_orphan_interval_seconds: 2592000
      native_orphan_inactive_interval_seconds: 7776000
      native_orphan_min_age_hours: 168
      native_orphan_dry_run: true
      worker_temp_directory: /tmp/rivus-maintenance
```

The 256 MiB value is an additional compaction trigger. It is not required once
the active small-file count reaches 200 or equality deletes reach 50.

The executor modes are:

- `hybrid` — default; the worker chooses native or Spark for each compaction task.
- `native` — compaction stays in `iceberg-go`; no Spark fallback.
- `spark` — every automatic compaction task is submitted to Spark.

Snapshot expiration and orphan cleanup stay native in every executor mode. `enabled: false` disables automatic maintenance completely. The old `native_enabled` rollout flag is retired and is no longer part of the user-facing configuration.

In hybrid mode, total table size is not the routing metric: Rivus sums only selected rewrite groups. By default, selected work up to 512 MiB and 100 files can run natively. Larger rewrites, position-delete workloads, and multiple substantial groups route to the existing Spark maintenance integration.

Native compaction uses atomic `RewriteDataFiles`, applies equality deletes while reading, and uses Iceberg's dead-equality-delete logic before removing equality-delete files. Snapshot expiration is native and keeps at least ten snapshots with a seven-day default maximum age. Orphan cleanup is native and disk-bucketed so the complete referenced/candidate sets do not need to live in memory at once.

CDC is not paused for maintenance. The worker checks the initial snapshot barrier before scheduling a table; commit conflicts are maintenance retries, so CDC wins concurrent writes.

For the complete scheduler schema, executor behavior, safety model, API and rollout guide, see [`docs/iceberg-maintenance.md`](docs/iceberg-maintenance.md).

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
RIVUS_MAINTENANCE_GOMAXPROCS=4
RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS=30
RIVUS_MAINTENANCE_LEASE_SECONDS=900
RIVUS_MAINTENANCE_TASK_PAGE_SIZE=1
RIVUS_MAINTENANCE_DUE_PAGE_SIZE=100
GOMEMLIMIT=3GiB

# Global overrides for native compaction boundaries (optional)
RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_INPUT_BYTES=1073741824   # 1 GB
RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_FILES=300
RIVUS_MAINTENANCE_NATIVE_MAX_EQUALITY_DELETE_FILES=150

# Global overrides for snapshot expiration & orphan cleanup (optional)
RIVUS_MAINTENANCE_NATIVE_EXPIRE_INTERVAL_SECONDS=86400         # Check 1x per day
RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_MAX_AGE_HOURS=168            # Delete snapshots > 7 days
RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_RETAIN_LAST=10               # Retain last 10 snapshots
RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS=2592000       # 1x per 30 days
RIVUS_MAINTENANCE_NATIVE_ORPHAN_MIN_AGE_HOURS=168              # Delete orphan files > 7 days
RIVUS_MAINTENANCE_NATIVE_ORPHAN_DRY_RUN=false
```

Example Docker Compose service setup:

```yaml
  rivus-maintenance:
    image: ghcr.io/gerinsp/rivus:latest
    container_name: rivus-maintenance
    command: ["/app/rivus", "maintenance-worker", "--queue"]
    restart: unless-stopped
    mem_limit: 4g
    depends_on:
      meta-mysql:
        condition: service_healthy
    environment:
      TZ: Asia/Jakarta
      GOMEMLIMIT: 3GiB
      RIVUS_MAINTENANCE_GOMAXPROCS: 4
      RIVUS_META_MYSQL_DSN: rivus:password@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
    volumes:
      - ./maintenance-tmp:/tmp/rivus-maintenance
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
RIVUS_WORKER_ROLE=all
RIVUS_WORKER_ID=
RIVUS_WORKER_POLL_INTERVAL_SECONDS=2
RIVUS_WORKER_LEASE_SECONDS=30
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

### Isolated snapshot worker

Large initial snapshots can run in a separate container so their temporary
heap, local spool, and CPU usage do not compete with CDC streaming. Both
containers use the same image, metadata DSN, Iceberg catalog, and object-store
credentials.

```yaml
services:
  rivus-streaming:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "-addr", ":8080", "-ui-dir", "./ui"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ROLE: streaming
      RIVUS_WORKER_ID: streaming-1
      GOMEMLIMIT: 4GiB
    ports:
      - "8080:8080"

  rivus-snapshot:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "snapshot-worker", "--worker-id", "snapshot-1"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_MAX_CONCURRENT_SNAPSHOT_JOBS: "1"
      GOMEMLIMIT: 6GiB
    volumes:
      - ./snapshot-spool:/tmp/rivus-snapshot-spool
```

Submit the normal job YAML to the streaming API with `mode: initial`. The API
stores it as durable snapshot work instead of running it locally. The snapshot
worker leases the job, writes the initial snapshot, and stops before CDC. It
then changes the durable assignment to `STREAMING`; the streaming process
leases the same job and resumes from the binlog position captured before the
snapshot began.

The lease is renewed while work is active. If a worker exits, another replica
of the same role may continue after `RIVUS_WORKER_LEASE_SECONDS`. Interrupted
snapshots resume from saved table progress and still stop at the handoff
boundary. `snapshot-only` jobs finish in the snapshot worker and are not handed
to streaming.

`RIVUS_WORKER_ROLE=all` is the default and preserves the original
single-container behavior. Do not run an old `all` process beside the split
workers for the same metadata database.

For memory-sensitive workloads, start with one concurrent snapshot and a
source `snapshot_batch_size` around 5,000. Raising
`RIVUS_MAX_CONCURRENT_SNAPSHOT_JOBS` to `2` permits two large snapshot batches,
Parquet writers, and spools to exist at the same time, so size the snapshot
container independently from streaming.

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
