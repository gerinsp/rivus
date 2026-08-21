<p align="center">
  <img src="ui/rivus-logo.png" alt="Rivus logo" width="96" height="96">
</p>

<h1 align="center">Rivus</h1>

<p align="center">
  <strong>Lakehouse-first streaming engine.</strong>
</p>

<p align="center">
  Stream MySQL snapshots and binlog CDC into Apache Iceberg or Apache Doris with durable state, isolated runtime roles, and built-in lakehouse maintenance.
</p>

<p align="center">
  <a href="https://github.com/gerinsp/rivus/actions/workflows/ci.yml"><img src="https://github.com/gerinsp/rivus/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/gerinsp/rivus/actions/workflows/publish-container.yml"><img src="https://github.com/gerinsp/rivus/actions/workflows/publish-container.yml/badge.svg" alt="Publish Container"></a>
  <a href="https://github.com/gerinsp/rivus/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gerinsp/rivus" alt="License"></a>
  <a href="https://github.com/gerinsp/rivus/pkgs/container/rivus"><img src="https://img.shields.io/badge/ghcr.io-rivus-blue" alt="GHCR"></a>
</p>

Rivus is a focused CDC engine for continuously moving operational MySQL data into analytical systems. It is designed around the operational needs of a lakehouse rather than general-purpose stream processing: initial snapshots, resumable binlog CDC, durable job ownership, Iceberg commits, table maintenance, and a lightweight control-plane UI all live in one project.

## Project status

Rivus is an early-stage, pre-1.0 open-source project. It is suitable for controlled production use when operators understand the source, sink, checkpoint, and metadata requirements, but the public compatibility and release guarantees are still evolving.

For production adoption, read:

- [Production deployment](docs/production-deployment.md)
- [Runtime roles](docs/runtime-roles.md)
- [Compatibility](docs/compatibility.md)
- [Migration and upgrades](docs/migration.md)
- [Release policy](docs/release-policy.md)
- [Benchmarking methodology](docs/benchmarking.md)
- [Security policy](SECURITY.md)

## Why Rivus

Rivus deliberately keeps the execution model small and lakehouse-oriented:

- **CDC-first** — MySQL binlog streaming is the primary workload.
- **Snapshot aware** — initial loads run independently from long-lived CDC.
- **Lakehouse native** — Apache Iceberg is a first-class sink with REST catalog support.
- **Analytical serving** — Apache Doris is supported as a native sink.
- **Durable control plane** — job state, checkpoints, leases, and maintenance state live in metadata MySQL.
- **Operational isolation** — master, streaming, snapshot, and maintenance run as separate processes from the same image.
- **Built-in maintenance** — Iceberg compaction, snapshot expiration, and orphan cleanup are integrated instead of being treated as an unrelated external workflow.
- **Maintenance monitors** — existing Iceberg tables can be continuously inventoried and maintained without creating a snapshot or CDC pipeline.
- **Small operational surface** — one binary, one image, YAML/JSON job definitions, REST API, and web dashboard.

Rivus is not intended to replace a general-purpose stream processor for arbitrary event-time SQL, complex DAGs, or broad message-bus processing. Its scope is intentionally narrower: reliable database-to-lakehouse and database-to-analytics replication.

## Architecture

One Rivus image can run four independent runtime roles:

```text
                         ┌──────────────────────┐
                         │     rivus-master     │
                         │ API / UI / lifecycle │
                         └──────────┬───────────┘
                                    │
                         durable metadata MySQL
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
      rivus-streaming        rivus-snapshot       rivus-maintenance
         binlog CDC          initial snapshot     compact / expire / orphan
              │                     │                     │
              └──────────────┬──────┴─────────────────────┘
                             │
                    Iceberg / Doris
```

The master is a control plane. It does not own CDC or snapshot pipelines. Streaming and snapshot workers claim durable jobs through leases, while the maintenance worker consumes an independent durable maintenance queue.

See [docs/runtime-roles.md](docs/runtime-roles.md) for lifecycle and handoff details.

## Features

- Chunked initial snapshots from MySQL.
- `snapshot-only` mode for one-time loads without CDC.
- MySQL binlog CDC using `go-mysql`.
- Durable offsets, job registry, execution roles, and worker leases in MySQL metadata storage.
- Snapshot-to-streaming handoff from a captured binlog checkpoint.
- Graceful pause, resume, cancel, restart, and checkpoint drain behavior.
- Doris sink with table creation, DDL forwarding, batching, retries, and stream-load support.
- Native Iceberg REST catalog sink for object-storage-backed tables.
- Hybrid Iceberg maintenance with durable scheduling, native `iceberg-go` operations, and Spark fallback for heavy compaction.
- Master API/UI with durable worker progress and maintenance state.
- Central log viewer over role-separated rotating files.
- Optional UI login and API-token protection.
- YAML/JSON job configs with `${ENV_VAR}` placeholder expansion.
- Multi-architecture container images for `linux/amd64` and `linux/arm64`.

## Quick start

Prerequisites:

- Docker with Compose support.
- A MySQL source with binlog access for CDC jobs.
- Sink infrastructure for the job you want to run: Apache Doris or an Iceberg REST catalog plus object storage.
- Go 1.25 or newer only when building from source.

Clone the repository, create an environment file, and start the reference stack:

```sh
git clone https://github.com/gerinsp/rivus.git
cd rivus
cp .env.example .env
docker compose up -d
```

The repository Compose file starts metadata MySQL plus the four Rivus runtime roles. Open the dashboard at `http://localhost:8080` unless `RIVUS_HTTP_PORT` was changed.

Before using the reference Compose definition outside local testing, change the example credentials and read [docs/production-deployment.md](docs/production-deployment.md).

## Runtime commands

The same binary is used for every role:

```sh
/app/rivus master
/app/rivus streaming-worker
/app/rivus snapshot-worker
/app/rivus maintenance-worker --queue
```

Job YAML does not need a worker field. Rivus assigns snapshot-capable modes to the snapshot worker and normal CDC/resume execution to the streaming worker through durable metadata.

## Job configuration

Generic examples are available in:

- [`examples/mysql-to-doris.yaml`](examples/mysql-to-doris.yaml)
- [`examples/mysql-to-iceberg.yaml`](examples/mysql-to-iceberg.yaml)

Jobs can be submitted through the dashboard or API:

```sh
curl -X POST \
  -H 'Content-Type: application/x-yaml' \
  --data-binary @examples/mysql-to-iceberg.yaml \
  http://localhost:8080/api/jobs
```

When `RIVUS_API_TOKEN` is configured, pass either:

```text
X-Rivus-Token: <token>
Authorization: Bearer <token>
```

Do not commit source credentials, API tokens, object-storage keys, or metadata database passwords into job files.

## Iceberg maintenance

Rivus can maintain Iceberg tables through the same durable platform used by CDC.

Supported maintenance operations include:

- data-file rewrite / compaction;
- snapshot expiration;
- orphan-file cleanup.

The maintenance worker can use native `iceberg-go` operations and optionally route heavy compaction to an external Spark runner when `executor: hybrid` or `executor: spark` is configured.

CDC is not paused for routine maintenance. Maintenance uses its own durable queue and retry model.

See [docs/iceberg-maintenance.md](docs/iceberg-maintenance.md) and [docs/maintenance-concurrency.md](docs/maintenance-concurrency.md).

## Logs and observability

The recommended split-runtime layout uses one shared log root with one subdirectory per role:

```text
/app/logs/
├── master/
│   └── rivus-*.log
├── streaming/
│   └── rivus-streaming-*.log
├── snapshot/
│   └── rivus-snapshot-*.log
└── maintenance/
    └── rivus-maintenance-*.log
```

The master can read all role directories for the dashboard, while job log views prefer the streaming log for CDC jobs. High-volume application logging is file-based and rotated by Rivus; `RIVUS_LOG_STDERR=false` is the recommended default so CDC output is not duplicated into Docker's log driver.

## Production notes

For production deployments:

- pin a release or immutable `sha-*` image instead of `latest`;
- keep metadata MySQL durable and backed up;
- stop Rivus workers gracefully so active jobs can drain checkpoints;
- keep Docker `stop_grace_period` longer than `RIVUS_SHUTDOWN_TIMEOUT_SECONDS`;
- enable UI/API authentication before exposing the master outside a trusted network;
- persist the snapshot spool when interrupted snapshots must resume across container replacement;
- verify source binlog retention is long enough for planned outages and snapshot duration;
- monitor worker leases, job progress, maintenance queues, storage growth, and checkpoint freshness.

The complete checklist is in [docs/production-deployment.md](docs/production-deployment.md).

## Releases and images

Images are published to GitHub Container Registry.

The publish workflow produces:

- `latest` from the default branch;
- branch/tag-derived image tags;
- immutable `sha-<commit>` tags.

For production, prefer a version tag or immutable SHA tag. `latest` follows `main` and should be treated as a moving development target.

See [docs/release-policy.md](docs/release-policy.md).

## Compatibility and benchmarking

Rivus currently documents implemented integrations separately from formally certified version ranges. Do not infer a compatibility guarantee from a dependency compiling successfully.

See [docs/compatibility.md](docs/compatibility.md) for the current support matrix.

Public benchmark numbers are not published yet. The project intentionally avoids unverified throughput claims; [docs/benchmarking.md](docs/benchmarking.md) defines a reproducible methodology for future results.

## Development

Run the test suite:

```sh
go test ./...
```

The CI workflow also validates the browser ES modules with Node before running the Go tests.

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

Do not open a public issue containing credentials, tokens, private hostnames, or vulnerability details. Follow [SECURITY.md](SECURITY.md) for reporting guidance and the current supported-version policy.

## License

Rivus is licensed under the [Apache License 2.0](LICENSE).
