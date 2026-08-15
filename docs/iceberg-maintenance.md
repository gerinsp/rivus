# Hybrid Iceberg maintenance

Rivus supports a durable Iceberg maintenance worker that runs separately from the CDC/API server while using the same binary and image.

## Process model

```text
MySQL CDC -> rivus server -> Iceberg
                 |
                 | persisted jobs / maintenance schedules
                 v
          metadata MySQL
                 ^
                 |
       rivus maintenance-worker --queue
          | native: expire snapshots, bounded orphan cleanup, tiny compaction
          ` Spark: heavy compaction through the existing runner/Spark backend
```

Run the two processes in separate containers. The default image command remains the HTTP/CDC server.

```sh
/app/rivus -addr :8080 -ui-dir ./ui
/app/rivus maintenance-worker --queue
```

Both processes require the same `RIVUS_META_MYSQL_DSN`. The worker also needs the Iceberg REST/object-storage credentials used by the persisted jobs. Spark fallback needs the existing runner-app or direct Spark configuration in the job.

## Durable metadata schema

The worker creates migration-safe tables with `CREATE TABLE IF NOT EXISTS`:

- `iceberg_maintenance_state`: one row per canonical `catalog.namespace.table`.
- `iceberg_maintenance_tasks`: durable, idempotent tasks and leases.
- `iceberg_maintenance_runs`: parent runs grouping a bounded page of tasks.
- `iceberg_maintenance_results`: per-table operation results and routing metadata.

The queue uses MySQL 8 `FOR UPDATE SKIP LOCKED`, lease expiry/recovery, deterministic jitter, and bounded pages. Iceberg table metadata is loaded only after a task has been claimed.

## Configuration

Native maintenance is opt-in. During the first rollout, set `native_enabled: true` and leave legacy `enabled` disabled so the CDC container does not also start the previous automatic Spark monitor. Add these keys under the existing Iceberg `table_maintenance` block:

```yaml
table_maintenance:
  native_enabled: true

  # CDC signals compact work after a short quiet period; inactive tables get
  # only a weekly safety check.
  native_signal_delay_seconds: 300
  native_idle_check_interval_seconds: 604800

  # existing Spark/runner configuration remains the heavy fallback
  runner_uri: http://runner-app:8001
  runner_api_token: ${RUNNER_API_TOKEN}
  runner_resource_profile: small
  catalog_name: rivus

  # native compaction routing
  small_file_size_bytes: 67108864
  small_files_min_count: 10
  small_files_min_total_bytes: 268435456
  native_max_selected_input_bytes: 536870912
  native_max_selected_files: 100
  native_target_file_size_bytes: 134217728
  native_scan_concurrency: 1
  native_timeout_seconds: 600

  # native snapshot expiration
  native_expire_interval_seconds: 86400
  native_snapshot_max_age_hours: 168
  native_snapshot_retain_last: 10

  # native orphan cleanup
  native_orphan_interval_seconds: 2592000
  # tables with no Rivus write in 30 days are checked only every 90 days
  native_orphan_inactive_interval_seconds: 7776000
  native_orphan_min_age_hours: 168
  native_orphan_dry_run: false
  worker_temp_directory: /tmp/rivus-maintenance

  # retry and Spark status polling
  retry_limit: 5
  retry_base_backoff_seconds: 60
  spark_poll_interval_seconds: 5
  spark_timeout_seconds: 7200
```

Defaults are deliberately conservative: native compaction is limited to 512 MiB and 100 selected files, snapshot expiration is checked daily and keeps at least ten snapshots while using a seven-day maximum age. Orphan cleanup runs every 30 days only for tables that received a Rivus write in those 30 days; inactive tables are deferred to a 90-day schedule. The worker uses one Go CPU with a 256 MiB soft memory limit unless overridden.

### Worker environment

```env
RIVUS_META_MYSQL_DSN=rivus:change-me@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
RIVUS_MAINTENANCE_GOMAXPROCS=1
RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS=30
RIVUS_MAINTENANCE_LEASE_SECONDS=900
RIVUS_MAINTENANCE_TASK_PAGE_SIZE=1
RIVUS_MAINTENANCE_DUE_PAGE_SIZE=100
GOMEMLIMIT=256MiB
```

Command flags override the corresponding worker defaults:

```sh
/app/rivus maintenance-worker \
  --queue \
  --poll-interval-seconds 30 \
  --lease-seconds 900 \
  --task-page-size 1 \
  --due-page-size 100
```

## Native versus Spark routing

Rivus runs the `iceberg-go` compaction planner first. The routing decision uses the selected rewrite groups rather than total table size.

Native compaction is allowed when the selected workload stays within the configured file and byte limits. It uses an atomic `RewriteDataFiles` transaction, applies equality deletes while reading, and uses `CollectDeadEqualityDeletes` before removing equality-delete files.

Compaction is routed to Spark when any of these first-rollout conditions apply:

- selected input is greater than 512 MiB by default;
- more than 100 input files are selected by default;
- position-delete files are present;
- multiple substantial compaction groups make process isolation preferable.

Sort/Z-order and other Spark-specific rewrite strategies remain Spark-only. The worker records `engine` and `routing_reason` for every result.

CDC has priority over maintenance. Each successful Rivus commit sends only its snapshot id and added-file counters to a bounded asynchronous queue. Reaching the configured data-file or equality-delete threshold brings the table's next check forward; inactive tables are checked only every seven days by default. Snapshot commits are counted but remain blocked until the snapshot-complete barrier. Native compaction validates that the starting snapshot has not changed before staging work, and Iceberg REST commit conflicts are treated as retryable maintenance failures. CDC is never paused for maintenance.

Heavy Spark submissions use a stable task idempotency key when runner-app is configured. A retry therefore reuses the existing runner job instead of starting a duplicate. The native ten-minute timeout applies only to native planning and execution; Spark uses its separate two-hour default timeout.

## Snapshot expiration

Snapshot expiration uses the public Iceberg Go transaction API. Rivus first stages expiration without a catalog commit and calculates the eligible snapshot count. A no-op is recorded as `skipped`. The real transaction is committed only when snapshots are eligible. Iceberg reference handling protects branches and tags.

## Bounded-memory orphan cleanup

The worker does not use the upstream all-in-memory orphan collection path. It creates 256 hash buckets below `worker_temp_directory`:

1. Stream all files referenced by current Iceberg metadata/snapshots into disk buckets.
2. Stream the table object-storage listing and place old-enough candidates into matching disk buckets.
3. Load one reference bucket at a time, compare its candidate stream, and delete only unreferenced paths.
4. Remove the temporary directory on success, failure, or cancellation.

The default prefix mismatch policy is fail-closed. A referenced or candidate URI with a different scheme/authority from the table location aborts cleanup. The minimum orphan age cannot be configured below seven days. `native_orphan_dry_run: true` performs identification without deletion.

## Read-only API

The existing per-job Iceberg maintenance API remains available. The server additionally exposes authenticated read-only endpoints backed by the durable maintenance tables:

```text
GET /api/iceberg/maintenance/summary
GET /api/iceberg/maintenance/runs?limit=50&offset=0
GET /api/iceberg/maintenance/runs/{id}?limit=100
GET /api/iceberg/maintenance/tables/{catalog.namespace.table}
```

The responses include queue/lease counts, run state, operation result metrics, execution engine, routing reason, attempts, durations and errors. DSNs, object-store credentials and tokens are never returned.

## Rollout

Recommended rollout:

1. Enable native maintenance for the Tiketux workload only. Start with one worker and orphan dry-run.
2. Observe queue age, commit conflicts, native execution time, Spark routing and object-store request volume.
3. Expand to roughly 136 tables, then 1,000 tables.
4. Move to 6,000+ tables only after confirming bounded queue pages and acceptable catalog/object-store pressure.

Keep one CDC/API container. Scale maintenance workers independently only after the single-worker rollout is stable; MySQL leases are designed to allow multiple worker instances safely.

## Initial rollout limitations

- Native sort/Z-order is not implemented; use Spark.
- Tables whose selected compaction groups contain position deletes route to Spark.
- Heavy Spark fallback keeps the existing per-table runner/Spark submission behavior; runner batching is not added in this phase.
- The scheduler periodically seeds/refreshes table state from persisted Rivus jobs, while CDC commit signals wake hot tables without scanning every inactive table hourly.
- Native rewrite output is committed atomically, but this phase does not yet expose exact per-attempt output-file cleanup from the high-level `iceberg-go` rewrite API; failed uncommitted files remain protected by the seven-day orphan-cleanup safety window.
