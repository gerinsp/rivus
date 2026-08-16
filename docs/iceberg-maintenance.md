# Hybrid Iceberg maintenance

Rivus runs automatic Iceberg maintenance in a separate durable worker. The CDC/API server never owns the maintenance schedule.

## Process model

```text
MySQL CDC -> rivus server -> Iceberg
                 |
                 | lightweight commit signals
                 v
          metadata MySQL
          | state / tasks / runs / results
          ^
          |
       rivus maintenance-worker --queue
          | native: compaction / expiration / orphan cleanup
          ` Spark: compaction through the existing runner/Spark backend
```

Run the server and worker as separate containers/processes using the same Rivus image:

```sh
/app/rivus -addr :8080 -ui-dir ./ui
/app/rivus maintenance-worker --queue
```

Both processes use the same `RIVUS_META_MYSQL_DSN`. The worker also needs the Iceberg REST/object-storage credentials from the persisted jobs. `hybrid` and `spark` executor modes need the existing runner-app or Spark configuration whenever compaction is sent to Spark.

## Configuration semantics

Automatic maintenance has one master switch and one executor selector:

```yaml
table_maintenance:
  enabled: true
  executor: hybrid
```

`enabled` controls whether automatic maintenance exists at all:

- `enabled: false` — no automatic maintenance tasks are scheduled.
- `enabled: true` — the durable maintenance worker owns scheduling and execution.

`executor` controls **compaction** execution:

- `hybrid` — default. The worker analyzes the selected rewrite workload and chooses native or Spark.
- `native` — compaction stays in `iceberg-go`; there is no Spark fallback.
- `spark` — every automatic compaction task is submitted to Spark.

Snapshot expiration and orphan cleanup remain native worker operations in all three executor modes. `executor` only selects the compaction engine.

The historical `native_enabled` rollout flag is retired. Rivus removes it while decoding persisted/job configuration and does not use it to select a runtime path.

The CDC-side `tableMaintenanceMonitor` no longer schedules maintenance. Its compatibility layer only projects durable MySQL worker state into the existing job-details status shape so the UI can keep its inventory panel while MySQL remains the source of truth.

Recommended configuration:

```yaml
sink:
  type: iceberg_native
  config:
    # normal Iceberg REST/S3 settings...
    table_maintenance:
      enabled: true
      executor: hybrid

      # CDC signals compact work after a short quiet period; inactive tables get
      # only a weekly safety check.
      native_signal_delay_seconds: 300
      native_idle_check_interval_seconds: 604800

      # runner/Spark integration used by hybrid fallback or executor: spark
      runner_uri: http://runner-app:8001
      runner_api_token: ${RUNNER_API_TOKEN}
      runner_resource_profile: small
      catalog_name: rivus

      # hybrid native-compaction boundary
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

If `executor` is omitted while maintenance is enabled, Rivus defaults it to `hybrid` for backward compatibility.

## Durable metadata schema

The worker owns these MySQL tables:

- `iceberg_maintenance_state`: one row per canonical `catalog.namespace.table`.
- `iceberg_maintenance_tasks`: durable, idempotent tasks and leases.
- `iceberg_maintenance_runs`: parent runs grouping claimed tasks.
- `iceberg_maintenance_results`: per-table operation results and routing metadata.

The queue uses MySQL 8 `FOR UPDATE SKIP LOCKED`, lease expiry/recovery, deterministic jitter, and bounded pages. Iceberg table metadata is loaded only after a task is claimed.

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

## UI and API

The job-details UI keeps the existing Iceberg file-inventory panel, but its automatic-maintenance state comes from the durable maintenance tables. It no longer depends on a CDC-process scheduler.

The **View maintenance runs** action opens:

```text
/ui/admin/iceberg-maintenance/runs/
```

That page shows global queue health plus recent worker runs. Run details include operation, execution engine (`native` or `spark`), routing reason, attempts, input/output file counts and bytes, duration, and errors.

Read-only API endpoints:

```text
GET /api/iceberg/maintenance/summary
GET /api/iceberg/maintenance/runs?limit=50&offset=0
GET /api/iceberg/maintenance/runs/{id}?limit=100
GET /api/iceberg/maintenance/tables/{catalog.namespace.table}
```

The existing per-job manual maintenance endpoint remains available. Manual requests are separate from automatic worker scheduling.

## Compaction executor behavior

### `executor: hybrid`

Rivus runs the `iceberg-go` compaction planner and bases routing on selected rewrite groups rather than total table size. By default, compaction routes to Spark when any of these apply:

- selected input is greater than 512 MiB;
- more than 100 selected input files are involved;
- position-delete files are present;
- multiple substantial compaction groups make process isolation preferable.

Otherwise the rewrite runs natively. Every result records the actual `engine` and `routing_reason`.

### `executor: native`

The same planner determines the rewrite groups, but Spark routing is disabled. Even when the hybrid policy would have preferred Spark, the task stays in the native `iceberg-go` rewrite path. A native failure is retried/failed according to the normal worker retry policy; it is never silently handed to Spark.

### `executor: spark`

A compaction task is sent directly to the configured Spark/runner backend rather than using native analysis to choose an engine. Snapshot expiration and orphan cleanup are still performed natively by the maintenance worker.

Native compaction uses atomic `RewriteDataFiles`, applies equality deletes while reading, and uses Iceberg dead-equality-delete cleanup before removing equality-delete files. Sort/Z-order and other Spark-specific rewrite strategies remain Spark-only.

## CDC priority and snapshot barrier

CDC never pauses for maintenance. Successful Rivus commits send only snapshot/file-count signals to the durable store. Reaching data-file or equality-delete thresholds brings a table check forward.

Initial snapshots remain protected by a snapshot-complete barrier. Native compaction verifies its starting snapshot before staging work. Iceberg commit conflicts are retryable maintenance failures, so CDC wins concurrent writes.

## Snapshot expiration

Snapshot expiration uses the public Iceberg Go transaction API. Rivus stages expiration first, calculates eligible snapshots, records a no-op as `skipped`, and commits only when expiration is needed. Iceberg branch/tag references remain protected by Iceberg semantics.

## Bounded-memory orphan cleanup

The worker buckets referenced and candidate object paths to disk so full table listings do not need to reside in RAM at once. Prefix mismatch is fail-closed. The orphan minimum age is seven days, and dry-run mode identifies candidates without deleting them.

## Rollout

Recommended rollout:

1. Start with `enabled: true` and `executor: hybrid` on a small workload.
2. Start one maintenance worker and use orphan dry-run first.
3. Observe queue age, retries/commit conflicts, native execution time, Spark routing, and object-store request volume.
4. Use `executor: native` only when you explicitly want no Spark compaction fallback, or `executor: spark` when all compaction should be isolated in Spark.
5. Expand gradually to hundreds and then thousands of tables.
6. Scale workers only after the single-worker behavior is stable; MySQL leases support multiple workers safely.

## Current limitations

- Native sort/Z-order is not implemented; use Spark for those strategies.
- Hybrid mode routes position-delete compaction to Spark. Native mode explicitly overrides that routing guard.
- Heavy Spark execution keeps the existing per-table runner/Spark submission behavior.
- Native rewrite output is committed atomically, but exact per-attempt uncommitted output paths are not exposed by the current high-level `iceberg-go` rewrite API; the seven-day orphan-cleanup safety window protects cleanup.
