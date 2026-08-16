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
          | native: expire snapshots, bounded orphan cleanup, tiny compaction
          ` Spark: heavy compaction through the existing runner/Spark backend
```

Run the server and worker as separate containers/processes using the same Rivus image:

```sh
/app/rivus -addr :8080 -ui-dir ./ui
/app/rivus maintenance-worker --queue
```

Both processes use the same `RIVUS_META_MYSQL_DSN`. The worker also needs the Iceberg REST/object-storage credentials from the persisted jobs. Spark fallback needs the existing runner-app or Spark configuration.

## Configuration semantics

`enabled` is the public master switch. `native_enabled` currently selects the durable worker/hybrid path and is retained for compatibility with the first worker rollout.

```yaml
table_maintenance:
  enabled: true
  native_enabled: true
```

The combinations mean:

| enabled | native_enabled | Result |
| --- | --- | --- |
| false | false | Automatic maintenance off |
| false | true | Automatic maintenance off; the master switch wins |
| true | true | Durable maintenance worker enabled |
| true | false | No worker scheduling; legacy CDC-side automatic scheduling is retired |

The CDC-side `tableMaintenanceMonitor` no longer schedules Spark maintenance. Its runtime compatibility layer only projects durable worker state into the existing job-details status shape so the UI can keep its current inventory panel while using MySQL as the source of truth.

Recommended configuration:

```yaml
sink:
  type: iceberg_native
  config:
    # normal Iceberg REST/S3 settings...
    table_maintenance:
      enabled: true
      native_enabled: true

      # CDC signals compact work after a short quiet period; inactive tables get
      # only a weekly safety check.
      native_signal_delay_seconds: 300
      native_idle_check_interval_seconds: 604800

      # existing Spark/runner integration remains the heavy fallback
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

## Native versus Spark routing

Rivus runs the `iceberg-go` compaction planner before choosing an executor. The routing decision uses selected rewrite groups rather than total table size.

Native compaction is allowed when selected work stays within the configured file/byte limits. It uses atomic `RewriteDataFiles`, applies equality deletes while reading, and uses Iceberg dead-equality-delete cleanup before removing equality-delete files.

Compaction routes to Spark when, by default, any of these apply:

- selected input is greater than 512 MiB;
- more than 100 selected input files are involved;
- position-delete files are present;
- multiple substantial compaction groups make process isolation preferable.

Sort/Z-order and other Spark-specific rewrite strategies remain Spark-only. Every result records `engine` and `routing_reason`.

## CDC priority and snapshot barrier

CDC never pauses for maintenance. Successful Rivus commits send only snapshot/file-count signals to the durable store. Reaching data-file or equality-delete thresholds brings a table check forward.

Initial snapshots remain protected by a snapshot-complete barrier. Native compaction verifies its starting snapshot before staging work. Iceberg commit conflicts are retryable maintenance failures, so CDC wins concurrent writes.

## Snapshot expiration

Snapshot expiration uses the public Iceberg Go transaction API. Rivus stages expiration first, calculates eligible snapshots, records a no-op as `skipped`, and commits only when expiration is needed. Iceberg branch/tag references remain protected by Iceberg semantics.

## Bounded-memory orphan cleanup

The worker buckets referenced and candidate object paths to disk so full table listings do not need to reside in RAM at once. Prefix mismatch is fail-closed. The orphan minimum age is seven days, and dry-run mode identifies candidates without deleting them.

## Rollout

Recommended rollout:

1. Set `enabled: true` and `native_enabled: true` for a small workload.
2. Start one maintenance worker and use orphan dry-run first.
3. Observe queue age, retries/commit conflicts, native execution time, Spark routing, and object-store request volume.
4. Expand gradually to hundreds and then thousands of tables.
5. Scale workers only after the single-worker behavior is stable; MySQL leases support multiple workers safely.

## Current limitations

- Native sort/Z-order is not implemented; use Spark.
- Compaction groups containing position deletes route to Spark.
- Heavy Spark fallback keeps the existing per-table runner/Spark submission behavior.
- Native rewrite output is committed atomically, but exact per-attempt uncommitted output paths are not exposed by the current high-level `iceberg-go` rewrite API; the seven-day orphan-cleanup safety window protects cleanup.
