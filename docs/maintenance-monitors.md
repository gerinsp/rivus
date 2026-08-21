# Iceberg maintenance monitors

A maintenance monitor is a durable, long-running registration for existing
Iceberg tables. It does not create a source connector, run an initial snapshot,
or attach to a CDC stream. The shared `maintenance-worker --queue` process
inventories registered tables and schedules eligible compaction, snapshot
expiration, and orphan cleanup.

## Lifecycle

- `ACTIVE` monitors are inventoried and scheduled continuously.
- `PAUSED` monitors do not create or claim new work. An operation that already
  owns a lease is allowed to finish.
- Resuming a monitor schedules an immediate inventory refresh and brings its
  maintenance checks forward.
- Deleting a monitor disables future schedules and cancels queued/retry work.
  Iceberg data and historical maintenance runs are preserved.

The maintenance worker discovers monitor changes on its normal poll interval
(30 seconds by default). A monitor is a logical long-running owner; Rivus does
not create one goroutine or Spark application per monitor.

## Configuration

Submit the YAML through **Maintenance Monitors → New monitor** or
`POST /api/iceberg/maintenance/monitors` with content type
`application/x-yaml`.

```yaml
id: barayax-maintenance
name: Barayax Iceberg Maintenance
mode: maintenance-only
sink:
  type: iceberg_native
  config:
    rest_uri: "${ICEBERG_REST_URI}"
    warehouse: "${ICEBERG_WAREHOUSE}"
    table_maintenance:
      enabled: true
      executor: hybrid
      catalog_name: asmat
      runner_uri: "${RIVUS_RUNNER_URI}"
      runner_api_token: "${RUNNER_API_TOKEN}"
      runner_resource_profile: small
      namespace:
        - barayax_bronze
      tables:
        - tbl_absen
        - tbl_employee
        - attendance_daily
```

The compact form accepts exactly one `namespace` (either a string or a
one-item list) and several table names. The existing explicit form remains
available when a monitor spans namespaces:

```yaml
tables:
  - namespace: barayax_bronze
    table: tbl_absen
  - namespace: barayax_silver
    table: attendance_daily
```

Wildcards are rejected so a monitor cannot accidentally enroll an entire
warehouse.

## API

```text
GET    /api/iceberg/maintenance/monitors
POST   /api/iceberg/maintenance/monitors
GET    /api/iceberg/maintenance/monitors/{id}
POST   /api/iceberg/maintenance/monitors/{id}/pause
POST   /api/iceberg/maintenance/monitors/{id}/resume
POST   /api/iceberg/maintenance/monitors/{id}/run
DELETE /api/iceberg/maintenance/monitors/{id}
```

API responses expose table names and safe backend labels but never return the
stored sink configuration or credentials.
