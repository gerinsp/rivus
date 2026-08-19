# Rivus runtime roles

Rivus can run one container image as four independent runtime roles. Job YAML stays unchanged; the durable metadata database decides which execution worker owns each job.

## Topology

```text
                         ┌──────────────────────┐
                         │     rivus-master     │
                         │ API / UI / lifecycle │
                         └──────────┬───────────┘
                                    │
                         RIVUS_META_MYSQL_DSN
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
   rivus-streaming-worker  rivus-snapshot-worker  rivus-maintenance-worker
        CDC / resume          initial snapshot      compact / expire / orphan
```

All roles use the same Rivus image and the same durable metadata database. Snapshot and streaming workers use job leases so only one worker owns a job at a time. Automatic table maintenance keeps its independent durable queue and table leases.

The master never claims snapshot or streaming jobs. The legacy explicit `POST /api/jobs/{id}/iceberg/orphans` administration endpoint remains synchronous for API compatibility; normal scheduled compaction, snapshot expiration, and orphan cleanup run only in `maintenance-worker --queue`.

## Commands

```yaml
services:
  rivus-master:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "master"]
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90

  rivus-streaming:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "streaming-worker"]
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_STREAMING_WORKER_ID:-}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90

  rivus-snapshot:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "snapshot-worker"]
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_SNAPSHOT_WORKER_ID:-}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90
    volumes:
      - ./snapshot-spool:/var/lib/rivus/snapshot-spool

  rivus-maintenance:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "maintenance-worker", "--queue"]
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_MAINTENANCE_WORKER_ID:-}
```

The example intentionally shows only role-specific settings. Keep the existing source database, Iceberg/object-storage, runner, Trino, Telegram, auth, and other environment variables on the roles that need them.

When `RIVUS_WORKER_ID` is empty, Rivus generates a unique `hostname-pid` identity. This is the safe default for copied/scaled Compose deployments. Set an explicit worker ID only when you can guarantee that no two live worker processes share it.

## Job routing

No worker field is added to job YAML.

- `initial`, `snapshot-only`, and internal snapshot handoff modes are assigned to the snapshot worker.
- Normal CDC/resume execution is assigned to the streaming worker.
- After a successful `initial` snapshot, Rivus changes the durable execution role to `STREAMING`; the streaming worker then resumes from the snapshot checkpoint.
- Automatic table maintenance remains independent from job execution and is consumed by `maintenance-worker --queue`.

## Lifecycle control

The master does not own source or sink goroutines. Pause, cancel, and resubmit therefore use the durable job registry:

- **Pause** writes a `PAUSING` request while retaining the worker lease. The owning worker stops only the source and lets the sink drain and commit its checkpoint before becoming `PAUSED`.
- **Cancel** sets the durable desired state to `STOPPED` and clears the lease. Guarded worker writes can no longer overwrite the stop request, and the worker stops its local pipeline when ownership is lost.
- **Resubmit** preserves the durable execution role, clears a terminal job's stale lease, and makes the job claimable from its existing checkpoint. A conditional update prevents resubmit from fencing a job that has already started again.
- **Delete** removes the durable registry record; late claimed-worker writes are already fenced by the existing guarded save path.

Workers poll durable pause requests by lease owner instead of performing one metadata query per running job. A transient control-observer database error is logged and retried; it does not terminate the data worker.

## Graceful restart and auto-deploy

All four roles are safe to stop with Docker's normal `SIGTERM` flow. This matters for image auto-deployers that stop the old container before starting the replacement.

- **master** stops accepting work through the process shutdown path and exits without owning data pipelines.
- **streaming worker** stops claiming jobs, extends its current durable job leases beyond the drain window, stops the source, lets the sink drain/commit the latest checkpoint, and exits. The replacement worker resumes from that durable checkpoint.
- **snapshot worker** uses the same lease fencing and drain path. An interrupted snapshot remains resumable from its durable snapshot checkpoint instead of being started concurrently by the replacement worker.
- **maintenance worker** cancels in-flight native maintenance on `SIGTERM`, then finalizes the durable result through a fresh short-lived context and moves interrupted work back to `retry`. It does not leave the task stuck in `leased` until the normal lease timeout.

Set Docker `stop_grace_period` longer than `RIVUS_SHUTDOWN_TIMEOUT_SECONDS`. The repository Compose defaults are `90s` Rivus shutdown timeout and `2m` Docker grace, leaving a 30-second process-exit buffer before Docker may send `SIGKILL`.

A rolling updater should still avoid intentionally starting two containers with the same explicit `RIVUS_WORKER_ID`. If no explicit ID is configured, each process uses its generated `hostname-pid` identity.

## Environment placement

A useful deployment split is:

- **master**: metadata DB, API/UI auth, and control-plane settings.
- **streaming worker**: metadata DB, source connectivity, Iceberg/storage credentials, streaming/commit limits, and streaming job health/failure notification settings when enabled.
- **snapshot worker**: metadata DB, source connectivity, Iceberg/storage credentials, snapshot concurrency, snapshot spool volume, and snapshot job failure notification settings when enabled.
- **maintenance worker**: metadata DB, Iceberg/storage credentials, Trino/runner settings when used, maintenance concurrency/timeouts, and maintenance queue notification settings.

Give every explicitly named execution worker a unique `RIVUS_WORKER_ID`, especially when workers run on different servers. The repository Compose example leaves worker service names scalable rather than fixing `container_name` for those roles.

## Migration from the all-in-one server

1. Publish/deploy a Rivus image containing the separated runtime commands.
2. Stop the old all-in-one executor cleanly so its active jobs drain to their latest checkpoint.
3. Start `rivus-master`.
4. Start `rivus-streaming-worker` and `rivus-snapshot-worker` against the same metadata DB.
5. Start or keep `rivus-maintenance-worker --queue`.
6. Verify `job_registry.execution_role`, `lease_owner`, job status, and snapshot-to-streaming handoff before removing the old deployment definition.

The legacy default server mode and `RIVUS_WORKER_ROLE=all/snapshot/streaming` remain available for backward compatibility during migration.
