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

All roles use the same Rivus image and the same durable metadata database. Snapshot and streaming workers use job leases so only one worker owns a job at a time. Maintenance keeps its independent table-level queue and leases.

## Commands

```yaml
services:
  rivus-master:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "master"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}

  rivus-streaming:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "streaming-worker"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: rivus-streaming-1

  rivus-snapshot:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "snapshot-worker"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: rivus-snapshot-1
    volumes:
      - ./snapshot-spool:/var/lib/rivus/snapshot-spool

  rivus-maintenance:
    image: ghcr.io/gerinsp/rivus:latest
    command: ["/app/rivus", "maintenance-worker", "--queue"]
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: rivus-maintenance-1
```

The example intentionally shows only role-specific settings. Keep the existing source database, Iceberg/object-storage, runner, Trino, Telegram, auth, and other environment variables on the roles that need them.

## Job routing

No worker field is added to job YAML.

- `initial`, `snapshot-only`, and internal snapshot handoff modes are assigned to the snapshot worker.
- Normal CDC/resume execution is assigned to the streaming worker.
- After a successful `initial` snapshot, Rivus changes the durable execution role to `STREAMING`; the streaming worker then resumes from the snapshot checkpoint.
- Table maintenance remains independent from job execution and is consumed by `maintenance-worker --queue`.

## Lifecycle control

The master does not own source or sink goroutines. Pause, cancel, and resubmit therefore use the durable job registry:

- **Pause** writes a `PAUSING` request while retaining the worker lease. The owning worker stops only the source and lets the sink drain and commit its checkpoint before becoming `PAUSED`.
- **Cancel** sets the durable desired state to `STOPPED` and clears the lease. Guarded worker writes can no longer overwrite the stop request, and the worker stops its local pipeline when ownership is lost.
- **Resubmit** preserves the durable execution role, clears any stale lease, and makes the job claimable from its existing checkpoint.
- **Delete** removes the durable registry record; late claimed-worker writes are already fenced by the existing guarded save path.

## Environment placement

A useful deployment split is:

- **master**: API/UI auth, metadata DB, control-plane notification settings.
- **streaming worker**: metadata DB, source connectivity, Iceberg/storage credentials, streaming/commit limits.
- **snapshot worker**: metadata DB, source connectivity, Iceberg/storage credentials, snapshot concurrency, snapshot spool volume.
- **maintenance worker**: metadata DB, Iceberg/storage credentials, Trino/runner settings when used, maintenance concurrency and timeout settings.

Give every execution worker a unique `RIVUS_WORKER_ID`, especially when workers run on different servers.

## Migration from the all-in-one server

1. Publish/deploy a Rivus image containing the separated runtime commands.
2. Stop the old all-in-one executor cleanly so its active jobs drain to their latest checkpoint.
3. Start `rivus-master`.
4. Start `rivus-streaming-worker` and `rivus-snapshot-worker` against the same metadata DB.
5. Start or keep `rivus-maintenance-worker --queue`.
6. Verify `job_registry.execution_role`, `lease_owner`, job status, and snapshot-to-streaming handoff before removing the old deployment definition.

The legacy default server mode and `RIVUS_WORKER_ROLE=all/snapshot/streaming` remain available for backward compatibility during migration.
