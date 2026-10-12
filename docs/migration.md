# Migration and upgrades

Rivus stores execution state durably, so upgrades should preserve metadata and checkpoint ownership rather than treating workers as stateless throwaway processes.

This guide covers the main upgrade paths for a pre-1.0 deployment.

## v0.5.0 reliability hardening

This update changes snapshot completion validation and split Iceberg worker requirements. Follow the general upgrade procedure below, stop all old workers before starting new ones, and retain a metadata backup. Mixed worker versions are unsafe because older code does not enforce the new completion and intent checks.

- Offset-store initialization adds `job_snapshots.completion_version` (default `0`) and creates/extends `iceberg_commit_intents` with operation, target, source-range and replay identity fields. Metadata credentials need the existing startup `CREATE`/`ALTER` privileges.
- Legacy `done=1, completion_version=0` does not prove the sink committed all required snapshot data. Such completion is blocked pending operator verification. Do not promote it to version `1` solely because `done=1` exists.
- Split `iceberg_native` workers require job registry and checkpoint stores on the same MySQL network endpoint and database, with transactional InnoDB tables. Endpoint aliases are compared conservatively; different addresses are rejected even if they resolve to the same server. Unsupported stores are rejected before connector startup.
- Preserve PREPARED and uncovered COMMITTED intents. Missing catalog evidence is not proof that a commit failed. Reset/delete is blocked while unresolved state remains; do not remove that state to force replay.
- Recovery uses the existing job `retry` configuration with bounded verification. No new production environment variable is required. Jobs already FAILED remain manual; exhausted or unproven recovery does not auto-resubmit them.

Downgrade is unsafe while new completion state or unresolved/uncovered commit intents exist: older workers may ignore those correctness barriers. Stop workers and assess source, target, checkpoint and intent consistency before choosing a rollback. A metadata backup alone does not undo Iceberg commits; restoring an earlier checkpoint can replay already committed data.

See [release notes](releases/v0.5.0-reliability-hardening.md) and the [isolated recovery test runbook](../tests/recovery/README.md).

## General upgrade rules

Before every production upgrade:

1. read the release notes and compare configuration changes;
2. pin the exact target image tag or SHA;
3. back up metadata MySQL;
4. verify current jobs have fresh checkpoints and no unexpected failures;
5. stop Rivus workers gracefully;
6. keep metadata MySQL and persistent snapshot spool data intact;
7. start the new roles against the same metadata database;
8. verify leases, progress, checkpoints, and sink freshness before completing the rollout.

Do not use `docker compose down -v` on a Compose project that owns the metadata volume unless destroying Rivus state is intentional.

## From the legacy all-in-one runtime

Older Rivus deployments may run API, CDC, and snapshot execution in one process. The current recommended deployment separates four runtime roles:

```text
master
streaming-worker
snapshot-worker
maintenance-worker --queue
```

Migration procedure:

1. deploy an image containing the split-runtime commands;
2. stop the old all-in-one process with enough grace time for checkpoint drain;
3. confirm the old process has exited before starting workers against the same metadata database;
4. start `master`;
5. start `streaming-worker`;
6. start `snapshot-worker`;
7. start or keep `maintenance-worker --queue`;
8. verify durable `execution_role`, `desired_state`, `lease_owner`, and current progress for active jobs;
9. verify an active CDC job advances after the new streaming worker claims it;
10. remove the legacy all-in-one deployment only after validation.

Do not intentionally run the legacy `all` executor beside split streaming/snapshot workers for the same metadata database.

See [runtime-roles.md](runtime-roles.md) for routing details.

## Snapshot-to-streaming handoff validation

For an `initial` job, the expected lifecycle is:

```text
snapshot worker
    ↓
initial snapshot
    ↓
persist captured checkpoint
    ↓
durable execution role → STREAMING
    ↓
streaming worker claims job
    ↓
CDC resumes from captured position
```

During an upgrade that interrupts this sequence, preserve metadata and snapshot spool state. After restart, verify the job is owned by the expected role and continues from durable progress rather than starting a second concurrent snapshot.

## Moving metadata MySQL to a separate lifecycle

A production deployment may move metadata MySQL out of the same Compose project as Rivus.

Recommended migration:

1. stop Rivus roles gracefully;
2. stop metadata MySQL only after Rivus workers have exited;
3. preserve the existing data volume or take a logical/physical backup;
4. start metadata MySQL under its new lifecycle management;
5. confirm the same schema and data are available;
6. update `RIVUS_META_MYSQL_DSN` only if host/credentials changed;
7. start Rivus roles;
8. verify existing jobs/checkpoints are visible before submitting new work.

Do not initialize an empty metadata database under the same deployment name and assume workers will reconstruct durable state automatically.

## Log layout migration

Current split-runtime logging uses one shared root with role subdirectories:

```text
/app/logs/
├── master/
├── streaming/
├── snapshot/
└── maintenance/
```

Each role should mount the same host root:

```yaml
volumes:
  - ./logs:/app/logs
```

while using a role-specific directory:

```text
master      → /app/logs/master
streaming   → /app/logs/streaming
snapshot    → /app/logs/snapshot
maintenance → /app/logs/maintenance
```

The master log API can still read legacy flat `rivus-*.log` files during migration. They can be left to age out according to retention or archived by the operator.

## Image rollback

If an upgrade fails operational validation:

1. stop the affected new workers gracefully;
2. leave metadata MySQL unchanged;
3. redeploy the previous known-good image;
4. verify old workers reclaim jobs after leases permit it;
5. verify checkpoint freshness and sink updates;
6. investigate the failed version before retrying.

A code rollback is safest when the previous version understands the current metadata schema. Because Rivus is pre-1.0, release notes must call out metadata migrations or backwards-incompatible changes when they occur.

## Configuration migration

Treat configuration as versioned infrastructure.

Before upgrading, compare:

- `.env.example`;
- `docker-compose.yml`;
- example job YAML files;
- runtime-role documentation;
- Iceberg maintenance configuration documentation.

Do not copy the new `.env.example` over a production `.env`; merge new keys intentionally while preserving secrets and environment-specific values.

## Verification queries

The exact metadata schema may evolve, but the operational questions remain the same:

- Which jobs are expected to be `RUNNING`?
- Which execution role owns each job?
- Is the worker lease fresh?
- Is checkpoint/progress time advancing?
- Are failed/paused jobs expected?
- Are maintenance tasks moving through the queue?

Use the dashboard and metadata database together when validating a high-risk rollout.

## Pre-1.0 breaking changes

Until Rivus reaches 1.0, minor releases may include breaking configuration or operational changes. The project should document them in the release notes and migration guide rather than silently changing deployment semantics.

For image/tag expectations, see [release-policy.md](release-policy.md).
