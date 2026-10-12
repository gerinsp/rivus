# Recovering legacy snapshot completion after an upgrade

## Why Doris and Iceberg jobs fail with the same error

The MySQL source checks snapshot completion before resuming either sink. Upgrading to the reliability-hardening version adds `job_snapshots.completion_version` with default `0`. Old `done=1` rows therefore lack the version `1` receipt that new sinks persist after all required snapshot writes are committed.

The old source saved `done=1` and cleared snapshot progress **before** waiting for the final sink ACK. It also allowed a later restart to enter CDC using that flag. Consequently, `done=1`, absent snapshot progress, or a later CDC checkpoint alone cannot prove that the last snapshot batch committed. Automatic promotion of all old rows would reintroduce the original data-loss window.

This is a metadata upgrade block, not evidence of a new Doris load or Iceberg catalog failure. It can affect healthy existing jobs as well as genuinely incomplete snapshots. Resubmitting unchanged state repeats the block. New snapshots completed entirely by the new workers receive version `1` from the sink.

## Inspect the affected job

Use the metadata database for this job. These queries are read-only and omit job configurations and credentials:

```sql
SET @job_id = '<affected job ID>';

SELECT job_id, meta_key, submission_id, execution_role, desired_state,
       last_status, lease_owner, lease_until
FROM job_registry
WHERE job_id = @job_id;
```

Copy the exact `meta_key` returned above, or the `state_key` in the updated error. Internal state keys usually differ from the public job ID. If the registry key is missing or the stores are separate, inspect the job detail's checkpoint metadata key before continuing; do not guess it or edit every snapshot row.

```sql
SET @state_key = '<exact metadata key for this job>';

SELECT job_id, start_file, start_pos, done, completion_version, updated_at
FROM job_snapshots WHERE job_id = @state_key;

SELECT job_id, binlog_file, binlog_pos, updated_at
FROM job_offsets WHERE job_id = @state_key;

SELECT job_id, table_name, next_offset, updated_at
FROM job_snapshot_progress WHERE job_id = @state_key;

SELECT operation_id, state_key, kind, status, source_start_file,
       source_start_pos, source_end_file, source_end_pos, created_at
FROM iceberg_commit_intents WHERE state_key = @state_key;
```

The expected upgrade case is `done=1, completion_version=0`. If a supposedly fresh snapshot has this state, verify the versions of **all** workers: an old source can still write the legacy flag. Stop mixed-version execution before investigating further. Version `1` with the same error requires checking that the inspected database/key matches the worker's actual configuration.

## Choose recovery using evidence

1. Stop the affected job and wait for its pipeline to exit. Stop old workers; do not run mixed versions. Back up its metadata before making a change.
2. Verify that the original snapshot's required writes actually committed for every configured table. Use completion/ACK and sink commit evidence from the same snapshot attempt, together with target data validation at a consistent source/target point. Table existence, row counts alone, progress phase, or CDC activity alone are insufficient.
3. For Iceberg, reconcile any retained intent against catalog evidence first. Do not delete PREPARED or uncovered COMMITTED receipts to bypass the block. The conservative manual migration below requires no remaining receipts.
4. If completion is established, perform a one-time, per-job operator attestation below and resubmit using the saved checkpoint. This does not run the snapshot again or advance its checkpoint.
5. If completion cannot be established, keep the job blocked. Plan a fresh snapshot with appropriate target cleanup/replacement semantics in an isolated or reviewed workflow. Do not reset `done=0` and resume from the old progress blindly: progress may already have been cleared and append replay can duplicate data.

## One-time operator attestation

This changes metadata and **must only be used after the verification above**. It records the operator's verified completion; the SQL checks prevent obvious state/ownership mistakes but do not prove target completeness. Do not bulk-update all legacy rows.

The example requires the registry and checkpoint tables in the same database, with InnoDB transactions, and no active pipeline or administrative resubmit during maintenance. For separate stores, a cross-database ownership transaction is not established by this example; keep workers stopped and prepare a deployment-specific procedure instead.

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
START TRANSACTION;

SELECT job_id, meta_key, desired_state, last_status, lease_owner, lease_until
FROM job_registry WHERE job_id = @job_id FOR UPDATE;

SELECT job_id, done, completion_version, start_file, start_pos
FROM job_snapshots WHERE job_id = @state_key FOR UPDATE;

UPDATE job_snapshots AS s
JOIN job_registry AS j ON j.job_id = @job_id AND j.meta_key = s.job_id
SET s.completion_version = 1
WHERE s.job_id = @state_key AND s.done = 1 AND s.completion_version = 0
  AND j.desired_state = 'STOPPED'
  AND j.last_status IN ('FAILED', 'STOPPED')
  AND (j.lease_owner IS NULL OR j.lease_owner = ''
       OR j.lease_until < UTC_TIMESTAMP(6))
  AND NOT EXISTS (
    SELECT 1 FROM iceberg_commit_intents AS i
    WHERE i.state_key = s.job_id OR i.job_id = @job_id
  );

SELECT ROW_COUNT() AS migrated_rows;
```

Require exactly **one** migrated row, then run `COMMIT`. If the result is zero or any error occurs, run `ROLLBACK` and investigate the unmet conditions; do not relax the predicates. Keep a record of the job, state key, original row, verification evidence and migration time. The original offset and snapshot start must remain unchanged.

Resubmit that job once through the existing UI/API. The source repeats the completion handshake, the sink accepts the confirmed receipt idempotently, and normal resume selects the existing saved checkpoint. Snapshot-only jobs finish without starting CDC. If the saved binlog is unavailable, existing admission/recovery behavior still applies; this migration does not restore purged source history.

## Verification boundaries

Local regression tests cover legacy version `0` with cleared progress and an advanced CDC offset in both resume modes: it remains blocked without emitting an ACK barrier or mutating durable state. Existing confirmed-completion tests cover repeated handshake, lost ACK, and snapshot-only restart. Production metadata and targets have not been inspected or modified by this investigation. Operator attestation still requires evidence specific to each affected job.
