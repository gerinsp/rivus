# Worker ownership loss

## What the error means

`worker ownership lost` means the Iceberg worker cannot prove that it still owns this job's current submission and an unexpired lease. It stops before continuing processing or writing checkpoint/commit state. This is separate from snapshot `completion_version` and must not be repaired by editing that flag.

The ownership check validates job ID, submission ID, worker owner, lease expiration using the metadata server's UTC clock, and allowed job lifecycle state. A genuine takeover, cancellation, expired lease, or replacement submission must reject the old worker. Do not disable the fence or manually assign leases to force it to continue.

## Claim-poll contention fixed in the follow-up patch

Previously, every `ClaimJobs` poll ran this legacy backfill before selecting candidates:

```sql
UPDATE job_registry
SET submission_id = LOWER(REPLACE(UUID(), '-', ''))
WHERE submission_id = '';
```

`submission_id` is not indexed. Under the existing InnoDB isolation behavior, this update can scan and lock unrelated running rows even when their submission IDs are already populated. Repeating it during normal polling conflicts with fenced checkpoint transactions, registry persistence, and lease renewal.

Operational investigation found grouped ownership failures, progress-persistence deadline errors, and normalized MySQL statistics showing waits of roughly 50 seconds for this query, exceeding the default 30-second job lease. These observations support registry contention as a cause; historical aggregate statistics do not establish the exact wait chain for every failed job.

The minimal change removes the global backfill from the polling path. Each already-locked claim candidate gets an empty submission ID initialized atomically with its lease update. Existing nonempty submission IDs are preserved. Startup `Init` still performs its existing migration; no metadata index or ownership predicate is changed by this patch.

## Regression and verification

An isolated Colima MySQL test holds a running job's registry row lock, then attempts to claim an unrelated queued job. Before the fix, claiming times out behind the running row. After the fix, claim and renewal succeed while that lock remains held.

Two cases cover a legacy queued row inserted with an empty submission after initialization and a queued row with an existing submission token. The test checks durable token creation/preservation and leaves the running job's identity unchanged. Existing real stale-worker and lease-expiry rollback tests continue to enforce the ownership fence.

See [the isolated test runbook](../tests/recovery/README.md) for reproducible fixture setup. This follow-up patch must be built and deployed separately from the original reliability-hardening tag; the original tag's code still contains the polling backfill.

## Operator recovery

1. Inspect the affected job's latest error, lease owner/expiration, desired state, and submission. Check worker logs for renewal errors and progress-persistence timeouts around the failure.
2. Determine whether a replacement worker is already running the job. Do not resubmit or overwrite its lease while it is active.
3. Deploy the follow-up patch through the normal graceful worker upgrade procedure. Retain job registry, offsets, snapshot confirmation and commit-intent metadata.
4. For jobs still FAILED/STOPPED, resubmit through the existing UI/API after the old pipeline has exited. Normal claim and startup reconciliation handle the saved state. An unresolved Iceberg outcome may still block resume and requires evidence; do not delete its intent.
5. Observe fresh leases and checkpoint advancement after resume. A populated `completion_version=1` only confirms snapshot completion; it does not resolve lease failures.

Read-only metadata inspection:

```sql
SELECT job_id, submission_id, desired_state, last_status, lease_owner,
       lease_until, UTC_TIMESTAMP(6) AS metadata_server_utc
FROM job_registry
WHERE job_id = '<affected job ID>';
```

## Remaining limits

Removing the polling scan does not guarantee availability during metadata outages, slow startup migrations, severe lock contention, process pauses, or clock discrepancies. Lease renewal still uses the existing orchestration and connection pool. Expired or changed ownership must continue to stop writes. The patch does not cancel catalog requests already in flight and does not automatically resubmit failed jobs.
