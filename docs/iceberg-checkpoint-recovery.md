# Retained Iceberg receipts and missing binlogs

## Meaning of the cleanup block

`cleanup legacy checkpoint failed: job state cleanup BLOCKED ... retained Iceberg commit intent ... requires recovery` means a checkpoint reset would discard state needed to recover an Iceberg operation. The word `legacy` describes the cleanup call, not the validity of the intent. Cleanup by public job ID also protects receipts stored under an internal `rivus/v1/...` state key.

`COMMITTED` means the sink recorded an accepted commit. It does **not** mean the durable source checkpoint covers it. Startup verifies catalog evidence and uses the receipt to skip exact committed events during replay. Reconciliation can report `RECOVERED` while receipts remain necessary. They are removed when the checkpoint covers them; reconciliation does not independently advance it.

Do not delete receipts or advance the offset to their maximum end position. Receipts cover particular batches, not every intervening source event. Snapshot `completion_version` cannot repair this CDC gap.

## Observed failure and local fix

Read-only operational investigation found:

- Saved offset: `mysql-bin.000667:529006537`.
- Six retained CDC intents with status `COMMITTED`.
- Source diagnostics exposed only `mysql-bin.000668`; the saved checkpoint was `purged`.
- Resubmit selected a fresh snapshot (`snapshot-handoff`). Reconciliation proved the writes, but source cleanup correctly refused to erase their replay receipts.

The follow-up patch checks retained intents before resubmit switches to a fresh snapshot, and again before constructing connectors for `initial`, `snapshot-only`, or `snapshot-handoff`. It checks the public job ID and internal state key, including earlier state keys sharing the public ID. Retained intents, metadata read errors, unsupported metadata stores, or ownership loss block startup.

When missing source history triggers this decision, the resubmit error starts with `saved MySQL binlog has been purged`, followed by the receipt block and recovery guidance. This keeps the triggering cause visible instead of reporting only an internal cleanup failure.

Actual cleanup retains its serializable transaction and locking intent check. Admission is advisory and does not replace that protection. Normal resume and bounded automatic reconciliation remain available when source history is present. This patch improves admission and error guidance; it cannot restore removed binlogs. It needs a new build and deployment separate from the original reliability-hardening tag.

## Operator recovery

1. Inspect checkpoint diagnostics, lease/submission, intent status and source ranges. Confirm no replacement worker is active before changing anything; keep the old job stopped when planning a rebuild.
2. If required binlog history can be restored in a supported source recovery environment, preserve original metadata and source event identity. Verify the entire required history before resuming. Startup must prove retained commits and replay from the saved checkpoint.
3. If history is unavailable, plan a controlled target rebuild into a clean dedicated target with a new recovery job identity. Preserve the old receipts and metadata. Validate snapshot completeness, primary keys, representative payloads and CDC continuity before a planned consumer cutover. Reusing populated append targets can duplicate data. Ordinary resubmit does not perform this rebuild.
4. After successful replay, verify durable checkpoint advancement and receipt cleanup. A `RECOVERED` log alone is insufficient.

Read-only checks:

```sql
SELECT job_id, meta_key, last_status, desired_state, execution_role,
       submission_id, lease_owner, lease_until
FROM job_registry WHERE job_id = '<job ID>';

SELECT job_id, binlog_file, binlog_pos, updated_at
FROM job_offsets WHERE job_id = '<internal state key>';

SELECT operation_id, state_key, status, kind, operation,
       source_start_file, source_start_pos, source_end_file, source_end_pos
FROM iceberg_commit_intents
WHERE job_id = '<job ID>' OR state_key = '<internal state key>';
```

Use job-detail checkpoint diagnostics to compare the saved file with available source history. These rows alone do not prove target completeness or justify cleanup.

## Verification and limits

Tests written before the patch failed because fresh startup constructed connectors despite blocked cleanup. They now cover three fresh modes, PREPARED/COMMITTED receipts, metadata unavailability, missing capability, ownership loss during reads, a fresh start with no receipts, and unchanged normal resume. Metadata tests verify non-mutating admission and SQL error propagation.

Four Colima integration cases use real source and metadata MySQL: ALL/master resubmit with PREPARED/COMMITTED receipts. A saved file preceding fixture history exercises the real `purged` decision without deleting binlogs. Rejected resubmit preserves registry status, role, submission, lease, checkpoint and intent. The public ID finds receipts under an internal state key.

PASS: related core/meta/MySQL/Iceberg/Doris suites, full `go test ./... -count=1`, targeted race tests, and **20 integration scenarios with no skips**, including existing worker SIGKILL, metadata-server SIGKILL, restart and ownership-failover cases. See [the fixture runbook](../tests/recovery/README.md). Evidence: `/private/tmp/rivus-fresh-snapshot-integration.jsonl`.

New cases test missing-history classification and admission, not actual source purge/crash, binlog restoration or target rebuild. Production REST/S3 faults were not injected. Admission cannot atomically cancel in-flight catalog requests; existing ownership and transactional cleanup protections remain necessary. No exactly-once claim is made. Production inspection was read-only: no deployment, intent deletion, checkpoint change or resubmission was performed.
