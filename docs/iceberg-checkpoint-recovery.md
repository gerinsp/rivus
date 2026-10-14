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

Actual cleanup retains its serializable transaction and locking intent check. Admission is advisory and does not replace that protection. Normal resume and bounded automatic reconciliation remain available when source history is present. The admission patch shipped in `ff300c0`; the follow-up below requires a separate deployment.

## Live checkpoint and replay follow-up

Normal checkpoint cleanup previously invoked startup reconciliation. If a live batch had already committed rows beyond the latest transaction checkpoint, cleanup installed replay counters for events the running sink had already consumed. A later checkpoint then failed with `checkpoint reached commit range before all source event identities replayed` even without a lost commit response.

Normal checkpoint cleanup now removes only `COMMITTED` CDC receipts covered by the saved offset. It leaves uncovered and `PREPARED` receipts intact and never installs startup replay counters or issues catalog calls. Restart still requires positive catalog evidence and exact event replay.

A checkpoint may also lie inside a committed batch. New CDC receipts use a version 2 replay manifest containing each identity hash and source offset, bound into the catalog intent fingerprint. Startup requires replay only for events beyond the durable checkpoint. Missing suffix events, changed payloads, invalid offsets, missing proof and ownership loss still block progress.

Legacy hash-only arrays remain supported without modifying their catalog fingerprint. They cannot identify an already-checkpointed prefix inside a batch. Such existing receipts can still block after restart; this patch does not guess prefix coverage or repair existing production receipts. Repeated ordinary resubmit cannot resolve that limitation. Recovery requires verified historical identities through an operator recovery procedure or a controlled rebuild.

## Resubmit after purge with an explicitly configured full reset

For durable split-worker jobs, an explicit **Resubmit** after source diagnostics confirm purge can now rebuild when all of these conditions hold:

- The old job is terminal, desired `STOPPED`, its submission and state key still match, and its worker has released ownership.
- Registry and offsets share the same MySQL database.
- Every retained intent is `COMMITTED`; `PREPARED`/`UNKNOWN`, another state key or another job's receipt blocks the reset.
- Source tables are explicitly selected and every target is configured for full truncate: `truncate-append`, or `auto` with covering `snapshot_truncate_tables` and the required wildcard opt-in. Excluded targets and `auto` with `snapshot_replace_filters` are rejected.
- Selected source tables have no snapshot filters. Target reset uses the native executor and durable catalog commit guard; Trino reset is rejected for this path.

One serializable transaction archives the original config, public/internal checkpoints, snapshot state/progress and unchanged receipts in `job_recovery_archives`, clears active recovery state, replaces the submission token and queues a fresh snapshot with `resume_requested=FALSE`. Failure to archive or clear any row rolls back every change. The new worker follows the existing snapshot/handoff/CDC flow; stale checkpoint/intent writes fail the existing ownership fence.

The archive contains credentials from the original config and must receive the same access protection as `job_registry`. Logs expose only job/archive/submission IDs. Archives have no automatic expiry. They preserve evidence; they are not a target-data backup or an automatic rollback procedure.

The new submission carries a submission-bound rebuild marker and the persisted `snapshot_write_mode=rebuild-truncate-append-v1`. The new factory validates the marker against the current submission and requires durable metadata before translating this mode to the native reset implementation. A deployed `ff300c0` worker rejects this unknown mode before catalog discovery; it cannot silently perform an incomplete rebuild. Such a worker can still claim and fail the job, so this is a correctness guard, not seamless mixed-version availability.

Authoritative preflight resets all selected targets before saving the snapshot start, including targets whose source tables are empty. Once that start is durable, snapshot restart appends the remaining rows without truncating the already-committed prefix again. A missing start, metadata error or ownership loss blocks that handshake.

Source snapshot filters are rejected for this full-reset path. Removing filters to restore full source contents requires an explicit operator decision. In `auto`, replace filters take precedence over truncate patterns; a truncate wildcard alone does not authorize full reset for those tables. Normal Delete retains its receipt protection; for stopped jobs it now reports HTTP 409 before removing the UI job instead of returning `deleted` and restoring a FAILED job later. Running jobs still drain and repeat transactional cleanup checks.

The archived rebuild is not supported for unfenced `ALL` jobs. No automatic resubmission of arbitrary FAILED jobs, blind commit retry, or checkpoint fast-forward is introduced.

## Local work in progress: filtered Trino rebuild

The current worktree adds a separate submission-bound `rebuild-scoped-trino-v1` protocol. **This feature is not released or ready for production rollout. Real Trino end-to-end verification is still pending.** The native full-reset path above remains separate.

The scoped path preserves the submitted source filters and requires equivalent single-column target replacement predicates. Filtered tables may remain excluded from full truncate. Unfiltered selected tables still require explicitly configured full reset. Wildcard source selection and arbitrary compound filter SQL are rejected.

The existing serializable archive transaction still rejects old PREPARED/UNKNOWN receipts and active ownership. For an eligible job it queues a new submission, persists the scoped protocol, and disables rolling snapshot batches. Source and sink must agree on that protocol. An actual `ff300c0` binary rejects both rebuild protocol modes before catalog discovery; mixed-version availability is not guaranteed.

Before each DELETE or MERGE, the sink stores a PREPARED intent with the original target UUID and statement digest. It saves the returned Trino query ID before polling. Successful terminal responses are marked COMMITTED through the existing worker fence. Recovery only reads the original query execution state and native catalog identity; it does not repeat an ambiguous mutation. Missing query identity, expired/unavailable query history, identity mismatch, or lost ownership keeps recovery BLOCKED. Existing native receipt fingerprints retain their old encoding when the added fields are empty.

All selected targets, including empty-source tables, receive a reset barrier before snapshot start. Restart requires the same job, submission, target mapping/UUID and reset statement scope. A reset that is already confirmed is not repeated. Snapshot rows use MERGE by primary key across the whole target, so a source row moving into the filtered range updates its previous row outside that range. COMMITTED statement receipts remain until sink-confirmed snapshot completion; they do not advance the CDC checkpoint independently.

Current verification: full unit suite PASS; 20 real MySQL archive/rebuild cases (10 native and 10 filtered Trino configurations), two query-identity SQL/fence cases, four ownership cases, six actual baseline-binary/schema compatibility cases, and 19 existing native Iceberg crash/restart cases PASS. One metadata-container crash case was skipped because the current isolated Colima profile is not the default context required by its safety guard. These results do **not** exercise real Trino execution.

Nine Trino execution/process-crash/pipeline cases are written, but remain unexecuted because the local disk cannot accommodate the fixture image. Target duplicate-PK admission, external/shared target writers, Trino query-history expiry, complete worker snapshot-progress crash coverage, production REST/S3 and performance must be resolved or explicitly bounded before release. Do not deploy this worktree or resubmit production jobs on the assumption that filtered rebuild is already verified. See [the Trino fixture instructions](../tests/recovery/README.md#filtered-trino-fixture-work-in-progress).

## Upgrade and rollback compatibility

Compatibility is tested against an actual test binary built from deployed revision `ff300c01937e5bbd408aa268a4b0923b53e211c6`, with test probes added and production code unchanged. Schema migration tests use separate disposable MySQL databases with old DDL. See [the reproducible compatibility commands](../tests/recovery/README.md#compatibility-upgrade-dan-rollback).

| Existing state / version direction | Expected behavior |
| --- | --- |
| Legacy registry without submission/role/lease columns | Add columns, create a stable submission token; preserve config, desired state and persisted status. Repeated migration preserves the token. |
| Legacy snapshot without `completion_version` or progress cursor | Preserve start, checkpoint and progress; version remains `0`. Legacy `done=1` is not automatically attested. |
| `ff300c0` legacy receipt, complete replay, new worker | Prove the original catalog fingerprint, persist COMMITTED, skip exact replayed events, checkpoint and clean covered receipt without another commit. |
| Legacy hash-only receipt with checkpoint inside its batch | Remain BLOCKED; upgrade does not infer the covered prefix. |
| New version 2 receipt, rollback to `ff300c0` | Old reader rejects unsupported identities; retain checkpoint and receipt. Safe rejection does not provide resumability on the old image. |
| New rebuild job, `ff300c0` worker | Reject protocol mode before catalog access. Run the rebuild with compatible workers. |
| Old worker after new worker takes expired lease | Reject stale checkpoint mutation using the existing ownership fence. |
| Persisted `STOPPING` | Restore `STOPPING`, not fallback `CREATED`; do not automatically turn it into a resubmittable state. |

The CI compatibility job runs six integration cases (four binary cases and two schema migrations); the complete Colima suite combines these with 34 existing crash/restart cases. These tests do not establish compatibility with every earlier release, all rolling deployment schedules, production REST/S3 or Doris upgrades. Once version 2 receipts or rebuild protocol jobs exist, rolling back to `ff300c0` can block those jobs. Inspect retained state before rollback; never erase receipts to make an older worker resume.

## Timeout and status diagnostics

A recovery deadline now retains the last verification error in the BLOCKED reason. For example, `context deadline exceeded; last verification error: matching operation not found in retained main snapshot ancestry` describes why the bounded verification failed. Missing catalog evidence remains UNKNOWN; an available MySQL binlog does not prove an Iceberg commit. This diagnostic change does not resolve existing PREPARED operations.

The local follow-up also preserves the initial write or COMMITTED-persistence error when live automatic recovery fails. Returned errors and recovery progress/logs include both `initial commit error` and `reconciliation error`; callers can still identify cancellation and ownership loss with `errors.Is`. Retry decisions continue to use only the verification result. Positive proof still resumes normally, while failed verification retains PREPARED and the checkpoint without repeating the write. Receipt-validation failure also retains the initial cause.

Six deterministic regression cases failed before this change: missing proof, unavailable metadata, deadline, ownership loss, invalid replay identities, and failed COMMITTED persistence. This is a diagnostic correction, not a filtered Trino rebuild implementation. The original cause is not added to the durable intent schema or fingerprint; a fresh process cannot reconstruct an error that an earlier worker never recorded. Existing production UNKNOWN operations remain unresolved. Trino completion follows the client protocol's absent `nextUri`/error fields; this patch does not introduce a requirement to infer completion from diagnostic `stats.state`.

The six regressions, Iceberg/core/meta unit suites, and automatic-recovery race tests pass locally. No new production fault injection or end-to-end filtered rebuild test was run for this diagnostic change. The combined follow-up is now locally verified; rollout status is tracked separately.

`STOPPING` previously fell through status parsing to `CREATED`, hiding why Resubmit was unavailable. The status mapping now preserves `STOPPING`. Automatic requeue is not introduced: an asynchronous delete/cleanup may still be running even when the worker lease is empty. Repairing an orphan control-plane operation remains separate work.

## Operator recovery

1. Inspect checkpoint diagnostics, lease/submission, intent status and source ranges. Confirm no replacement worker is active before changing anything; keep the old job stopped when planning a rebuild.
2. If required binlog history can be restored in a supported source recovery environment, preserve original metadata and source event identity. Verify the entire required history before resuming. Startup must prove retained commits and replay from the saved checkpoint.
3. If history is unavailable, choose the intended snapshot scope and target strategy. A configured full reset can use the archived Resubmit path above after that build is deployed. For filtered replacement or populated append targets, use a clean dedicated target and recovery identity while preserving original metadata, or explicitly plan a full reset. Validate snapshot completeness, primary keys, representative payloads and CDC continuity before consumer cutover.
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

Follow-up verification: the original 20 integration scenarios plus 14 new scenarios pass in Colima, with no skips. A worker is killed with SIGKILL after a real MySQL transaction checkpoint inside a committed Iceberg batch; a fresh metadata connection and catalog then consume actual suffix binlog events, verify two visible rows/one snapshot, and continue CDC to three rows/two snapshots. Rebuild tests cover archive/reset rollback, PREPARED, active ownership, stale submission/key, unrelated receipts, duplicate request, restart claim and stale writes. An API integration test verifies HTTP 409 and unchanged metadata on blocked Delete. A real master/snapshot/streaming pipeline archives and resets a purged job, removes old rows from populated and empty-source targets, confirms snapshot completion, hands off ownership and continues CDC. Test doubles additionally verify snapshot suffix restart without another truncate. Full unit and related race suites pass.

The target rebuild pipeline uses a local Hadoop catalog; production REST/S3 target rebuild, consumer cutover and SIGKILL during rebuild snapshot/rolling spool remain untested. Source-server purge/crash, filtered recovery outside the selected range and restoration of legacy prefix identities remain untested. This follow-up is local and has not changed production metadata or deployed a new image. Final evidence: `/private/tmp/rivus-rebuild-verified-integration.jsonl`. All 34 scenarios passed after native-executor admission was finalized; fixture containers and volumes were removed afterward.

## Filtered Trino rebuild setelah purge

Resubmit dapat mengarsipkan original state dan memulai snapshot baru pada target existing jika seluruh retained intents COMMITTED, submission/state key masih cocok, worker sudah release ownership, dan source/sink filter scopes terbukti equivalent. `PREPARED`/UNKNOWN tetap ditolak. Trino executor memakai durable operation intent dan query ID sebelum polling. Missing response tidak menyebabkan POST retry: restart hanya memakai exact terminal QueryInfo proof, target UUID, statement digest, submission dan ownership yang cocok. Submission binding dipasang sebelum constructor reconciliation.

Full-reset tables dibersihkan penuh; filtered tables hanya membersihkan selected scope dan mempertahankan out-of-scope/NULL date rows. Snapshot MERGE mencocokkan PK pada seluruh target agar row yang berpindah melintasi cutoff tidak menjadi duplicate. Target duplicate/NULL PK ditolak pada preflight filtered rebuild. Setelah snapshot selesai, existing sink confirmation/ACK dan CDC checkpoint flow digunakan. Snapshot filters tidak menjadi permanent CDC filters.

Sembilan Trino execution scenarios PASS pada shared JDBC catalog/native Iceberg files: success, lost responses, duplicate PK, empat process SIGKILL/restart points, serta snapshot/CDC lifecycle. Native count mempertimbangkan positional/equality deletes; physical file records bukan logical rows. Production REST/S3 fault injection, external concurrent data writers, query-history expiry dan seluruh rolling-spool crash points belum dibuktikan. Query proof yang hilang tetap BLOCKED. Jangan melakukan exactly-once claim dari hasil ini.

UNKNOWN dengan binlog purged membutuhkan target dan recovery identity baru, dengan original metadata/target tetap disimpan. Job 2 dan Trikusuma/WBShuttle menggunakan jalur isolation tersebut, bukan memaksa old receipt menjadi COMMITTED. Consumer cutover ke recovery targets wajib diverifikasi dan dipetakan secara eksplisit.

Filtered rebuild membuktikan selected snapshot scope, bukan seluruh historical data: rows di luar cutoff dipertahankan, tetapi purged binlogs membuat freshness rows tersebut tidak dapat dibuktikan. Jangan menganggap out-of-scope rows sudah disinkronkan ulang. Jika historical correctness seluruh table diperlukan, pilih full source snapshot dengan target rebuild/isolation yang sesuai, bukan menghapus filter tanpa review scope.

## Native snapshot recovery writer (14 October 2026)

The production v1 scoped writer embedded full snapshot rows into sequential, 512 KiB Trino MERGE statements. Tiketux executed roughly 990 statements in 30 minutes; sampled queries each processed roughly four million target rows. Baraya rejected an individual SQL-encoded row over the statement bound.

The new submission-bound mode `rebuild-scoped-native-v2` keeps durable, filtered Trino reset receipts but writes data through native Parquet batches. Unfiltered, freshly reset tables append directly. Filtered tables, and tables partially written by v1 MERGE, look up only batch primary keys and partition before-images through read-only Trino SELECTs. Existing keys are replaced with native equality deletes plus new data in one RowDelta; unmatched keys append. This handles rows moving across cutoff/partition values without duplicating retained outside-scope keys. NULL/duplicate source keys fail before the batch writes. Historical active partition-spec differences block native key replacement rather than silently leave rows behind. External writers remain outside the proven ownership model.

Native receipts bind target UUID, rebuild epoch and source row range to the catalog fingerprint. Source ACK follows commit confirmation. Restart subtracts positively proven row ranges before splitting batches, including after a batch-size change. Rolling remains disabled for this recovery protocol; bounded native chunks are durably acknowledged. The ordinary rolling snapshot path is unchanged. Completion must be sink-confirmed before receipt cleanup and CDC handoff.

An inactive, unfinished v1 scoped job upgrades on explicit resubmit through one MySQL transaction that archives the old config/state/receipts, preserves its epoch and source cursor, and changes only the writer protocol and non-rolling flag. Active owners, unrelated epochs/receipts, missing durable query identities, and completed snapshots reject the upgrade. PREPARED outcomes are retained: the next fenced worker must prove them before source replay. Old workers reject v2 before catalog access; all roles must be upgraded and old heartbeats stale before resubmitting.

For a v1 MERGE whose query history returns HTTP 404, an exact persisted query ID on a retained current-main append/overwrite snapshot can positively prove the original single-table RowDelta. This requires the original target UUID, current epoch, source mapping and digest-bound batch ID. Other query errors, missing IDs/markers, abandoned history, changed identity, and reset/no-op operations remain blocked. No mutation POST is retried to obtain evidence.

Local validation: full units and 39 real fixture integration leaf cases pass, including native large-row/partition movement, lost response with expired query history, real SIGKILL after catalog acceptance/receipt persistence, native snapshot-to-CDC, durable upgrade admission and atomic archives. A fixture wrote 2,001 wide rows including a 1.33 MB payload with one native commit in about 1.25 seconds; this is not a production benchmark or a completeness/exactly-once guarantee.
