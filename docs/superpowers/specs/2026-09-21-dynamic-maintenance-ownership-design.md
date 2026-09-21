# Dynamic Iceberg Maintenance Ownership Design

Date: 2026-09-21

## Purpose

Allow one Metalake maintenance monitor to discover newly created Iceberg
catalogs, namespaces, and tables without job resubmission, while guaranteeing
that Rivus never runs general maintenance concurrently with a streaming or
incomplete snapshot job targeting the same physical table.

The design must remain practical for approximately 6,700 tables, support
immutable-data exclusions, and allow more-specific maintenance monitors to
override broader monitors.

## Success Criteria

- New matching non-streaming tables become eligible automatically after the
  configured discovery interval.
- Active streaming targets are never owned or executed by a general
  maintenance monitor.
- Snapshot-only targets remain reserved until their snapshot succeeds. After
  success, they become eligible for general monitoring automatically.
- Failed or stopped incomplete snapshots remain reserved until the job is
  deleted.
- Catalog, namespace, and table exclusions are scoped to their physical
  catalog and do not affect identically named objects elsewhere.
- A table has exactly one effective maintenance owner.
- Reconciliation writes only ownership changes rather than rewriting every
  discovered table on every worker poll.
- Discovery failure never removes the last known-good target set.
- Initial inventory work is paced through the background queue instead of
  creating a high-priority burst for every discovered table.

## Non-Goals

- Replacing streaming-job maintenance.
- Inferring that an arbitrary external writer is a Rivus streaming job.
- Automatically deciding that a table is immutable based on observed write
  frequency. Immutable data remains an explicit exclusion.
- Adding a second nested table-list API.

## Canonical Table Identity

All ownership and exclusion decisions use this canonical identity:

```text
physical Gravitino catalog / Iceberg namespace / Iceberg table
```

The physical catalog is the Gravitino/Iceberg warehouse selector, such as
`asmat`. A Spark catalog alias such as `rivus` is execution configuration and
must not participate in ownership identity.

One shared resolver produces the physical catalog for monitor discovery,
streaming reservations, snapshot reservations, state reconciliation, task
claims, and execution checks. A discovered target already carries its
physical catalog and bypasses alias inference.

If a streaming target's physical catalog cannot be resolved safely, Rivus
fails closed by reserving the relevant configured catalog scope, or rejects
job start when no safe scope can be identified. It must not silently fall back
to a Spark alias.

## Ownership Model

`iceberg_maintenance_state.table_key` is already unique and becomes the
authoritative ownership record for each concrete table. Existing `owner_type`,
`owner_job_id`, snapshot barrier, and lease fields are used rather than adding
a parallel concrete-table registry that could disagree with maintenance
state.

Two supporting structures are required:

- `iceberg_maintenance_reservations` stores active streaming and incomplete
  snapshot selectors, including wildcard namespace/table patterns. A
  reservation prevents a monitor from claiming both current and future
  matching tables, but it does not duplicate concrete maintenance state.
- `iceberg_maintenance_catalog_guards` provides one transaction-lock row per
  physical catalog. Reservation changes, monitor ownership changes, and
  operation-lease acquisition lock this row, closing the race between job
  startup and maintenance execution without globally serializing all
  catalogs.

Eligibility and ownership precedence is:

1. An active streaming reservation blocks every maintenance monitor.
2. An incomplete snapshot reservation blocks every maintenance monitor.
3. A monitor's explicit exclusion makes that monitor ineligible.
4. The most-specific remaining active maintenance monitor owns the table.
5. A table without an eligible monitor remains unmanaged.

Maintenance-monitor specificity is deterministic:

```text
explicit table > namespace scope > catalog scope > whole Metalake
```

Dynamic specificity is calculated from the selector that matched the concrete
table: exact catalog + namespace + table is table scope; exact catalog +
namespace with a wildcard table is namespace scope; exact catalog with a
wildcard namespace/table is catalog scope; and a wildcard catalog is Metalake
scope. An explicit table list is always table scope. If several selector
combinations match, the most specific combination is used.

When monitors at the same specificity match the same table, the current owner
remains stable. The losing monitor records a visible ownership conflict; it
does not steal the table based on iteration order. Exclusions are
monitor-local: excluding `asmat.analytics` from a Metalake monitor does not
prevent an intentionally configured, more-specific monitor from owning it.

Ownership transitions occur in a database transaction that also cancels
queued/retry tasks for the old owner and clears future schedules when the
table becomes ineligible. Historical state and completed results are retained.

## Streaming and Snapshot Reservations

Job startup writes durable selector reservations before ingestion begins.
This applies even when the ingestion job does not enable its own table
maintenance. Reservation creation locks each affected physical catalog guard,
then cancels or transfers matching monitor-owned concrete states in the same
transaction.

- Streaming mode retains its reservation while the job is active.
- A paused streaming job remains reserved so resume does not cause ownership
  churn. Stopping or deleting it releases the reservation; a later restart
  must reserve the targets again before ingestion resumes.
- Snapshot-only mode retains its reservation until the durable snapshot
  completion record reports success.
- A successful snapshot releases its reservation, making the target eligible
  for the next monitor reconciliation.
- Failed, stopped, or missing-completion snapshot jobs stay reserved until
  deletion because the destination can be incomplete.

A reservation cancels queued/retry general-maintenance tasks. If a maintenance
operation already holds the table lease, ingestion waits for that lease to
finish or returns a retryable start error. The system never starts ingestion
and maintenance concurrently on the same table.

Before executing an operation, a maintenance worker locks the physical
catalog guard, verifies that no reservation matches, verifies that the task
owner still owns the table, and acquires the operation lease in one
transaction. A stale task is cancelled without calling the maintenance
backend.

## Streaming Target Projection

Exact source selectors are resolved through the same sink mapping used by
ingestion.

Wildcard selectors retain their pattern so future streaming tables are also
reserved. Namespace-only and table-only target overrides inherit the missing
component from the source/default mapping. All matching overrides are
considered, including partial overrides.

When a wildcard plus override combination cannot be projected without
under-reserving, Rivus conservatively reserves the physical catalog rather
than risk general maintenance touching a streaming target. This fallback must
be observable in logs and monitor details.

## Discovery and Reconciliation

Each monitor persists its last successful discovery timestamp. A new
`iceberg_maintenance_monitor_targets` membership table stores its
last-known-good concrete target set, selector specificity, and claim status.
This supports restart-safe reconciliation and visible reserved/conflicted
counts without encoding thousands of tables in one JSON value.

Discovery loads the existing membership keys and compares them in memory.
Unchanged memberships receive no database update. The configured discovery
interval is therefore honored across worker cache refreshes and process
restarts without producing a full-table write cycle.

On successful discovery, reconciliation computes a set difference:

- **Added:** insert monitor membership, then atomically claim eligible tables
  under the physical catalog guard and schedule a paced initial inventory.
- **Unchanged:** perform no state write.
- **Removed or newly excluded:** mark membership retired, retire monitor
  ownership, clear future schedules, and cancel queued/retry tasks.
- **Reserved by ingestion:** do not claim; expose the reservation in monitor
  details.
- **Matched by a more-specific monitor:** transfer monitor ownership
  transactionally when no ingestion reservation or active operation lease
  blocks the transfer.

On discovery failure, Rivus keeps the last-known-good target set, records the
error, and performs no retirement.

Paused monitors retain configuration and history but do not own runnable
schedules. Resuming triggers reconciliation rather than immediately setting
every table to high priority.

## Scheduling and Scale

Newly discovered tables receive normal background inventory priority with
deterministic jitter. User-requested refreshes remain high priority. The
worker processes a bounded number of background inventories per poll, so
thousands of discovered tables cannot monopolize the maintenance process.

Active job/reservation information is loaded during the normal state-sync
cycle and cached for task processing. Task execution does not deserialize all
persisted jobs for every table operation. Database ownership remains the final
authority for the atomic preflight check.

## API and Configuration

Dynamic selection remains under `catalog_monitoring`; explicit table monitors
keep their current flat table list. The two forms are not nested.

```yaml
table_maintenance:
  enabled: true
  catalog_monitoring:
    enabled: true
    api_uri: ${GRAVITINO_URI}
    metalake: metalake_demo
    catalog_patterns: ["*"]
    namespace_patterns: ["*"]
    table_patterns: ["*"]
    discovery_interval_seconds: 1800
    exclude:
      - catalog: asmat
        namespace: analytics
        reason: immutable
```

Static and dynamic monitors may coexist. The API no longer rejects all such
combinations; ownership specificity resolves overlaps. Invalid glob syntax,
a table exclusion without a namespace, and unresolved physical identity are
rejected with actionable validation errors.

Monitor details expose discovered, owned, reserved, excluded, conflicted, and
retired counts plus the last successful discovery time and latest error.

## Failure Handling

- Gravitino or Iceberg discovery timeout: retain last-known targets and retry
  after the interval/backoff.
- Database reconciliation failure: roll back the complete ownership change;
  no partial target retirement.
- Lease conflict during monitor transfer: keep the current owner and retry on
  the next reconciliation.
- Lease conflict during ingestion reservation: do not start ingestion; return
  a retryable result until the operation completes or its lease expires.
- Worker crash: database leases expire normally; ownership remains durable.
- Deleted catalog/table: retire schedules after successful discovery while
  preserving history.

## Verification

Tests must cover:

- Physical warehouse/catalog identity is used instead of Spark aliases.
- Streaming jobs reserve targets even when table maintenance is disabled.
- Snapshot-only jobs reserve before completion and release after durable
  success.
- Failed snapshots stay reserved.
- Partial overrides and wildcard mappings cannot under-reserve targets.
- Ambiguous mappings fail closed.
- Maintenance execution cannot proceed after ownership changes.
- Ingestion cannot begin during an active maintenance lease.
- Discovery intervals survive worker state refresh and restart.
- Successful reconciliation adds and retires only changed targets.
- Failed discovery does not retire targets.
- Initial discovery of thousands of tables does not create a priority burst
  or rewrite unchanged states on subsequent polls.
- Table, namespace, catalog, and Metalake monitor precedence is deterministic.
- Equal-specificity conflicts remain stable and observable.
- Catalog/namespace/table manual exclusions are properly scoped.
- Existing static maintenance-monitor behavior remains compatible.

Run focused package tests, race tests for the worker/store paths, the full Go
test suite, vet, and diff validation before completion.

## Alternatives Considered

### Recompute ownership only from job configuration

Rejected because it requires repeated full job deserialization, cannot close
the submission-versus-execution race by itself, and makes wildcard projection
the sole safety barrier.

### Manual exclusions only

Rejected because new streaming tables could become temporarily eligible and
because it reintroduces the manual resubmission burden this feature is meant
to remove.

### Separate concrete-table ownership table

Not selected because the maintenance state already has a unique physical
table key, owner, snapshot barrier, and lease. A second concrete-table
authority would add synchronization risk. Selector reservations and monitor
memberships are separate supporting facts, not competing ownership records.
