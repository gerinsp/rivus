# Dynamic Iceberg Maintenance Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Metalake maintenance discovery automatic and scalable while preventing general maintenance from ever overlapping Rivus streaming or incomplete snapshot ownership.

**Architecture:** Concrete table ownership remains in `iceberg_maintenance_state`; durable wildcard ingestion reservations and per-catalog transaction guards close startup/execution races. Monitor target membership is persisted separately and reconciled by set difference, with deterministic specificity deciding which monitor owns an overlapping table.

**Tech Stack:** Go, MySQL 8 transactional row locks, existing Rivus connector/job lifecycle, Gravitino management API, Iceberg REST catalog, vanilla Go tests.

**Spec:** `docs/superpowers/specs/2026-09-21-dynamic-maintenance-ownership-design.md`

## Global Constraints

- Physical ownership identity is `Gravitino catalog / Iceberg namespace / Iceberg table`; Spark aliases never form ownership keys.
- Streaming reservations outrank snapshot reservations, and both outrank maintenance monitors.
- Snapshot-only reservations release only after durable snapshot success; failed or stopped incomplete snapshots stay reserved until deletion.
- A paused streaming job remains reserved; a stopped or deleted streaming job releases its reservation.
- Explicit exclusions are local to their monitor.
- Monitor precedence is `specific table > namespace > catalog > whole Metalake`.
- Failed discovery retains the last-known-good target set and performs no retirement.
- Unchanged discovered targets produce no state or membership writes.
- Background inventory is paced; bulk discovery never marks every table high priority.
- Preserve existing user changes and do not expose secrets from deployment configuration.

## Review Focus

- A warehouse value that is a URI/path instead of a Gravitino catalog must fail safely and never fall back to the Spark alias; Task 1 adds this test.
- A wildcard source selector with a namespace-only or table-only override must reserve every possible physical destination; Task 1 adds these tests.
- A reservation racing a leased maintenance task must lose safely with a retryable busy result, while a reservation arriving first must cancel the task; Tasks 2 and 4 add both orderings.
- A discovery result that is empty because of an upstream error must not be treated as a successful empty catalog; Task 5 tests error and valid-empty results separately.
- Equal-specificity monitors created in different orders must keep stable ownership and expose a conflict instead of oscillating; Task 5 adds both ordering tests.

---

### Task 1: Canonical physical identity and safe selector projection

**Files:**
- Create: `pkg/meta/maintenance_ownership.go`
- Create: `pkg/connectors/iceberg/maintenance_ownership.go`
- Modify: `pkg/connectors/iceberg/maintenance_worker.go`
- Modify: `pkg/connectors/iceberg/maintenance_signals.go`
- Test: `pkg/connectors/iceberg/maintenance_ownership_test.go`
- Test: `pkg/connectors/iceberg/maintenance_worker_test.go`

**Interfaces:**
- Consumes: `config.JobConfig`, `config.IcebergConfig`, `Sink.ResolveTarget`, MySQL source table selectors, Iceberg target overrides.
- Produces: `maintenanceIdentityCatalogName(config.IcebergConfig) (string, error)`, `maintenanceReservationSelectors(*config.JobConfig, config.IcebergConfig) ([]meta.IcebergMaintenanceReservationSelector, error)`, `normalizedStreamingSourceSelectors(*config.JobConfig) ([]sourceTableSelector, error)`, `projectSourceSelector(sourceTableSelector, config.IcebergConfig) ([]meta.IcebergMaintenanceReservationSelector, bool)`, and one canonical target-key helper used by every ownership path.

- [ ] **Step 1: Write failing physical-catalog tests**

Add table-driven tests proving warehouse wins over Spark alias and unsafe warehouse values do not silently become `catalog_name`:

```go
func TestMaintenanceIdentityCatalogNameUsesPhysicalWarehouse(t *testing.T) {
    cfg := config.IcebergConfig{
        Warehouse:   "asmat",
        CatalogName: "rivus",
        TableMaintenance: config.IcebergTableMaintenanceConfig{
            CatalogName: "rivus",
        },
    }
    got, err := maintenanceIdentityCatalogName(cfg)
    if err != nil || got != "asmat" {
        t.Fatalf("catalog = %q, err = %v, want asmat", got, err)
    }
}

func TestMaintenanceIdentityCatalogNameRejectsPathWarehouse(t *testing.T) {
    cfg := config.IcebergConfig{Warehouse: "s3://bucket/warehouse", CatalogName: "rivus"}
    if got, err := maintenanceIdentityCatalogName(cfg); err == nil || got != "" {
        t.Fatalf("catalog = %q, err = %v, want safe failure", got, err)
    }
}
```

- [ ] **Step 2: Write failing selector-projection tests**

Cover exact selection, wildcard selection, partial overrides, and ambiguous mapping fallback:

```go
func TestMaintenanceReservationSelectorsApplyPartialOverrides(t *testing.T) {
    job := mysqlIcebergJob("stream-orders", []string{"sales.*"})
    cfg := config.IcebergConfig{
        Warehouse: "asmat",
        Overrides: map[string]config.IcebergTarget{
            "sales.orders_*": {Namespace: "orders_stream"},
        },
    }
    got, err := maintenanceReservationSelectors(job, cfg)
    if err != nil {
        t.Fatal(err)
    }
    assertReservationSelector(t, got, "asmat", "orders_stream", "orders_*")
}

func TestMaintenanceReservationSelectorsFailClosedForAmbiguousWildcard(t *testing.T) {
    job := mysqlIcebergJob("stream-all", []string{"*.*"})
    cfg := config.IcebergConfig{
        Warehouse: "asmat",
        Overrides: map[string]config.IcebergTarget{
            "sales.orders_*": {Table: "current_orders"},
        },
    }
    got, err := maintenanceReservationSelectors(job, cfg)
    if err != nil {
        t.Fatal(err)
    }
    assertReservationSelector(t, got, "asmat", "*", "*")
}
```

- [ ] **Step 3: Run the focused tests and confirm failure**

Run:

```bash
go test ./pkg/connectors/iceberg -run 'TestMaintenanceIdentityCatalogName|TestMaintenanceReservationSelectors'
```

Expected: FAIL because the new identity and selector functions do not exist.

- [ ] **Step 4: Implement canonical identity and projection**

First define the dependency-free selector value in `pkg/meta`:

```go
type IcebergMaintenanceReservationSelector struct {
    Catalog          string
    NamespacePattern string
    TablePattern     string
}
```

Then create these focused helpers:

```go
func maintenanceIdentityCatalogName(cfg config.IcebergConfig) (string, error) {
    warehouse := strings.TrimSpace(cfg.Warehouse)
    if sparkCatalogNamePattern.MatchString(warehouse) {
        return warehouse, nil
    }
    return "", fmt.Errorf("physical Iceberg catalog cannot be resolved from warehouse %q", warehouse)
}

func maintenanceReservationSelectors(
    job *config.JobConfig,
    cfg config.IcebergConfig,
) ([]meta.IcebergMaintenanceReservationSelector, error) {
    catalog, err := maintenanceIdentityCatalogName(cfg)
    if err != nil {
        return nil, err
    }
    sources, err := normalizedStreamingSourceSelectors(job)
    if err != nil {
        return nil, err
    }
    var projected []meta.IcebergMaintenanceReservationSelector
    for _, source := range sources {
        targets, safe := projectSourceSelector(source, cfg)
        if !safe {
            return []meta.IcebergMaintenanceReservationSelector{{
                Catalog: catalog, NamespacePattern: "*", TablePattern: "*",
            }}, nil
        }
        projected = append(projected, targets...)
    }
    return dedupeReservationSelectors(projected), nil
}
```

Replace ownership-key uses of `maintenanceCatalogName` and remove `streamingMaintenanceCatalogName`. Keep `maintenanceCatalogName` only for Spark execution configuration.

- [ ] **Step 5: Run focused and package tests**

Run:

```bash
go test ./pkg/connectors/iceberg -run 'TestMaintenanceIdentityCatalogName|TestMaintenanceReservationSelectors|TestStreaming'
go test ./pkg/connectors/iceberg
```

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add pkg/meta/maintenance_ownership.go pkg/connectors/iceberg/maintenance_ownership.go pkg/connectors/iceberg/maintenance_ownership_test.go pkg/connectors/iceberg/maintenance_worker.go pkg/connectors/iceberg/maintenance_worker_test.go pkg/connectors/iceberg/maintenance_signals.go
git commit -m "fix: canonicalize Iceberg maintenance ownership"
```

### Task 2: Durable reservations and catalog transaction guards

**Files:**
- Modify: `pkg/meta/maintenance_ownership.go`
- Create: `pkg/meta/maintenance_ownership_test.go`
- Modify: `pkg/meta/maintenance_store.go`

**Interfaces:**
- Consumes: canonical physical selectors from Task 1 and existing maintenance state/task leases.
- Produces: `IcebergMaintenanceReservationSelector`, `IcebergMaintenanceReservation`, `ErrMaintenanceOwnershipBusy`, `SyncMaintenanceReservations`, `ReleaseMaintenanceReservations`, `ReleaseMaintenanceReservationsExcept`, and guarded task-claim primitives.

- [ ] **Step 1: Write failing matching and validation tests**

```go
func TestMaintenanceReservationMatchesCatalogScopedPatterns(t *testing.T) {
    reservation := IcebergMaintenanceReservation{
        Catalog: "asmat", NamespacePattern: "orders_*", TablePattern: "*",
    }
    if !reservation.Matches("asmat", "orders_live", "events") {
        t.Fatal("expected reservation to match")
    }
    if reservation.Matches("ds", "orders_live", "events") {
        t.Fatal("reservation leaked across catalogs")
    }
}

func TestValidateReservationSelectorRejectsTableWithoutNamespace(t *testing.T) {
    selector := IcebergMaintenanceReservationSelector{Catalog: "asmat", TablePattern: "orders"}
    if err := selector.Validate(); err == nil {
        t.Fatal("expected namespace validation error")
    }
}
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
go test ./pkg/meta -run 'TestMaintenanceReservation|TestValidateReservationSelector'
```

Expected: FAIL because reservation types are undefined.

- [ ] **Step 3: Add ownership schema**

Add idempotent DDL to `IcebergMaintenanceStore.Init`:

```sql
CREATE TABLE IF NOT EXISTS iceberg_maintenance_catalog_guards (
  catalog VARCHAR(255) NOT NULL PRIMARY KEY,
  updated_at DATETIME(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS iceberg_maintenance_reservations (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_job_id VARCHAR(255) NOT NULL,
  submission_id VARCHAR(64) NOT NULL,
  reservation_kind VARCHAR(32) NOT NULL,
  catalog VARCHAR(255) NOT NULL,
  namespace_pattern VARCHAR(512) NOT NULL,
  table_pattern VARCHAR(255) NOT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  UNIQUE KEY uq_maintenance_reservation
    (owner_job_id, submission_id, catalog, namespace_pattern, table_pattern),
  INDEX idx_maintenance_reservation_catalog (catalog, active)
);
```

Use the existing duplicate-safe migration helpers and initialize guard rows with `INSERT IGNORE` before locking them.

- [ ] **Step 4: Implement reservation matching and transactional synchronization**

Define:

```go
var ErrMaintenanceOwnershipBusy = errors.New("maintenance ownership is busy")

type IcebergMaintenanceReservation struct {
    IcebergMaintenanceReservationSelector
    OwnerJobID     string
    SubmissionID  string
    Kind          string // streaming or snapshot
    Active        bool
}

func (s *IcebergMaintenanceStore) SyncMaintenanceReservations(
    ctx context.Context,
    ownerJobID, submissionID, kind string,
    selectors []IcebergMaintenanceReservationSelector,
    now time.Time,
) error

func (s *IcebergMaintenanceStore) ReleaseMaintenanceReservations(
    ctx context.Context,
    ownerJobID, submissionID string,
    now time.Time,
) error

func (s *IcebergMaintenanceStore) ReleaseMaintenanceReservationsExcept(
    ctx context.Context,
    keep map[string]string, // owner job ID -> current submission ID
    now time.Time,
) error
```

For every affected catalog, the transaction must lock the catalog guard, load
matching concrete states, return `ErrMaintenanceOwnershipBusy` if an unexpired
operation lease exists, upsert reservations, transfer monitor-owned state,
clear schedules, and cancel queued/retry tasks. It must never steal concrete
state from another ingestion owner. Any active matching reservation blocks a
monitor; streaming reservations sort ahead of snapshot reservations only when
an unowned/monitor-owned concrete state needs an ingestion owner. Release must
be fenced by `submission_id`, re-evaluate other matching active reservations,
and never make a table monitor-eligible while another reservation still
matches.

- [ ] **Step 5: Add optional MySQL transaction-order integration tests**

Create tests gated by `RIVUS_TEST_MYSQL_DSN`. When unset, call `t.Skip`; when set, create uniquely prefixed catalog/table keys and verify:

```go
func TestReservationBeforeClaimCancelsMonitorTask(t *testing.T) {
    store, tableKey := newMaintenanceOwnershipIntegrationStore(t)
    seedQueuedMonitorTask(t, store, tableKey, "monitor:general", nil)
    err := store.SyncMaintenanceReservations(context.Background(), "stream-orders", "submission-1", "streaming",
        []IcebergMaintenanceReservationSelector{{Catalog: "test_catalog", NamespacePattern: "sales", TablePattern: "orders"}}, time.Now())
    if err != nil {
        t.Fatal(err)
    }
    if got := taskStatus(t, store, tableKey); got != MaintenanceTaskCancelled {
        t.Fatalf("task status = %q, want cancelled", got)
    }
}

func TestReservationDuringActiveLeaseReturnsBusy(t *testing.T) {
    store, tableKey := newMaintenanceOwnershipIntegrationStore(t)
    leaseUntil := time.Now().Add(time.Minute)
    seedQueuedMonitorTask(t, store, tableKey, "monitor:general", &leaseUntil)
    err := store.SyncMaintenanceReservations(context.Background(), "stream-orders", "submission-1", "streaming",
        []IcebergMaintenanceReservationSelector{{Catalog: "test_catalog", NamespacePattern: "sales", TablePattern: "orders"}}, time.Now())
    if !errors.Is(err, ErrMaintenanceOwnershipBusy) {
        t.Fatalf("error = %v, want ErrMaintenanceOwnershipBusy", err)
    }
}

func TestOldSubmissionCannotReleaseNewReservation(t *testing.T) {
    store, _ := newMaintenanceOwnershipIntegrationStore(t)
    selector := []IcebergMaintenanceReservationSelector{{Catalog: "test_catalog", NamespacePattern: "sales", TablePattern: "orders"}}
    if err := store.SyncMaintenanceReservations(context.Background(), "stream-orders", "submission-2", "streaming", selector, time.Now()); err != nil {
        t.Fatal(err)
    }
    if err := store.ReleaseMaintenanceReservations(context.Background(), "stream-orders", "submission-1", time.Now()); err != nil {
        t.Fatal(err)
    }
    if got := activeReservationCount(t, store, "stream-orders", "submission-2"); got != 1 {
        t.Fatalf("active reservations = %d, want 1", got)
    }
}
```

Each test must delete only its uniquely prefixed rows in `t.Cleanup`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
go test ./pkg/meta -run 'TestMaintenanceReservation|TestValidateReservationSelector|TestReservationBeforeClaim|TestReservationDuring|TestOldSubmission'
go test ./pkg/meta
```

Expected: PASS; MySQL-only tests report SKIP when the test DSN is absent.

- [ ] **Step 7: Commit Task 2**

```bash
git add pkg/meta/maintenance_ownership.go pkg/meta/maintenance_ownership_test.go pkg/meta/maintenance_store.go
git commit -m "feat: add durable maintenance reservations"
```

### Task 3: Reserve ownership through the job lifecycle

**Files:**
- Modify: `pkg/connector/interfaces.go`
- Modify: `pkg/core/job.go`
- Modify: `pkg/core/manager.go`
- Modify: `pkg/core/manager_persistence_test.go`
- Modify: `pkg/connectors/iceberg/register.go`
- Modify: `pkg/connectors/iceberg/sink.go`
- Modify: `pkg/connectors/iceberg/maintenance_signals.go`
- Modify: `pkg/connectors/iceberg/maintenance_worker.go`
- Test: `pkg/connectors/iceberg/maintenance_ownership_test.go`

**Interfaces:**
- Consumes: reservation projection from Task 1 and reservation store from Task 2.
- Produces: generic `connector.MaintenanceOwnershipLifecycle`, job-context source/submission fields, preflight reservation, snapshot release, streaming stop/delete release, and paused-job retention.

- [ ] **Step 1: Write failing lifecycle tests**

Add a fake lifecycle implementing counters and errors:

```go
type fakeMaintenanceOwnershipLifecycle struct {
    reserveCalls int
    releaseCalls int
    completeCalls int
    reserveErr error
}

func TestMaintenanceOwnershipReleaseRules(t *testing.T) {
    cases := []struct {
        name string
        mode config.JobMode
        status JobStatus
        deleted bool
        want bool
    }{
        {"paused streaming", config.JobModeLatest, JobStatusPaused, false, false},
        {"stopped streaming", config.JobModeLatest, JobStatusStopped, false, true},
        {"failed streaming", config.JobModeLatest, JobStatusFailed, false, true},
        {"failed snapshot", config.JobModeSnapshotOnly, JobStatusFailed, false, false},
        {"stopped snapshot", config.JobModeSnapshotOnly, JobStatusStopped, false, false},
        {"completed snapshot", config.JobModeSnapshotOnly, JobStatusDone, false, true},
        {"deleted snapshot", config.JobModeSnapshotOnly, JobStatusFailed, true, true},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            if got := shouldReleaseMaintenanceOwnership(tc.mode, tc.status, tc.deleted); got != tc.want {
                t.Fatalf("release = %t, want %t", got, tc.want)
            }
        })
    }
}

func TestJobReservesOwnershipBeforePreflight(t *testing.T) {
    order := []string{}
    lifecycle := &recordingOwnershipLifecycle{record: func(v string) { order = append(order, v) }}
    job := newPreflightOrderTestJob(t, lifecycle, func() { order = append(order, "preflight") })
    if err := job.startWithMode(config.JobModeLatest); err != nil {
        t.Fatal(err)
    }
    if got, want := strings.Join(order[:2], ","), "reserve,preflight"; got != want {
        t.Fatalf("order = %q, want %q", got, want)
    }
}
```

- [ ] **Step 2: Run lifecycle tests and confirm failure**

Run:

```bash
go test ./pkg/core -run 'TestJobReservesOwnership|TestMaintenanceOwnershipReleaseRules'
```

Expected: FAIL because the lifecycle interface and callbacks do not exist.

- [ ] **Step 3: Add the connector lifecycle interface and job context fields**

```go
type MaintenanceOwnershipLifecycle interface {
    Reserve(context.Context) error
    SnapshotCompleted(context.Context) error
    Release(context.Context) error
}

type MaintenanceOwnershipProvider interface {
    MaintenanceOwnershipLifecycle() MaintenanceOwnershipLifecycle
}
```

Extend `connector.JobContext` with `SubmissionID`, `SourceType`, and `SourceConfig`. Populate them in `Job.startWithMode`.

- [ ] **Step 4: Implement Iceberg ownership coordination**

In `pkg/connectors/iceberg/maintenance_ownership.go`, add a coordinator that uses the shared maintenance store and the projected selectors:

```go
type icebergMaintenanceOwnership struct {
    store        *meta.IcebergMaintenanceStore
    ownerJobID   string
    submissionID string
    kind         string
    selectors    []meta.IcebergMaintenanceReservationSelector
}
```

`Reserve` calls `SyncMaintenanceReservations`; `SnapshotCompleted` releases only when `StoredMode == snapshot-only`; `Release` uses the submission fence. The Iceberg sink exposes the lifecycle through `MaintenanceOwnershipLifecycle()`.

The coordinator classifies only `snapshot-only` as reservation kind
`snapshot`; initial/snapshot-handoff jobs retain a `streaming` reservation
through their CDC handoff. `SnapshotCompleted` must query the durable
`job_snapshots.done` record using `MetaKey` before releasing; observing an
in-memory completion event alone is insufficient.

- [ ] **Step 5: Wire lifecycle transitions**

In `Job.startWithMode`, call `Reserve` after sink construction and before table preflight. Store the lifecycle on the `Job`. Apply these terminal rules in the manager/status path:

```text
PAUSED streaming     -> keep reservation
STOPPED streaming    -> release
FAILED streaming     -> release
DONE snapshot-only   -> release after durable snapshot completion
FAILED/STOPPED snapshot-only -> keep
DELETE any job       -> force release after stopping the pipeline
```

An `ErrMaintenanceOwnershipBusy` start failure remains retryable and must not start source or sink goroutines.

- [ ] **Step 6: Reconcile reservations from durable job state**

Add `reconcilePersistedMaintenanceReservations` to the existing maintenance
state-sync cycle. Its keep/release rules are:

```go
func reservationMustRemain(job meta.PersistedJob, snapshotDone bool) bool {
    if normalizeMaintenanceMode(job.Config.Mode) == config.JobModeSnapshotOnly {
        return !snapshotDone
    }
    if strings.EqualFold(job.LastStatus, "PAUSED") {
        return true
    }
    return job.DesiredState == meta.DesiredStateRunning
}
```

For every kept job, re-project and synchronize its selectors. Then call
`ReleaseMaintenanceReservationsExcept` with the current job/submission map so
reservations from deleted rows or older submissions cannot survive a crash.
Snapshot-only failed/stopped rows stay in the keep map until deletion.

Add a worker test that seeds one current reservation and one reservation whose
job row is absent, runs reconciliation, and asserts only the absent owner's
reservation becomes inactive.

- [ ] **Step 7: Run lifecycle and package tests**

Run:

```bash
go test ./pkg/core -run 'TestJobReservesOwnership|TestMaintenanceOwnershipReleaseRules'
go test ./pkg/connectors/iceberg -run 'TestMaintenanceOwnership'
go test ./pkg/core ./pkg/connectors/iceberg
```

Expected: PASS.

- [ ] **Step 8: Commit Task 3**

```bash
git add pkg/connector/interfaces.go pkg/core/job.go pkg/core/manager.go pkg/core/manager_persistence_test.go pkg/connectors/iceberg/register.go pkg/connectors/iceberg/sink.go pkg/connectors/iceberg/maintenance_signals.go pkg/connectors/iceberg/maintenance_ownership.go pkg/connectors/iceberg/maintenance_ownership_test.go pkg/connectors/iceberg/maintenance_worker.go
git commit -m "feat: reserve maintenance ownership before ingestion"
```

### Task 4: Make maintenance claims atomic with ownership

**Files:**
- Modify: `pkg/meta/maintenance_operation_claim.go`
- Modify: `pkg/meta/maintenance_store.go`
- Modify: `pkg/connectors/iceberg/maintenance_executor_pool.go`
- Modify: `pkg/connectors/iceberg/maintenance_worker.go`
- Modify: `pkg/connectors/iceberg/maintenance_worker_bounded.go`
- Test: `pkg/meta/maintenance_ownership_test.go`
- Test: `pkg/connectors/iceberg/maintenance_worker_test.go`

**Interfaces:**
- Consumes: catalog guards and reservations from Task 2.
- Produces: task/inventory claims that verify concrete owner and reservation status atomically; execution paths no longer reload all jobs for a last-second exclusion check.

- [ ] **Step 1: Write failing stale-owner and no-reload tests**

Add tests proving a task cannot be claimed when `task.owner_job_id != state.owner_job_id`, and instrument the fake `JobStore` to prove processing a claimed task does not call `LoadJobs`:

```go
func TestClaimRejectsTaskAfterOwnerTransfer(t *testing.T) {
    store, tableKey := newMaintenanceOwnershipIntegrationStore(t)
    seedQueuedMonitorTask(t, store, tableKey, "monitor:general", nil)
    setStateOwner(t, store, tableKey, "stream-orders")
    tasks, err := store.ClaimTasksForOperation(context.Background(), "worker-1", time.Now(), time.Minute, "compact", 1)
    if err != nil {
        t.Fatal(err)
    }
    if len(tasks) != 0 {
        t.Fatalf("claimed %d stale tasks, want 0", len(tasks))
    }
    if got := taskStatus(t, store, tableKey); got != MaintenanceTaskCancelled {
        t.Fatalf("task status = %q, want cancelled", got)
    }
}

func TestProcessClaimedMonitorTaskDoesNotReloadAllJobs(t *testing.T) {
    store := &countingJobStore{}
    // Process a task using the already-synchronized job map.
    if store.loadCalls != 0 {
        t.Fatalf("LoadJobs calls = %d, want 0", store.loadCalls)
    }
}
```

- [ ] **Step 2: Run focused tests and confirm failure**

Run:

```bash
go test ./pkg/meta ./pkg/connectors/iceberg -run 'TestClaimRejectsTaskAfterOwnerTransfer|TestProcessClaimedMonitorTaskDoesNotReloadAllJobs'
```

Expected: FAIL under the current claim and execution behavior.

- [ ] **Step 3: Harden task and inventory claims**

Update claims so they:

1. Select the concrete state and task.
2. Lock `iceberg_maintenance_catalog_guards` for `state.catalog`.
3. Re-read state ownership and active reservations inside the transaction.
4. Cancel a stale task rather than lease it.
5. Acquire task and table leases only after all checks pass.

Add `state.owner_job_id = task.owner_job_id` to due-task selection and retain the existing paused/deleted monitor checks.

- [ ] **Step 4: Remove configuration-based last-second checks**

Delete the per-task `jobStore.LoadJobs`/stream-exclusion scan from both executor paths. Use the atomic database claim as the safety boundary. Keep the synchronized in-memory job map only for execution settings.

- [ ] **Step 5: Run focused, package, and race tests**

Run:

```bash
go test ./pkg/meta ./pkg/connectors/iceberg -run 'TestClaimRejectsTaskAfterOwnerTransfer|TestProcessClaimedMonitorTaskDoesNotReloadAllJobs'
go test ./pkg/meta ./pkg/connectors/iceberg
go test -race ./pkg/meta ./pkg/connectors/iceberg
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add pkg/meta/maintenance_operation_claim.go pkg/meta/maintenance_store.go pkg/meta/maintenance_ownership_test.go pkg/connectors/iceberg/maintenance_executor_pool.go pkg/connectors/iceberg/maintenance_worker.go pkg/connectors/iceberg/maintenance_worker_bounded.go pkg/connectors/iceberg/maintenance_worker_test.go
git commit -m "fix: make maintenance claims ownership-safe"
```

### Task 5: Persist monitor membership and reconcile only differences

**Files:**
- Modify: `pkg/meta/maintenance_store.go`
- Modify: `pkg/meta/maintenance_monitor_store.go`
- Create: `pkg/meta/maintenance_monitor_target_store.go`
- Test: `pkg/meta/maintenance_store_test.go`
- Modify: `pkg/connectors/iceberg/maintenance_monitor.go`
- Modify: `pkg/connectors/iceberg/maintenance_worker.go`
- Test: `pkg/connectors/iceberg/maintenance_monitor_test.go`
- Test: `pkg/connectors/iceberg/maintenance_worker_test.go`

**Interfaces:**
- Consumes: discovered `maintenanceMonitorTarget` values, reservation matching, concrete ownership, and monitor configuration.
- Produces: persisted last-success time, durable monitor memberships, delta reconciliation, stale retirement, deterministic monitor precedence, `monitorSpecificity`, `maintenanceDiscoveryDelta`, and `preferMonitorOwner`.

- [ ] **Step 1: Write failing specificity and delta tests**

```go
func TestMonitorSpecificityOrder(t *testing.T) {
    if !(specificityTable > specificityNamespace &&
        specificityNamespace > specificityCatalog &&
        specificityCatalog > specificityMetalake) {
        t.Fatal("monitor specificity order is invalid")
    }
}

func TestMonitorDiscoveryDeltaLeavesUnchangedTargetsUntouched(t *testing.T) {
    previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
    delta := maintenanceDiscoveryDelta(previous, previous)
    if len(delta.Added) != 0 || len(delta.Removed) != 0 {
        t.Fatalf("unexpected delta: %#v", delta)
    }
}

func TestFailedDiscoveryDoesNotRetireTargets(t *testing.T) {
    previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
    got := targetsAfterDiscovery(previous, nil, errors.New("gravitino timeout"))
    if !reflect.DeepEqual(got, previous) {
        t.Fatalf("targets = %#v, want previous %#v", got, previous)
    }
}

func TestSuccessfulEmptyDiscoveryRetiresTargets(t *testing.T) {
    previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
    delta := maintenanceDiscoveryDelta(previous, []maintenanceMonitorTarget{})
    if len(delta.Removed) != 1 || delta.Removed[0] != previous[0] {
        t.Fatalf("removed = %#v, want %#v", delta.Removed, previous)
    }
}

func TestEqualSpecificityOwnershipIsStable(t *testing.T) {
    current := monitorOwnershipCandidate{OwnerID: "monitor:first", Specificity: specificitySchema}
    candidate := monitorOwnershipCandidate{OwnerID: "monitor:second", Specificity: specificitySchema}
    if got := preferMonitorOwner(current, candidate); got != current {
        t.Fatalf("winner = %#v, want stable current owner %#v", got, current)
    }
    if got := preferMonitorOwner(candidate, current); got != candidate {
        t.Fatalf("winner = %#v, want stable current owner %#v", got, candidate)
    }
}
```

- [ ] **Step 2: Run focused tests and confirm failure**

Run:

```bash
go test ./pkg/connectors/iceberg -run 'TestMonitorSpecificity|TestMonitorDiscoveryDelta|TestFailedDiscovery|TestSuccessfulEmpty|TestEqualSpecificity'
```

Expected: FAIL because durable delta/specificity behavior is absent.

- [ ] **Step 3: Add monitor membership schema and model**

Add `last_discovery_at DATETIME(6) NULL` and `last_discovery_error LONGTEXT NULL` to `iceberg_maintenance_monitors`. Add:

```sql
CREATE TABLE IF NOT EXISTS iceberg_maintenance_monitor_targets (
  monitor_id VARCHAR(255) NOT NULL,
  table_key VARCHAR(512) NOT NULL,
  catalog VARCHAR(255) NOT NULL,
  namespace_name VARCHAR(512) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  specificity INT NOT NULL,
  claim_status VARCHAR(32) NOT NULL,
  last_error LONGTEXT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (monitor_id, table_key),
  INDEX idx_monitor_target_table (table_key, specificity, claim_status)
);
```

Define `IcebergMaintenanceMonitorTarget` and typed claim statuses `owned`,
`reserved`, `conflicted`, and `retired`. Track exclusions as an
`excluded_scope_count` on the monitor so catalog/namespace exclusions can be
reported without enumerating all tables under an intentionally skipped scope.

- [ ] **Step 4: Implement membership reads and successful-discovery delta**

Add:

```go
func (s *IcebergMaintenanceStore) ListMonitorTargets(
    ctx context.Context, monitorID string,
) ([]IcebergMaintenanceMonitorTarget, error)

func (s *IcebergMaintenanceStore) ApplyMonitorDiscovery(
    ctx context.Context,
    monitor IcebergMaintenanceMonitor,
    targets []IcebergMaintenanceMonitorTarget,
    now time.Time,
) (IcebergMaintenanceMonitorDelta, error)

func (s *IcebergMaintenanceStore) RecordMonitorDiscoveryFailure(
    ctx context.Context, monitorID, message string, now time.Time,
) error
```

`ApplyMonitorDiscovery` must insert only added memberships, update only changed statuses/owners, retire only removed memberships, and write `last_discovery_at` once. It must lock the catalog guard before concrete ownership transfer and use specificity to choose the winner.

- [ ] **Step 5: Replace in-memory monitor reconciliation**

In `syncMaintenanceMonitorStates`:

- Read persisted `LastDiscoveryAt` to decide whether discovery is due.
- On failure, call `RecordMonitorDiscoveryFailure` and continue using persisted memberships.
- On success, call `ApplyMonitorDiscovery`.
- Build worker targets from active persisted memberships.
- Do not call `UpsertState` for unchanged targets.
- Do not call `RequestTableInventoryRefresh` for bulk discovery.
- Give added targets background priority `0` and a deterministic jittered `next_inventory_check_at`.

- [ ] **Step 6: Add a 6,700-table write-volume test**

Use a fake membership repository that counts mutations:

```go
func TestRepeatedLargeDiscoveryWritesNoUnchangedTargets(t *testing.T) {
    targets := makeMaintenanceTargets(6700)
    repo := newCountingMonitorTargetRepository()
    reconcileDiscovery(t, repo, targets)
    repo.resetCounts()
    reconcileDiscovery(t, repo, targets)
    if repo.targetWrites != 0 {
        t.Fatalf("unchanged target writes = %d, want 0", repo.targetWrites)
    }
}
```

- [ ] **Step 7: Run focused and package tests**

Run:

```bash
go test ./pkg/connectors/iceberg ./pkg/meta -run 'TestMonitor|TestRepeatedLargeDiscovery|TestFailedDiscovery|TestSuccessfulEmpty|TestEqualSpecificity'
go test ./pkg/connectors/iceberg ./pkg/meta
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

```bash
git add pkg/meta/maintenance_store.go pkg/meta/maintenance_monitor_store.go pkg/meta/maintenance_monitor_target_store.go pkg/meta/maintenance_store_test.go pkg/connectors/iceberg/maintenance_monitor.go pkg/connectors/iceberg/maintenance_monitor_test.go pkg/connectors/iceberg/maintenance_worker.go pkg/connectors/iceberg/maintenance_worker_test.go
git commit -m "feat: reconcile maintenance discovery by delta"
```

### Task 6: Allow layered monitors and expose clear status

**Files:**
- Modify: `pkg/api/maintenance_monitor_handlers.go`
- Modify: `pkg/api/maintenance_monitor_handlers_test.go`
- Modify: `pkg/meta/maintenance_monitor_store.go`
- Modify: `ui/maintenance-monitors.js`
- Modify: `docs/maintenance-monitors.md`

**Interfaces:**
- Consumes: persisted target claim statuses and specificity from Task 5.
- Produces: coexistence of Metalake/catalog/schema/table monitors and user-visible discovery/ownership counts.

- [ ] **Step 1: Write failing API coexistence and status tests**

```go
func TestCreateSpecificMonitorAlongsideMetalakeMonitor(t *testing.T) {
    repo := newMemoryMaintenanceMonitorRepository()
    repo.monitors = append(repo.monitors, metalakeMonitor("general"))
    rr := submitMaintenanceMonitor(t, repo, tableMonitor("orders-special"))
    if rr.Code != http.StatusCreated {
        t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
    }
}

func TestMonitorResponseIncludesOwnershipCounts(t *testing.T) {
    // Assert discovered, owned, reserved, excluded, conflicted, and retired.
}
```

- [ ] **Step 2: Run API tests and confirm failure**

Run:

```bash
go test ./pkg/api -run 'TestCreateSpecificMonitorAlongsideMetalakeMonitor|TestMonitorResponseIncludesOwnershipCounts'
```

Expected: FAIL because the handler currently rejects dynamic/static coexistence and does not return the counts.

- [ ] **Step 3: Remove the blanket coexistence rejection**

Delete the `auto:` catalog-label overlap rule from the create handler. Retain validation that one monitor config cannot define both explicit tables and dynamic catalog monitoring. Let transactional specificity resolve cross-monitor overlap.

- [ ] **Step 4: Return and render monitor status counts**

Extend `IcebergMaintenanceMonitor` with:

```go
DiscoveredCount int        `json:"discovered_count"`
OwnedCount      int        `json:"owned_count"`
ReservedCount   int        `json:"reserved_count"`
ExcludedScopeCount int     `json:"excluded_scope_count"`
ConflictCount   int        `json:"conflict_count"`
RetiredCount    int        `json:"retired_count"`
LastDiscoveryAt *time.Time `json:"last_discovery_at,omitempty"`
```

Aggregate table counts from `iceberg_maintenance_monitor_targets` and the
excluded-scope count from validated monitor configuration. Update the monitor
list/details UI with plain terms: “Discovered”, “Maintained here”, “Used by
streaming/snapshot”, “Excluded scopes”, and “Owned by another monitor”.

- [ ] **Step 5: Update operator documentation**

Document one general Metalake example, the `asmat.analytics` immutable exclusion, automatic streaming/snapshot protection, and precedence examples. Explicitly state that `catalog_name` is a Spark alias while physical ownership comes from `warehouse`.

- [ ] **Step 6: Run API/UI-adjacent tests and diff validation**

Run:

```bash
go test ./pkg/api ./pkg/meta ./pkg/connectors/iceberg
git diff --check
```

Expected: PASS.

- [ ] **Step 7: Commit Task 6**

```bash
git add pkg/api/maintenance_monitor_handlers.go pkg/api/maintenance_monitor_handlers_test.go pkg/meta/maintenance_monitor_store.go ui/maintenance-monitors.js docs/maintenance-monitors.md
git commit -m "feat: support layered maintenance monitors"
```

### Task 7: End-to-end regression and completion verification

**Files:**
- Modify only files required to fix failures directly caused by Tasks 1-6.

**Interfaces:**
- Consumes: the complete implementation.
- Produces: verified repository state with no known P0/P1 findings from the original review.

- [ ] **Step 1: Run focused ownership suites**

```bash
go test ./pkg/connectors/iceberg ./pkg/meta ./pkg/core ./pkg/api
```

Expected: PASS.

- [ ] **Step 2: Run race tests on concurrent ownership paths**

```bash
go test -race ./pkg/connectors/iceberg ./pkg/meta ./pkg/core ./pkg/api
```

Expected: PASS. A macOS linker warning is acceptable only when the test command exits successfully.

- [ ] **Step 3: Run the complete suite serially**

```bash
go test -vet=off -p=1 ./...
```

Expected: PASS.

- [ ] **Step 4: Run static and whitespace validation**

```bash
go vet ./...
git diff --check
```

Expected: PASS with no diagnostics.

- [ ] **Step 5: Verify deployment example without printing secrets**

Inspect only the maintenance section of `/Users/gerin/app/rivus/configs/maintenance.yaml`. Confirm it contains one Metalake monitor, the intended exclusions, and no obsolete explicit monitors. Do not print `.env` or credential-bearing YAML values.

- [ ] **Step 6: Request final code review**

Use `superpowers:requesting-code-review` with the design, this plan, the original P0/P1 findings, and the final diff. Resolve every blocking finding with `superpowers:receiving-code-review` and rerun the affected tests.

- [ ] **Step 7: Commit final regression fixes if needed**

Inspect `git diff --name-only`, stage only files changed to resolve review or
verification failures, and commit them with:

```bash
git commit -m "fix: close dynamic maintenance regressions"
```

- [ ] **Step 8: Re-run completion verification**

Use `superpowers:verification-before-completion`, repeat Steps 1-4, and report the exact commands and outcomes.
