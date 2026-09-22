package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

func TestMaintenanceReservationMatchesCatalogScopedPatterns(t *testing.T) {
	reservation := IcebergMaintenanceReservation{
		IcebergMaintenanceReservationSelector: IcebergMaintenanceReservationSelector{
			Catalog: "asmat", NamespacePattern: "orders_*", TablePattern: "*",
		},
		Active: true,
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

func TestValidateReservationSelectorRejectsInvalidGlob(t *testing.T) {
	selector := IcebergMaintenanceReservationSelector{
		Catalog: "asmat", NamespacePattern: "sales[", TablePattern: "*",
	}
	if err := selector.Validate(); err == nil {
		t.Fatal("expected invalid glob error")
	}
}

func TestPreferredMaintenanceReservationPrioritizesStreaming(t *testing.T) {
	snapshot := IcebergMaintenanceReservation{Kind: MaintenanceReservationSnapshot, OwnerJobID: "snapshot"}
	streaming := IcebergMaintenanceReservation{Kind: MaintenanceReservationStreaming, OwnerJobID: "streaming"}
	if got := preferredMaintenanceReservation(snapshot, streaming); got.OwnerJobID != "streaming" {
		t.Fatalf("preferred owner = %q, want streaming", got.OwnerJobID)
	}
}

func TestReservationBeforeClaimCancelsMonitorTask(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	seedCurrentOwnershipJob(t, store, "stream-orders", "submission-1")
	seedQueuedMonitorTask(t, store, tableKey, MaintenanceMonitorOwnerID("general-"+catalog), nil)
	err := store.SyncMaintenanceReservations(
		context.Background(),
		"stream-orders",
		"submission-1",
		MaintenanceReservationStreaming,
		[]IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "orders"}},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := maintenanceTaskStatus(t, store, tableKey); got != MaintenanceTaskCancelled {
		t.Fatalf("task status = %q, want cancelled", got)
	}
	state, err := store.GetState(context.Background(), tableKey)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.OwnerJobID != "stream-orders" {
		t.Fatalf("state owner = %#v, want stream-orders", state)
	}
}

func TestReservationDuringActiveLeaseReturnsBusy(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	seedCurrentOwnershipJob(t, store, "stream-orders", "submission-1")
	leaseUntil := time.Now().Add(time.Minute)
	seedQueuedMonitorTask(t, store, tableKey, MaintenanceMonitorOwnerID("general-"+catalog), &leaseUntil)
	err := store.SyncMaintenanceReservations(
		context.Background(),
		"stream-orders",
		"submission-1",
		MaintenanceReservationStreaming,
		[]IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "orders"}},
		time.Now(),
	)
	if !errors.Is(err, ErrMaintenanceOwnershipBusy) {
		t.Fatalf("error = %v, want ErrMaintenanceOwnershipBusy", err)
	}
}

func TestClaimRejectsTaskAfterOwnerTransfer(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	seedQueuedMonitorTask(t, store, tableKey, MaintenanceMonitorOwnerID("general-"+catalog), nil)
	if _, err := store.db.ExecContext(context.Background(), `UPDATE iceberg_maintenance_state
		SET owner_type='streaming', owner_job_id='stream-orders' WHERE table_key=?`, tableKey); err != nil {
		t.Fatal(err)
	}

	tasks, err := store.ClaimTasksForOperation(
		context.Background(), "worker-1", time.Now(), time.Minute, "compact", 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("claimed %d stale tasks, want 0", len(tasks))
	}
	if got := maintenanceTaskStatus(t, store, tableKey); got != MaintenanceTaskCancelled {
		t.Fatalf("task status = %q, want %q", got, MaintenanceTaskCancelled)
	}
}

func TestLegacyClaimRejectsTaskAfterOwnerTransfer(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	seedQueuedMonitorTask(t, store, tableKey, MaintenanceMonitorOwnerID("general-"+catalog), nil)
	if _, err := store.db.ExecContext(context.Background(), `UPDATE iceberg_maintenance_state
		SET owner_type='streaming', owner_job_id='stream-orders' WHERE table_key=?`, tableKey); err != nil {
		t.Fatal(err)
	}

	tasks, err := store.ClaimTasks(context.Background(), "worker-1", time.Now(), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("claimed %d stale tasks, want 0", len(tasks))
	}
	if got := maintenanceTaskStatus(t, store, tableKey); got != MaintenanceTaskCancelled {
		t.Fatalf("task status = %q, want %q", got, MaintenanceTaskCancelled)
	}
}

func TestOldSubmissionCannotReleaseNewReservation(t *testing.T) {
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	seedCurrentOwnershipJob(t, store, "stream-orders", "submission-2")
	selector := []IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "orders"}}
	if err := store.SyncMaintenanceReservations(context.Background(), "stream-orders", "submission-2", MaintenanceReservationStreaming, selector, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseMaintenanceReservations(context.Background(), "stream-orders", "submission-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := activeMaintenanceReservationCount(t, store, "stream-orders", "submission-2"); got != 1 {
		t.Fatalf("active reservations = %d, want 1", got)
	}
}

func TestStaleSubmissionCannotCreateReservations(t *testing.T) {
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	seedCurrentOwnershipJob(t, store, "stream-orders", "submission-2")
	selector := []IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "orders"}}
	if err := store.SyncMaintenanceReservations(context.Background(), "stream-orders", "submission-1", MaintenanceReservationStreaming, selector, time.Now()); !errors.Is(err, ErrMaintenanceOwnershipStale) {
		t.Fatalf("error = %v, want ErrMaintenanceOwnershipStale", err)
	}
	if got := activeMaintenanceReservationCount(t, store, "stream-orders", "submission-2"); got != 0 {
		t.Fatalf("active reservations = %d, want 0", got)
	}
}

func newMaintenanceOwnershipIntegrationStore(t *testing.T) (*IcebergMaintenanceStore, string, string) {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("RIVUS_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("RIVUS_TEST_MYSQL_DSN is not set")
	}
	store, err := NewIcebergMaintenanceStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Init(context.Background()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	jobStore, err := NewMySQLJobStore(dsn)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := jobStore.Init(context.Background()); err != nil {
		_ = jobStore.db.Close()
		store.Close()
		t.Fatal(err)
	}
	suffix := time.Now().UTC().UnixNano()
	catalog := fmt.Sprintf("test_ownership_%d", suffix)
	monitorID := "general-" + catalog
	tableKey := strings.ToLower(catalog + ".sales.orders")
	now := time.Now().UTC()
	if err := store.UpsertState(context.Background(), IcebergMaintenanceState{
		TableKey: tableKey, Catalog: catalog, Namespace: "sales", Table: "orders",
		OwnerType: "monitor", OwnerJobID: MaintenanceMonitorOwnerID(monitorID), SnapshotComplete: true,
		NextInventoryCheckAt: &now,
	}, now, now, now); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.CreateMonitor(context.Background(), IcebergMaintenanceMonitor{
		ID: monitorID, Name: "General", Status: MaintenanceMonitorActive, Config: &config.JobConfig{},
	}); err != nil {
		_ = jobStore.db.Close()
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.db.ExecContext(ctx, `DELETE FROM iceberg_maintenance_tasks WHERE table_key=?`, tableKey)
		_, _ = store.db.ExecContext(ctx, `DELETE FROM iceberg_maintenance_state WHERE table_key=?`, tableKey)
		_, _ = store.db.ExecContext(ctx, `DELETE FROM iceberg_maintenance_reservations WHERE catalog=?`, catalog)
		_, _ = store.db.ExecContext(ctx, `DELETE FROM iceberg_maintenance_catalog_guards WHERE catalog=?`, catalog)
		_, _ = store.db.ExecContext(ctx, `DELETE FROM iceberg_maintenance_monitors WHERE monitor_id=?`, monitorID)
		_ = store.Close()
		_ = jobStore.db.Close()
	})
	return store, catalog, tableKey
}

func seedCurrentOwnershipJob(t *testing.T, store *IcebergMaintenanceStore, ownerID, submissionID string) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `INSERT INTO job_registry
		(job_id, submission_id, job_name, config_json, desired_state, execution_role, last_status, created_at, updated_at)
		VALUES (?, ?, ?, '{}', 'RUNNING', 'STREAMING', 'RUNNING', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE submission_id=VALUES(submission_id), desired_state='RUNNING',
		execution_role='STREAMING', last_status='RUNNING', updated_at=UTC_TIMESTAMP(6)`, ownerID, submissionID, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM job_registry WHERE job_id=?`, ownerID)
	})
}

func seedQueuedMonitorTask(t *testing.T, store *IcebergMaintenanceStore, tableKey, ownerID string, leaseUntil *time.Time) {
	t.Helper()
	now := time.Now().UTC()
	_, err := store.db.ExecContext(context.Background(), `INSERT INTO iceberg_maintenance_tasks
		(idempotency_key, table_key, owner_job_id, operation, priority, status, attempt_count,
		 not_before, schedule_window, created_at, updated_at)
		VALUES (?, ?, ?, 'compact', 100, ?, 0, ?, 'test', ?, ?)`,
		fmt.Sprintf("test-%d", now.UnixNano()), tableKey, ownerID, MaintenanceTaskQueued, now, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if leaseUntil != nil {
		if _, err := store.db.ExecContext(context.Background(), `UPDATE iceberg_maintenance_state
			SET lease_owner='test-worker', lease_until=? WHERE table_key=?`, leaseUntil.UTC(), tableKey); err != nil {
			t.Fatal(err)
		}
	}
}

func maintenanceTaskStatus(t *testing.T, store *IcebergMaintenanceStore, tableKey string) string {
	t.Helper()
	var status string
	if err := store.db.QueryRowContext(context.Background(), `SELECT status FROM iceberg_maintenance_tasks
		WHERE table_key=? ORDER BY id DESC LIMIT 1`, tableKey).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func activeMaintenanceReservationCount(t *testing.T, store *IcebergMaintenanceStore, ownerID, submissionID string) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM iceberg_maintenance_reservations
		WHERE owner_job_id=? AND submission_id=? AND active=1`, ownerID, submissionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
