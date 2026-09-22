package meta

import (
	"context"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

func TestApplyMonitorDiscoveryDoesNotRewriteUnchangedTargetOrState(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	monitor := IcebergMaintenanceMonitor{
		ID:     "delta-" + catalog,
		Name:   "Delta monitor",
		Status: MaintenanceMonitorActive,
		Config: &config.JobConfig{},
	}
	if err := store.CreateMonitor(context.Background(), monitor); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitor_targets WHERE monitor_id=?`, monitor.ID)
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitors WHERE monitor_id=?`, monitor.ID)
	})

	now := time.Now().UTC().Truncate(time.Microsecond)
	inventory := now.Add(time.Hour)
	target := IcebergMaintenanceMonitorTarget{
		MonitorID: monitor.ID, TableKey: tableKey, Catalog: catalog, Namespace: "sales", Table: "orders",
		Specificity: 1, NextInventoryAt: &inventory,
		NextCompactionAt: now.Add(2 * time.Hour), NextExpireAt: now.Add(3 * time.Hour), NextOrphanAt: now.Add(4 * time.Hour),
	}
	if _, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now); err != nil {
		t.Fatal(err)
	}
	var firstTargetUpdate, firstStateUpdate time.Time
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND table_key=?`, monitor.ID, tableKey).Scan(&firstTargetUpdate); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_state
		WHERE table_key=?`, tableKey).Scan(&firstStateUpdate); err != nil {
		t.Fatal(err)
	}

	if delta, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	} else if delta.Added != 0 || delta.Removed != 0 {
		t.Fatalf("unchanged discovery delta = %#v", delta)
	}
	var secondTargetUpdate, secondStateUpdate time.Time
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND table_key=?`, monitor.ID, tableKey).Scan(&secondTargetUpdate); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_state
		WHERE table_key=?`, tableKey).Scan(&secondStateUpdate); err != nil {
		t.Fatal(err)
	}
	if !secondTargetUpdate.Equal(firstTargetUpdate) || !secondStateUpdate.Equal(firstStateUpdate) {
		t.Fatalf("unchanged discovery rewrote rows: target %s -> %s, state %s -> %s",
			firstTargetUpdate, secondTargetUpdate, firstStateUpdate, secondStateUpdate)
	}

	if _, err := store.db.ExecContext(context.Background(), `UPDATE iceberg_maintenance_state
		SET owner_type='unmanaged', owner_job_id='unmanaged', snapshot_complete=0 WHERE table_key=?`, tableKey); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetState(context.Background(), tableKey)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.OwnerJobID != MaintenanceMonitorOwnerID(monitor.ID) || !state.SnapshotComplete {
		t.Fatalf("reclaimed state = %#v, want monitor ownership with snapshot inventory required", state)
	}
}
