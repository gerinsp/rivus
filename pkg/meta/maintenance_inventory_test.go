package meta

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestMaintenanceInventoryPersistsPartitioningWithoutGuessingUnknownTables(t *testing.T) {
	store, _, key := newMaintenanceOwnershipIntegrationStore(t)
	ctx := context.Background()
	for _, fields := range [][]string{nil, {}, {"date"}, {"day(created_at)", "bucket[16](id)"}} {
		if err := store.UpdateInventory(ctx, key, 42, 648, 648, 6000000, 0, 0, 0, 0, 0, 0, 0, fields); err != nil {
			t.Fatal(err)
		}
		state, err := store.GetState(ctx, key)
		if err != nil || !reflect.DeepEqual(state.PartitionFields, fields) {
			t.Fatalf("partition metadata=%+v error=%v", state, err)
		}
		states, err := store.ListStatesForOwner(ctx, state.OwnerJobID, 10)
		if err != nil || len(states) != 1 || !reflect.DeepEqual(states[0].PartitionFields, fields) {
			t.Fatalf("owner partition metadata=%+v error=%v", states, err)
		}
		now := time.Now().UTC()
		if _, err := store.RequestInventoryRefresh(ctx, state.OwnerJobID, now, true); err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimPendingInventoryStateForMaintenance(ctx, "partition-worker", now, time.Minute, 100)
		if err != nil || claimed == nil || claimed.TableKey != key || !reflect.DeepEqual(claimed.PartitionFields, fields) {
			t.Fatalf("claimed partition metadata=%+v error=%v", claimed, err)
		}
		if err := store.FinishInventoryClaim(ctx, key, "partition-worker", claimed.LastSnapshotID); err != nil {
			t.Fatal(err)
		}
	}
	// Re-running schema initialization preserves the collected partition data.
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetState(ctx, key)
	if err != nil || len(state.PartitionFields) != 2 {
		t.Fatalf("initialization lost partition data: %+v error=%v", state, err)
	}
	if err := store.MarkInventoryMissing(ctx, key); err != nil {
		t.Fatal(err)
	}
	state, err = store.GetState(ctx, key)
	if err != nil || state.PartitionFields != nil {
		t.Fatalf("missing table kept stale partition data: %+v error=%v", state, err)
	}
}

func TestMaintenanceInventoryAutomaticallyRevisitsStaleTables(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue_%t", queue), func(t *testing.T) {
			testMaintenanceInventoryAutomaticallyRevisitsStaleTables(t, queue)
		})
	}
}

func testMaintenanceInventoryAutomaticallyRevisitsStaleTables(t *testing.T, queue bool) {
	for _, scenario := range []string{"stale", "never_scanned", "fresh", "future_retry", "leased", "paused", "snapshot_pending", "priority_pass", "unmanaged", "stream-excluded", "deleted-monitor", "streaming", "snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			store, catalog, key := newMaintenanceOwnershipIntegrationStore(t)
			claim := store.ClaimPendingInventoryState
			if queue {
				claim = store.ClaimPendingInventoryStateForMaintenance
			}
			ctx := context.Background()
			now := time.Now().UTC()
			old := now.Add(-2 * MaintenanceInventoryMaxAge)
			var lastInventory, nextInventory, leaseUntil *time.Time
			lastInventory = &old
			complete, minimumPriority := true, 0
			wantClaim := scenario == "stale" || scenario == "never_scanned"
			switch scenario {
			case "never_scanned":
				lastInventory = nil
			case "fresh":
				lastInventory = &now
			case "future_retry":
				later := now.Add(time.Minute)
				nextInventory = &later
			case "leased":
				later := now.Add(time.Minute)
				leaseUntil = &later
			case "paused":
				if err := store.SetMonitorStatus(ctx, "general-"+catalog, MaintenanceMonitorPaused, now); err != nil {
					t.Fatal(err)
				}
			case "snapshot_pending":
				complete = false
			case "priority_pass":
				minimumPriority = 1
			}
			_, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
				SET last_inventory_at=?, next_inventory_check_at=?, inventory_lease_until=?, snapshot_complete=?
				WHERE table_key=?`, lastInventory, nextInventory, leaseUntil, complete, key)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "unmanaged", "stream-excluded", "deleted-monitor", "streaming", "snapshot":
				if _, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type=? WHERE table_key=?`, scenario, key); err != nil {
					t.Fatal(err)
				}
			}
			state, err := claim(ctx, "inventory-worker", now, time.Minute, minimumPriority)
			if err != nil {
				t.Fatal(err)
			}
			if (state != nil) != wantClaim {
				t.Fatalf("claimed=%+v, want claim=%t", state, wantClaim)
			}
			if !wantClaim {
				return
			}
			if state.TableKey != key {
				t.Fatalf("claimed another table: %s", state.TableKey)
			}
			// A second worker cannot scan the same stale table concurrently.
			other, err := claim(ctx, "other-worker", now, time.Minute, 0)
			if err != nil || other != nil {
				t.Fatalf("second claim=%+v error=%v", other, err)
			}
			if err := store.UpdateInventory(ctx, key, 42, 5, 5, 100, 0, 0, 0, 0, 0, 0, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.FinishInventoryClaim(ctx, key, "inventory-worker", state.LastSnapshotID); err != nil {
				t.Fatal(err)
			}
			fresh, err := store.GetState(ctx, key)
			if err != nil || fresh.NextInventoryCheckAt != nil || fresh.InventoryLeaseUntil != nil || fresh.InventorySnapshotID != 42 {
				t.Fatalf("completed scan=%+v error=%v", fresh, err)
			}
			other, err = claim(ctx, "other-worker", now, time.Minute, 0)
			if err != nil || other != nil {
				t.Fatalf("fresh inventory was immediately reclaimed: %+v error=%v", other, err)
			}
		})
	}
}

func TestMaintenanceInventoryRequestsTakePriorityOverStaleTables(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue_%t", queue), func(t *testing.T) {
			testMaintenanceInventoryRequestsTakePriorityOverStaleTables(t, queue)
		})
	}
}

func testMaintenanceInventoryRequestsTakePriorityOverStaleTables(t *testing.T, queue bool) {
	store, _, staleKey := newMaintenanceOwnershipIntegrationStore(t)
	_, _, requestedKey := newMaintenanceOwnershipIntegrationStore(t)
	claim := store.ClaimPendingInventoryState
	if queue {
		claim = store.ClaimPendingInventoryStateForMaintenance
	}
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
		SET last_inventory_at=?, next_inventory_check_at=NULL WHERE table_key=?`, now.Add(-48*time.Hour), staleKey); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
		SET inventory_priority=100, next_inventory_check_at=? WHERE table_key=?`, now, requestedKey); err != nil {
		t.Fatal(err)
	}
	state, err := claim(ctx, "worker", now, time.Minute, 0)
	if err != nil || state == nil || state.TableKey != requestedKey {
		t.Fatalf("manual request lost priority: %+v error=%v", state, err)
	}
	state, err = claim(ctx, "worker", now, time.Minute, 0)
	if err != nil || state == nil || state.TableKey != staleKey {
		t.Fatalf("background scan did not resume: %+v error=%v", state, err)
	}
}

func TestMaintenanceInventoryWaitsForTableExecutionLease(t *testing.T) {
	store, _, key := newMaintenanceOwnershipIntegrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
		SET last_inventory_at=?, next_inventory_check_at=NULL, lease_owner='compactor', lease_until=?
		WHERE table_key=?`, now.Add(-48*time.Hour), now.Add(time.Minute), key); err != nil {
		t.Fatal(err)
	}
	state, err := store.ClaimPendingInventoryStateForMaintenance(ctx, "worker", now, time.Minute, 0)
	if err != nil || state != nil {
		t.Fatalf("inventory raced a maintenance task: %+v error=%v", state, err)
	}
	state, err = store.ClaimPendingInventoryStateForMaintenance(ctx, "worker", now.Add(2*time.Minute), time.Minute, 0)
	if err != nil || state == nil || state.TableKey != key {
		t.Fatalf("inventory did not resume after lease expiry: %+v error=%v", state, err)
	}
}
