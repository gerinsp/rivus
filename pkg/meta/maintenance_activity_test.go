package meta

import (
	"context"
	"testing"
	"time"
)

func TestInventoryWriteActivityPreservesSignalsAndCommitTime(t *testing.T) {
	store, _, key := newMaintenanceOwnershipIntegrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-10 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	for _, tc := range []struct {
		name    string
		written *time.Time
		want    time.Time
	}{
		{"first scan of old table stays old", &old, old},
		{"external write reactivates table", &recent, recent},
		{"unchanged scan preserves commit time", &recent, recent},
		{"older inventory cannot regress signal", &old, recent},
		{"missing history preserves activity", nil, recent},
		{"future metadata cannot poison activity", &future, recent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.RecordTableWrite(ctx, key, tc.written); err != nil {
				t.Fatal(err)
			}
			state, err := store.GetState(ctx, key)
			if err != nil || state == nil || state.LastWriteAt == nil || !state.LastWriteAt.Equal(tc.want) {
				t.Fatalf("activity=%+v err=%v, want %s", state, err, tc.want)
			}
			if state.LastSnapshotID != 0 {
				t.Fatal("inventory activity changed the CDC signal cursor")
			}
		})
	}
	// A newer CDC signal wins over an inventory scan of an older snapshot.
	_, err := store.CoalesceSignal(ctx, key, 42, 1, 0, true, 50, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTableWrite(ctx, key, &recent); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetState(ctx, key)
	if err != nil || state == nil || state.LastWriteAt == nil || state.LastWriteAt.Before(now) || state.LastSnapshotID != 42 {
		t.Fatalf("inventory overwrote CDC signal: %+v err=%v", state, err)
	}
}

func TestCDCWriteSignalPreservesWorkerOrphanSchedule(t *testing.T) {
	store, _, key := newMaintenanceOwnershipIntegrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	// A longer worker override must not be shortened by the streaming process's
	// default cadence. Signals still refresh write activity and inventory.
	deadline := now.Add(3 * 24 * time.Hour)
	if err := store.AdvanceSchedule(ctx, key, "remove_orphan_files", deadline); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{42, 43} {
		updated, err := store.CoalesceSignal(ctx, key, id, 1, 0, true, 50, now.Add(5*time.Minute))
		if err != nil || !updated {
			t.Fatalf("persist signal: updated=%t err=%v", updated, err)
		}
		state, err := store.GetState(ctx, key)
		if err != nil || state == nil || state.NextOrphanCheckAt == nil || !state.NextOrphanCheckAt.Equal(deadline) {
			t.Fatalf("CDC signal changed the worker schedule: state=%+v err=%v", state, err)
		}
		if state.LastWriteAt == nil || state.LastWriteAt.Before(now) || state.LastSnapshotID != id {
			t.Fatalf("CDC signal did not record write activity: %+v", state)
		}
	}
}

func TestOrphanScheduleUpgradeAndReactivationDoNotPostponeDueWork(t *testing.T) {
	for _, scenario := range []string{"active", "idle", "unknown", "overdue", "already_sooner", "paused", "snapshot_pending"} {
		t.Run(scenario, func(t *testing.T) {
			store, catalog, key := newMaintenanceOwnershipIntegrationStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			activeDeadline, idleDeadline := now.Add(24*time.Hour), now.Add(7*24*time.Hour)
			original := now.Add(90 * 24 * time.Hour)
			written := now.Add(-time.Hour)
			activity := &written
			want := activeDeadline
			switch scenario {
			case "idle":
				written = now.Add(-10 * 24 * time.Hour)
				want = idleDeadline
			case "unknown":
				activity = nil
				want = idleDeadline
			case "overdue":
				original = now.Add(-time.Hour)
				want = original
			case "already_sooner":
				original = now.Add(time.Hour)
				want = original
			case "paused":
				if err := store.SetMonitorStatus(ctx, "general-"+catalog, MaintenanceMonitorPaused, now); err != nil {
					t.Fatal(err)
				}
				want = original
			case "snapshot_pending":
				if _, err := store.db.ExecContext(ctx, "UPDATE iceberg_maintenance_state SET snapshot_complete=0 WHERE table_key=?", key); err != nil {
					t.Fatal(err)
				}
				want = original
			}
			if _, err := store.db.ExecContext(ctx, "UPDATE iceberg_maintenance_state SET last_write_at=?, next_orphan_check_at=? WHERE table_key=?", activity, original, key); err != nil {
				t.Fatal(err)
			}
			owner := MaintenanceMonitorOwnerID("general-" + catalog)
			clamp := func(at time.Time) {
				t.Helper()
				if err := store.ClampOrphanSchedules(ctx, owner, at, at.Add(-7*24*time.Hour), at.Add(24*time.Hour), at.Add(7*24*time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			clamp(now)
			clamp(now.Add(time.Hour)) // Repeated sync must not slide the deadline.
			state, err := store.GetState(ctx, key)
			if err != nil || state == nil || state.NextOrphanCheckAt == nil || !state.NextOrphanCheckAt.Equal(want) {
				t.Fatalf("schedule=%+v err=%v, want %s", state, err, want)
			}
			if scenario == "idle" || scenario == "unknown" {
				if err := store.RecordTableWrite(ctx, key, &now); err != nil {
					t.Fatal(err)
				}
				clamp(now)
				state, err = store.GetState(ctx, key)
				if err != nil || state == nil || state.NextOrphanCheckAt == nil || !state.NextOrphanCheckAt.Equal(activeDeadline) {
					t.Fatalf("external write did not bring weekly cleanup forward: %+v err=%v", state, err)
				}
			}
		})
	}
}
