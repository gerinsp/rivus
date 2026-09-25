package iceberg

import (
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestCompactionTaskPriorityPromotesSevereBacklog(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 50
	state := meta.IcebergMaintenanceState{
		ActiveDataFiles:  5550,
		ActiveSmallFiles: 5550,
		CreatedAt:        now.Add(-time.Hour),
	}

	if got := compactionTaskPriority(state, settings, now); got != 1 {
		t.Fatalf("priority for 5550/50 small files = %d, want 1", got)
	}
}

func TestCompactionTaskPriorityKeepsThresholdWorkBelowCritical(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 50
	state := meta.IcebergMaintenanceState{
		ActiveDataFiles:  50,
		ActiveSmallFiles: 50,
		CreatedAt:        now.Add(-time.Hour),
	}

	if got := compactionTaskPriority(state, settings, now); got != 4 {
		t.Fatalf("priority for 50/50 small files = %d, want 4", got)
	}
}

func TestProactiveCompactionFollowsFastGrowth(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 100
	settings.MinSmallFiles = 10
	state := meta.IcebergMaintenanceState{
		NewDataFiles: 40,
		CreatedAt:    now.Add(-30 * time.Minute),
	}
	inventory := activeFileInventory{DataFiles: 40, SmallFiles: 40}

	if !proactiveCompactionDue(inventory, state, settings, now) {
		t.Fatal("fast-growing table should be scheduled before reaching its hard threshold")
	}
	observed := stateWithActiveInventory(state, inventory)
	if got := compactionTaskPriority(observed, settings, now); got != 5 {
		t.Fatalf("fast-growing table priority = %d, want 5", got)
	}
}

func TestProactiveCompactionDoesNotChaseSlowGrowth(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 100
	settings.MinSmallFiles = 10
	state := meta.IcebergMaintenanceState{
		NewDataFiles: 10,
		CreatedAt:    now.Add(-10 * time.Hour),
	}
	inventory := activeFileInventory{DataFiles: 40, SmallFiles: 40}

	if proactiveCompactionDue(inventory, state, settings, now) {
		t.Fatal("slow-growing table should remain on the normal maintenance schedule")
	}
}

func TestFollowUpCompactionContinuesUntilBacklogIsHealthy(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 50
	state := meta.IcebergMaintenanceState{CreatedAt: now.Add(-time.Hour)}

	followUp, priority := followUpCompactionForInventory(activeFileInventory{
		DataFiles: 5000, SmallFiles: 5000,
	}, state, settings, now)
	if !followUp || priority != 1 {
		t.Fatalf("severe remaining backlog follow-up = (%v, %d), want (true, 1)", followUp, priority)
	}

	followUp, priority = followUpCompactionForInventory(activeFileInventory{
		DataFiles: 20, SmallFiles: 20,
	}, state, settings, now)
	if followUp || priority != 0 {
		t.Fatalf("healthy inventory follow-up = (%v, %d), want (false, 0)", followUp, priority)
	}
}
