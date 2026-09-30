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
		ActiveDataFiles:        5550,
		ActiveSmallFiles:       5550,
		ActiveCompactableFiles: 5550,
		CreatedAt:              now.Add(-time.Hour),
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
		ActiveDataFiles:        50,
		ActiveSmallFiles:       50,
		ActiveCompactableFiles: 50,
		CreatedAt:              now.Add(-time.Hour),
	}

	if got := compactionTaskPriority(state, settings, now); got != 4 {
		t.Fatalf("priority for 50/50 small files = %d, want 4", got)
	}
}

func TestCompactionTaskPriorityDoesNotPromoteFilesSplitAcrossPartitions(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	settings.DataFilesThreshold = 50
	state := meta.IcebergMaintenanceState{
		ActiveSmallFiles:       5550,
		ActiveCompactableFiles: 10,
		ActiveCompactionGroups: 1,
		CreatedAt:              now.Add(-time.Hour),
	}
	if got := compactionTaskPriority(state, settings, now); got != defaultCompactionTaskPriority {
		t.Fatalf("priority for 10 compactable files across 5550 small files = %d, want %d", got, defaultCompactionTaskPriority)
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
	inventory := activeFileInventory{DataFiles: 40, SmallFiles: 40, CompactableFiles: 40, CompactableBytes: 40, CompactionGroups: 1}

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
	inventory := activeFileInventory{DataFiles: 40, SmallFiles: 40, CompactableFiles: 40, CompactableBytes: 40, CompactionGroups: 1}

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
		DataFiles: 5000, SmallFiles: 5000, CompactableFiles: 5000, CompactableBytes: 5000, CompactionGroups: 20,
	}, state, settings, now)
	if !followUp || priority != 1 {
		t.Fatalf("severe remaining backlog follow-up = (%v, %d), want (true, 1)", followUp, priority)
	}

	followUp, priority = followUpCompactionForInventory(activeFileInventory{
		DataFiles: 20, SmallFiles: 20, CompactableFiles: 20, CompactableBytes: 20, CompactionGroups: 1,
	}, state, settings, now)
	if followUp || priority != 0 {
		t.Fatalf("healthy inventory follow-up = (%v, %d), want (false, 0)", followUp, priority)
	}
}
