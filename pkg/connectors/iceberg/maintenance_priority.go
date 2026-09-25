package iceberg

import (
	"math"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	defaultCompactionTaskPriority = 10
	growthLookahead               = time.Hour
	minimumGrowthWindow           = 5 * time.Minute
)

// Lower priority values are claimed first.
func compactionTaskPriority(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings, now time.Time) int {
	pressure := compactionPressure(state, settings)
	switch {
	case pressure >= 10:
		return 1
	case pressure >= 4:
		return 2
	case pressure >= 2:
		return 3
	case pressure >= 1:
		return 4
	}

	eta := projectedCompactionThresholdETA(state, settings, now)
	switch {
	case eta > 0 && eta <= time.Hour:
		return 5
	case eta > 0 && eta <= 4*time.Hour:
		return 7
	default:
		return defaultCompactionTaskPriority
	}
}

func compactionPressure(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) float64 {
	pressure := 0.0
	pressure = math.Max(pressure, ratio(state.ActiveSmallFiles, settings.DataFilesThreshold))
	pressure = math.Max(pressure, ratio(state.ActiveEqualityDeleteFiles, settings.EqualityDeleteThreshold))
	pressure = math.Max(pressure, ratio(state.ActivePositionDeleteFiles, settings.PositionDeleteThreshold))
	if settings.MinSmallFiles > 0 && settings.MinSmallBytes > 0 {
		countPressure := ratio(state.ActiveSmallFiles, settings.MinSmallFiles)
		bytePressure := float64(state.ActiveSmallBytes) / float64(settings.MinSmallBytes)
		pressure = math.Max(pressure, math.Min(countPressure, bytePressure))
	}
	return pressure
}

func ratio(value, threshold int) float64 {
	if threshold <= 0 {
		return 0
	}
	return float64(value) / float64(threshold)
}

// Start early only when growth will cross the threshold within the lookahead.
func proactiveCompactionDue(inventory activeFileInventory, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings, now time.Time) bool {
	observed := stateWithActiveInventory(state, inventory)
	return proactiveCompactionStateDue(observed, settings, now)
}

func proactiveCompactionStateDue(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings, now time.Time) bool {
	if compactionTriggersFor(state, settings).Any() {
		return true
	}
	if state.ActiveSmallFiles < settings.MinSmallFiles {
		return false
	}
	eta := projectedCompactionThresholdETA(state, settings, now)
	return eta > 0 && eta <= growthLookahead
}

func followUpCompactionForInventory(inventory activeFileInventory, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings, now time.Time) (bool, int) {
	if !inventoryTriggersCompaction(inventory, settings) {
		return false, 0
	}
	observed := stateWithActiveInventory(state, inventory)
	return true, compactionTaskPriority(observed, settings, now)
}

func projectedCompactionThresholdETA(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings, now time.Time) time.Duration {
	windowStart := state.CreatedAt
	if state.LastCompactionAt != nil && state.LastCompactionAt.After(windowStart) {
		windowStart = *state.LastCompactionAt
	}
	if windowStart.IsZero() || !now.After(windowStart) || now.Sub(windowStart) < minimumGrowthWindow {
		return 0
	}
	hours := now.Sub(windowStart).Hours()
	best := time.Duration(0)

	if settings.DataFilesThreshold > state.ActiveSmallFiles && state.NewDataFiles > 0 {
		smallFraction := 1.0
		if state.ActiveDataFiles > 0 {
			smallFraction = float64(state.ActiveSmallFiles) / float64(state.ActiveDataFiles)
		}
		rate := float64(state.NewDataFiles) * smallFraction / hours
		best = earlierPositiveDuration(best, thresholdETA(settings.DataFilesThreshold-state.ActiveSmallFiles, rate))
	}
	if settings.EqualityDeleteThreshold > state.ActiveEqualityDeleteFiles && state.NewEqualityDeleteFiles > 0 {
		rate := float64(state.NewEqualityDeleteFiles) / hours
		best = earlierPositiveDuration(best, thresholdETA(settings.EqualityDeleteThreshold-state.ActiveEqualityDeleteFiles, rate))
	}
	return best
}

func thresholdETA(remaining int, ratePerHour float64) time.Duration {
	if remaining <= 0 || ratePerHour <= 0 {
		return 0
	}
	hours := float64(remaining) / ratePerHour
	if hours > float64((time.Duration(1<<63-1))/time.Hour) {
		return 0
	}
	return time.Duration(hours * float64(time.Hour))
}

func earlierPositiveDuration(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func stateWithActiveInventory(state meta.IcebergMaintenanceState, inventory activeFileInventory) meta.IcebergMaintenanceState {
	state.InventorySnapshotID = inventory.SnapshotID
	state.ActiveDataFiles = inventory.DataFiles
	state.ActiveSmallFiles = inventory.SmallFiles
	state.ActiveSmallBytes = inventory.SmallBytes
	state.ActiveEqualityDeleteFiles = inventory.EqualityDeletes
	state.ActivePositionDeleteFiles = inventory.PositionDeletes
	return state
}
