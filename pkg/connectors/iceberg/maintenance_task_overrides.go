package iceberg

import (
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// maintenanceSettingsForTask applies only narrow, operation-specific values
// carried by an explicitly queued task. Scheduled maintenance continues to use
// the job/environment settings unchanged.
func maintenanceSettingsForTask(settings nativeMaintenanceSettings, task meta.IcebergMaintenanceTask) nativeMaintenanceSettings {
	if task.Operation != maintenanceOperationOrphan || len(task.Payload) == 0 {
		return settings
	}
	if value, ok := maintenancePayloadBool(task.Payload["dry_run"]); ok {
		settings.OrphanDryRun = value
	}
	if hours, ok := maintenancePayloadFloat64(task.Payload["older_than_hours"]); ok && hours > 0 {
		settings.OrphanMinAge = time.Duration(hours * float64(time.Hour))
	}
	return settings
}

func maintenancePayloadBool(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return false, false
	}
}

func maintenancePayloadFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
