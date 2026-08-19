package iceberg

import (
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestMaintenanceSettingsForTaskAppliesManualOrphanOverrides(t *testing.T) {
	settings := defaultNativeMaintenanceSettings()
	settings.OrphanDryRun = false
	settings.OrphanMinAge = 7 * 24 * time.Hour

	task := meta.IcebergMaintenanceTask{
		Operation: maintenanceOperationOrphan,
		Payload: map[string]any{
			"dry_run":          true,
			"older_than_hours": float64(336),
		},
	}

	got := maintenanceSettingsForTask(settings, task)
	if !got.OrphanDryRun {
		t.Fatal("expected dry_run task override")
	}
	if got.OrphanMinAge != 14*24*time.Hour {
		t.Fatalf("expected 14-day orphan age, got %s", got.OrphanMinAge)
	}
}

func TestMaintenanceSettingsForTaskIgnoresPayloadForOtherOperations(t *testing.T) {
	settings := defaultNativeMaintenanceSettings()
	settings.OrphanDryRun = false
	settings.OrphanMinAge = 7 * 24 * time.Hour

	task := meta.IcebergMaintenanceTask{
		Operation: maintenanceOperationCompact,
		Payload: map[string]any{
			"dry_run":          true,
			"older_than_hours": float64(336),
		},
	}

	got := maintenanceSettingsForTask(settings, task)
	if got.OrphanDryRun != settings.OrphanDryRun || got.OrphanMinAge != settings.OrphanMinAge {
		t.Fatalf("non-orphan task changed orphan settings: before=%+v after=%+v", settings, got)
	}
}

func TestMaintenanceSettingsForTaskAcceptsDecodedStringValues(t *testing.T) {
	settings := defaultNativeMaintenanceSettings()
	task := meta.IcebergMaintenanceTask{
		Operation: maintenanceOperationOrphan,
		Payload: map[string]any{
			"dry_run":          "true",
			"older_than_hours": "240",
		},
	}

	got := maintenanceSettingsForTask(settings, task)
	if !got.OrphanDryRun {
		t.Fatal("expected string dry_run override")
	}
	if got.OrphanMinAge != 10*24*time.Hour {
		t.Fatalf("expected 10-day orphan age, got %s", got.OrphanMinAge)
	}
}
