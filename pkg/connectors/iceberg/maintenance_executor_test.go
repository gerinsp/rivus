package iceberg

import "testing"

func maintenanceSinkConfigForTest(values map[string]any) map[string]any {
	return map[string]any{"table_maintenance": values}
}

func TestMaintenanceWorkerUsesEnabledAsOnlyGate(t *testing.T) {
	if nativeMaintenanceEnabledFromRaw(maintenanceSinkConfigForTest(map[string]any{
		"enabled":        false,
		"native_enabled": true,
	})) {
		t.Fatal("native_enabled must not turn maintenance on when enabled=false")
	}
	if !nativeMaintenanceEnabledFromRaw(maintenanceSinkConfigForTest(map[string]any{
		"enabled": true,
	})) {
		t.Fatal("enabled=true must activate maintenance without native_enabled")
	}
}

func TestMaintenanceExecutorDefaultsToHybrid(t *testing.T) {
	settings, err := nativeMaintenanceSettingsFromRaw(maintenanceSinkConfigForTest(map[string]any{
		"enabled": true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if settings.Executor != maintenanceExecutorHybrid {
		t.Fatalf("executor = %q, want hybrid", settings.Executor)
	}
}

func TestMaintenanceExecutorModes(t *testing.T) {
	for _, executor := range []string{maintenanceExecutorHybrid, maintenanceExecutorNative, maintenanceExecutorSpark} {
		t.Run(executor, func(t *testing.T) {
			settings, err := nativeMaintenanceSettingsFromRaw(maintenanceSinkConfigForTest(map[string]any{
				"enabled":  true,
				"executor": executor,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if settings.Executor != executor {
				t.Fatalf("executor = %q, want %q", settings.Executor, executor)
			}
		})
	}
}

func TestMaintenanceExecutorRejectsUnknownMode(t *testing.T) {
	_, err := nativeMaintenanceSettingsFromRaw(maintenanceSinkConfigForTest(map[string]any{
		"enabled":  true,
		"executor": "flink",
	}))
	if err == nil {
		t.Fatal("expected invalid executor to be rejected")
	}
}
