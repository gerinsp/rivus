package iceberg

import (
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestDeterministicJitterStableAndBounded(t *testing.T) {
	window := 24 * time.Hour
	first := deterministicJitter("rivus.ns.table|expire", window)
	second := deterministicJitter("rivus.ns.table|expire", window)
	if first != second {
		t.Fatalf("jitter not deterministic: %s != %s", first, second)
	}
	if first < 0 || first >= window {
		t.Fatalf("jitter %s outside [0,%s)", first, window)
	}
	if first == deterministicJitter("rivus.ns.other|expire", window) {
		t.Fatalf("different table keys unexpectedly produced identical jitter")
	}
}

func TestNativeMaintenanceSettingsDefaults(t *testing.T) {
	settings, err := nativeMaintenanceSettingsFromRaw(map[string]any{
		"table_maintenance": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !settings.Enabled {
		t.Fatalf("maintenance should be enabled")
	}
	if settings.Executor != maintenanceExecutorHybrid {
		t.Fatalf("unexpected executor: %q", settings.Executor)
	}
	if settings.MaxSelectedInputBytes != 512*1024*1024 {
		t.Fatalf("unexpected native byte limit: %d", settings.MaxSelectedInputBytes)
	}
	if settings.MaxSelectedFiles != 100 {
		t.Fatalf("unexpected native file limit: %d", settings.MaxSelectedFiles)
	}
	if settings.OrphanMinAge < 7*24*time.Hour {
		t.Fatalf("orphan minimum age below safety floor: %s", settings.OrphanMinAge)
	}
	if settings.ExpireInterval != 24*time.Hour {
		t.Fatalf("unexpected expire interval: %s", settings.ExpireInterval)
	}
	if settings.OrphanInterval != 30*24*time.Hour {
		t.Fatalf("unexpected orphan interval: %s", settings.OrphanInterval)
	}
	if settings.OrphanInactiveInterval != 90*24*time.Hour {
		t.Fatalf("unexpected inactive orphan interval: %s", settings.OrphanInactiveInterval)
	}
	if settings.IdleCompactionInterval != 7*24*time.Hour {
		t.Fatalf("unexpected idle compaction interval: %s", settings.IdleCompactionInterval)
	}
}

func TestOrphanCleanupSkipsInactiveTables(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	settings := defaultNativeMaintenanceSettings()
	inactive := meta.IcebergMaintenanceState{TableKey: "rivus.ns.idle"}
	if orphanCleanupActive(inactive, now, settings.OrphanInterval) {
		t.Fatal("table without a write signal must be inactive")
	}
	next := nextMaintenanceSchedule(inactive, "remove_orphan_files", now, settings)
	if next.Before(now.Add(settings.OrphanInactiveInterval)) || next.After(now.Add(settings.OrphanInactiveInterval+time.Hour)) {
		t.Fatalf("inactive orphan schedule=%s, want around %s", next, now.Add(settings.OrphanInactiveInterval))
	}

	recent := now.Add(-24 * time.Hour)
	active := meta.IcebergMaintenanceState{TableKey: "rivus.ns.hot", LastWriteAt: &recent}
	if !orphanCleanupActive(active, now, settings.OrphanInterval) {
		t.Fatal("recent write must keep orphan cleanup active")
	}
	next = nextMaintenanceSchedule(active, "remove_orphan_files", now, settings)
	if next.Before(now.Add(settings.OrphanInterval)) || next.After(now.Add(settings.OrphanInterval+time.Hour)) {
		t.Fatalf("active orphan schedule=%s, want around %s", next, now.Add(settings.OrphanInterval))
	}
}

func TestMaintenanceStartsCompleteOnlyForStreamingResumeModes(t *testing.T) {
	if maintenanceStartsComplete("initial") || maintenanceStartsComplete("snapshot_only") {
		t.Fatal("initial snapshot modes must keep maintenance blocked")
	}
	if !maintenanceStartsComplete("resume") || !maintenanceStartsComplete("latest") || !maintenanceStartsComplete("latest-offset") {
		t.Fatal("resume/latest modes must allow maintenance immediately")
	}
}

func TestNativeMaintenanceRejectsUnsafeOrphanAge(t *testing.T) {
	_, err := nativeMaintenanceSettingsFromRaw(map[string]any{
		"table_maintenance": map[string]any{
			"enabled":                      true,
			"native_orphan_min_age_hours": 24,
		},
	})
	if err == nil {
		t.Fatalf("expected unsafe orphan age to be rejected")
	}
}

func TestCompactionRoutingBoundaries(t *testing.T) {
	settings := defaultNativeMaintenanceSettings()
	cases := []struct {
		name string
		work compactionWorkload
		want bool
	}{
		{name: "tiny", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 400 * 1024 * 1024, GroupCount: 1}, want: false},
		{name: "too many bytes", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 513 * 1024 * 1024, GroupCount: 1}, want: true},
		{name: "too many files", work: compactionWorkload{SelectedFiles: 101, SelectedBytes: 400 * 1024 * 1024, GroupCount: 1}, want: true},
		{name: "position deletes", work: compactionWorkload{SelectedFiles: 20, SelectedBytes: 300 * 1024 * 1024, PositionDeletes: 1, GroupCount: 1}, want: true},
		{name: "multiple substantial groups", work: compactionWorkload{SelectedFiles: 80, SelectedBytes: 300 * 1024 * 1024, GroupCount: 2}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := shouldRouteCompactionToSpark(tc.work, settings)
			if got != tc.want {
				t.Fatalf("route to Spark=%t, want %t", got, tc.want)
			}
		})
	}
}

func TestMaintenanceMetaKeyIgnoresCDCDeleteExecutor(t *testing.T) {
	base := map[string]any{
		"rest_uri":            "http://catalog",
		"cdc_delete_executor": "trino",
	}
	changed := map[string]any{
		"rest_uri":            "http://catalog",
		"cdc_delete_executor": "equality",
	}
	key1 := maintenanceMetaKey("job", "initial", "mysql", map[string]any{"database": "db"}, "iceberg_native", base)
	key2 := maintenanceMetaKey("job", "initial", "mysql", map[string]any{"database": "db"}, "iceberg_native", changed)
	if key1 != key2 {
		t.Fatalf("maintenance meta key must match core stable meta key behavior")
	}
}
