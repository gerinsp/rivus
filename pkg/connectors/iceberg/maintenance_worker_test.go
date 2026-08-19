package iceberg

import (
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/gerinsp/rivus/pkg/config"
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
	if settings.DataFilesThreshold != 200 || settings.EqualityDeleteThreshold != 50 || settings.PositionDeleteThreshold != 25 {
		t.Fatalf("unexpected maintenance thresholds: data=%d equality=%d position=%d", settings.DataFilesThreshold, settings.EqualityDeleteThreshold, settings.PositionDeleteThreshold)
	}
	if settings.MaxSelectedFiles != 250 {
		t.Fatalf("unexpected native file limit: %d", settings.MaxSelectedFiles)
	}
	if settings.MaxEqualityDeleteFiles != 100 {
		t.Fatalf("unexpected native equality-delete limit: %d", settings.MaxEqualityDeleteFiles)
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

func TestNativeMaintenanceSettingsEnvOverrides(t *testing.T) {
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_INPUT_BYTES", "1073741824")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_FILES", "300")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_MAX_EQUALITY_DELETE_FILES", "150")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_MAX_AGE_HOURS", "48")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_RETAIN_LAST", "20")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_MIN_AGE_HOURS", "240")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_DRY_RUN", "true")

	settings, err := nativeMaintenanceSettingsFromRaw(map[string]any{
		"table_maintenance": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if settings.MaxSelectedInputBytes != 1073741824 {
		t.Fatalf("max input bytes = %d, want 1073741824", settings.MaxSelectedInputBytes)
	}
	if settings.MaxSelectedFiles != 300 {
		t.Fatalf("max selected files = %d, want 300", settings.MaxSelectedFiles)
	}
	if settings.MaxEqualityDeleteFiles != 150 {
		t.Fatalf("max equality delete files = %d, want 150", settings.MaxEqualityDeleteFiles)
	}
	if settings.SnapshotMaxAge != 48*time.Hour {
		t.Fatalf("snapshot max age = %s, want 48h", settings.SnapshotMaxAge)
	}
	if settings.SnapshotRetainLast != 20 {
		t.Fatalf("snapshot retain last = %d, want 20", settings.SnapshotRetainLast)
	}
	if settings.OrphanMinAge != 240*time.Hour {
		t.Fatalf("orphan min age = %s, want 240h", settings.OrphanMinAge)
	}
	if !settings.OrphanDryRun {
		t.Fatalf("orphan dry run = false, want true")
	}
}

func TestAccumulateActiveFileInventory(t *testing.T) {
	const megabyte = int64(1024 * 1024)
	inventory := activeFileInventory{SnapshotID: 123}
	file := func(path string, size int64, content iceberg.ManifestEntryContent) iceberg.DataFile {
		builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content, path, iceberg.ParquetFile,
			map[int]any{}, map[int]string{}, map[int]int{}, 1, size)
		if err != nil {
			t.Fatal(err)
		}
		return builder.Build()
	}
	files := []iceberg.DataFile{
		file("small-a.parquet", 10*megabyte, iceberg.EntryContentData),
		file("small-b.parquet", 63*megabyte, iceberg.EntryContentData),
		file("large.parquet", 64*megabyte, iceberg.EntryContentData),
		file("eq-delete.parquet", megabyte, iceberg.EntryContentEqDeletes),
		file("pos-delete.parquet", megabyte, iceberg.EntryContentPosDeletes),
	}
	for _, file := range files {
		accumulateActiveFile(&inventory, file, 64*megabyte)
	}
	if inventory.DataFiles != 3 || inventory.SmallFiles != 2 || inventory.SmallBytes != 73*megabyte {
		t.Fatalf("data inventory = %#v", inventory)
	}
	if inventory.EqualityDeletes != 1 || inventory.PositionDeletes != 1 {
		t.Fatalf("delete inventory = %#v", inventory)
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

func TestDurableMaintenanceTableStateShowsInventoryScanning(t *testing.T) {
	now := time.Now().UTC()
	leaseUntil := now.Add(time.Minute)
	queuedAt := now.Add(time.Minute)

	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete:    true,
		InventoryLeaseUntil: &leaseUntil,
	}, config.IcebergTableMaintenanceConfig{}); got != "scanning" {
		t.Fatalf("active inventory lease state = %q, want scanning", got)
	}
	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete:     true,
		LastInventoryAt:      &now,
		NextInventoryCheckAt: &queuedAt,
	}, config.IcebergTableMaintenanceConfig{}); got != "inventory_pending" {
		t.Fatalf("queued inventory state = %q, want inventory_pending", got)
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
			"enabled":                     true,
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
		name  string
		work  compactionWorkload
		state meta.IcebergMaintenanceState
		want  bool
	}{
		{name: "tiny", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 400 * 1024 * 1024, GroupCount: 1}, want: false},
		{name: "200 selected files", work: compactionWorkload{SelectedFiles: 200, SelectedBytes: 14 * 1024 * 1024, GroupCount: 1}, want: false},
		{name: "too many bytes", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 513 * 1024 * 1024, GroupCount: 1}, want: true},
		{name: "too many files", work: compactionWorkload{SelectedFiles: 251, SelectedBytes: 400 * 1024 * 1024, GroupCount: 1}, want: true},
		{name: "100 equality deletes", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 14 * 1024 * 1024, EqualityDeletes: 100, GroupCount: 1}, want: false},
		{name: "too many equality deletes", work: compactionWorkload{SelectedFiles: 50, SelectedBytes: 14 * 1024 * 1024, EqualityDeletes: 101, GroupCount: 1}, want: true},
		{name: "one position delete stays native", work: compactionWorkload{SelectedFiles: 20, SelectedBytes: 300 * 1024 * 1024, PositionDeletes: 1, GroupCount: 1}, want: false},
		{name: "25 position deletes route to Spark", work: compactionWorkload{SelectedFiles: 44, SelectedBytes: 300 * 1024 * 1024, PositionDeletes: 25, GroupCount: 1}, want: true},
		{name: "multiple groups within native limits", work: compactionWorkload{SelectedFiles: 80, SelectedBytes: 300 * 1024 * 1024, GroupCount: 2}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := shouldRouteCompactionToSpark(tc.work, tc.state, settings)
			if got != tc.want {
				t.Fatalf("route to Spark=%t, want %t", got, tc.want)
			}
		})
	}
}

func TestCompactionTriggersFor(t *testing.T) {
	const megabyte = 1024 * 1024
	settings := defaultNativeMaintenanceSettings()
	cases := []struct {
		name  string
		state meta.IcebergMaintenanceState
		want  compactionTriggers
	}{
		{
			name: "57 active equality deletes trigger below byte floor",
			state: meta.IcebergMaintenanceState{
				ActiveSmallFiles:          1,
				ActiveSmallBytes:          14 * megabyte,
				ActiveEqualityDeleteFiles: 57,
			},
			want: compactionTriggers{EqualityDelete: true},
		},
		{
			name: "25 active position deletes trigger Spark maintenance",
			state: meta.IcebergMaintenanceState{
				ActivePositionDeleteFiles: 25,
			},
			want: compactionTriggers{PositionDelete: true},
		},
		{
			name: "position deletes below threshold wait",
			state: meta.IcebergMaintenanceState{
				ActivePositionDeleteFiles: 24,
			},
			want: compactionTriggers{},
		},
		{
			name: "200 active small files trigger below byte floor",
			state: meta.IcebergMaintenanceState{
				ActiveSmallFiles: 200,
				ActiveSmallBytes: 14 * megabyte,
			},
			want: compactionTriggers{SmallFileCount: true},
		},
		{
			name: "199 small files and 49 equality deletes do not trigger below byte floor",
			state: meta.IcebergMaintenanceState{
				ActiveSmallFiles:          199,
				ActiveSmallBytes:          14 * megabyte,
				ActiveEqualityDeleteFiles: 49,
			},
			want: compactionTriggers{},
		},
		{
			name: "unscanned commit counter alone does not trigger compaction",
			state: meta.IcebergMaintenanceState{
				NewEqualityDeleteFiles: 57,
			},
			want: compactionTriggers{},
		},
		{
			name: "10 small files trigger at byte floor",
			state: meta.IcebergMaintenanceState{
				ActiveSmallFiles: 10,
				ActiveSmallBytes: 256 * megabyte,
			},
			want: compactionTriggers{SmallFileBytes: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := compactionTriggersFor(tc.state, settings); got != tc.want {
				t.Fatalf("compaction triggers = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestDurableMaintenanceTableStateUsesSmallFileAndDeleteTriggers(t *testing.T) {
	now := time.Now().UTC()
	cfg := config.IcebergTableMaintenanceConfig{
		DataFilesThreshold:           200,
		EqualityDeleteFilesThreshold: 50,
		PositionDeleteFilesThreshold: 25,
		SmallFilesMinCount:           10,
		SmallFilesMinTotalBytes:      256 * 1024 * 1024,
	}

	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete: true,
		LastInventoryAt:  &now,
		ActiveSmallFiles: 200,
		ActiveSmallBytes: 14 * 1024 * 1024,
	}, cfg); got != "ready" {
		t.Fatalf("200 small files below 256 MiB state=%q, want ready", got)
	}
	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete:          true,
		LastInventoryAt:           &now,
		ActiveEqualityDeleteFiles: 50,
	}, cfg); got != "ready" {
		t.Fatalf("50 equality deletes state=%q, want ready", got)
	}
	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete:          true,
		LastInventoryAt:           &now,
		ActivePositionDeleteFiles: 25,
	}, cfg); got != "ready" {
		t.Fatalf("25 position deletes state=%q, want ready", got)
	}
	if got := durableMaintenanceTableState(meta.IcebergMaintenanceState{
		SnapshotComplete:          true,
		LastInventoryAt:           &now,
		ActivePositionDeleteFiles: 24,
	}, cfg); got != "healthy" {
		t.Fatalf("24 position deletes state=%q, want healthy", got)
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

func TestMaintenancePreflightFailureResultReplacesRunningPlaceholder(t *testing.T) {
	task := meta.IcebergMaintenanceTask{
		ID:           42,
		TableKey:     "tiketux.tiketux_bronze.reservasi",
		Operation:    "compact",
		AttemptCount: 2,
	}
	result := maintenancePreflightFailureResult(7, task, "snapshot barrier is not complete")
	if result.RunID != 7 || result.TaskID != task.ID || result.Attempt != task.AttemptCount {
		t.Fatalf("result identity does not match claimed task: %#v", result)
	}
	if result.Status != "failed" || result.Engine != "none" || result.Error == "" {
		t.Fatalf("unexpected preflight result: %#v", result)
	}
}
