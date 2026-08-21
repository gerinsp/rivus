package iceberg

import (
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
)

func maintenanceMonitorConfigForTest() *config.JobConfig {
	return &config.JobConfig{
		ID:   "warehouse-maintenance",
		Name: "Warehouse Maintenance",
		Mode: config.JobModeMaintenanceOnly,
		Sink: &config.ConnectorSpec{
			Type: "iceberg_native",
			Config: map[string]any{
				"rest_uri":  "http://iceberg-rest:8181",
				"warehouse": "s3://warehouse",
				"table_maintenance": map[string]any{
					"enabled":      true,
					"executor":     "native",
					"catalog_name": "asmat",
					"tables": []any{
						map[string]any{"namespace": "barayax_bronze", "table": "tbl_absen"},
						map[string]any{"namespace": "barayax_bronze", "table": "tbl_absen"},
					},
				},
			},
		},
	}
}

func TestPrepareMaintenanceMonitorConfigNormalizesExplicitTables(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["native_expire_interval_seconds"] = 12345
	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatalf("PrepareMaintenanceMonitorConfig returned error: %v", err)
	}
	if normalized.Mode != config.JobModeMaintenanceOnly || normalized.Source != nil {
		t.Fatalf("normalized monitor mode/source = %s/%#v", normalized.Mode, normalized.Source)
	}
	if len(targets) != 1 || targets[0].Namespace != "barayax_bronze" || targets[0].Table != "tbl_absen" {
		t.Fatalf("normalized targets = %#v", targets)
	}
	if got := rawInt(rawMaintenanceMap(normalized.Sink.Config), "native_expire_interval_seconds", 0); got != 12345 {
		t.Fatalf("raw maintenance setting was not preserved: got %d", got)
	}
	catalog, executor, profile, described, err := DescribeMaintenanceMonitorConfig(normalized)
	if err != nil {
		t.Fatalf("DescribeMaintenanceMonitorConfig returned error: %v", err)
	}
	if catalog != "asmat" || executor != "native" || profile != "small" || len(described) != 1 {
		t.Fatalf("description catalog=%q executor=%q profile=%q targets=%#v", catalog, executor, profile, described)
	}
}

func TestPrepareMaintenanceMonitorConfigRejectsIngestionAndWildcards(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{}}
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "must not define a source") {
		t.Fatalf("source validation error = %v", err)
	}

	cfg = maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["tables"] = []any{map[string]any{"namespace": "barayax_bronze", "table": "*"}}
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "wildcards") {
		t.Fatalf("wildcard validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigRequiresRunnerForHybrid(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["executor"] = "hybrid"
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "runner_uri") {
		t.Fatalf("hybrid backend validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigEnforcesOwnerIDLimit(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	cfg.ID = strings.Repeat("a", maxMaintenanceMonitorIDLength+1)
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("monitor id length validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigAcceptsCompactTableList(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["namespace"] = []any{"barayax_bronze"}
	maintenance["tables"] = []any{"tbl_absen", "tbl_employee", "attendance_daily"}

	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatalf("PrepareMaintenanceMonitorConfig returned error: %v", err)
	}
	if len(targets) != 3 {
		t.Fatalf("normalized targets = %#v", targets)
	}
	for _, target := range targets {
		if target.Namespace != "barayax_bronze" {
			t.Fatalf("target namespace = %q", target.Namespace)
		}
	}
	maintenance = rawMaintenanceMap(normalized.Sink.Config)
	if _, exists := maintenance["namespace"]; exists {
		t.Fatalf("compact namespace was not removed from normalized config: %#v", maintenance)
	}
}
