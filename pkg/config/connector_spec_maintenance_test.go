package config

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMaintenanceRemovesHistoricalNativeEnabled(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: false
    native_enabled: true
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if _, ok := maintenance["native_enabled"]; ok {
		t.Fatalf("historical native_enabled must be removed: %#v", maintenance)
	}
}

func TestMaintenanceDefaultsExecutorToHybrid(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance["executor"] != "hybrid" {
		t.Fatalf("executor = %#v, want hybrid", maintenance["executor"])
	}
	if _, ok := maintenance["native_enabled"]; ok {
		t.Fatalf("native_enabled leaked into normalized config: %#v", maintenance)
	}
}

func TestMaintenanceNormalizesExplicitExecutor(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
    executor: SPARK
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance["executor"] != "spark" {
		t.Fatalf("executor = %#v, want spark", maintenance["executor"])
	}
}

func TestMaintenanceJSONDefaultsExecutorToHybrid(t *testing.T) {
	var spec ConnectorSpec
	if err := json.Unmarshal([]byte(`{"type":"iceberg_native","config":{"table_maintenance":{"enabled":true}}}`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance["executor"] != "hybrid" {
		t.Fatalf("executor = %#v, want hybrid", maintenance["executor"])
	}
}

func TestRuntimeMaintenanceDecoderUsesEnabledOnly(t *testing.T) {
	var cfg struct {
		Maintenance IcebergTableMaintenanceConfig `yaml:"table_maintenance"`
	}
	if err := yaml.Unmarshal([]byte(`table_maintenance:
  enabled: true
  native_enabled: false
`), &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Maintenance.Enabled {
		t.Fatal("runtime Enabled must follow the public enabled switch")
	}
	if !cfg.Maintenance.NativeEnabled {
		t.Fatal("internal worker compatibility gate must be derived from enabled")
	}
}
