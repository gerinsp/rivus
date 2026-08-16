package config

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMaintenanceMasterSwitchNormalizesYAMLConnectorSpec(t *testing.T) {
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
	if maintenance["native_enabled"] != false {
		t.Fatalf("native_enabled = %#v, want false when master enabled is false", maintenance["native_enabled"])
	}
}

func TestMaintenanceMasterSwitchNormalizesJSONConnectorSpec(t *testing.T) {
	var spec ConnectorSpec
	if err := json.Unmarshal([]byte(`{"type":"iceberg_native","config":{"table_maintenance":{"enabled":false,"native_enabled":true}}}`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance["native_enabled"] != false {
		t.Fatalf("native_enabled = %#v, want false when master enabled is false", maintenance["native_enabled"])
	}
}

func TestMaintenanceMasterSwitchKeepsWorkerEnabled(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
    native_enabled: true
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance["native_enabled"] != true {
		t.Fatalf("native_enabled = %#v, want true when master and worker switches are true", maintenance["native_enabled"])
	}
}

func TestRuntimeMaintenanceDecoderRetiresLegacyEnabledGate(t *testing.T) {
	var cfg struct {
		Maintenance IcebergTableMaintenanceConfig `yaml:"table_maintenance"`
	}
	if err := yaml.Unmarshal([]byte(`table_maintenance:
  enabled: true
  native_enabled: true
`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Maintenance.Enabled {
		t.Fatal("legacy runtime Enabled gate must be false")
	}
	if !cfg.Maintenance.NativeEnabled {
		t.Fatal("worker runtime gate must remain enabled")
	}
}
