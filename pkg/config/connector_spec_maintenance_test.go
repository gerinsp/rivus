package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMaintenanceEnabledFalseDisablesInternalWorkerGate(t *testing.T) {
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
	if maintenance[internalMaintenanceWorkerGate] != false {
		t.Fatalf("internal worker gate = %#v, want false", maintenance[internalMaintenanceWorkerGate])
	}
}

func TestMaintenanceEnabledAloneEnablesInternalWorkerGateYAML(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance[internalMaintenanceWorkerGate] != true {
		t.Fatalf("internal worker gate = %#v, want true", maintenance[internalMaintenanceWorkerGate])
	}
}

func TestMaintenanceEnabledAloneEnablesInternalWorkerGateJSON(t *testing.T) {
	var spec ConnectorSpec
	if err := json.Unmarshal([]byte(`{"type":"iceberg_native","config":{"table_maintenance":{"enabled":true}}}`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance[internalMaintenanceWorkerGate] != true {
		t.Fatalf("internal worker gate = %#v, want true", maintenance[internalMaintenanceWorkerGate])
	}
}

func TestHistoricalNativeEnabledCannotDisableWorker(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
    native_enabled: false
`), &spec); err != nil {
		t.Fatal(err)
	}
	maintenance := spec.Config["table_maintenance"].(map[string]any)
	if maintenance[internalMaintenanceWorkerGate] != true {
		t.Fatalf("historical native_enabled changed runtime path: %#v", maintenance)
	}
}

func TestInternalWorkerGateIsNotSerialized(t *testing.T) {
	var spec ConnectorSpec
	if err := yaml.Unmarshal([]byte(`type: iceberg_native
config:
  table_maintenance:
    enabled: true
`), &spec); err != nil {
		t.Fatal(err)
	}

	encodedJSON, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedJSON), "native_enabled") {
		t.Fatalf("serialized JSON leaked internal worker gate: %s", encodedJSON)
	}

	encodedYAML, err := yaml.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedYAML), "native_enabled") {
		t.Fatalf("serialized YAML leaked internal worker gate: %s", encodedYAML)
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
