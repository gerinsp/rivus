package config

import (
	"encoding/json"
	"strings"

	"gopkg.in/yaml.v3"
)

const internalMaintenanceWorkerGate = "native_enabled"

// ConnectorSpec keeps connector configs as raw maps so persisted jobs can be
// consumed by both the CDC server and the separate maintenance worker.
// table_maintenance.enabled is the only public feature switch. The historical
// native_enabled key is now an internal compatibility detail derived from
// enabled; user supplied values cannot change the selected runtime path.
func (c *ConnectorSpec) UnmarshalYAML(value *yaml.Node) error {
	type plain ConnectorSpec
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	c.normalizeMaintenanceFeatureGate()
	return nil
}

func (c *ConnectorSpec) UnmarshalJSON(data []byte) error {
	type plain ConnectorSpec
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	c.normalizeMaintenanceFeatureGate()
	return nil
}

// MarshalJSON removes the historical internal worker gate so persisted jobs and
// API responses expose only table_maintenance.enabled.
func (c ConnectorSpec) MarshalJSON() ([]byte, error) {
	type plain ConnectorSpec
	clean := plain(c)
	clean.Config = connectorConfigWithoutInternalMaintenanceGate(c.Config)
	return json.Marshal(clean)
}

// MarshalYAML keeps generated/exported YAML aligned with the public one-switch
// configuration model.
func (c ConnectorSpec) MarshalYAML() (any, error) {
	type plain ConnectorSpec
	clean := plain(c)
	clean.Config = connectorConfigWithoutInternalMaintenanceGate(c.Config)
	return clean, nil
}

func (c *ConnectorSpec) normalizeMaintenanceFeatureGate() {
	if c == nil || !strings.EqualFold(strings.TrimSpace(c.Type), "iceberg_native") || c.Config == nil {
		return
	}
	raw, ok := c.Config["table_maintenance"]
	if !ok {
		return
	}
	maintenance, ok := raw.(map[string]any)
	if !ok || maintenance == nil {
		return
	}
	enabled, _ := maintenance["enabled"].(bool)
	// Keep the old key only inside the in-memory raw map until worker internals
	// are fully renamed. It is always derived from enabled and never serialized.
	maintenance[internalMaintenanceWorkerGate] = enabled
}

func connectorConfigWithoutInternalMaintenanceGate(config map[string]any) map[string]any {
	if config == nil {
		return nil
	}
	out := make(map[string]any, len(config))
	for key, value := range config {
		out[key] = value
	}
	raw, ok := config["table_maintenance"]
	if !ok {
		return out
	}
	maintenance, ok := raw.(map[string]any)
	if !ok || maintenance == nil {
		return out
	}
	cleanMaintenance := make(map[string]any, len(maintenance))
	for key, value := range maintenance {
		if key == internalMaintenanceWorkerGate {
			continue
		}
		cleanMaintenance[key] = value
	}
	out["table_maintenance"] = cleanMaintenance
	return out
}
