package config

import (
	"encoding/json"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConnectorSpec keeps connector configs as raw maps so persisted jobs can be
// consumed by both the CDC server and the separate maintenance worker. These
// unmarshallers normalize only the maintenance feature gates: enabled is the
// master switch, so native_enabled can never turn the worker on by itself.
func (c *ConnectorSpec) UnmarshalYAML(value *yaml.Node) error {
	type plain ConnectorSpec
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	c.normalizeMaintenanceFeatureGates()
	return nil
}

func (c *ConnectorSpec) UnmarshalJSON(data []byte) error {
	type plain ConnectorSpec
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	c.normalizeMaintenanceFeatureGates()
	return nil
}

func (c *ConnectorSpec) normalizeMaintenanceFeatureGates() {
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
	if !enabled {
		maintenance["native_enabled"] = false
	}
}
