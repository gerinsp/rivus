package config

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConnectorSpec keeps connector configs as raw maps so persisted jobs can be
// consumed by both the CDC server and the separate maintenance worker.
//
// table_maintenance.enabled is the only feature gate. executor selects how
// compaction is executed: hybrid, native, or spark. The historical
// native_enabled key is removed during decode and is never persisted again.
func (c *ConnectorSpec) UnmarshalYAML(value *yaml.Node) error {
	type plain ConnectorSpec
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	return c.normalizeMaintenanceConfig()
}

func (c *ConnectorSpec) UnmarshalJSON(data []byte) error {
	type plain ConnectorSpec
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = ConnectorSpec(decoded)
	return c.normalizeMaintenanceConfig()
}

func (c *ConnectorSpec) normalizeMaintenanceConfig() error {
	if c == nil || !strings.EqualFold(strings.TrimSpace(c.Type), "iceberg_native") || c.Config == nil {
		return nil
	}
	raw, ok := c.Config["table_maintenance"]
	if !ok {
		return nil
	}
	maintenance, ok := raw.(map[string]any)
	if !ok || maintenance == nil {
		return nil
	}

	// native_enabled belonged to the first worker rollout. Scheduling now has a
	// single master switch, so do not preserve or honor the old flag.
	delete(maintenance, "native_enabled")

	enabled, _ := maintenance["enabled"].(bool)
	if !enabled {
		return nil
	}
	executor, _ := maintenance["executor"].(string)
	executor = strings.ToLower(strings.TrimSpace(executor))
	if executor == "" {
		executor = "hybrid"
	}
	switch executor {
	case "hybrid", "native", "spark":
		maintenance["executor"] = executor
		return nil
	default:
		return fmt.Errorf("table_maintenance.executor must be one of hybrid, native, spark")
	}
}
