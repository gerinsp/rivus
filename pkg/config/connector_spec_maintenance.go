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
	if err := normalizeIcebergMaintenanceTargets(maintenance); err != nil {
		return err
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

// NormalizeIcebergMaintenanceTargets accepts both target syntaxes used by
// maintenance monitors. The compact syntax is normalized to the original
// explicit representation before connector validation or persistence.
func NormalizeIcebergMaintenanceTargets(root map[string]any) error {
	if root == nil {
		return nil
	}
	raw, ok := root["table_maintenance"]
	if !ok {
		return nil
	}
	maintenance, ok := raw.(map[string]any)
	if !ok || maintenance == nil {
		return nil
	}
	return normalizeIcebergMaintenanceTargets(maintenance)
}

func normalizeIcebergMaintenanceTargets(maintenance map[string]any) error {
	rawNamespace, compact := maintenance["namespace"]
	if !compact {
		return nil
	}
	namespaces, err := maintenanceStringList(rawNamespace, "table_maintenance.namespace")
	if err != nil {
		return err
	}
	if len(namespaces) != 1 {
		return fmt.Errorf("table_maintenance.namespace compact form requires exactly one namespace")
	}

	rawTables, ok := maintenance["tables"]
	if !ok {
		return fmt.Errorf("table_maintenance.tables is required with compact namespace form")
	}
	tables, err := maintenanceStringList(rawTables, "table_maintenance.tables")
	if err != nil {
		return fmt.Errorf("compact table_maintenance.tables must contain table names: %w", err)
	}
	explicit := make([]any, 0, len(tables))
	for _, table := range tables {
		explicit = append(explicit, map[string]any{"namespace": namespaces[0], "table": table})
	}
	maintenance["tables"] = explicit
	delete(maintenance, "namespace")
	return nil
}

func maintenanceStringList(raw any, field string) ([]string, error) {
	var values []string
	switch typed := raw.(type) {
	case string:
		values = []string{typed}
	case []string:
		values = typed
	case []any:
		values = make([]string, 0, len(typed))
		for _, item := range typed {
			value, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s must contain only strings", field)
			}
			values = append(values, value)
		}
	default:
		return nil, fmt.Errorf("%s must be a string or list of strings", field)
	}

	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s contains an empty value", field)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}
