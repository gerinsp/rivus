package config

import "gopkg.in/yaml.v3"

// UnmarshalYAML translates the public maintenance feature gates into runtime
// executor gates. The raw ConnectorSpec map remains the persisted/user-facing
// definition, where enabled is still the master switch.
//
// The CDC-side legacy automatic monitor keys off Enabled. It is intentionally
// forced off here because scheduling now belongs exclusively to the durable
// maintenance worker. NativeEnabled is retained only when both public switches
// were enabled, so the CDC signaler can feed that worker.
func (c *IcebergTableMaintenanceConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain IcebergTableMaintenanceConfig
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}

	workerEnabled := decoded.Enabled && decoded.NativeEnabled
	*c = IcebergTableMaintenanceConfig(decoded)
	c.Enabled = false
	c.NativeEnabled = workerEnabled
	return nil
}
