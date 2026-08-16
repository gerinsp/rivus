package config

import "gopkg.in/yaml.v3"

// UnmarshalYAML makes table_maintenance.enabled the single runtime feature
// switch. NativeEnabled remains only as an internal compatibility field while
// worker internals are being renamed; it is always derived from Enabled and the
// historical native_enabled YAML key is ignored.
func (c *IcebergTableMaintenanceConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain IcebergTableMaintenanceConfig
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}

	*c = IcebergTableMaintenanceConfig(decoded)
	c.Enabled = decoded.Enabled
	c.NativeEnabled = decoded.Enabled
	return nil
}
