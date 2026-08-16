package config

import "gopkg.in/yaml.v3"

// UnmarshalYAML keeps table_maintenance.enabled as the user-facing master
// switch while preventing the removed CDC-side automatic maintenance monitor
// from being activated at runtime. NativeEnabled becomes the internal worker
// signal switch and is true only when both user-facing switches are true.
//
// The original raw connector map is retained in JobConfig and is what the
// separate maintenance worker reads, so this runtime normalization does not
// rewrite the persisted job definition.
func (c *IcebergTableMaintenanceConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain IcebergTableMaintenanceConfig
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}

	masterEnabled := decoded.Enabled
	workerEnabled := masterEnabled && decoded.NativeEnabled

	*c = IcebergTableMaintenanceConfig(decoded)
	// The legacy in-process automatic monitor is no longer part of the runtime
	// architecture. Keep this false so NewSink never starts it.
	c.Enabled = false
	c.NativeEnabled = workerEnabled
	return nil
}
