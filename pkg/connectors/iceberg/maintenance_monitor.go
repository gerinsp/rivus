package iceberg

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/gerinsp/rivus/pkg/config"
	"gopkg.in/yaml.v3"
)

var maintenanceMonitorIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

const maxMaintenanceMonitorIDLength = 247 // "monitor:" + ID must fit owner_job_id VARCHAR(255).

// PrepareMaintenanceMonitorConfig validates and normalizes a standalone,
// long-running Iceberg maintenance registration. It deliberately rejects a
// source connector so submitting a monitor can never start ingestion.
func PrepareMaintenanceMonitorConfig(cfg *config.JobConfig) (*config.JobConfig, []config.IcebergTarget, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("maintenance monitor config is nil")
	}
	cloned := *cfg
	config.ApplyDefaults(&cloned)
	cloned.ID = strings.TrimSpace(cloned.ID)
	cloned.Name = strings.TrimSpace(cloned.Name)
	if cloned.ID == "" {
		return nil, nil, fmt.Errorf("maintenance monitor id is required")
	}
	if len(cloned.ID) > maxMaintenanceMonitorIDLength {
		return nil, nil, fmt.Errorf("maintenance monitor id exceeds %d characters", maxMaintenanceMonitorIDLength)
	}
	if !maintenanceMonitorIDPattern.MatchString(cloned.ID) {
		return nil, nil, fmt.Errorf("maintenance monitor id must contain only letters, numbers, dots, underscores, or hyphens")
	}
	if cloned.Name == "" {
		cloned.Name = cloned.ID
	}
	if len(cloned.Name) > 255 {
		return nil, nil, fmt.Errorf("maintenance monitor name exceeds 255 characters")
	}
	if cloned.Mode == "" {
		cloned.Mode = config.JobModeMaintenanceOnly
	}
	if cloned.Mode != config.JobModeMaintenanceOnly {
		return nil, nil, fmt.Errorf("maintenance monitor mode must be %q", config.JobModeMaintenanceOnly)
	}
	if cloned.Source != nil {
		return nil, nil, fmt.Errorf("maintenance-only monitors must not define a source connector")
	}

	sinkType, sinkCfg := jobSinkSpec(&cloned)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return nil, nil, fmt.Errorf("maintenance monitor sink is %q, not iceberg_native", sinkType)
	}
	normalizedSinkCfg, err := configMap(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	if err := config.NormalizeIcebergMaintenanceTargets(normalizedSinkCfg); err != nil {
		return nil, nil, err
	}
	sinkCfg = normalizedSinkCfg
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	if settings.Executor != maintenanceExecutorNative {
		if err := validateMaintenanceBackend(iceCfg.TableMaintenance); err != nil {
			return nil, nil, err
		}
	}
	if catalogName := maintenanceCatalogName(iceCfg); !sparkCatalogNamePattern.MatchString(catalogName) {
		return nil, nil, fmt.Errorf("invalid Iceberg catalog name %q", catalogName)
	}
	if !iceCfg.TableMaintenance.Enabled {
		return nil, nil, fmt.Errorf("iceberg table_maintenance.enabled must be true")
	}
	rawTargets := iceCfg.TableMaintenance.Tables
	if len(rawTargets) == 0 {
		return nil, nil, fmt.Errorf("iceberg table_maintenance.tables must contain at least one namespace/table target")
	}
	for _, target := range rawTargets {
		if strings.TrimSpace(target.Namespace) == "" || strings.TrimSpace(target.Table) == "" {
			return nil, nil, fmt.Errorf("maintenance table namespace and table are required")
		}
		if strings.Contains(target.Namespace, "*") || strings.Contains(target.Table, "*") {
			return nil, nil, fmt.Errorf("maintenance-only monitors require explicit tables; wildcards are not supported")
		}
	}
	targets := dedupeTargets(rawTargets)
	if len(targets) > maxMaintenanceTables {
		return nil, nil, fmt.Errorf("maintenance monitor selects %d tables; maximum is %d", len(targets), maxMaintenanceTables)
	}

	// Preserve the complete generic sink configuration persisted by the API.
	// Some maintenance settings are intentionally consumed from the raw map for
	// backward compatibility, so re-encoding only the typed IcebergConfig would
	// silently discard them. Only replace the validated table list here.
	encoded, err := configMap(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	maintenance := rawMaintenanceMap(encoded)
	if maintenance == nil {
		return nil, nil, fmt.Errorf("iceberg table_maintenance config is required")
	}
	maintenance["tables"] = targets
	cloned.Sink = &config.ConnectorSpec{Type: "iceberg_native", Config: encoded}
	return &cloned, targets, nil
}

// DescribeMaintenanceMonitorConfig returns the safe fields needed by the
// monitor API and UI without exposing catalog or runner credentials.
func DescribeMaintenanceMonitorConfig(cfg *config.JobConfig) (string, string, string, []config.IcebergTarget, error) {
	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		return "", "", "", nil, err
	}
	_, sinkCfg := jobSinkSpec(normalized)
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return "", "", "", nil, err
	}
	executor := strings.ToLower(strings.TrimSpace(iceCfg.TableMaintenance.Executor))
	if executor == "" {
		executor = "hybrid"
	}
	profile := strings.ToLower(strings.TrimSpace(iceCfg.TableMaintenance.RunnerResourceProfile))
	if profile == "" {
		profile = "small"
	}
	return maintenanceCatalogName(iceCfg), executor, profile, targets, nil
}

func configMap(value any) (map[string]any, error) {
	payload, err := yaml.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode normalized maintenance config: %w", err)
	}
	var out map[string]any
	if err := yaml.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("decode normalized maintenance config: %w", err)
	}
	return out, nil
}
