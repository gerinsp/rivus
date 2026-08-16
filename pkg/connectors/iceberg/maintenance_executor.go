package iceberg

import (
	"fmt"
	"strings"
)

const (
	maintenanceExecutorHybrid = "hybrid"
	maintenanceExecutorNative = "native"
	maintenanceExecutorSpark  = "spark"
)

func normalizeMaintenanceExecutor(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return maintenanceExecutorHybrid
	}
	return value
}

func validateMaintenanceExecutor(value string) error {
	switch normalizeMaintenanceExecutor(value) {
	case maintenanceExecutorHybrid, maintenanceExecutorNative, maintenanceExecutorSpark:
		return nil
	default:
		return fmt.Errorf("table_maintenance.executor must be one of hybrid, native, spark")
	}
}
