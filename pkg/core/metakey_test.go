package core

import "testing"

func TestBuildMetaKeyIgnoresCDCDeleteExecutor(t *testing.T) {
	sourceCfg := map[string]any{
		"addr": "mysql:3306",
	}
	baseSinkCfg := map[string]any{
		"warehouse":                        "generic",
		"snapshot_replace_delete_executor": "trino",
		"trino_delete": map[string]any{
			"uri": "https://trino:8443",
		},
	}
	withCDCExecutor := map[string]any{
		"warehouse":                        "generic",
		"snapshot_replace_delete_executor": "trino",
		"cdc_delete_executor":              "trino",
		"trino_delete": map[string]any{
			"uri": "https://trino:8443",
		},
	}

	before := buildMetaKey("job-1", "initial", "mysql", sourceCfg, "iceberg_native", baseSinkCfg)
	after := buildMetaKey("job-1", "initial", "mysql", sourceCfg, "iceberg_native", withCDCExecutor)

	if before != after {
		t.Fatalf("metakey changed after adding cdc_delete_executor: before=%s after=%s", before, after)
	}
}
