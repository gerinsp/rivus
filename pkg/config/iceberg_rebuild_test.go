package config

import "testing"

func TestIcebergSnapshotRebuildRequiresExplicitFullReset(t *testing.T) {
	for _, mode := range []string{"auto", "truncate-append", "append", "overwrite", "", "replace-filter-append"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &JobConfig{Source: &ConnectorSpec{Type: "mysql", Config: map[string]any{"database": "app", "table": "orders"}}, Sink: &ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": mode, "snapshot_truncate_tables": []string{"app.orders"}}}}
			err := ValidateIcebergSnapshotRebuild(cfg)
			if (mode == "auto" || mode == "truncate-append") != (err == nil) {
				t.Fatalf("mode=%q error=%v", mode, err)
			}
		})
	}
	for _, fault := range []string{"partial selection", "excluded", "replace filter", "source filter", "untracked reset executor", "wildcard source", "wildcard truncate without opt-in", "no selection"} {
		t.Run(fault, func(t *testing.T) {
			source := map[string]any{"databases": []string{"app"}, "table_names": []string{"orders", "customers"}}
			sink := map[string]any{"snapshot_write_mode": "auto", "snapshot_truncate_tables": []string{"app.*"}, "snapshot_truncate_allow_patterns": true}
			switch fault {
			case "partial selection":
				sink["snapshot_truncate_tables"] = []string{"app.orders"}
			case "excluded":
				sink["snapshot_truncate_exclude_tables"] = []string{"*.orders"}
			case "replace filter":
				sink["snapshot_replace_filters"] = map[string]any{"app.orders": map[string]any{"column": "id", "op": "gte", "value": 1}}
			case "source filter":
				source["table_configs"] = map[string]any{"app.*": map[string]any{"filter": "id > 100"}}
			case "untracked reset executor":
				sink["snapshot_replace_delete_executor"] = "trino"
			case "wildcard source":
				source = map[string]any{"tables": []string{"app.*"}}
			case "wildcard truncate without opt-in":
				sink["snapshot_truncate_allow_patterns"] = false
			case "no selection":
				source = map[string]any{}
			}
			cfg := &JobConfig{Source: &ConnectorSpec{Type: "mysql", Config: source}, Sink: &ConnectorSpec{Type: "iceberg_native", Config: sink}}
			if err := ValidateIcebergSnapshotRebuild(cfg); err == nil {
				t.Fatal("unsafe reset was allowed")
			}
		})
	}
}
