package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNormalizeMySQLConfigExpandsSeparatedSelections(t *testing.T) {
	cfg := NormalizeMySQLConfig(MySQLConfig{
		Databases:  []string{" App ", "logs"},
		TableNames: []string{"Orders", " audit ", "orders"},
		Tables:     []string{"legacy.users", "app.orders"},
	})

	want := []string{
		"legacy.users",
		"app.orders",
		"app.audit",
		"logs.orders",
		"logs.audit",
	}
	if got, wantLen := len(cfg.Tables), len(want); got != wantLen {
		t.Fatalf("tables length = %d, want %d (%#v)", got, wantLen, cfg.Tables)
	}
	for i, table := range want {
		if cfg.Tables[i] != table {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, cfg.Tables[i], table, cfg.Tables)
		}
	}
}

func TestNormalizeMySQLConfigFallsBackToLegacySingleTable(t *testing.T) {
	cfg := NormalizeMySQLConfig(MySQLConfig{
		Database: "App",
		Table:    "Orders",
	})

	if got, want := len(cfg.Tables), 1; got != want {
		t.Fatalf("tables length = %d, want %d", got, want)
	}
	if cfg.Tables[0] != "app.orders" {
		t.Fatalf("tables = %#v, want legacy single table to be normalized", cfg.Tables)
	}
}

func TestNormalizeMySQLConfigDefaultsSnapshotBatchSize(t *testing.T) {
	cfg := NormalizeMySQLConfig(MySQLConfig{
		Tables: []string{"app.orders"},
	})
	if got, want := cfg.SnapshotBatchSize, 10000; got != want {
		t.Fatalf("SnapshotBatchSize = %d, want %d", got, want)
	}
}

func TestNormalizeMySQLConfigTrimsSnapshotKeyColumns(t *testing.T) {
	cfg := NormalizeMySQLConfig(MySQLConfig{
		Tables: []string{"app.orders"},
		TableConfigs: map[string]MySQLTableConfig{
			"App.Orders": {
				SnapshotKeyColumns: []string{" created_at ", "", " id "},
			},
		},
	})

	cols := cfg.TableConfigs["app.orders"].SnapshotKeyColumns
	want := []string{"created_at", "id"}
	if len(cols) != len(want) {
		t.Fatalf("SnapshotKeyColumns = %#v, want %#v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("SnapshotKeyColumns[%d] = %q, want %q", i, cols[i], want[i])
		}
	}
}

func TestLoadJobConfigParsesIcebergByteSize(t *testing.T) {
	var icebergCfg IcebergConfig
	if err := yaml.Unmarshal([]byte(`max_batch_bytes: 128MB`), &icebergCfg); err != nil {
		t.Fatalf("yaml.Unmarshal returned error: %v", err)
	}
	if got, want := int64(icebergCfg.MaxBatchBytes), int64(128*1024*1024); got != want {
		t.Fatalf("MaxBatchBytes = %d, want %d", got, want)
	}
}

func TestLoadJobConfigsFromBytesParsesMultiDocumentYAML(t *testing.T) {
	configs, err := LoadJobConfigsFromBytes([]byte(`
id: job-a
name: Job A
source:
  type: mysql
  config: {}
sink:
  type: iceberg_native
  config: {}
---
id: job-b
name: Job B
source:
  type: mysql
  config: {}
sink:
  type: iceberg_native
  config: {}
`))
	if err != nil {
		t.Fatalf("LoadJobConfigsFromBytes returned error: %v", err)
	}
	if got, want := len(configs), 2; got != want {
		t.Fatalf("configs length = %d, want %d", got, want)
	}
	if configs[0].ID != "job-a" || configs[1].ID != "job-b" {
		t.Fatalf("config IDs = %q, %q; want job-a, job-b", configs[0].ID, configs[1].ID)
	}
}
