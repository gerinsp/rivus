package mysql

import "testing"

func TestDecodeMySQLConfigNormalizesTableConfigs(t *testing.T) {
	cfg, err := decodeMySQLConfig(map[string]any{
		"database": "legacy_db",
		"table":    "legacy_table",
		"tables":   []any{"App.Orders", " app.Users "},
		"table_configs": map[string]any{
			"App.Orders": map[string]any{
				"filter": " WHERE status = 'PAID'; ",
			},
			"users": map[string]any{
				"filter": " deleted_at IS NULL ",
			},
		},
	})
	if err != nil {
		t.Fatalf("decodeMySQLConfig returned error: %v", err)
	}

	if got, want := len(cfg.Tables), 2; got != want {
		t.Fatalf("tables length = %d, want %d", got, want)
	}
	if cfg.Tables[0] != "app.orders" || cfg.Tables[1] != "app.users" {
		t.Fatalf("tables = %#v, want normalized full names", cfg.Tables)
	}

	full, ok := cfg.TableConfigs["app.orders"]
	if !ok {
		t.Fatalf("expected full table config to be normalized, got %#v", cfg.TableConfigs)
	}
	if full.Filter != "status = 'PAID'" {
		t.Fatalf("full filter = %q, want %q", full.Filter, "status = 'PAID'")
	}

	short, ok := cfg.TableConfigs["users"]
	if !ok {
		t.Fatalf("expected short table config to be preserved, got %#v", cfg.TableConfigs)
	}
	if short.Filter != "deleted_at IS NULL" {
		t.Fatalf("short filter = %q, want %q", short.Filter, "deleted_at IS NULL")
	}
}

func TestDecodeMySQLConfigKeepsWildcardTablePatterns(t *testing.T) {
	cfg, err := decodeMySQLConfig(map[string]any{
		"tables": []any{"App.*", " Logs.Audit "},
	})
	if err != nil {
		t.Fatalf("decodeMySQLConfig returned error: %v", err)
	}

	if got, want := len(cfg.Tables), 2; got != want {
		t.Fatalf("tables length = %d, want %d", got, want)
	}
	if cfg.Tables[0] != "app.*" || cfg.Tables[1] != "logs.audit" {
		t.Fatalf("tables = %#v, want wildcard pattern to be normalized but preserved", cfg.Tables)
	}
}

func TestDecodeMySQLConfigExpandsSeparatedDatabasesAndTableNames(t *testing.T) {
	cfg, err := decodeMySQLConfig(map[string]any{
		"tables":      []any{"app.orders", " legacy.users ", "Logs.Audit"},
		"databases":   []any{"App", " logs "},
		"table_names": []any{" Orders ", "audit", "orders"},
	})
	if err != nil {
		t.Fatalf("decodeMySQLConfig returned error: %v", err)
	}

	want := []string{
		"app.orders",
		"legacy.users",
		"logs.audit",
		"app.audit",
		"logs.orders",
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

func TestDecodeMySQLConfigExpandsWildcardTableNamesAcrossDatabases(t *testing.T) {
	cfg, err := decodeMySQLConfig(map[string]any{
		"databases":   []any{"App", "Logs"},
		"table_names": []any{"*"},
	})
	if err != nil {
		t.Fatalf("decodeMySQLConfig returned error: %v", err)
	}

	if got, want := len(cfg.Tables), 2; got != want {
		t.Fatalf("tables length = %d, want %d", got, want)
	}
	if cfg.Tables[0] != "app.*" || cfg.Tables[1] != "logs.*" {
		t.Fatalf("tables = %#v, want wildcard expansion across databases", cfg.Tables)
	}
}
