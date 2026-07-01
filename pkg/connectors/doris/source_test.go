package doris

import (
	"context"
	"testing"
)

func TestNormalizeDorisSourceConfigExpandsDatabasesAndTableNames(t *testing.T) {
	cfg := normalizeDorisSourceConfig(SourceConfig{
		Databases:  []string{" Source_Alpha ", "source_beta"},
		TableNames: []string{" Tbl_Reservasi_Backup "},
		Tables:     []string{"legacy.orders"},
	})

	want := []string{
		"legacy.orders",
		"source_alpha.tbl_reservasi_backup",
		"source_beta.tbl_reservasi_backup",
	}
	if got := len(cfg.Tables); got != len(want) {
		t.Fatalf("tables length = %d, want %d (%#v)", got, len(want), cfg.Tables)
	}
	for i := range want {
		if cfg.Tables[i] != want[i] {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, cfg.Tables[i], want[i], cfg.Tables)
		}
	}
}

func TestExpandDorisConfiguredTablesExpandsTableGlobInOrder(t *testing.T) {
	got, err := expandDorisConfiguredTables(context.Background(), []string{
		"raw.tbl_reservasi_backup_*",
	}, func(ctx context.Context, dbName string) ([]string, error) {
		if dbName != "raw" {
			t.Fatalf("listTables called with dbName=%q, want raw", dbName)
		}
		return []string{
			"tbl_reservasi",
			"tbl_reservasi_backup_202501",
			"tbl_reservasi_backup_202502",
		}, nil
	})
	if err != nil {
		t.Fatalf("expandDorisConfiguredTables returned error: %v", err)
	}

	want := []string{
		"raw.tbl_reservasi_backup_202501",
		"raw.tbl_reservasi_backup_202502",
	}
	if gotLen := len(got); gotLen != len(want) {
		t.Fatalf("length = %d, want %d (%#v)", gotLen, len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, got[i], want[i], got)
		}
	}
}

func TestDorisColumnFromDescNormalizesTypes(t *testing.T) {
	col := dorisColumnFromDesc("amount", "DECIMALV3(18, 2)", "No", "true")
	if col.DataType != "decimal" {
		t.Fatalf("DataType = %q, want decimal", col.DataType)
	}
	if col.NumPrec == nil || *col.NumPrec != 18 {
		t.Fatalf("NumPrec = %v, want 18", col.NumPrec)
	}
	if col.NumScale == nil || *col.NumScale != 2 {
		t.Fatalf("NumScale = %v, want 2", col.NumScale)
	}
	if col.IsNullable {
		t.Fatal("IsNullable = true, want false")
	}
	if !col.IsPK {
		t.Fatal("IsPK = false, want true")
	}

	col = dorisColumnFromDesc("created_at", "DATETIMEV2(3)", "Yes", "")
	if col.DataType != "datetime" {
		t.Fatalf("DataType = %q, want datetime", col.DataType)
	}
	if !col.IsNullable {
		t.Fatal("IsNullable = false, want true")
	}
}

func TestDorisSourceMySQLAddrUsesHTTPHostAndPort(t *testing.T) {
	got, err := dorisSourceMySQLAddr(SourceConfig{
		HTTPHost:  "http://doris-fe.example:8030",
		MySQLPort: 9030,
	})
	if err != nil {
		t.Fatalf("dorisSourceMySQLAddr returned error: %v", err)
	}
	if got != "doris-fe.example:9030" {
		t.Fatalf("addr = %q, want doris-fe.example:9030", got)
	}
}
