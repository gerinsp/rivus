package meta

import (
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

func TestNormalizeJobStoreDSNEnablesClientFoundRows(t *testing.T) {
	dsn, err := normalizeJobStoreDSN("rivus:secret@tcp(127.0.0.1:3306)/rivus_meta?parseTime=true&timeout=5s")
	if err != nil {
		t.Fatalf("normalizeJobStoreDSN returned error: %v", err)
	}

	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN returned error: %v", err)
	}
	if !cfg.ClientFoundRows {
		t.Fatal("ClientFoundRows = false, want true")
	}
	if !cfg.ParseTime {
		t.Fatal("ParseTime was not preserved")
	}
	if cfg.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %s, want 5s", cfg.Timeout)
	}
}
