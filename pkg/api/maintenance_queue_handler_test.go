package api

import (
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestValidQueuedMaintenanceOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		want      bool
	}{
		{name: "compact", operation: "compact", want: true},
		{name: "expire snapshots", operation: "expire_snapshots", want: true},
		{name: "remove orphan files", operation: "remove_orphan_files", want: true},
		{name: "misspelled expiration", operation: "expire_snapshoot", want: false},
		{name: "unknown operation", operation: "delete_table", want: false},
		{name: "empty operation", operation: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validQueuedMaintenanceOperation(tt.operation); got != tt.want {
				t.Fatalf("validQueuedMaintenanceOperation(%q) = %v, want %v", tt.operation, got, tt.want)
			}
		})
	}
}

func TestNormalizeQueuedMaintenanceOptionsRequiresTable(t *testing.T) {
	_, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{})
	if err == nil || !strings.Contains(err.Error(), "at least one table") {
		t.Fatalf("expected missing-table error, got %v", err)
	}
}

func TestNormalizeQueuedMaintenanceOptionsDeduplicatesTables(t *testing.T) {
	opts, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{
		Tables: []string{"bronze.orders", " BRONZE.ORDERS ", ""},
	})
	if err != nil {
		t.Fatalf("normalize options: %v", err)
	}
	if len(opts.Tables) != 1 || opts.Tables[0] != "bronze.orders" {
		t.Fatalf("expected one normalized table, got %#v", opts.Tables)
	}
}

func TestNormalizeQueuedMaintenanceOptionsDefaultsToDryRun(t *testing.T) {
	opts, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{Tables: []string{"orders"}})
	if err != nil {
		t.Fatalf("normalize options: %v", err)
	}
	if !opts.DryRun {
		t.Fatal("expected dry_run to default to true")
	}
}

func TestNormalizeQueuedMaintenanceOptionsAcceptsExplicitDryRunFalse(t *testing.T) {
	dryRun := false
	opts, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{
		DryRun: &dryRun,
		Tables: []string{"orders"},
	})
	if err != nil {
		t.Fatalf("normalize options: %v", err)
	}
	if opts.DryRun {
		t.Fatal("expected explicit dry_run=false to be preserved")
	}
}

func TestNormalizeQueuedMaintenanceOptionsRejectsUnsafeAge(t *testing.T) {
	_, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{
		OlderThanHours: 72,
		Tables:         []string{"orders"},
	})
	if err == nil || !strings.Contains(err.Error(), "at least 168") {
		t.Fatalf("expected seven-day safety-floor error, got %v", err)
	}
}

func TestNormalizeQueuedMaintenanceOptionsRejectsNegativeAge(t *testing.T) {
	_, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{
		OlderThanHours: -1,
		Tables:         []string{"orders"},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("expected negative-age error, got %v", err)
	}
}

func TestNormalizeQueuedMaintenanceOptionsRejectsPerRequestConcurrency(t *testing.T) {
	_, err := normalizeQueuedMaintenanceOptions(queuedMaintenanceRequest{
		MaxConcurrency: 2,
		Tables:         []string{"orders"},
	})
	if err == nil || !strings.Contains(err.Error(), "RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY") {
		t.Fatalf("expected global-concurrency guidance, got %v", err)
	}
}

func TestSelectQueuedMaintenanceTablesMatchesCommonIdentifiers(t *testing.T) {
	states := []meta.IcebergMaintenanceState{
		{
			TableKey:         "catalog_a|bronze|orders",
			Catalog:          "catalog_a",
			Namespace:        "bronze",
			Table:            "orders",
			SnapshotComplete: true,
		},
		{
			TableKey:         "catalog_a|bronze|customers",
			Catalog:          "catalog_a",
			Namespace:        "bronze",
			Table:            "customers",
			SnapshotComplete: true,
		},
	}

	selected, missing := selectQueuedMaintenanceTables(states, []string{
		"bronze.orders",
		"CATALOG_A.bronze.customers",
	})
	if len(missing) != 0 {
		t.Fatalf("unexpected missing tables: %v", missing)
	}
	if !selected[states[0].TableKey] || !selected[states[1].TableKey] {
		t.Fatalf("expected both tables selected, got %#v", selected)
	}
}

func TestSelectQueuedMaintenanceTablesReportsMissingSorted(t *testing.T) {
	states := []meta.IcebergMaintenanceState{{
		TableKey:  "catalog_a|bronze|orders",
		Catalog:   "catalog_a",
		Namespace: "bronze",
		Table:     "orders",
	}}

	selected, missing := selectQueuedMaintenanceTables(states, []string{"z_missing", "orders", "a_missing"})
	if !selected[states[0].TableKey] {
		t.Fatalf("expected orders to be selected, got %#v", selected)
	}
	if len(missing) != 2 || missing[0] != "a_missing" || missing[1] != "z_missing" {
		t.Fatalf("expected sorted missing tables, got %v", missing)
	}
}
