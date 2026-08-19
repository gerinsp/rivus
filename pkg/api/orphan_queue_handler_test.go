package api

import (
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestNormalizeQueuedOrphanOptionsDefaultsToDryRun(t *testing.T) {
	opts, err := normalizeQueuedOrphanOptions(queuedIcebergOrphanCleanupRequest{})
	if err != nil {
		t.Fatalf("normalize options: %v", err)
	}
	if !opts.DryRun {
		t.Fatal("expected manual orphan cleanup to default to dry_run=true")
	}
	if opts.OlderThanHours != 0 {
		t.Fatalf("expected configured maintenance age when omitted, got %v", opts.OlderThanHours)
	}
}

func TestNormalizeQueuedOrphanOptionsRejectsUnsafeAge(t *testing.T) {
	_, err := normalizeQueuedOrphanOptions(queuedIcebergOrphanCleanupRequest{OlderThanHours: 72})
	if err == nil || !strings.Contains(err.Error(), "at least 168") {
		t.Fatalf("expected seven-day safety floor error, got %v", err)
	}
}

func TestNormalizeQueuedOrphanOptionsRejectsPerRequestConcurrency(t *testing.T) {
	_, err := normalizeQueuedOrphanOptions(queuedIcebergOrphanCleanupRequest{MaxConcurrency: 2})
	if err == nil || !strings.Contains(err.Error(), "RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY") {
		t.Fatalf("expected global concurrency guidance, got %v", err)
	}
}

func TestSelectQueuedOrphanTablesMatchesCommonIdentifiers(t *testing.T) {
	states := []meta.IcebergMaintenanceState{
		{
			TableKey:          "catalog_a|bronze|orders",
			Catalog:           "catalog_a",
			Namespace:         "bronze",
			Table:             "orders",
			SnapshotComplete:  true,
		},
		{
			TableKey:          "catalog_a|bronze|customers",
			Catalog:           "catalog_a",
			Namespace:         "bronze",
			Table:             "customers",
			SnapshotComplete:  true,
		},
	}

	selected, missing := selectQueuedOrphanTables(states, []string{"bronze.orders", "CATALOG_A.bronze.customers"})
	if len(missing) != 0 {
		t.Fatalf("unexpected missing tables: %v", missing)
	}
	if !selected[states[0].TableKey] || !selected[states[1].TableKey] {
		t.Fatalf("expected both tables selected, got %#v", selected)
	}
}

func TestSelectQueuedOrphanTablesReportsMissing(t *testing.T) {
	states := []meta.IcebergMaintenanceState{{
		TableKey:  "catalog_a|bronze|orders",
		Catalog:   "catalog_a",
		Namespace: "bronze",
		Table:     "orders",
	}}
	_, missing := selectQueuedOrphanTables(states, []string{"orders", "missing"})
	if len(missing) != 1 || missing[0] != "missing" {
		t.Fatalf("expected missing table to be reported, got %v", missing)
	}
}
