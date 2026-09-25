package api

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestDurableMaintenanceOverallStateReportsStaleInventory(t *testing.T) {
	summary := meta.IcebergMaintenanceOwnerSummary{Tables: 1}
	got := durableMaintenanceOverallState(summary, 1, 0, 0, false, false, true)
	if got != "stale" {
		t.Fatalf("durable maintenance state = %q, want stale", got)
	}
}

func TestDurableMaintenanceOverallStatePreservesErrorPrecedence(t *testing.T) {
	summary := meta.IcebergMaintenanceOwnerSummary{Tables: 1}
	got := durableMaintenanceOverallState(summary, 1, 0, 1, false, false, true)
	if got != "error" {
		t.Fatalf("durable maintenance state = %q, want error", got)
	}
}
