package meta

import (
	"reflect"
	"strings"
	"testing"
)

func TestMaintenanceRunFilterWhereGlobal(t *testing.T) {
	where, args := maintenanceRunFilterWhere(IcebergMaintenanceRunFilter{
		Status:    "finished_with_errors",
		Search:    "reservasi",
		Operation: "compact",
		Engine:    "spark",
	}, false)
	for _, fragment := range []string{"r.status=?", "search_res.table_key LIKE ?", "filter_res.operation=?", "filter_res.engine=?"} {
		if !strings.Contains(where, fragment) {
			t.Fatalf("filter SQL missing %q: %s", fragment, where)
		}
	}
	want := []any{"finished_with_errors", "%reservasi%", "%reservasi%", "%reservasi%", "compact", "spark"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("filter args = %#v, want %#v", args, want)
	}
}

func TestMaintenanceRunFilterWhereOwnerScoped(t *testing.T) {
	where, args := maintenanceRunFilterWhere(IcebergMaintenanceRunFilter{
		OwnerJobID: "cluster-iceberg-tiketux",
		Search:     "customer",
		Operation:  "expire_snapshots",
	}, true)
	if !strings.Contains(where, "search_task.owner_job_id=?") || !strings.Contains(where, "filter_task.owner_job_id=?") {
		t.Fatalf("owner filter SQL does not preserve job scope: %s", where)
	}
	want := []any{"%customer%", "%customer%", "cluster-iceberg-tiketux", "%customer%", "cluster-iceberg-tiketux", "expire_snapshots"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("filter args = %#v, want %#v", args, want)
	}
}

func TestSnapshotIDsDifferTreatsIDsAsOpaque(t *testing.T) {
	if !snapshotIDsDiffer(9187669318630075949, 7281588297543022022) {
		t.Fatal("a numerically smaller current snapshot must still be treated as new")
	}
	if snapshotIDsDiffer(7281588297543022022, 7281588297543022022) {
		t.Fatal("the same snapshot must not be counted twice")
	}
	if snapshotIDsDiffer(7281588297543022022, 0) {
		t.Fatal("zero is not a valid current snapshot signal")
	}
}

func TestMaintenanceMonitorOwnerIDIsNamespaced(t *testing.T) {
	if got, want := MaintenanceMonitorOwnerID("  daily-tables  "), "monitor:daily-tables"; got != want {
		t.Fatalf("MaintenanceMonitorOwnerID() = %q, want %q", got, want)
	}
}
