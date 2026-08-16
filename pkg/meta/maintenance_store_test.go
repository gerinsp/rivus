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
