package iceberg

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/meta"
)

// Keep the positional-delete regression that required the temporary v0.6 fork
// covered through public APIs, including the scan used by native compaction.
func TestIcebergLibraryUpgradePositionDeletesCompactionAndExpiration(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberglib.Properties{
		icetable.WriteDeleteModeKey:      icetable.WriteModeMergeOnRead,
		icetable.ParquetRowGroupLimitKey: "5",
	}); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]interface{}, 12)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": int64(i + 1), "status": "seed"}
	}
	reader, release, err := buildRecordReader(tbl.Schema(), rows)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tbl.Delete(ctx, iceberglib.EqualTo(iceberglib.Reference("id"), int64(7)), nil)
	if err != nil {
		t.Fatal(err)
	}
	scan := tbl.Scan(icetable.WithSelectedFields("id"), icetable.WithArrowBatchSize(2))
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || len(tasks[0].DeleteFiles) == 0 {
		t.Fatalf("expected one data file with positional deletes: %v", tasks)
	}
	check := func() {
		t.Helper()
		_, records, err := tbl.Scan(icetable.WithSelectedFields("id"), icetable.WithArrowBatchSize(2)).ToArrowRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for record, err := range records {
			if err != nil {
				t.Fatal(err)
			}
			col := record.Column(0).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				ids = append(ids, col.Value(i))
			}
			record.Release()
		}
		if want := []int64{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12}; !reflect.DeepEqual(ids, want) {
			t.Fatalf("visible rows = %v, want %v", ids, want)
		}
	}
	check()
	tx = tbl.NewTransaction()
	_, err = tx.RewriteDataFiles(ctx, []icetable.CompactionTaskGroup{{Tasks: tasks}}, icetable.RewriteDataFilesOptions{
		GroupOptions: []icetable.CompactionGroupOption{icetable.WithCompactionArrowBatchSize(2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check()
	out := executeNativeSnapshotExpiration(ctx, tbl, meta.IcebergMaintenanceResult{}, nativeMaintenanceSettings{SnapshotMaxAge: 0, SnapshotRetainLast: 1})
	if out.Result.Status != "succeeded" {
		t.Fatalf("snapshot expiration: %+v", out.Result)
	}
	if err := tbl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tbl.Metadata().Snapshots()) != 1 {
		t.Fatalf("retained snapshots = %d, want 1", len(tbl.Metadata().Snapshots()))
	}
	check()
}
