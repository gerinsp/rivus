package iceberg

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestRolledSnapshotCombinesSourceBatchesIntoOneDataFile(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)
	spoolDir, err := prepareSnapshotSpoolDirectory(t.TempDir(), "job-1", "state-1")
	if err != nil {
		t.Fatalf("prepareSnapshotSpoolDirectory: %v", err)
	}
	sink := &Sink{
		jobID:            "job-1",
		cfg:              normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: snapshotWriteModeAppend, SnapshotTargetFileSizeBytes: 1024 * 1024}),
		snapshotSpoolDir: spoolDir,
		states:           make(map[string]*tableState),
	}
	state := &tableState{
		sourceKey:       "app.orders",
		targetNamespace: "bronze",
		targetTable:     "orders",
		sourceSchema: &model.TableSchema{
			SchemaName: "app",
			TableName:  "orders",
			Columns: []model.TableColumn{
				{Name: "id", DataType: "bigint", IsPK: true},
				{Name: "status", DataType: "varchar"},
			},
		},
		table:              tbl,
		snapshotAppendSafe: true,
	}
	sink.states[state.sourceKey] = state

	for batchIndex, rows := range [][]map[string]interface{}{
		{{"id": int64(1), "status": "new"}, {"id": int64(2), "status": "new"}},
		{{"id": int64(3), "status": "paid"}, {"id": int64(4), "status": "paid"}},
	} {
		if err := sink.appendSnapshotSpool(ctx, state, rows, time.Now(), int64(batchIndex*2)); err != nil {
			t.Fatalf("appendSnapshotSpool batch %d: %v", batchIndex, err)
		}
	}
	if state.snapshotSpool == nil || state.snapshotSpool.batchCount != 2 || state.snapshotSpool.rowCount != 4 {
		t.Fatalf("snapshot spool = %#v, want 2 batches and 4 rows", state.snapshotSpool)
	}
	if err := sink.finalizeSnapshotSpool(ctx, state); err != nil {
		t.Fatalf("finalizeSnapshotSpool: %v", err)
	}
	if got, want := catalog.commits, 2; got != want {
		t.Fatalf("catalog commits = %d, want %d", got, want)
	}
	if got, want := state.table.Metadata().Properties().GetInt(icetable.ParquetRowGroupLimitKey, 0), 50000; got != want {
		t.Fatalf("Parquet row group rows = %d, want %d", got, want)
	}
	if state.snapshotSpool != nil {
		t.Fatal("snapshot spool should be detached after commit")
	}
	snapshot := state.table.CurrentSnapshot()
	if snapshot == nil || snapshot.Summary == nil {
		t.Fatal("rolled snapshot summary is missing")
	}
	if got, want := snapshot.Summary.Properties["added-data-files"], "1"; got != want {
		t.Fatalf("added-data-files = %q, want %q", got, want)
	}

	_, records, err := state.table.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan rolled snapshot: %v", err)
	}
	rowCount := 0
	for record, recordErr := range records {
		if recordErr != nil {
			t.Fatalf("read rolled snapshot: %v", recordErr)
		}
		rowCount += int(record.NumRows())
		if _, ok := record.Column(0).(*array.Int64); !ok {
			t.Fatalf("id column type = %T, want *array.Int64", record.Column(0))
		}
		record.Release()
	}
	if rowCount != 4 {
		t.Fatalf("visible rows = %d, want 4", rowCount)
	}
}

func TestRollingSnapshotBoundsLargeSourceBatchBeforeBuildingArrow(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newEqualityDeltaTestTable(t)
	spoolDir, err := prepareSnapshotSpoolDirectory(t.TempDir(), "job-1", "state-1")
	if err != nil {
		t.Fatalf("prepareSnapshotSpoolDirectory: %v", err)
	}
	sink := &Sink{
		jobID: "job-1",
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			SnapshotWriteMode: snapshotWriteModeAppend,
			SnapshotBatchSize: 100,
			MaxBatchBytes:     300,
		}),
		snapshotSpoolDir: spoolDir,
	}
	state := &tableState{
		sourceKey: "app.orders",
		sourceSchema: &model.TableSchema{
			SchemaName: "app",
			TableName:  "orders",
			Columns: []model.TableColumn{
				{Name: "id", DataType: "bigint", IsPK: true},
				{Name: "status", DataType: "varchar"},
			},
		},
		table:              tbl,
		snapshotAppendSafe: true,
	}

	err = sink.appendSnapshotSpool(ctx, state, []map[string]interface{}{
		{"id": int64(1), "status": strings.Repeat("a", 256)},
		{"id": int64(2), "status": strings.Repeat("b", 256)},
		{"id": int64(3), "status": strings.Repeat("c", 256)},
	}, time.Now(), 0)
	if err != nil {
		t.Fatalf("appendSnapshotSpool: %v", err)
	}
	if state.snapshotSpool == nil {
		t.Fatal("snapshot spool was not created")
	}
	if got, want := state.snapshotSpool.batchCount, 3; got != want {
		t.Fatalf("spool batches = %d, want %d; a large source batch must be bounded by MaxBatchBytes before Arrow conversion", got, want)
	}
	if got, want := state.snapshotSpool.rowCount, int64(3); got != want {
		t.Fatalf("spool rows = %d, want %d", got, want)
	}
	sink.resetSnapshotSpool(state)
}

func TestResetSnapshotSpoolDiscardsPartialTable(t *testing.T) {
	tbl, _ := newEqualityDeltaTestTable(t)
	spoolDir, err := prepareSnapshotSpoolDirectory(t.TempDir(), "job-1", "state-1")
	if err != nil {
		t.Fatal(err)
	}
	sink := &Sink{
		jobID:            "job-1",
		cfg:              normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: snapshotWriteModeAppend}),
		snapshotSpoolDir: spoolDir,
	}
	state := &tableState{
		sourceKey:          "app.orders",
		sourceSchema:       &model.TableSchema{SchemaName: "app", TableName: "orders", Columns: []model.TableColumn{{Name: "id", DataType: "bigint", IsPK: true}, {Name: "status", DataType: "varchar"}}},
		table:              tbl,
		snapshotAppendSafe: true,
	}
	if err := sink.appendSnapshotSpool(context.Background(), state, []map[string]interface{}{{"id": int64(1), "status": "partial"}}, time.Now(), 0); err != nil {
		t.Fatal(err)
	}
	path := state.snapshotSpool.path
	sink.resetSnapshotSpool(state)
	if state.snapshotSpool != nil {
		t.Fatal("partial spool was not detached")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial spool stat error = %v, want not exist", err)
	}
}
