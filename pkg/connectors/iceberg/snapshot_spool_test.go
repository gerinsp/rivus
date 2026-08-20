package iceberg

import (
	"context"
	"encoding/base64"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestPlanSnapshotWriteSizingUsesBytesForWideRows(t *testing.T) {
	spool := &snapshotSpool{
		rowCount: 20662,
		bytes:    6498236156,
	}
	sizing := planSnapshotWriteSizing(spool, 128*1024*1024, 50000)

	if sizing.averageRowBytes <= 300*1024 {
		t.Fatalf("average row bytes = %d, want a wide-row estimate above 300 KiB", sizing.averageRowBytes)
	}
	if sizing.rowGroupTargetBytes != 64*1024*1024 {
		t.Fatalf("row group target bytes = %d, want 64 MiB", sizing.rowGroupTargetBytes)
	}
	if sizing.rowGroupRows <= 0 || sizing.rowGroupRows >= 500 {
		t.Fatalf("row group rows = %d, want a dynamic value between 1 and 499", sizing.rowGroupRows)
	}
	if sizing.readBatchRows != sizing.rowGroupRows {
		t.Fatalf("read batch rows = %d, want row group rows %d for a wide snapshot", sizing.readBatchRows, sizing.rowGroupRows)
	}
}

func TestPlanSnapshotWriteSizingKeepsCeilingForSmallSnapshot(t *testing.T) {
	spool := &snapshotSpool{rowCount: 100, bytes: 64 * 1024}
	sizing := planSnapshotWriteSizing(spool, 128*1024*1024, 50000)

	if got, want := sizing.rowGroupRows, int64(50000); got != want {
		t.Fatalf("row group rows = %d, want ceiling %d", got, want)
	}
	if got, want := sizing.readBatchRows, snapshotSpoolMaxReadBatchRows; got != want {
		t.Fatalf("read batch rows = %d, want max batch %d", got, want)
	}
}

func TestPlanSnapshotWriteSizingRespectsConfiguredCeiling(t *testing.T) {
	spool := &snapshotSpool{rowCount: 100000, bytes: 100 * 1024 * 1024}
	sizing := planSnapshotWriteSizing(spool, 128*1024*1024, 500)

	if got, want := sizing.rowGroupRows, int64(500); got != want {
		t.Fatalf("row group rows = %d, want configured ceiling %d", got, want)
	}
	if got, want := sizing.readBatchRows, int64(500); got != want {
		t.Fatalf("read batch rows = %d, want configured ceiling %d", got, want)
	}
}

func TestPlanSnapshotWriteSizingAllowsSingleOversizedRow(t *testing.T) {
	spool := &snapshotSpool{rowCount: 1, bytes: 256 * 1024 * 1024}
	sizing := planSnapshotWriteSizing(spool, 128*1024*1024, 50000)

	if got, want := sizing.rowGroupRows, int64(1); got != want {
		t.Fatalf("row group rows = %d, want %d", got, want)
	}
	if got, want := sizing.readBatchRows, int64(1); got != want {
		t.Fatalf("read batch rows = %d, want %d", got, want)
	}
}

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

func TestRolledSnapshotSplitsWideRowsUsingDynamicSizing(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newEqualityDeltaTestTable(t)
	spoolDir, err := prepareSnapshotSpoolDirectory(t.TempDir(), "job-wide", "state-wide")
	if err != nil {
		t.Fatalf("prepareSnapshotSpoolDirectory: %v", err)
	}
	sink := &Sink{
		jobID: "job-wide",
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			SnapshotWriteMode:           snapshotWriteModeAppend,
			SnapshotTargetFileSizeBytes: 32 * 1024,
		}),
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

	random := rand.New(rand.NewSource(42))
	rows := make([]map[string]interface{}, 32)
	for i := range rows {
		payload := make([]byte, 4*1024)
		if _, err := random.Read(payload); err != nil {
			t.Fatalf("build random payload: %v", err)
		}
		rows[i] = map[string]interface{}{
			"id":     int64(i + 1),
			"status": base64.StdEncoding.EncodeToString(payload),
		}
	}
	if err := sink.appendSnapshotSpool(ctx, state, rows, time.Now(), 0); err != nil {
		t.Fatalf("appendSnapshotSpool: %v", err)
	}
	if err := sink.finalizeSnapshotSpool(ctx, state); err != nil {
		t.Fatalf("finalizeSnapshotSpool: %v", err)
	}

	rowGroupRows := state.table.Metadata().Properties().GetInt(icetable.ParquetRowGroupLimitKey, 0)
	if rowGroupRows <= 0 || rowGroupRows >= 50000 {
		t.Fatalf("Parquet row group rows = %d, want a dynamically reduced value", rowGroupRows)
	}
	snapshot := state.table.CurrentSnapshot()
	if snapshot == nil || snapshot.Summary == nil {
		t.Fatal("rolled snapshot summary is missing")
	}
	addedFiles, err := strconv.Atoi(snapshot.Summary.Properties["added-data-files"])
	if err != nil {
		t.Fatalf("parse added-data-files: %v", err)
	}
	if addedFiles <= 1 {
		t.Fatalf("added-data-files = %d, want multiple rolling files for wide rows", addedFiles)
	}
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
