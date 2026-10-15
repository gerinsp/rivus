package iceberg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/util"
)

type equalityDeltaTestCatalog struct {
	metadata                  icetable.Metadata
	commits                   int
	commitAttempts            int
	commitErr                 error
	failCommitAt              int
	failResponseAfterCommitAt int
	onConflict                func(*equalityDeltaTestCatalog)
}

func TestEqualityDeleteFilesHonorTableCompression(t *testing.T) {
	keys := make([]map[string]interface{}, 50000)
	for i := range keys {
		keys[i] = map[string]interface{}{"id": int64(i)}
	}
	sizes := map[string]int64{}
	for _, tc := range []struct {
		name  string
		codec compress.Compression
	}{
		{"default", compress.Codecs.Zstd},
		{"snappy", compress.Codecs.Snappy},
		{"gzip", compress.Codecs.Gzip},
		{"lz4raw", compress.Codecs.Lz4Raw},
		{"uncompressed", compress.Codecs.Uncompressed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			tbl, _ := newEqualityDeltaTestTable(t)
			if tc.name != "default" {
				tx := tbl.NewTransaction()
				if err := tx.SetProperties(iceberglib.Properties{icetable.ParquetCompressionKey: tc.name}); err != nil {
					t.Fatal(err)
				}
				var err error
				tbl, err = tx.Commit(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			out, err := writeEqualityDeleteFile(ctx, tbl, iceio.LocalFS{}, keys, []string{"id"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := file.OpenParquetFile(out.FilePath(), false)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			column, err := reader.MetaData().RowGroup(0).ColumnChunk(0)
			if err != nil {
				t.Fatal(err)
			}
			if column.Compression() != tc.codec || reader.NumRows() != int64(len(keys)) || out.Count() != int64(len(keys)) {
				t.Fatalf("codec=%v rows=%d manifest rows=%d", column.Compression(), reader.NumRows(), out.Count())
			}
			sizes[tc.name] = out.FileSizeBytes()
		})
	}
	if sizes["default"] >= sizes["uncompressed"] {
		t.Fatalf("default compression did not reduce equality delete size: %v", sizes)
	}
	t.Logf("50,000 int64 keys, file bytes by codec: %v", sizes)
}

func (c *equalityDeltaTestCatalog) LoadTable(_ context.Context, ident icetable.Identifier) (*icetable.Table, error) {
	return icetable.New(ident, c.metadata, filepath.Join(c.metadata.Location(), "metadata", "v0.metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *equalityDeltaTestCatalog) CommitTable(_ context.Context, _ icetable.Identifier, _ []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	c.commitAttempts++
	if c.commitErr != nil {
		return nil, "", c.commitErr
	}
	if c.commitAttempts == c.failCommitAt {
		if c.onConflict != nil {
			c.onConflict(c)
		}
		return nil, "", icetable.ErrCommitFailed
	}
	metadata, err := icetable.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = metadata
	c.commits++
	if c.commitAttempts == c.failResponseAfterCommitAt {
		return nil, "", errors.New("catalog accepted commit but response was lost")
	}
	return metadata, "", nil
}

func TestCommitEqualityDeltaCommitsUpdateAsOneSnapshot(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)

	seedReader, releaseSeed, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{
		{"id": int64(10), "status": "pending"},
	})
	if err != nil {
		t.Fatalf("build seed reader: %v", err)
	}
	seeded, err := tbl.Append(ctx, seedReader, nil)
	releaseSeed()
	if err != nil {
		t.Fatalf("append seed row: %v", err)
	}
	if got, want := len(seeded.Metadata().Snapshots()), 1; got != want {
		t.Fatalf("seed snapshot count = %d, want %d", got, want)
	}

	state := &tableState{
		sourceKey:       "app.orders",
		targetNamespace: "bronze",
		targetTable:     "orders",
		table:           seeded,
	}
	batch := &reducedBatch{
		deleteKeys: []map[string]interface{}{{"id": int64(10)}},
		deleteRows: []pendingDelete{{
			key: map[string]interface{}{"id": int64(10)},
			row: map[string]interface{}{"id": int64(10), "status": "pending"},
		}},
		pkCols:      []string{"id"},
		rows:        []map[string]interface{}{{"id": int64(10), "status": "complete"}},
		deleteCount: 1,
	}

	sink := &Sink{jobID: "job-1", jobName: "orders CDC"}
	updated, err := sink.commitEqualityDelta(ctx, state, batch)
	if err != nil {
		t.Fatalf("commit equality delta: %v", err)
	}

	if got, want := catalog.commits, 2; got != want {
		t.Fatalf("catalog commits including seed = %d, want %d", got, want)
	}
	if got, want := len(updated.Metadata().Snapshots()), 2; got != want {
		t.Fatalf("snapshot count including seed = %d, want %d", got, want)
	}

	snapshot := updated.CurrentSnapshot()
	if snapshot == nil || snapshot.Summary == nil {
		t.Fatal("current snapshot or summary is nil")
	}
	if got, want := snapshot.Summary.Operation, icetable.OpOverwrite; got != want {
		t.Fatalf("snapshot operation = %q, want %q", got, want)
	}
	for key, want := range map[string]string{
		"added-data-files":            "1",
		"added-delete-files":          "1",
		"added-equality-delete-files": "1",
		"rivus.job_id":                "job-1",
	} {
		if got := snapshot.Summary.Properties[key]; got != want {
			t.Fatalf("snapshot summary %s = %q, want %q", key, got, want)
		}
	}

	_, records, err := updated.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan updated table: %v", err)
	}
	var statuses []string
	for record, recordErr := range records {
		if recordErr != nil {
			t.Fatalf("read updated table: %v", recordErr)
		}
		statusColumn := record.Column(1).(*array.String)
		for idx := 0; idx < statusColumn.Len(); idx++ {
			statuses = append(statuses, statusColumn.Value(idx))
		}
		record.Release()
	}
	if got, want := len(statuses), 1; got != want {
		t.Fatalf("visible row count = %d, want %d (statuses=%v)", got, want, statuses)
	}
	if got, want := statuses[0], "complete"; got != want {
		t.Fatalf("visible status = %q, want %q", got, want)
	}
}

func TestCommitEqualityDeltaCleansStagedDeleteOnPreCommitFailure(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newEqualityDeltaTestTable(t)
	state := &tableState{sourceKey: "app.orders", table: tbl}
	batch := &reducedBatch{
		deleteKeys: []map[string]interface{}{{"id": int64(10)}},
		deleteRows: []pendingDelete{{
			key: map[string]interface{}{"id": int64(10)},
			row: map[string]interface{}{"id": int64(10), "status": "pending"},
		}},
		pkCols:      []string{"id"},
		rows:        []map[string]interface{}{{"id": "not-an-integer", "status": "complete"}},
		deleteCount: 1,
	}

	_, err := (&Sink{jobID: "job-1"}).commitEqualityDelta(ctx, state, batch)
	if err == nil {
		t.Fatal("expected invalid data row to fail before commit")
	}
	entries, readErr := os.ReadDir(filepath.Join(tbl.Location(), "data"))
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		t.Fatalf("read staged data directory: %v", readErr)
	}
	if got := len(entries); got != 0 {
		t.Fatalf("staged file count after pre-commit failure = %d, want 0", got)
	}
}

func TestCommitEqualityDeltaPreservesFilesOnUnknownCommitOutcome(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)
	catalog.commitErr = errors.New("catalog transport failed after request")
	state := &tableState{sourceKey: "app.orders", table: tbl}
	batch := &reducedBatch{
		deleteKeys: []map[string]interface{}{{"id": int64(10)}},
		deleteRows: []pendingDelete{{
			key: map[string]interface{}{"id": int64(10)},
			row: map[string]interface{}{"id": int64(10), "status": "pending"},
		}},
		pkCols:      []string{"id"},
		rows:        []map[string]interface{}{{"id": int64(10), "status": "complete"}},
		deleteCount: 1,
	}

	_, err := (&Sink{jobID: "job-1"}).commitEqualityDelta(ctx, state, batch)
	if err == nil {
		t.Fatal("expected catalog commit failure")
	}
	entries, readErr := os.ReadDir(filepath.Join(tbl.Location(), "data"))
	if readErr != nil {
		t.Fatalf("read staged data directory: %v", readErr)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("staged file count after unknown commit outcome = %d, want %d", got, want)
	}
}

func TestCommitEqualityDeltaDeleteOnlyRemainsOneSnapshot(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)

	seedReader, releaseSeed, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{
		{"id": int64(10), "status": "pending"},
	})
	if err != nil {
		t.Fatalf("build seed reader: %v", err)
	}
	seeded, err := tbl.Append(ctx, seedReader, nil)
	releaseSeed()
	if err != nil {
		t.Fatalf("append seed row: %v", err)
	}

	state := &tableState{
		sourceKey:       "app.orders",
		targetNamespace: "bronze",
		targetTable:     "orders",
		table:           seeded,
	}
	batch := &reducedBatch{
		deleteKeys: []map[string]interface{}{{"id": int64(10)}},
		deleteRows: []pendingDelete{{
			key: map[string]interface{}{"id": int64(10)},
			row: map[string]interface{}{"id": int64(10), "status": "pending"},
		}},
		pkCols:      []string{"id"},
		deleteCount: 1,
	}

	sink := &Sink{jobID: "job-1"}
	updated, err := sink.commitEqualityDelta(ctx, state, batch)
	if err != nil {
		t.Fatalf("commit delete-only equality delta: %v", err)
	}

	if got, want := catalog.commits, 2; got != want {
		t.Fatalf("catalog commits including seed = %d, want %d", got, want)
	}
	if got, want := len(updated.Metadata().Snapshots()), 2; got != want {
		t.Fatalf("snapshot count including seed = %d, want %d", got, want)
	}
	if got, want := updated.CurrentSnapshot().Summary.Operation, icetable.OpDelete; got != want {
		t.Fatalf("snapshot operation = %q, want %q", got, want)
	}
}

// A catalog may accept a RowDelta and lose its response. The sink must return
// UNKNOWN without clearing pending events, advancing the offset, or retrying.
func TestFlushStateEqualityUnknownCommitDoesNotDuplicateRow(t *testing.T) {
	t.Run("unpartitioned", func(t *testing.T) {
		verifyEqualityUnknownCommitReplay(t, iceberglib.UnpartitionedSpec)
	})
	t.Run("partition_move", func(t *testing.T) {
		spec := iceberglib.NewPartitionSpecID(1, iceberglib.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "status", Transform: iceberglib.IdentityTransform{},
		})
		verifyEqualityUnknownCommitReplay(t, &spec)
	})
}

func verifyEqualityUnknownCommitReplay(t *testing.T, spec *iceberglib.PartitionSpec) {
	t.Helper()
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTableWithSpec(t, spec)
	seedReader, releaseSeed, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{
		{"id": int64(10), "status": "pending"},
	})
	if err != nil {
		t.Fatalf("build seed reader: %v", err)
	}
	seeded, err := tbl.Append(ctx, seedReader, nil)
	releaseSeed()
	if err != nil {
		t.Fatalf("append seed row: %v", err)
	}

	catalog.failResponseAfterCommitAt = 2
	store := newGuardTestStore()
	state := &tableState{
		sourceKey:       "app.orders",
		targetNamespace: "bronze",
		targetTable:     "orders",
		sourceSchema: &model.TableSchema{
			SchemaName: "app", TableName: "orders",
			Columns: []model.TableColumn{
				{Name: "id", DataType: "bigint", IsPK: true},
				{Name: "status", DataType: "varchar"},
			},
		},
		table: seeded,
		pending: []model.Event{{
			Type: model.EventTypeUpdate, Schema: "app", Table: "orders",
			OldData: map[string]interface{}{"id": int64(10), "status": "pending"},
			Data:    map[string]interface{}{"id": int64(10), "status": "complete"},
		}},
	}
	sink := &Sink{
		jobID:         "job-1",
		stateKey:      "state-1",
		cfg:           config.IcebergConfig{CDCDeleteExecutor: cdcDeleteExecutorEquality},
		retry:         config.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond},
		states:        map[string]*tableState{state.sourceKey: state},
		offsetSto:     store,
		pendingOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42},
	}
	in := make(chan model.Event)
	close(in)
	if err := sink.Run(ctx, in); err == nil || !util.IsPermanent(err) || !strings.Contains(err.Error(), "outcome UNKNOWN") {
		t.Fatalf("Run after accepted commit/lost response = %v; want permanent UNKNOWN error", err)
	}
	if got := catalog.commitAttempts; got != 2 {
		t.Fatalf("catalog commit attempts = %d, want seed + accepted/lost only", got)
	}
	if got := catalog.commits; got != 2 {
		t.Fatalf("accepted catalog commits = %d, want seed + one equality delta", got)
	}
	if got := len(state.pending); got != 1 {
		t.Fatalf("pending events = %d, want 1", got)
	}
	if sink.pendingOffset == nil || sink.pendingOffset.BinlogPos != 42 {
		t.Fatalf("pending offset = %v, want position 42", sink.pendingOffset)
	}
	if got := store.saveCount; got != 0 {
		t.Fatalf("checkpoint saves = %d, want 0", got)
	}
	restarted := &Sink{jobID: "job-1", stateKey: "state-1", offsetSto: store}
	if err := restarted.Run(ctx, in); err == nil || !strings.Contains(err.Error(), "recovery BLOCKED") {
		t.Fatalf("restart after UNKNOWN = %v, want recovery BLOCKED", err)
	}
	if err := state.table.Refresh(ctx); err != nil {
		t.Fatalf("refresh accepted catalog state: %v", err)
	}

	_, records, err := state.table.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan replayed table: %v", err)
	}
	var statuses []string
	for record, recordErr := range records {
		if recordErr != nil {
			t.Fatalf("read replayed table: %v", recordErr)
		}
		idColumn := record.Column(0).(*array.Int64)
		statusColumn := record.Column(1).(*array.String)
		for i := 0; i < statusColumn.Len(); i++ {
			if got := idColumn.Value(i); got != 10 {
				t.Fatalf("visible row id = %d, want 10", got)
			}
			statuses = append(statuses, statusColumn.Value(i))
		}
		record.Release()
	}
	if len(statuses) != 1 || statuses[0] != "complete" {
		t.Fatalf("visible rows after accepted/lost commit without retry = %v; want one complete row", statuses)
	}
}

func TestCommitAppendStopsAfterAcceptedCommitLostResponse(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)
	catalog.failResponseAfterCommitAt = 1
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{
		{"id": int64(10), "status": "complete"},
	})
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	defer release()

	updated, err := commitAppend(ctx, tbl, reader, nil)
	if updated != nil || err == nil || !util.IsPermanent(err) || !strings.Contains(err.Error(), "outcome UNKNOWN") {
		t.Fatalf("commitAppend = (%v, %v), want nil and permanent UNKNOWN", updated, err)
	}
	if catalog.commitAttempts != 1 || catalog.commits != 1 {
		t.Fatalf("catalog attempts=%d accepted=%d, want 1 each", catalog.commitAttempts, catalog.commits)
	}
	if got := len(catalog.metadata.Snapshots()); got != 1 {
		t.Fatalf("accepted snapshots = %d, want 1", got)
	}
}

func TestCommitDeleteStopsAfterAcceptedCommitLostResponse(t *testing.T) {
	ctx := context.Background()
	tbl, catalog := newEqualityDeltaTestTable(t)
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{
		{"id": int64(10), "status": "pending"},
	})
	if err != nil {
		t.Fatalf("build seed reader: %v", err)
	}
	seeded, err := tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatalf("append seed: %v", err)
	}
	catalog.failResponseAfterCommitAt = 2
	updated, err := commitDelete(ctx, seeded, iceberglib.AlwaysTrue{}, nil)
	if updated != nil || err == nil || !util.IsPermanent(err) || !strings.Contains(err.Error(), "outcome UNKNOWN") {
		t.Fatalf("commitDelete = (%v, %v), want nil and permanent UNKNOWN", updated, err)
	}
	if catalog.commitAttempts != 2 || catalog.commits != 2 {
		t.Fatalf("catalog attempts=%d accepted=%d, want seed + one delete", catalog.commitAttempts, catalog.commits)
	}
	if got := len(catalog.metadata.Snapshots()); got != 2 {
		t.Fatalf("accepted snapshots = %d, want 2", got)
	}
}

func TestCatalogCommitClassificationAllowsProvenConflictRetry(t *testing.T) {
	if err := classifyCatalogCommitError(nil, icetable.ErrCommitFailed); err == nil || util.IsPermanent(err) || !errors.Is(err, icetable.ErrCommitFailed) {
		t.Fatalf("proven rejected commit = %v, want retryable ErrCommitFailed", err)
	}
	attempts := 0
	err := util.RetryWithBackoff(context.Background(), config.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond}, func() error {
		attempts++
		if attempts == 1 {
			return classifyCatalogCommitError(nil, icetable.ErrCommitFailed)
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("proven conflict retry = (%d attempts, %v), want (2, nil)", attempts, err)
	}
	for _, cause := range []error{errors.New("connection lost after request"), context.DeadlineExceeded} {
		if err := classifyCatalogCommitError(nil, cause); err == nil || !util.IsPermanent(err) || !errors.Is(err, cause) {
			t.Fatalf("ambiguous commit cause %v = %v, want permanent error preserving cause", cause, err)
		}
	}
}

func newEqualityDeltaTestTable(t *testing.T) (*icetable.Table, *equalityDeltaTestCatalog) {
	return newEqualityDeltaTestTableWithSpec(t, iceberglib.UnpartitionedSpec)
}

func newEqualityDeltaTestTableWithSpec(t *testing.T, spec *iceberglib.PartitionSpec) (*icetable.Table, *equalityDeltaTestCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberglib.NewSchema(0,
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "status", Type: iceberglib.PrimitiveTypes.String, Required: false},
	)
	metadata, err := icetable.NewMetadata(
		schema,
		spec,
		icetable.UnsortedSortOrder,
		location,
		iceberglib.Properties{icetable.PropertyFormatVersion: "2"},
	)
	if err != nil {
		t.Fatalf("create table metadata: %v", err)
	}

	catalog := &equalityDeltaTestCatalog{metadata: metadata}
	tbl := icetable.New(
		icetable.Identifier{"bronze", "orders"},
		metadata,
		location+"/metadata/v0.metadata.json",
		func(context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		catalog,
	)
	return tbl, catalog
}
