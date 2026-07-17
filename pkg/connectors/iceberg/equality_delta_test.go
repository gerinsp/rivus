package iceberg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
)

type equalityDeltaTestCatalog struct {
	metadata  icetable.Metadata
	commits   int
	commitErr error
}

func (c *equalityDeltaTestCatalog) LoadTable(context.Context, icetable.Identifier) (*icetable.Table, error) {
	return nil, nil
}

func (c *equalityDeltaTestCatalog) CommitTable(_ context.Context, _ icetable.Identifier, _ []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if c.commitErr != nil {
		return nil, "", c.commitErr
	}
	metadata, err := icetable.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = metadata
	c.commits++
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

func newEqualityDeltaTestTable(t *testing.T) (*icetable.Table, *equalityDeltaTestCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberglib.NewSchema(0,
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "status", Type: iceberglib.PrimitiveTypes.String, Required: false},
	)
	metadata, err := icetable.NewMetadata(
		schema,
		iceberglib.UnpartitionedSpec,
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
