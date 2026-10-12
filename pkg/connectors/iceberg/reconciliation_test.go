package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	iceberglib "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

// A real filesystem catalog, with response loss injected after durable commit.
type responseLossCatalog struct {
	icecatalog.Catalog
	lose bool
}

func (c *responseLossCatalog) LoadTable(ctx context.Context, ident icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return icetable.New(ident, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}
func (c *responseLossCatalog) CommitTable(ctx context.Context, ident icetable.Identifier, req []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	m, loc, err := c.Catalog.CommitTable(ctx, ident, req, updates)
	if err == nil && c.lose {
		c.lose = false
		return nil, "", errors.New("injected response loss after durable catalog commit")
	}
	return m, loc, err
}

func recoveryCatalog(t *testing.T) icecatalog.Catalog {
	t.Helper()
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	if err := os.MkdirAll(warehouse, 0755); err != nil {
		t.Fatal(err)
	}
	cat, err := hadoop.NewCatalog("recovery", warehouse, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(context.Background(), icetable.Identifier{"bronze"}, nil); err != nil {
		t.Fatal(err)
	}
	schema := iceberglib.NewSchema(0, iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true}, iceberglib.NestedField{ID: 2, Name: "status", Type: iceberglib.PrimitiveTypes.String})
	if _, err := cat.CreateTable(context.Background(), icetable.Identifier{"bronze", "orders"}, schema, icecatalog.WithProperties(iceberglib.Properties{icetable.PropertyFormatVersion: "2"})); err != nil {
		t.Fatal(err)
	}
	return cat
}

func recoverySink(t *testing.T, cat icecatalog.Catalog, store meta.OffsetStore) *Sink {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	state := &tableState{sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders", table: tbl, sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "orders", Columns: []model.TableColumn{{Name: "id", DataType: "bigint", IsPK: true}, {Name: "status", DataType: "varchar"}}}}
	return &Sink{jobID: "job", stateKey: "state", catalog: cat, offsetSto: store, cfg: config.IcebergConfig{DefaultNamespace: "bronze", BatchSize: 10, SnapshotBatchSize: 2, SnapshotWriteMode: "append", CDCDeleteExecutor: cdcDeleteExecutorEquality}, retry: config.RetryPolicy{MaxAttempts: 1}, states: map[string]*tableState{"app.orders": state}, sourceSchemas: map[string]*model.TableSchema{"app.orders": state.sourceSchema}}
}

func recoveryEvent(id int64, row int) model.Event {
	return model.Event{Type: model.EventTypeInsert, TraceID: fmt.Sprintf("mysql-bin.000001:42:insert:%d", row), Schema: "app", Table: "orders", Data: map[string]interface{}{"id": id, "status": "pending"}, SourceOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}
}

func TestIcebergReconciliationAcceptedCDCReplaysOnlyUncommittedRows(t *testing.T) {
	ctx := context.Background()
	cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: true}
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	accepted := recoveryEvent(1, 0)
	uncommitted := recoveryEvent(2, 1)
	first.states["app.orders"].pending = []model.Event{accepted}
	if err := first.flushState(ctx, first.states["app.orders"]); err == nil {
		t.Fatal("want response loss")
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 0 {
		t.Fatal("recovery advanced global checkpoint before replay")
	}
	if err := restarted.handleEvent(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	if err := restarted.commitPendingOffset(ctx, true); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 0 {
		t.Fatal("skipped data row advanced checkpoint before source checkpoint event")
	}

	if err := restarted.handleEvent(ctx, uncommitted); err != nil {
		t.Fatal(err)
	}
	if len(restarted.states["app.orders"].pending) != 1 {
		t.Fatal("replay skipped uncommitted row sharing the accepted row's binlog offset")
	}
	if err := restarted.handleEvent(ctx, model.Event{Type: model.EventTypeCheckpoint, SourceOffset: accepted.SourceOffset}); err != nil {
		t.Fatal(err)
	}

	if err := restarted.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := countIcebergTableRows(ctx, tbl); err != nil || count != 2 {
		t.Fatalf("rows=%d error=%v want 2", count, err)
	}
	if len(tbl.Metadata().Snapshots()) != 2 {
		t.Fatal("accepted row was committed again")
	}
	if len(store.intents) != 0 || store.saveCount != 1 {
		t.Fatalf("intents=%d saves=%d", len(store.intents), store.saveCount)
	}
}

func TestIcebergReconciliationSnapshotResumesWithDifferentChunkSize(t *testing.T) {
	ctx := context.Background()
	cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: true}
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	rows := []map[string]interface{}{{"id": int64(1), "status": "pending"}, {"id": int64(2), "status": "pending"}, {"id": int64(3), "status": "pending"}}
	ev := model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: rows}
	if err := first.handleSnapshotBatch(ctx, ev); err == nil {
		t.Fatal("want response loss")
	}
	restarted := recoverySink(t, cat, store)
	restarted.cfg.SnapshotBatchSize = 3
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleSnapshotBatch(ctx, ev); err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := countIcebergTableRows(ctx, tbl); err != nil || count != 3 {
		t.Fatalf("rows=%d error=%v want 3", count, err)
	}
	if len(tbl.Metadata().Snapshots()) != 2 {
		t.Fatal("snapshot receipt was replayed")
	}
}

func TestIcebergReconciliationMissingOrInvalidProofStaysBlocked(t *testing.T) {
	for _, mode := range []string{"no_request", "mismatch", "rolled_back", "missing_events", "mark_failure", "unsupported_operation"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cat := recoveryCatalog(t)
			store := newGuardTestStore()
			sink := recoverySink(t, cat, store)
			ev := recoveryEvent(1, 0)
			batch := cdcIcebergBatch("app.orders", []model.Event{ev})
			op := "append"
			if mode == "unsupported_operation" {
				op = "truncate"
			}
			if mode == "rolled_back" {
				tbl := sink.states["app.orders"].table
				reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(99), "status": "seed"}})
				if err != nil {
					t.Fatal(err)
				}
				updated, err := tbl.Append(ctx, reader, nil)
				release()
				if err != nil {
					t.Fatal(err)
				}
				sink.states["app.orders"].table = updated
			}

			err := sink.withCommitSlot(withIcebergBatch(ctx, batch), commitProgress{operation: op, icebergCommit: true, sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders"}, func(ctx context.Context) error {
				if mode == "no_request" {
					return errors.New("crash before request")
				}
				tbl := sink.states["app.orders"].table
				reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{ev.Data})
				if err != nil {
					return err
				}
				defer release()
				updated, err := commitAppend(ctx, tbl, reader, icebergCommitProperties(ctx, nil))
				if err != nil {
					return err
				}
				if mode == "rolled_back" {
					txn := updated.NewTransaction()
					if err := txn.RollbackToSnapshot(*updated.CurrentSnapshot().ParentSnapshotID); err != nil {
						return err
					}
					_, err = txn.Commit(context.Background())
					if err != nil {
						return err
					}
				}
				return errors.New("response lost")
			})
			if err == nil {
				t.Fatal("want failed original commit")
			}
			for id, intent := range store.intents {
				if mode == "mismatch" {
					intent.BatchID = "wrong-batch"
					store.intents[id] = intent
				}
				if mode == "missing_events" {
					intent.ReplayKeys = ""
					store.intents[id] = intent
				}
			}
			if mode == "mark_failure" {
				store.markErr = errors.New("metadata unavailable")
			}
			restarted := recoverySink(t, cat, store)
			if err := restarted.checkIcebergRecovery(ctx); err == nil {
				t.Fatal("recovery permitted without complete proof")
			}
			if len(store.intents) != 1 || store.saveCount != 0 {
				t.Fatal("recovery discarded evidence or advanced checkpoint")
			}
		})
	}
}

func TestIcebergReconciliationCheckpointRequiresSourceReplay(t *testing.T) {
	ctx := context.Background()
	cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: true}
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	first.states["app.orders"].pending = []model.Event{recoveryEvent(1, 0)}
	if err := first.flushState(ctx, first.states["app.orders"]); err == nil {
		t.Fatal("want response loss")
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	restarted.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 100}
	if err := restarted.commitPendingOffset(ctx, true); err == nil || !strings.Contains(err.Error(), "recovery BLOCKED") {
		t.Fatalf("checkpoint=%v", err)
	}
	if store.saveCount != 0 {
		t.Fatal("checkpoint advanced without replaying accepted event")
	}
}

func TestIcebergReconciliationChangedPayloadBlocksBeforeReplay(t *testing.T) {
	ctx := context.Background()
	cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: true}
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	ev := recoveryEvent(1, 0)
	first.states["app.orders"].pending = []model.Event{ev}
	if err := first.flushState(ctx, first.states["app.orders"]); err == nil {
		t.Fatal("want response loss")
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	ev.Data = map[string]interface{}{"id": int64(1), "status": "different"}
	if err := restarted.handleEvent(ctx, ev); err == nil || !strings.Contains(err.Error(), "payload changed") {
		t.Fatalf("changed payload replay=%v", err)
	}
	if len(restarted.states["app.orders"].pending) != 0 {
		t.Fatal("changed event queued for duplicate commit")
	}
}

func TestIcebergReconciliationMetadataProofRetainsSourceEvent(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	ev := recoveryEvent(1, 0)
	err := first.withCommitSlot(withIcebergBatch(ctx, cdcIcebergBatch("app.orders", []model.Event{ev})), commitProgress{operation: "schema", icebergCommit: true, sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders"}, func(ctx context.Context) error {
		txn := first.states["app.orders"].table.NewTransaction()
		if err := txn.SetProperties(icebergCommitProperties(ctx, nil)); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			return err
		}
		return errors.New("schema response lost")
	})
	if err == nil {
		t.Fatal("want unknown schema outcome")
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if len(store.intents) != 0 || store.saveCount != 0 {
		t.Fatal("metadata recovery failed cleanup or advanced data offset")
	}
	if err := restarted.handleEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if len(restarted.states["app.orders"].pending) != 1 {
		t.Fatal("metadata receipt consumed enclosing data event")
	}
}

func TestIcebergReconciliationEqualityDeltaSkipsAcceptedUpdate(t *testing.T) {
	ctx := context.Background()
	base := recoveryCatalog(t)
	tbl, err := base.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "pending"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	ev := recoveryEvent(1, 0)
	ev.Type = model.EventTypeUpdate
	ev.TraceID = "mysql-bin.000001:42:update:0"
	ev.OldData = ev.Data
	ev.Data = map[string]interface{}{"id": int64(1), "status": "complete"}
	first.states["app.orders"].pending = []model.Event{ev}
	if err := first.flushState(ctx, first.states["app.orders"]); err == nil {
		t.Fatal("want lost RowDelta response")
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, model.Event{Type: model.EventTypeCheckpoint, SourceOffset: ev.SourceOffset}); err != nil {
		t.Fatal(err)
	}

	if err := restarted.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	visible, err := base.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := visible.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for record, err := range records {
		if err != nil {
			t.Fatal(err)
		}
		count += record.NumRows()
		record.Release()
	}
	if count != 1 || len(visible.Metadata().Snapshots()) != 2 {
		t.Fatalf("visible rows=%d snapshots=%d; want 1 row, seed + one RowDelta", count, len(visible.Metadata().Snapshots()))
	}
	if len(store.intents) != 0 {
		t.Fatal("confirmed RowDelta receipt survived completed replay checkpoint")
	}
}
