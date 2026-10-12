package iceberg

import (
	"context"
	"errors"
	"testing"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type snapshotCompletionStore struct {
	*guardTestStore
	snapshot     *meta.SnapshotState
	doneErr      error
	marks        int
	doneAccepted bool
}

func (s *snapshotCompletionStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return s.snapshot, nil
}
func (s *snapshotCompletionStore) MarkSnapshotDone(context.Context, string) error {
	s.marks++
	if s.doneErr != nil && !s.doneAccepted {
		return s.doneErr
	}
	s.snapshot.Done = true
	s.snapshot.CompletionVersion = 1
	return s.doneErr
}

func completionEvent() model.Event {
	return model.Event{Type: model.EventTypeSnapshotComplete, Origin: model.EventOriginSnapshot, Ack: make(chan error, 1)}
}

func TestSnapshotCompletionIcebergPersistsBeforeAckAndIsIdempotent(t *testing.T) {
	store := &snapshotCompletionStore{guardTestStore: newGuardTestStore(), snapshot: &meta.SnapshotState{}}
	sink := &Sink{stateKey: "state", offsetSto: store}
	for i := 0; i < 2; i++ {
		ev := completionEvent()
		if err := sink.handleSnapshotBarrier(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
		if !store.snapshot.Done {
			t.Fatal("ACK sent before durable completion")
		}
		if err := <-ev.Ack; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotCompletionIcebergPersistenceFailureFailsAck(t *testing.T) {
	injected := errors.New("completion metadata unavailable after data commit")
	store := &snapshotCompletionStore{guardTestStore: newGuardTestStore(), snapshot: &meta.SnapshotState{}, doneErr: injected}
	sink := &Sink{stateKey: "state", offsetSto: store}
	ev := completionEvent()
	if err := sink.handleSnapshotBarrier(context.Background(), ev); !errors.Is(err, injected) {
		t.Fatalf("result = %v", err)
	}
	if err := <-ev.Ack; !errors.Is(err, injected) {
		t.Fatalf("ACK = %v", err)
	}
	if store.snapshot.Done {
		t.Fatal("failed status write marked completion")
	}
}

func TestSnapshotCompletionIcebergRejectsUncommittedSpool(t *testing.T) {
	store := &snapshotCompletionStore{guardTestStore: newGuardTestStore(), snapshot: &meta.SnapshotState{}}
	sink := &Sink{stateKey: "state", offsetSto: store, states: map[string]*tableState{"app.orders": {sourceKey: "app.orders", snapshotSpool: &snapshotSpool{rowCount: 2}}}}
	ev := completionEvent()
	if err := sink.handleSnapshotBarrier(context.Background(), ev); err == nil {
		t.Fatal("uncommitted rolling data was declared complete")
	}
	if store.snapshot.Done || store.marks != 0 {
		t.Fatal("uncommitted spool advanced completion")
	}
}

func TestSnapshotCompletionIcebergRejectsUnknownIntent(t *testing.T) {
	store := &snapshotCompletionStore{guardTestStore: newGuardTestStore(), snapshot: &meta.SnapshotState{}}
	store.intents["unknown"] = meta.IcebergCommitIntent{OperationID: "unknown", Status: meta.IcebergCommitPrepared, Kind: "snapshot"}
	sink := &Sink{stateKey: "state", offsetSto: store}
	ev := completionEvent()
	if err := sink.handleSnapshotBarrier(context.Background(), ev); err == nil {
		t.Fatal("unknown commit declared complete")
	}
	if store.snapshot.Done {
		t.Fatal("unknown intent advanced completion")
	}
}

func TestSnapshotCompletionIcebergCrashRecoveryDataOracle(t *testing.T) {
	for _, crash := range []string{"before_commit", "after_data_before_status", "after_status_before_ack", "status_response_lost"} {
		t.Run(crash, func(t *testing.T) {
			ctx := context.Background()
			cat := recoveryCatalog(t)
			store := &snapshotCompletionStore{guardTestStore: newGuardTestStore(), snapshot: &meta.SnapshotState{}}
			sink := recoverySink(t, cat, store)
			rows := []map[string]interface{}{{"id": int64(1), "status": "pending"}, {"id": int64(2), "status": "pending"}}
			batch := model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: rows}
			if crash == "before_commit" {
				err := sink.withCommitSlot(withIcebergBatch(ctx, snapshotIcebergBatch("app.orders", 0, 2)), commitProgress{operation: "append", icebergCommit: true, sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders"}, func(context.Context) error { return errors.New("crash before catalog request") })
				if err == nil {
					t.Fatal("expected interrupted commit")
				}
				restarted := recoverySink(t, cat, store)
				if err := restarted.checkIcebergRecovery(ctx); err == nil {
					t.Fatal("uncertain completion allowed restart")
				}
				if store.snapshot.Done || len(store.intents) != 1 || store.saveCount != 0 {
					t.Fatal("uncertain restart advanced state")
				}
				tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
				if err != nil {
					t.Fatal(err)
				}
				if count, err := countIcebergTableRows(ctx, tbl); err != nil || count != 0 {
					t.Fatalf("before-commit rows=%d error=%v", count, err)
				}
				return
			}
			if err := sink.handleSnapshotBatch(ctx, batch); err != nil {
				t.Fatal(err)
			}
			if crash == "after_data_before_status" || crash == "status_response_lost" {
				store.doneErr = errors.New("metadata response lost")
				store.doneAccepted = crash == "status_response_lost"
			}
			ev := completionEvent()
			err := sink.handleSnapshotBarrier(ctx, ev)
			if store.doneErr != nil && !errors.Is(err, store.doneErr) {
				t.Fatalf("metadata error=%v", err)
			}
			// ACK is never consumed: the source/process has gone away. Recreate
			// the entire sink; retain only catalog files and metadata test double.
			store.doneErr = nil
			restarted := recoverySink(t, cat, store)
			if err := restarted.checkIcebergRecovery(ctx); err != nil {
				t.Fatal(err)
			}
			if !store.snapshot.CompletionConfirmed() {
				if err := restarted.handleSnapshotBatch(ctx, batch); err != nil {
					t.Fatal(err)
				}
			}
			if err := restarted.handleSnapshotBarrier(ctx, completionEvent()); err != nil {
				t.Fatal(err)
			}
			if !store.snapshot.CompletionConfirmed() || store.saveCount != 0 {
				t.Fatal("recovery did not confirm completion safely")
			}
			tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
			if err != nil {
				t.Fatal(err)
			}
			count, err := countIcebergTableRows(ctx, tbl)
			if err != nil || count != 2 || len(tbl.Metadata().Snapshots()) != 1 {
				t.Fatalf("rows=%d snapshots=%d error=%v; replay lost or duplicated data", count, len(tbl.Metadata().Snapshots()), err)
			}
		})
	}
}
