package iceberg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type checkpointCleanupStore struct {
	*guardTestStore
	saveErr, listErr error
	saveCtx          context.Context
	cleanupCtx       context.Context
	listCalls        int
	afterSave        func(context.Context)
}

func (s *checkpointCleanupStore) SaveOffset(ctx context.Context, key string, off meta.Offset) error {
	s.saveCtx = ctx
	if s.saveErr != nil {
		return s.saveErr
	}
	if err := s.guardTestStore.SaveOffset(ctx, key, off); err != nil {
		return err
	}
	if s.afterSave != nil {
		s.afterSave(ctx)
	}
	return nil
}

func (s *checkpointCleanupStore) ListIcebergCommits(ctx context.Context, key string) ([]meta.IcebergCommitIntent, error) {
	s.listCalls++
	s.cleanupCtx = ctx
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.guardTestStore.ListIcebergCommits(ctx, key)
}

func checkpointCleanupFixture() (*Sink, *checkpointCleanupStore) {
	store := &checkpointCleanupStore{guardTestStore: newGuardTestStore()}
	store.intents["covered"] = meta.IcebergCommitIntent{
		OperationID: "covered", StateKey: "state", Kind: "cdc", Status: meta.IcebergCommitCommitted,
		SourceEndFile: "mysql-bin.000001", SourceEndPos: 42,
	}
	return &Sink{jobID: "job", stateKey: "state", offsetSto: store,
		pendingOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}, store
}

func TestCheckpointCleanupFailureDoesNotFailDurableCheckpoint(t *testing.T) {
	for _, fault := range []string{"list", "delete"} {
		t.Run(fault, func(t *testing.T) {
			s, store := checkpointCleanupFixture()
			if fault == "list" {
				store.listErr = context.DeadlineExceeded
			} else {
				store.deleteErr = context.DeadlineExceeded
			}
			if err := s.commitPendingOffset(context.Background(), true); err != nil {
				t.Fatalf("cleanup failure killed a successfully checkpointed job: %v", err)
			}
			if store.offset == nil || store.offset.BinlogPos != 42 || s.pendingOffset != nil || s.lastCheckpointCommitAt.IsZero() {
				t.Fatalf("durable checkpoint not acknowledged: saved=%+v pending=%+v", store.offset, s.pendingOffset)
			}
			if len(store.intents) != 1 {
				t.Fatal("failed cleanup discarded the receipt")
			}
		})
	}
}

func TestCheckpointCleanupHasIndependentBudget(t *testing.T) {
	s, store := checkpointCleanupFixture()
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if store.saveCtx == nil || store.cleanupCtx == nil || store.saveCtx == store.cleanupCtx {
		t.Fatal("checkpoint and cleanup share their timeout budget")
	}
}

func TestCheckpointCleanupDoesNotInheritExpiredSaveContext(t *testing.T) {
	s, store := checkpointCleanupFixture()
	// The database saved the offset, but returning the result consumed the
	// entire checkpoint budget. Cleanup still has the live worker context.
	store.afterSave = func(ctx context.Context) { <-ctx.Done() }
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatalf("expired save budget leaked into cleanup: %v", err)
	}
	if store.saveCtx.Err() != context.DeadlineExceeded || store.saveCount != 1 || len(store.intents) != 0 || s.pendingOffset != nil {
		t.Fatal("successfully saved checkpoint was not acknowledged and cleaned")
	}
}

func TestCheckpointCleanupHealthyCheckpointsDoNotAccumulateReceipts(t *testing.T) {
	s, store := checkpointCleanupFixture()
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	store.intents["next"] = meta.IcebergCommitIntent{OperationID: "next", StateKey: "state", Kind: "cdc", Status: meta.IcebergCommitCommitted,
		SourceEndFile: "mysql-bin.000001", SourceEndPos: 80}
	s.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 80}
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 2 || store.listCalls != 2 || len(store.intents) != 0 || s.pendingCheckpointCleanup != nil {
		t.Fatal("healthy final checkpoint left completed receipts waiting for another event")
	}
}

func TestCheckpointCleanupNeverRunsAfterFailedSave(t *testing.T) {
	s, store := checkpointCleanupFixture()
	store.saveErr = context.DeadlineExceeded
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if store.offset != nil || s.pendingOffset == nil || len(store.intents) != 1 || store.listCalls != 0 {
		t.Fatal("failed checkpoint was acknowledged or its receipt cleaned")
	}
}

func TestCheckpointCleanupOwnershipLossStillStopsWorker(t *testing.T) {
	s, store := checkpointCleanupFixture()
	store.deleteErr = meta.ErrWorkerOwnershipLost
	if err := s.commitPendingOffset(context.Background(), true); !errors.Is(err, meta.ErrWorkerOwnershipLost) {
		t.Fatalf("ownership loss was ignored: %v", err)
	}
	if len(store.intents) != 1 {
		t.Fatal("ownership loss discarded the receipt")
	}
}

func TestCheckpointCleanupRetriesWhileIdleWithoutResavingOffset(t *testing.T) {
	s, store := checkpointCleanupFixture()
	store.listErr = context.DeadlineExceeded
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	// Neither ordinary ticks nor forced flushes should hammer a slow store.
	for i := 0; i < 10; i++ {
		if err := s.commitPendingOffset(context.Background(), true); err != nil {
			t.Fatal(err)
		}
	}
	if store.listCalls != 1 || store.saveCount != 1 || s.pendingCheckpointCleanup == nil {
		t.Fatalf("cleanup retry lost or unbounded: reads=%d saves=%d", store.listCalls, store.saveCount)
	}
	store.listErr = nil
	s.lastCheckpointCleanupAt = time.Now().Add(-checkpointCleanupInterval)
	if err := s.commitPendingOffset(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if store.listCalls != 2 || store.saveCount != 1 || len(store.intents) != 0 || s.pendingCheckpointCleanup != nil {
		t.Fatal("idle tick failed to clean up the durable receipt without rewriting checkpoint")
	}
}

func TestCheckpointCleanupUsesOnlySavedWatermarkUnderBackpressure(t *testing.T) {
	s, store := checkpointCleanupFixture()
	for _, intent := range []meta.IcebergCommitIntent{
		{OperationID: "prepared", StateKey: "state", Kind: "cdc", Status: meta.IcebergCommitPrepared, SourceEndFile: "mysql-bin.000001", SourceEndPos: 40},
		{OperationID: "ahead", StateKey: "state", Kind: "cdc", Status: meta.IcebergCommitCommitted, SourceEndFile: "mysql-bin.000001", SourceEndPos: 80},
	} {
		store.intents[intent.OperationID] = intent
	}
	store.listErr = context.DeadlineExceeded
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	s.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 100}
	s.states = map[string]*tableState{"app.orders": {pending: []model.Event{{Type: model.EventTypeInsert}}}}
	store.listErr = nil
	s.lastCheckpointCleanupAt = time.Now().Add(-checkpointCleanupInterval)
	if err := s.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 1 || store.offset.BinlogPos != 42 || s.pendingOffset == nil || s.pendingOffset.BinlogPos != 100 {
		t.Fatal("backpressure advanced or acknowledged an uncommitted checkpoint")
	}
	if _, found := store.intents["covered"]; found || len(store.intents) != 2 {
		t.Fatal("cleanup removed PREPARED or uncovered evidence")
	}
}

func TestCheckpointCleanupPreservesParentCancellation(t *testing.T) {
	s, store := checkpointCleanupFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A retained receipt from an earlier successful checkpoint must not cause
	// cleanup to detach from the stopped worker and continue writing metadata.
	s.pendingCheckpointCleanup = &meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}
	if err := s.commitPendingOffset(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("worker cancellation lost: %v", err)
	}
	if store.saveCount != 0 || store.listCalls != 0 || len(store.intents) != 1 {
		t.Fatal("cancelled worker continued checkpoint or receipt operations")
	}
}
