package mysql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type completionStore struct {
	*testOffsetStore
	clearErr error
}

func (s *completionStore) ClearSnapshotProgress(ctx context.Context, key string) error {
	if s.clearErr != nil {
		return s.clearErr
	}
	return s.testOffsetStore.ClearSnapshotProgress(ctx, key)
}

func incompleteCompletionSource() (*Source, *completionStore) {
	store := &completionStore{testOffsetStore: &testOffsetStore{
		snapshot: &meta.SnapshotState{JobID: "state", StartFile: "mysql-bin.000001", StartPos: 4},
		progress: &meta.SnapshotProgress{TableName: "app.orders", NextOffset: 2},
	}}
	return &Source{jobID: "job", stateKey: "state", offsetSto: store, snapshotBatchEvents: true, retry: config.RetryPolicy{MaxAttempts: 1}}, store
}

func completionTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx, cancel
}

func TestSnapshotCompletionFailedFlushPreservesIncompleteState(t *testing.T) {
	src, store := incompleteCompletionSource()
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	injected := errors.New("data commit failed")
	observed := make(chan bool, 1)
	go func() {
		start := <-out
		start.Ack <- nil
		ev := <-out
		observed <- !store.snapshot.Done && store.progress != nil
		ev.Ack <- injected
	}()
	err := src.RunFull(ctx, out, config.JobModeSnapshotHandoffResume)
	if !errors.Is(err, injected) {
		t.Fatalf("completion error = %v", err)
	}
	if !<-observed || store.snapshot.Done || store.progress == nil {
		t.Fatal("completion became durable or progress was discarded before successful data commit")
	}
}

func TestSnapshotCompletionLostAckRestartsWithoutSnapshotReplay(t *testing.T) {
	src, store := incompleteCompletionSource()
	ctx, cancel := completionTestContext(t)
	out := make(chan model.Event)
	go func() {
		start := <-out
		start.Ack <- nil
		<-out // Sink commits data and completion, then crashes before ACK.
		_ = store.MarkSnapshotDone(ctx, "state")
		cancel()
	}()
	if err := src.RunFull(ctx, out, config.JobModeSnapshotHandoffResume); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost ACK = %v", err)
	}
	if store.progress == nil {
		t.Fatal("lost ACK discarded recovery progress")
	}
	// New source instance shares only durable state. No snapshot row replay.
	restarted := &Source{jobID: "job", stateKey: "state", offsetSto: store, snapshotBatchEvents: true}
	ctx2, _ := completionTestContext(t)
	out2 := make(chan model.Event, 1)
	go func() {
		select {
		case ev := <-out2:
			if ev.Type != model.EventTypeSnapshotComplete {
				t.Errorf("restart emitted %s", ev.Type)
			}
			ev.Ack <- nil
		case <-ctx2.Done():
		}
	}()
	if err := restarted.RunFull(ctx2, out2, config.JobModeSnapshotHandoffResume); err != nil {
		t.Fatal(err)
	}
	if store.progress != nil {
		t.Fatal("restart did not finish acknowledged completion cleanup")
	}
}

func TestSnapshotCompletionClosedAckIsNotSuccess(t *testing.T) {
	src, _ := incompleteCompletionSource()
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	go func() { ev := <-out; close(ev.Ack) }()
	if err := src.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotComplete); err == nil {
		t.Fatal("closed ACK without a result must fail")
	}
}

func TestSnapshotCompletionClosedDataAckIsNotSuccess(t *testing.T) {
	for _, kind := range []string{"batch", "table"} {
		t.Run(kind, func(t *testing.T) {
			src, _ := incompleteCompletionSource()
			ctx, _ := completionTestContext(t)
			out := make(chan model.Event)
			go func() { ev := <-out; close(ev.Ack) }()
			var err error
			if kind == "batch" {
				err = src.emitSnapshotBatch(ctx, out, model.Event{Type: model.EventTypeSnapshotBatch}, "app.orders", 1, 1, 2, nil)
			} else {
				err = src.emitSnapshotTableComplete(ctx, out, "app", "orders", 2)
			}
			if err == nil {
				t.Fatal("closed data ACK without result was accepted")
			}
		})
	}
}

func TestSnapshotCompletionSnapshotOnlyPersistsDone(t *testing.T) {
	src, store := incompleteCompletionSource()
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	go func() {
		start := <-out
		start.Ack <- nil
		ev := <-out
		if store.snapshot == nil {
			ev.Ack <- errors.New("missing snapshot start state")
			return
		}
		_ = store.MarkSnapshotDone(ctx, "state")
		ev.Ack <- nil
	}()
	if err := src.RunFull(ctx, out, config.JobModeSnapshotOnly); err != nil {
		t.Fatal(err)
	}
	if store.snapshot == nil || !store.snapshot.Done {
		t.Fatal("snapshot-only completion not durable")
	}
}

func TestSnapshotCompletionCleanupErrorBlocksHandoff(t *testing.T) {
	src, store := incompleteCompletionSource()
	injected := errors.New("metadata cleanup unavailable")
	store.clearErr = injected
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	go func() {
		start := <-out
		start.Ack <- nil
		ev := <-out
		_ = store.MarkSnapshotDone(ctx, "state")
		ev.Ack <- nil
	}()
	if err := src.RunFull(ctx, out, config.JobModeSnapshotHandoffResume); !errors.Is(err, injected) {
		t.Fatalf("cleanup error = %v", err)
	}
	if store.progress == nil {
		t.Fatal("failed cleanup discarded progress")
	}
}

func TestSnapshotCompletionLegacyDoneBlocksRestart(t *testing.T) {
	src, store := incompleteCompletionSource()
	store.snapshot.Done = true // Legacy source-before-flush receipt.
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event, 1)
	if err := src.RunFull(ctx, out, config.JobModeSnapshotHandoffResume); err == nil {
		t.Fatal("unproven legacy completion authorized handoff")
	}
	if len(out) != 0 || store.progress == nil {
		t.Fatal("legacy state was upgraded or discarded")
	}
}

func TestSnapshotCompletionAckWithoutReceiptBlocksCDC(t *testing.T) {
	src, store := incompleteCompletionSource()
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	go func() {
		start := <-out
		start.Ack <- nil
		complete := <-out
		complete.Ack <- nil // Broken sink protocol.
	}()
	if err := src.RunFull(ctx, out, config.JobModeResume); err == nil {
		t.Fatal("ACK alone authorized CDC")
	}
	if store.snapshot.Done || store.progress == nil || store.offset != nil {
		t.Fatal("ACK alone advanced state")
	}
}

func TestSnapshotCompletionCDCStartsOnlyAfterConfirmedAck(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprintf("ack_failure_%t", failure), func(t *testing.T) {
			src, store := incompleteCompletionSource()
			ctx, cancel := completionTestContext(t)
			out := make(chan model.Event)
			started := false
			src.progress = func(info connector.ProgressInfo) {
				if info.Phase == "streaming" {
					started = true
					if !store.snapshot.CompletionConfirmed() || store.progress != nil {
						t.Error("CDC started before completion handshake")
					}
					if info.CDCStartFile != "mysql-bin.000001" || info.CDCStartPos != 4 {
						t.Errorf("CDC skipped snapshot start: %#v", info)
					}
					cancel() // No canal connection or production access.
				}
			}
			go func() {
				start := <-out
				start.Ack <- nil
				complete := <-out
				if failure {
					complete.Ack <- errors.New("completion failed")
					return
				}
				_ = store.MarkSnapshotDone(ctx, "state")
				complete.Ack <- nil
			}()
			if err := src.RunFull(ctx, out, config.JobModeResume); err == nil {
				t.Fatal("expected cancellation or injected failure")
			}
			if started == failure {
				t.Fatalf("CDC started=%t ACK failure=%t", started, failure)
			}
			if store.offset != nil {
				t.Fatal("handoff advanced source checkpoint")
			}
		})
	}
}

func TestSnapshotCompletionSnapshotOnlyDoneRestartNeverStartsCDC(t *testing.T) {
	src, store := incompleteCompletionSource()
	_ = store.MarkSnapshotDone(context.Background(), "state")
	ctx, _ := completionTestContext(t)
	out := make(chan model.Event)
	go func() { ev := <-out; ev.Ack <- nil }()
	if err := src.RunFullWithStoredMode(ctx, out, config.JobModeResume, config.JobModeSnapshotOnly); err != nil {
		t.Fatal(err)
	}
	if store.offset != nil || store.progress != nil {
		t.Fatal("snapshot-only restart did not complete safely")
	}
}
