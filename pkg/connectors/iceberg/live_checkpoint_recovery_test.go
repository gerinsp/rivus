package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

func eventAt(id int64, pos uint32) model.Event {
	ev := recoveryEvent(id, 0)
	ev.SourceOffset.BinlogPos = pos
	ev.TraceID = fmt.Sprintf("mysql-bin.000001:%d:insert:0", pos)
	return ev
}

func TestLegacyReplayReceiptPreservesFingerprintAndFailsClosedForInteriorCheckpoint(t *testing.T) {
	for _, interior := range []bool{false, true} {
		t.Run(fmt.Sprintf("interior_%t", interior), func(t *testing.T) {
			ctx := context.Background()
			cat := recoveryCatalog(t)
			store := newGuardTestStore()
			first := recoverySink(t, cat, store)
			events := []model.Event{eventAt(1, 30), eventAt(2, 42)}
			batch := cdcIcebergBatch("app.orders", events)
			keys := []string{}
			for _, ev := range events {
				key, err := icebergReplayKey(ev)
				if err != nil {
					t.Fatal(err)
				}
				keys = append(keys, key)
			}
			encoded, err := json.Marshal(keys)
			if err != nil {
				t.Fatal(err)
			}
			batch.replayKeys = string(encoded)
			err = first.withCommitSlot(withIcebergBatch(ctx, batch), commitProgress{operation: "append", icebergCommit: true, sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders"}, func(ctx context.Context) error {
				tbl := first.states["app.orders"].table
				reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{events[0].Data, events[1].Data})
				if err != nil {
					return err
				}
				defer release()
				_, err = commitAppend(ctx, tbl, reader, icebergCommitProperties(ctx, nil))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if interior {
				if err := store.SaveOffset(ctx, "state", meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 40}); err != nil {
					t.Fatal(err)
				}
			}
			restarted := recoverySink(t, cat, store)
			if err := restarted.checkIcebergRecovery(ctx); err != nil {
				t.Fatalf("legacy fingerprint changed: %v", err)
			}
			if !interior {
				if err := restarted.handleEvent(ctx, events[0]); err != nil {
					t.Fatal(err)
				}
			}
			if err := restarted.handleEvent(ctx, events[1]); err != nil {
				t.Fatal(err)
			}
			restarted.rememberOffset(checkpointAt(42))
			err = restarted.commitPendingOffset(ctx, true)
			if interior {
				if err == nil || !strings.Contains(err.Error(), "before all source event identities replayed") || len(store.intents) != 1 {
					t.Fatalf("legacy opaque prefix was guessed: %v", err)
				}
			} else if err != nil || len(store.intents) != 0 {
				t.Fatalf("legacy full replay failed: %v", err)
			}
			tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
			if err != nil {
				t.Fatal(err)
			}
			if len(tbl.Metadata().Snapshots()) != 1 {
				t.Fatal("legacy recovery duplicated accepted commit")
			}
			assertAutomaticData(t, cat, 2, 1)
		})
	}
}

func TestReplayManifestRejectsMissingOrOutOfRangeOffsets(t *testing.T) {
	batch := cdcIcebergBatch("app.orders", []model.Event{eventAt(1, 30), eventAt(2, 42)})
	intent := meta.IcebergCommitIntent{SourceTable: "app.orders", SourceStartFile: "mysql-bin.000001", SourceStartPos: 30, SourceEndFile: "mysql-bin.000001", SourceEndPos: 42, ReplayKeys: batch.replayKeys}
	for _, fault := range []string{"nil offset", "zero position", "before range", "after range", "other file", "future version"} {
		t.Run(fault, func(t *testing.T) {
			var manifest icebergReplayManifest
			if err := json.Unmarshal([]byte(batch.replayKeys), &manifest); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "nil offset":
				manifest.Events[0].Offset = nil
			case "zero position":
				manifest.Events[0].Offset.BinlogPos = 0
			case "before range":
				manifest.Events[0].Offset.BinlogPos = 29
			case "after range":
				manifest.Events[0].Offset.BinlogPos = 43
			case "other file":
				manifest.Events[0].Offset.BinlogFile = "other-bin.000001"
			case "future version":
				manifest.Version = 3
			}
			b, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			changed := intent
			changed.ReplayKeys = string(b)
			if _, err := decodeIcebergReplayEvents(changed); err == nil {
				t.Fatal("invalid replay manifest allowed checkpoint filtering")
			}
		})
	}
}

type rebuildSnapshotStore struct {
	*guardTestStore
	snapshotState  *meta.SnapshotState
	readErr        error
	ownershipErr   error
	loseDuringRead bool
}

func (s *rebuildSnapshotStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	if s.loseDuringRead {
		s.ownershipErr = meta.ErrWorkerOwnershipLost
	}
	return s.snapshotState, s.readErr
}

func (s *rebuildSnapshotStore) CheckOwnership(context.Context) error { return s.ownershipErr }

func TestAuthoritativeRebuildStartFailsClosedWithoutCurrentSnapshotState(t *testing.T) {
	for _, fault := range []string{"missing state", "confirmed state", "missing position", "read failure", "ownership lost during read"} {
		t.Run(fault, func(t *testing.T) {
			store := &rebuildSnapshotStore{guardTestStore: newGuardTestStore(), snapshotState: &meta.SnapshotState{StartFile: "mysql-bin.000001", StartPos: 4}}
			switch fault {
			case "missing state":
				store.snapshotState = nil
			case "confirmed state":
				store.snapshotState.Done = true
				store.snapshotState.CompletionVersion = 1
			case "missing position":
				store.snapshotState.StartPos = 0
			case "read failure":
				store.readErr = errors.New("metadata unavailable")
			case "ownership lost during read":
				store.loseDuringRead = true
			}
			s := recoverySink(t, recoveryCatalog(t), store)
			s.authoritativeRebuild = true
			if err := s.handleSnapshotBarrier(context.Background(), model.Event{Type: model.EventTypeSnapshotStart}); err == nil {
				t.Fatal("unverified rebuild start acknowledged")
			}
			if s.states["app.orders"].snapshotTruncateApplied {
				t.Fatal("unverified metadata enabled snapshot append")
			}
			if len(store.intents) != 0 || store.saveCount != 0 {
				t.Fatal("failed start changed recovery metadata")
			}
		})
	}
}

func TestAuthoritativeRebuildSnapshotRestartAppendsSuffixWithoutAnotherTruncate(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := &rebuildSnapshotStore{guardTestStore: newGuardTestStore(), snapshotState: &meta.SnapshotState{JobID: "state", StartFile: "mysql-bin.000001", StartPos: 4}}
	first := recoverySink(t, cat, store)
	first.cfg.SnapshotWriteMode = snapshotWriteModeTruncateAppend
	if err := first.ResetTargetTable(ctx, "bronze", "orders"); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, cat, 0, 1)
	if err := first.handleSnapshotBatch(ctx, model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: []map[string]interface{}{{"id": int64(1), "status": "pending"}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshotProgress(ctx, "state", "app.orders", 1, `{"id":1}`); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, cat, 1, 2)
	restarted := recoverySink(t, cat, store)
	restarted.cfg.SnapshotWriteMode = snapshotWriteModeTruncateAppend
	restarted.authoritativeRebuild = true
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleSnapshotBarrier(ctx, model.Event{Type: model.EventTypeSnapshotStart}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleSnapshotBatch(ctx, model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", SnapshotStartOffset: 1, Rows: []map[string]interface{}{{"id": int64(2), "status": "pending"}}}); err != nil {
		t.Fatalf("durably started authoritative rebuild could not resume suffix: %v", err)
	}
	// The reset and two appended batches each create one snapshot. A second
	// reset on restart would discard row 1 and add an unexpected snapshot.
	assertAutomaticData(t, cat, 2, 3)
}

func checkpointAt(pos uint32) *model.SourceOffset {
	return &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: pos}
}

func TestLiveCommitWithLaggingCheckpointDoesNotRequireSecondReplay(t *testing.T) {
	for _, loseResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("response_loss_%t", loseResponse), func(t *testing.T) {
			ctx := context.Background()
			cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: loseResponse}
			store := &automaticStore{guardTestStore: newGuardTestStore()}
			s := automaticSink(t, cat, store)
			if err := s.handleEvent(ctx, eventAt(1, 42)); err != nil {
				t.Fatal(err)
			}
			s.rememberOffset(checkpointAt(40))
			if err := s.flushAll(ctx); err != nil {
				t.Fatal(err)
			}
			if len(store.intents) != 1 {
				t.Fatal("lagging checkpoint discarded the accepted receipt")
			}
			if store.saveCount != 1 {
				t.Fatal("lagging checkpoint was not saved")
			}
			s.rememberOffset(checkpointAt(42))
			if err := s.commitPendingOffset(ctx, true); err != nil {
				t.Fatalf("live commit incorrectly requires source event twice: %v", err)
			}
			if len(store.intents) != 0 || store.saveCount != 2 {
				t.Fatal("covered receipt was not cleaned up")
			}
			assertAutomaticData(t, cat, 1, 1)
		})
	}
}

func TestRestartWithCheckpointInsideCommittedBatchReplaysOnlySuffix(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	for _, ev := range []model.Event{eventAt(1, 30), eventAt(2, 42)} {
		if err := first.handleEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	first.rememberOffset(checkpointAt(40))
	if err := first.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(store.intents) != 1 {
		t.Fatal("receipt must survive checkpoint inside batch")
	}
	// The source restarts after checkpoint 40, so event 30 cannot be replayed.
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, eventAt(2, 42)); err != nil {
		t.Fatal(err)
	}
	restarted.rememberOffset(checkpointAt(42))
	if err := restarted.commitPendingOffset(ctx, true); err != nil {
		t.Fatalf("already checkpointed prefix incorrectly required replay: %v", err)
	}
	if len(store.intents) != 0 || store.saveCount != 2 {
		t.Fatal("replayed suffix did not cover receipt")
	}
	assertAutomaticData(t, cat, 2, 1)
}

func TestRestartWithCheckpointInsideBatchStillRequiresEverySuffixEvent(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := newGuardTestStore()
	first := recoverySink(t, cat, store)
	for _, ev := range []model.Event{eventAt(1, 30), eventAt(2, 42), eventAt(3, 44)} {
		if err := first.handleEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	first.rememberOffset(checkpointAt(40))
	if err := first.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := recoverySink(t, cat, store)
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, eventAt(3, 44)); err != nil {
		t.Fatal(err)
	}
	restarted.rememberOffset(checkpointAt(44))
	err := restarted.commitPendingOffset(ctx, true)
	if err == nil || !strings.Contains(err.Error(), "before all source event identities replayed") {
		t.Fatalf("missing suffix event must block checkpoint: %v", err)
	}
	if store.saveCount != 1 || len(store.intents) != 1 {
		t.Fatal("blocked replay modified checkpoint or receipt")
	}
	assertAutomaticData(t, cat, 3, 1)
}
