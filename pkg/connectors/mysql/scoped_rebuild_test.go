package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/gerinsp/rivus/pkg/meta"
)

type scopedInitialStore struct {
	*testOffsetStore
	intents               []meta.IcebergCommitIntent
	readErr, ownershipErr error
}

func (s *scopedInitialStore) CheckOwnership(context.Context) error { return s.ownershipErr }
func (s *scopedInitialStore) ListIcebergCommitsForJob(context.Context, string, string) ([]meta.IcebergCommitIntent, error) {
	return s.intents, s.readErr
}

func TestScopedRebuildSourceStartPreservesResetReceiptsAndRejectsForeignState(t *testing.T) {
	for _, fault := range []string{"confirmed", "prepared", "missing-query-id", "old-state-key", "old-submission", "metadata-unavailable", "checkpoint-exists", "ownership-lost"} {
		t.Run(fault, func(t *testing.T) {
			store := &scopedInitialStore{testOffsetStore: &testOffsetStore{}, intents: []meta.IcebergCommitIntent{{OperationID: "reset", JobID: "job", StateKey: "state", Kind: "rebuild", BatchID: "rebuild:submission:app.orders:reset", Status: meta.IcebergCommitCommitted, TrinoQueryID: "query", TargetUUID: "table"}}}
			switch fault {
			case "prepared":
				store.intents[0].Status = meta.IcebergCommitPrepared
			case "missing-query-id":
				store.intents[0].TrinoQueryID = ""
			case "old-state-key":
				store.intents[0].StateKey = "old-state"
			case "old-submission":
				store.intents[0].BatchID = "rebuild:previous:app.orders:reset"
			case "metadata-unavailable":
				store.readErr = errors.New("metadata unavailable")
			case "checkpoint-exists":
				store.offset = &meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}
			case "ownership-lost":
				store.ownershipErr = meta.ErrWorkerOwnershipLost
			}
			source := &Source{jobID: "job", stateKey: "state", scopedRebuildToken: "submission", offsetSto: store}
			err := source.resetInitialCheckpoint(context.Background())
			if (err == nil) != (fault == "confirmed") || len(store.deletedJobIDs) != 0 || len(store.intents) != 1 {
				t.Fatalf("unsafe initial reset error=%v deleted=%v intents=%d", err, store.deletedJobIDs, len(store.intents))
			}
		})
	}
}
