package core

import (
	"context"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

type completionCheckpointStore struct {
	closingOffsetStore
	state *meta.SnapshotState
}

func (s *completionCheckpointStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return s.state, nil
}

func TestSnapshotCompletionCheckpointRequiresSinkConfirmation(t *testing.T) {
	for _, version := range []int{0, 1} {
		job := NewJob(&config.JobConfig{ID: "job"}, nil)
		job.metaKey = "state"
		job.metaStore = &completionCheckpointStore{state: &meta.SnapshotState{Done: true, CompletionVersion: version}}
		cp := job.Checkpoint()
		if cp == nil || cp.SnapshotState == nil {
			t.Fatal("missing checkpoint view")
		}
		if cp.SnapshotState.Done != (version == 1) {
			t.Fatalf("version=%d checkpoint done=%t", version, cp.SnapshotState.Done)
		}
	}
}
