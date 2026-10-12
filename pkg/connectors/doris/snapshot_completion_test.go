package doris

import (
	"context"
	"errors"
	"testing"

	"github.com/gerinsp/rivus/pkg/model"
)

type completionOffsetStore struct {
	*recordingOffsetStore
	done   bool
	err    error
	loaded *bool
}

func (s *completionOffsetStore) MarkSnapshotDone(context.Context, string) error {
	if !*s.loaded {
		return errors.New("completion before data commit")
	}
	if s.err != nil {
		return s.err
	}
	s.done = true
	return nil
}

func TestSnapshotCompletionDorisFlushAndMetadataBeforeAck(t *testing.T) {
	for _, failure := range []string{"none", "data", "metadata"} {
		t.Run(failure, func(t *testing.T) {
			loaded := false
			injected := errors.New("injected " + failure)
			store := &completionOffsetStore{recordingOffsetStore: &recordingOffsetStore{}, loaded: &loaded}
			if failure == "metadata" {
				store.err = injected
			}
			sink := &Sink{stateKey: "state", offsetSto: store, sendBatchOverride: func(context.Context, string, string, []model.Event) error {
				if failure == "data" {
					return injected
				}
				loaded = true
				return nil
			}}
			state := newDorisRunState(sink)
			state.batches["app.orders"] = []model.Event{{Type: model.EventTypeInsert, Data: map[string]interface{}{"id": 1}}}
			ev := model.Event{Type: model.EventTypeSnapshotComplete, Ack: make(chan error, 1)}
			err := state.handleEvent(context.Background(), ev)
			ackErr := <-ev.Ack
			if failure == "none" {
				if err != nil || ackErr != nil || !loaded || !store.done {
					t.Fatalf("result=%v ACK=%v loaded=%t done=%t", err, ackErr, loaded, store.done)
				}
			} else if !errors.Is(err, injected) || !errors.Is(ackErr, injected) || store.done {
				t.Fatalf("failure result=%v ACK=%v done=%t", err, ackErr, store.done)
			}
			if len(store.saved) != 0 {
				t.Fatal("snapshot completion advanced CDC checkpoint")
			}
		})
	}
}
