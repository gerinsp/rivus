package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

type snapshotCleanupCheckStore struct {
	*closingOffsetStore
	checkErr error
	keys     []string
}

func (s *snapshotCleanupCheckStore) CheckJobStateCleanup(_ context.Context, key string) error {
	s.keys = append(s.keys, key)
	return s.checkErr
}

func TestFreshIcebergSnapshotChecksRecoveryBeforeConnectors(t *testing.T) {
	for _, mode := range []config.JobMode{config.JobModeInitial, config.JobModeSnapshotHandoff, config.JobModeSnapshotOnly} {
		for _, fault := range []string{"PREPARED", "COMMITTED", "metadata unavailable"} {
			t.Run(string(mode)+"/"+fault, func(t *testing.T) {
				injected := errors.New("retained intent or read failure: " + fault)
				store := &snapshotCleanupCheckStore{closingOffsetStore: &closingOffsetStore{}, checkErr: injected}
				job, constructed := freshSnapshotCheckJob(store)
				if err := job.startWithMode(mode); !errors.Is(err, injected) {
					t.Fatalf("fresh snapshot returned %v, want recovery block", err)
				}
				if *constructed != 0 || job.GetStatus() != JobStatusFailed {
					t.Fatalf("constructed=%d status=%s", *constructed, job.GetStatus())
				}
				_, deleted := store.counts()
				if deleted != 0 || len(store.keys) != 1 || store.keys[0] != job.Config.ID {
					t.Fatalf("check keys=%v deletes=%d", store.keys, deleted)
				}
				if last := job.GetLastError(); last == nil || !strings.Contains(last.Message, "docs/iceberg-checkpoint-recovery.md") {
					t.Fatalf("missing recovery guidance: %+v", last)
				}
			})
		}
	}
}

func freshSnapshotCheckJob(store *snapshotCleanupCheckStore) (*Job, *int) {
	constructed := new(int)
	reg := connector.NewRegistry()
	reg.RegisterSource("test_source", func(connector.JobContext, any) (connector.Source, error) {
		*constructed++
		return &closingSource{}, nil
	})
	reg.RegisterSink("iceberg_native", func(connector.JobContext, any) (connector.Sink, error) {
		*constructed++
		return &closingSink{}, nil
	})
	cfg := newTestJobConfig("fresh-iceberg")
	cfg.Sink.Type = "iceberg_native"
	job := NewJob(cfg, reg)
	job.metaStore = store
	job.metaKey = "internal-state"
	return job, constructed
}

func TestFreshIcebergSnapshotAllowsNoReceiptsAndResumeRetainsReceipts(t *testing.T) {
	for _, mode := range []config.JobMode{config.JobModeInitial, config.JobModeResume} {
		t.Run(string(mode), func(t *testing.T) {
			store := &snapshotCleanupCheckStore{closingOffsetStore: &closingOffsetStore{}}
			if mode == config.JobModeResume {
				store.checkErr = errors.New("receipt requires source replay")
			}
			job, constructed := freshSnapshotCheckJob(store)
			if err := job.startWithMode(mode); err != nil {
				t.Fatal(err)
			}
			if !job.waitRunDone(time.Second) || *constructed != 2 {
				t.Fatalf("pipeline did not run; constructed=%d", *constructed)
			}
			if mode == config.JobModeResume && len(store.keys) != 0 {
				t.Fatalf("resume was treated as cleanup: %v", store.keys)
			}
			if mode == config.JobModeInitial && (len(store.keys) != 2 || store.keys[0] != job.Config.ID || store.keys[1] != job.MetaKey()) {
				t.Fatalf("fresh snapshot check keys=%v", store.keys)
			}
		})
	}
}

type snapshotOwnershipCheckStore struct {
	*snapshotCleanupCheckStore
	ownershipChecks int
}

func (s *snapshotOwnershipCheckStore) CheckOwnership(context.Context) error {
	s.ownershipChecks++
	if s.ownershipChecks == 2 {
		return meta.ErrWorkerOwnershipLost
	}
	return nil
}

func TestFreshIcebergSnapshotFailsClosedWithoutCheckerOrOwnership(t *testing.T) {
	for _, fault := range []string{"unsupported store", "ownership lost during read"} {
		t.Run(fault, func(t *testing.T) {
			base := &snapshotCleanupCheckStore{closingOffsetStore: &closingOffsetStore{}}
			job, constructed := freshSnapshotCheckJob(base)
			var ownership *snapshotOwnershipCheckStore
			if fault == "unsupported store" {
				job.metaStore = base.closingOffsetStore
			} else {
				ownership = &snapshotOwnershipCheckStore{snapshotCleanupCheckStore: base}
				job.metaStore = ownership
			}
			err := job.startWithMode(config.JobModeInitial)
			if err == nil || *constructed != 0 {
				t.Fatalf("err=%v constructed=%d", err, *constructed)
			}
			if ownership != nil && (!errors.Is(err, meta.ErrWorkerOwnershipLost) || ownership.ownershipChecks != 2) {
				t.Fatalf("ownership checks=%d err=%v", ownership.ownershipChecks, err)
			}
			_, deleted := base.counts()
			if deleted != 0 {
				t.Fatal("failed admission mutated checkpoint")
			}
		})
	}
}

func TestArchivedRebuildPreflightResetsEmptyAndPopulatedTargets(t *testing.T) {
	cfg := newTestJobConfig("archived-full-rebuild")
	cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"tables": []string{"app.empty", "app.populated"}}}
	cfg.Sink = &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": "truncate-append"}}
	cfg.Metadata = map[string]string{"iceberg_rebuild_full_reset": "true"}
	job := NewJob(cfg, connector.NewRegistry())
	cfg.Metadata[config.IcebergRebuildFullResetKey] = job.SubmissionID()
	source := &countResumeSource{tables: []connector.TableRef{{Schema: "app", Table: "empty"}, {Schema: "app", Table: "populated"}}, sourceCounts: map[string]int64{"app.empty": 0, "app.populated": 10}}
	sink := &countResumeSink{targetCounts: map[string]int64{"target.empty": 3, "target.populated": 7}}
	if err := job.preflight(context.Background(), source, sink, config.JobModeSnapshotHandoff); err != nil {
		t.Fatal(err)
	}
	if len(sink.resetTargets) != 2 {
		t.Fatalf("rebuild failed to clear every target before snapshot: %v", sink.resetTargets)
	}
	resets := len(sink.resetTargets)
	if err := job.preflight(context.Background(), source, sink, config.JobModeSnapshotHandoffResume); err != nil {
		t.Fatal(err)
	}
	if len(sink.resetTargets) != resets {
		t.Fatal("snapshot resume erased committed snapshot prefix")
	}
}
