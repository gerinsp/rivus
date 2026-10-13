package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type closingOffsetStore struct {
	mu          sync.Mutex
	closeCount  int
	deleteCount int
	deleteErr   error
}

func (s *closingOffsetStore) GetOffset(context.Context, string) (*meta.Offset, error) {
	return nil, nil
}
func (s *closingOffsetStore) SaveOffset(context.Context, string, meta.Offset) error { return nil }
func (s *closingOffsetStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return nil, nil
}
func (s *closingOffsetStore) SaveSnapshotStart(context.Context, string, meta.Offset) error {
	return nil
}
func (s *closingOffsetStore) MarkSnapshotDone(context.Context, string) error { return nil }
func (s *closingOffsetStore) GetSnapshotProgress(context.Context, string) (*meta.SnapshotProgress, error) {
	return nil, nil
}
func (s *closingOffsetStore) SaveSnapshotProgress(context.Context, string, string, int64, string) error {
	return nil
}
func (s *closingOffsetStore) ClearSnapshotProgress(context.Context, string) error { return nil }
func (s *closingOffsetStore) DeleteJobState(context.Context, string) error {
	s.mu.Lock()
	s.deleteCount++
	s.mu.Unlock()
	return s.deleteErr
}
func (s *closingOffsetStore) Close() error {
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	return nil
}
func (s *closingOffsetStore) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount, s.deleteCount
}

type closingSource struct {
	mu         sync.Mutex
	closeCount int
}

func (s *closingSource) Run(context.Context, chan<- model.Event) error { return nil }
func (s *closingSource) Close() error {
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	return nil
}
func (s *closingSource) closes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

type closingSink struct {
	mu         sync.Mutex
	closeCount int
}

func (s *closingSink) Run(_ context.Context, in <-chan model.Event) error {
	for range in {
	}
	return nil
}
func (s *closingSink) Close() error {
	s.mu.Lock()
	s.closeCount++
	s.mu.Unlock()
	return nil
}
func (s *closingSink) closes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func TestJobClosesRuntimeResourcesAfterRun(t *testing.T) {
	store := &closingOffsetStore{}
	source := &closingSource{}
	sink := &closingSink{}
	reg := connector.NewRegistry()
	reg.RegisterSource("test_source", func(connector.JobContext, any) (connector.Source, error) {
		return source, nil
	})
	reg.RegisterSink("test_sink", func(connector.JobContext, any) (connector.Sink, error) {
		return sink, nil
	})

	job := NewJob(newTestJobConfig("close-runtime-resources"), reg)
	job.metaStore = store
	if err := job.startWithMode(config.JobModeLatest); err != nil {
		t.Fatal(err)
	}
	if !job.waitRunDone(2 * time.Second) {
		t.Fatal("job did not finish")
	}

	if got := source.closes(); got != 1 {
		t.Fatalf("source closes = %d, want 1", got)
	}
	if got := sink.closes(); got != 1 {
		t.Fatalf("sink closes = %d, want 1", got)
	}
	if closes, _ := store.counts(); closes != 1 {
		t.Fatalf("metadata store closes = %d, want 1", closes)
	}
}

func TestFailedStartClosesCreatedResources(t *testing.T) {
	store := &closingOffsetStore{}
	source := &closingSource{}
	reg := connector.NewRegistry()
	reg.RegisterSource("test_source", func(connector.JobContext, any) (connector.Source, error) {
		return source, nil
	})
	reg.RegisterSink("test_sink", func(connector.JobContext, any) (connector.Sink, error) {
		return nil, errors.New("sink setup failed")
	})

	job := NewJob(newTestJobConfig("close-failed-start"), reg)
	job.metaStore = store
	if err := job.startWithMode(config.JobModeLatest); err == nil {
		t.Fatal("start succeeded, want failure")
	}
	if got := source.closes(); got != 1 {
		t.Fatalf("source closes = %d, want 1", got)
	}
	if closes, _ := store.counts(); closes != 1 {
		t.Fatalf("metadata store closes = %d, want 1", closes)
	}
}

func TestCleanupMetaDeletesStateAndClosesStore(t *testing.T) {
	store := &closingOffsetStore{}
	job := NewJob(newTestJobConfig("cleanup-meta"), connector.NewRegistry())
	job.metaStore = store
	job.metaKey = "cleanup-meta-key"

	if err := job.CleanupMeta(); err != nil {
		t.Fatal(err)
	}
	closes, deletes := store.counts()
	if deletes != 1 {
		t.Fatalf("metadata deletes = %d, want 1", deletes)
	}
	if closes != 1 {
		t.Fatalf("metadata closes = %d, want 1", closes)
	}
}

func TestCleanupMetaPropagatesFailureAndClosesStore(t *testing.T) {
	injected := errors.New("cleanup BLOCKED: unresolved commit")
	store := &closingOffsetStore{deleteErr: injected}
	job := NewJob(newTestJobConfig("cleanup-failure"), connector.NewRegistry())
	job.metaStore = store
	job.metaKey = "key"
	if err := job.CleanupMeta(); !errors.Is(err, injected) {
		t.Fatalf("cleanup=%v", err)
	}
	if closes, deletes := store.counts(); closes != 1 || deletes != 1 {
		t.Fatalf("closes=%d deletes=%d", closes, deletes)
	}
}

func TestDeleteKeepsRecoverableJobAfterMetadataCleanupFailure(t *testing.T) {
	persisted := newMemoryJobStore()
	manager := NewJobManager(connector.NewRegistry(), WithJobStore(persisted))
	cfg := newTestJobConfig("blocked-delete")
	job := manager.newManagedJob(cfg)
	job.metaKey = "blocked-delete-key"
	job.metaStore = &closingOffsetStore{deleteErr: errors.New("cleanup BLOCKED: unresolved Iceberg commit intent")}
	manager.mu.Lock()
	manager.jobs[cfg.ID] = job
	manager.mu.Unlock()
	if err := manager.saveManagedJobRecord(context.Background(), job, meta.DesiredStateStopped, JobStatusStopped); err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(cfg.ID); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, "failed cleanup to keep durable recovery configuration", func() bool {
		restored, err := manager.Get(cfg.ID)
		if err != nil || restored.GetStatus() != JobStatusFailed {
			return false
		}
		record, ok := persisted.Get(cfg.ID)
		return ok && record.LastStatus == string(JobStatusFailed) && record.DesiredState == meta.DesiredStateStopped
	})
	if job.isPersistenceDeleted() {
		t.Fatal("failed cleanup disabled subsequent recovery persistence")
	}
	if len(job.GetErrors()) == 0 {
		t.Fatal("cleanup failure not visible on restored job")
	}
}

func TestDeleteReportsRetainedReceiptBeforeRemovingStoppedJob(t *testing.T) {
	persisted := newMemoryJobStore()
	manager := NewJobManager(connector.NewRegistry(), WithJobStore(persisted))
	cfg := newTestJobConfig("blocked-delete-admission")
	job := manager.newManagedJob(cfg)
	store := &snapshotCleanupCheckStore{closingOffsetStore: &closingOffsetStore{}, checkErr: fmt.Errorf("%w operation=accepted-but-uncheckpointed", meta.ErrJobStateCleanupBlocked)}
	job.metaStore = store
	job.metaKey = "blocked-delete-state"
	job.setStatus(JobStatusFailed)
	manager.mu.Lock()
	manager.jobs[cfg.ID] = job
	manager.mu.Unlock()
	if err := manager.saveManagedJobRecord(context.Background(), job, meta.DesiredStateStopped, JobStatusFailed); err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(cfg.ID); !errors.Is(err, meta.ErrJobStateCleanupBlocked) {
		t.Fatalf("delete did not report retained receipt immediately: %v", err)
	}
	if current, err := manager.Get(cfg.ID); err != nil || current != job || current.GetStatus() != JobStatusFailed {
		t.Fatal("rejected delete removed or changed job")
	}
	if record, ok := persisted.Get(cfg.ID); !ok || record.LastStatus != string(JobStatusFailed) || record.DesiredState != meta.DesiredStateStopped {
		t.Fatal("rejected delete changed durable job")
	}
	if closes, deletes := store.counts(); closes != 0 || deletes != 0 {
		t.Fatal("delete admission mutated or closed recovery state")
	}
}
