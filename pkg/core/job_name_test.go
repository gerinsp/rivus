package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"sync"
	"testing"
	"time"
)

type presentationStore struct {
	*memoryJobStore
	renameErr  error
	reconciled int
}

func (s *presentationStore) RenameJob(_ context.Context, id, name string) (bool, error) {
	if s.renameErr != nil {
		return false, s.renameErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.jobs[id]
	if !ok {
		return false, nil
	}
	r.Name = name
	cfg := *r.Config
	cfg.Name = name
	r.Config = &cfg
	s.jobs[id] = r
	return true, nil
}
func TestRenameJobPreservesRunningSubmissionAndCheckpoint(t *testing.T) {
	s := &presentationStore{memoryJobStore: newMemoryJobStore()}
	cfg := newTestJobConfig("rename")
	cfg.Name = "old"
	s.jobs[cfg.ID] = meta.PersistedJob{ID: cfg.ID, Name: cfg.Name, Config: cfg, MetaKey: "stable-key", SubmissionID: "stable-submission", DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "RUNNING", LeaseOwner: "worker", LeaseUntil: time.Now().Add(time.Minute)}
	m := NewJobManager(connector.NewRegistry(), WithJobStore(s), WithControlPlaneRole())
	defer m.Shutdown(context.Background())
	if err := m.RestorePersistedJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	j, _ := m.Get(cfg.ID)
	originalCfg := j.Config
	if err := m.RequestRename(cfg.ID, "  Sync (btm) "); err != nil {
		t.Fatal(err)
	}
	if j.Name() != "Sync (btm)" || j.Config != originalCfg || j.Config.Name != "old" || j.DisplayConfig().Name != j.Name() || j.GetStatus() != JobStatusRunning || j.MetaKey() != "stable-key" || j.SubmissionID() != "stable-submission" || j.runActive() {
		t.Fatal("rename changed pipeline/config identity")
	}
	s.renameErr = errors.New("DB unavailable")
	if err := m.RequestRename(cfg.ID, "failed"); err == nil || j.Name() != "Sync (btm)" {
		t.Fatal("failed persistence changed display name")
	}
	s.renameErr = nil
	s.RenameJob(context.Background(), cfg.ID, "external rename")
	if err := m.RefreshPersistedViews(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.List()[0].Name != "external rename" {
		t.Fatal("external durable name not refreshed")
	}
	if err := m.RequestRename("missing", "name"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal(err)
	}
	if err := m.RequestRename(cfg.ID, ""); err == nil {
		t.Fatal("empty rename accepted")
	}
}
func TestConcurrentRenameAndJobViews(t *testing.T) {
	m := NewJobManager(connector.NewRegistry(), WithControlPlaneRole())
	defer m.Shutdown(context.Background())
	j, err := m.Submit(newTestJobConfig("concurrent-name"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				j.Name()
				j.DisplayConfig()
				m.List()
				j.CreatedTime()
				j.UpdatedTime()
			}
		}()
	}
	for n := 0; n < 100; n++ {
		if err := m.RequestRename(j.Config.ID, fmt.Sprintf("name-%d", n)); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestExecutionConfigComparisonIgnoresDisplayRename(t *testing.T) {
	a := newTestJobConfig("job")
	b := newTestJobConfig("job")
	b.Name = "renamed"
	if !sameJobExecutionConfig(a, b) {
		t.Fatal("display rename changed execution config")
	}
	b.Sink.Config["snapshot_write_mode"] = config.IcebergRebuildScopedNativeWriteMode
	if sameJobExecutionConfig(a, b) {
		t.Fatal("writer upgrade did not change execution config")
	}
}
